"""Execute a pipeline N times under one run directory and compare the results.

Run 1 is the reference and runs 2 to N are each compared against it, so a step
gets `N - 1` comparisons and a fire rate of `k of m`. All-pairs clustering was
the alternative and is rejected for a reason that only bites in slice 2:
tolerant equality is not transitive, so "how many distinct answers" stops being
well defined the moment any tolerance exists. A fixed reference gives a
statistic that stays defined, and it is the one matching the question a user
actually has, which is whether a fresh run agrees with the answer they already
have.

Every run gets its own DuckDB connection rather than its own process. That was
measured rather than assumed: the float divergence fires at full strength across
fresh connections reading the same Parquet file, 493 to 696 groups of 1,000.
"""

from __future__ import annotations

import importlib.util
import re
import shutil
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from twicerun.manifest import Artifact, Environment, Manifest, RunRecord, StepRecord
from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings, compare
from twicerun.policy import Policy
from twicerun.report import Report
from twicerun.storage import StepContext

Step = Callable[[StepContext], None]

# Only directories matching this get pruned, so pointing --run-dir at a
# directory holding anything else cannot delete it.
RUN_DIR_NAME = re.compile(r"^run-\d{8}-\d{6}(-\d+)?$")


class PipelineError(RuntimeError):
    pass


def load_steps(path: Path) -> list[Step]:
    """Import a pipeline file and take its STEPS list.

    No decorator and no registry. A pipeline is a module with an explicit
    ordered list at the bottom, which means the execution order is greppable
    and there is no import-order magic to explain.
    """
    spec = importlib.util.spec_from_file_location(f"twicerun_pipeline_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise PipelineError(f"{path} is not importable as a Python module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    steps = getattr(module, "STEPS", None)
    if not steps:
        raise PipelineError(f"{path} defines no STEPS list, so there is nothing to run")
    return list(steps)


def new_run_dir(parent: Path) -> Path:
    """A fresh directory, never an existing one.

    Second resolution keeps the name readable, and two invocations inside one
    second used to collide and raise FileExistsError out of the CLI. Falling
    back to a counter rather than to microseconds keeps the common case legible.
    """
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    for suffix in ["", *(f"-{n}" for n in range(1, 1000))]:
        run_dir = parent / f"run-{stamp}{suffix}"
        try:
            run_dir.mkdir(parents=True, exist_ok=False)
            return run_dir
        except FileExistsError:
            continue
    raise PipelineError(f"1000 run directories already exist for {stamp} under {parent}")


def prune_run_dirs(parent: Path, keep: int) -> list[Path]:
    """Drop all but the most recent `keep` run directories.

    A five-run pass over the reference pipeline writes about 200 MB, most of it
    the generated inputs held once per run because the comparison needs a copy
    per run. Without pruning, following the README a dozen times leaves a couple
    of gigabytes behind and nothing ever reclaims it.

    The default keeps one directory, which is the current run. Nothing in the
    tool reads a previous run yet, so keeping more would be storing 200 MB
    against a feature that does not exist. Slice 7 regenerates the results table
    from a committed run artifact and may want more, and --keep is there for it.
    """
    if keep < 1:
        return []
    existing = sorted(
        (p for p in parent.glob("run-*") if p.is_dir() and RUN_DIR_NAME.match(p.name)),
        key=lambda p: p.name,
    )
    dropped = existing[: max(0, len(existing) - keep)]
    for path in dropped:
        shutil.rmtree(path)
    return dropped


def execute_run(
    steps: list[Step], run: int, run_dir: Path, carried: dict[str, Artifact]
) -> tuple[RunRecord, dict[str, Artifact]]:
    con = duckdb.connect()
    written: dict[str, Artifact] = {}
    record = RunRecord(run=run)
    began = time.perf_counter()
    try:
        for index, step in enumerate(steps):
            ctx = StepContext(
                con,
                run=run,
                step_index=index,
                step_name=step.__name__,
                run_dir=run_dir,
                written=written,
                carried=carried,
            )
            before = dict(written)
            step_began = time.perf_counter()
            step(ctx)
            record.steps.append(
                StepRecord(
                    index=index,
                    name=step.__name__,
                    seconds=time.perf_counter() - step_began,
                    rows_read=ctx.rows_read,
                    input_columns=sorted(ctx.input_columns),
                    artifacts=[a for name, a in written.items() if before.get(name) is not a],
                )
            )
    finally:
        con.close()
    record.seconds = time.perf_counter() - began
    return record, written


def step_findings(
    con: duckdb.DuckDBPyConnection,
    reference: StepRecord,
    candidate: StepRecord,
    keys: Mapping[str, Sequence[str]],
) -> list[ArtifactFindings]:
    """Findings for one step, over the union of the artifact names both runs wrote.

    The union matters. Walking only the reference run's names misses a step that
    starts producing an extra output on a later run, which is a divergence and
    would otherwise exit 0.

    An artifact one run wrote and the other did not is every row of it failing
    to pair, so it lands in ROW_MISSING or ROW_EXTRA with a hint saying which
    run skipped it. Inventing a sixth class for it would say no more.
    """
    left = {a.name: a for a in reference.artifacts}
    right = {a.name: a for a in candidate.artifacts}
    names = list(left) + [name for name in right if name not in left]

    found = []
    for name in names:
        before, after = left.get(name), right.get(name)
        if after is None:
            found.append(
                ArtifactFindings(
                    name=name,
                    key=(),
                    reference_rows=before.rows,
                    candidate_rows=0,
                    row_missing=before.rows,
                    hints=(f"the reference run wrote {name} and this run did not",),
                )
            )
        elif before is None:
            found.append(
                ArtifactFindings(
                    name=name,
                    key=(),
                    reference_rows=0,
                    candidate_rows=after.rows,
                    row_extra=after.rows,
                    hints=(f"this run wrote {name} and the reference run did not",),
                )
            )
        else:
            found.append(
                compare(con, before, after, keys.get(name), reference.input_columns)
            )
    return found


def compare_runs(
    reference: RunRecord, candidate: RunRecord, keys: Mapping[str, Sequence[str]]
) -> list[list[ArtifactFindings]]:
    """Per step, what the oracle found between one later run and the reference run."""
    con = duckdb.connect()
    try:
        return [
            step_findings(con, ref_step, cand_step, keys)
            for ref_step, cand_step in zip(reference.steps, candidate.steps, strict=True)
        ]
    finally:
        con.close()


def run_pipeline(
    pipeline: Path,
    runs: int,
    parent: Path,
    keep: int = 1,
    keys: Mapping[str, Sequence[str]] | None = None,
    policy: Policy | None = None,
) -> tuple[Report, Manifest]:
    if runs < 2:
        raise PipelineError(f"--runs must be at least 2 to have anything to compare, got {runs}")

    steps = load_steps(pipeline)
    run_dir = new_run_dir(parent)
    dropped = prune_run_dirs(parent, keep)
    probe = duckdb.connect()
    environment = Environment.observe(probe)
    probe.close()

    manifest = Manifest(pipeline=str(pipeline), root=run_dir, environment=environment)
    began = time.perf_counter()

    carried: dict[str, Artifact] = {}
    for run in range(1, runs + 1):
        record, written = execute_run(steps, run, run_dir / f"run-{run:02d}", carried)
        manifest.runs.append(record)
        carried = written

    keys = keys or {}
    measured = [
        StepMeasurement(index=s.index, name=s.name, comparisons=runs - 1, terms=s.rows_read)
        for s in manifest.runs[0].steps
    ]
    for later in manifest.runs[1:]:
        rounds = compare_runs(manifest.runs[0], later, keys)
        for step, found in zip(measured, rounds, strict=True):
            step.observe(found)

    manifest.save()
    report = Report(
        pipeline=str(pipeline),
        run_dir=str(run_dir),
        runs=runs,
        environment=environment,
        steps=measured,
        seconds=time.perf_counter() - began,
        policy=policy or Policy(),
        pruned=len(dropped),
        keep=keep,
    )
    return report, manifest
