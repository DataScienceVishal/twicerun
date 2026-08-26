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
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from twicerun.compare import ArtifactDiff, compare
from twicerun.manifest import Artifact, Environment, Manifest, RunRecord, StepRecord
from twicerun.report import Report, StepVerdict
from twicerun.storage import StepContext

Step = Callable[[StepContext], None]


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
                    artifacts=[a for name, a in written.items() if before.get(name) is not a],
                )
            )
    finally:
        con.close()
    record.seconds = time.perf_counter() - began
    return record, written


def step_diffs(
    con: duckdb.DuckDBPyConnection, reference: StepRecord, candidate: StepRecord
) -> list[ArtifactDiff]:
    """Diffs for one step, over the union of the artifact names both runs wrote.

    The union matters. Walking only the reference run's names misses a step that
    starts producing an extra output on a later run, which is a divergence and
    would otherwise exit 0.
    """
    left = {a.name: a for a in reference.artifacts}
    right = {a.name: a for a in candidate.artifacts}
    names = list(left) + [name for name in right if name not in left]

    diffs = []
    for name in names:
        before, after = left.get(name), right.get(name)
        if after is None:
            diffs.append(
                ArtifactDiff(
                    name=name,
                    reference_rows=before.rows,
                    candidate_rows=0,
                    only_in_reference=before.rows,
                    only_in_candidate=0,
                    note="written by the reference run, absent from this one",
                )
            )
        elif before is None:
            diffs.append(
                ArtifactDiff(
                    name=name,
                    reference_rows=0,
                    candidate_rows=after.rows,
                    only_in_reference=0,
                    only_in_candidate=after.rows,
                    note="written by this run, absent from the reference run",
                )
            )
        else:
            diffs.append(compare(con, before, after))
    return diffs


def compare_runs(reference: RunRecord, candidate: RunRecord) -> list[list[ArtifactDiff]]:
    """Per step, the diffs between one later run and the reference run."""
    con = duckdb.connect()
    try:
        return [
            step_diffs(con, ref_step, cand_step)
            for ref_step, cand_step in zip(reference.steps, candidate.steps, strict=True)
        ]
    finally:
        con.close()


def run_pipeline(pipeline: Path, runs: int, parent: Path) -> tuple[Report, Manifest]:
    if runs < 2:
        raise PipelineError(f"--runs must be at least 2 to have anything to compare, got {runs}")

    steps = load_steps(pipeline)
    run_dir = new_run_dir(parent)
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

    verdicts = [
        StepVerdict(index=s.index, name=s.name, comparisons=runs - 1)
        for s in manifest.runs[0].steps
    ]
    for later in manifest.runs[1:]:
        for verdict, diffs in zip(verdicts, compare_runs(manifest.runs[0], later), strict=True):
            verdict.observe(diffs)

    manifest.save()
    report = Report(
        pipeline=str(pipeline),
        run_dir=str(run_dir),
        runs=runs,
        environment=environment,
        steps=verdicts,
        seconds=time.perf_counter() - began,
    )
    return report, manifest
