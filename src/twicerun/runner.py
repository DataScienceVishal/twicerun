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
import os
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

# Dropped in a run directory while it is being written and removed at the end.
# It holds the pid, so a run killed part way through leaves one behind that the
# next invocation can tell is stale rather than blocking cleanup forever.
RUNNING = ".running"


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


def mark_running(run_dir: Path) -> Path:
    marker = run_dir / RUNNING
    marker.write_text(str(os.getpid()), encoding="utf-8")
    return marker


def is_running(run_dir: Path) -> bool:
    """Whether another invocation is still writing this directory.

    Signal 0 does no signalling: it only asks whether the process exists and
    whether we could signal it. A pid left by a crashed run fails that and the
    directory becomes prunable again, so a crash cannot pin a run directory in
    place forever.
    """
    marker = run_dir / RUNNING
    try:
        pid = int(marker.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return False
    if pid == os.getpid():
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Alive and owned by someone else, which still means do not delete it.
        return True
    return True


def prune_run_dirs(parent: Path, keep: int, current: Path | None = None) -> list[Path]:
    """Drop all but the most recent `keep` run directories, `current` always among them.

    A five-run pass over the reference pipeline writes about 200 MB, most of it
    the generated inputs held once per run because the comparison needs a copy
    per run. Without pruning, following the README a dozen times leaves a couple
    of gigabytes behind and nothing ever reclaims it.

    The default keeps one directory, which is the current run. Nothing in the
    tool reads a previous run yet, so keeping more would be storing 200 MB
    against a feature that does not exist. Slice 7 regenerates the results table
    from a committed run artifact and may want more, and --keep is there for it.

    Two things here were wrong until a flaky test in slice 2 caught them, and
    both come from `new_run_dir` reusing a name this function has freed. Names
    carry a second-resolution timestamp, so once `run-100715` is deleted the
    next invocation inside that second takes the name back. After that, name
    order and creation order disagree: the newest directory on disk sorts first
    and gets deleted next. That deleted the run that was starting, which then
    recreated its own directory as it wrote, leaving `keep + 1` behind.

    So recency comes from mtime rather than from the name, and `current` is
    excluded from the candidates outright. It still counts toward `keep`, so
    --keep 1 means one directory in total.

    `current` only covers this process. Two invocations sharing a --run-dir
    would have the second delete the first's artifacts mid-run, and the first
    would exit 3 blaming a missing Parquet file rather than the other process.
    A live run leaves a .running marker holding its pid, and a directory with a
    live marker is not a candidate. Skipping anything recently written would
    have been simpler and would have broken retention outright, since
    back-to-back runs are the normal case and every one of them is recent.
    """
    if keep < 1:
        return []
    everything = [p for p in parent.glob("run-*") if p.is_dir() and RUN_DIR_NAME.match(p.name)]
    candidates = sorted(
        (p for p in everything if p != current and not is_running(p)),
        key=lambda p: (p.stat().st_mtime, p.name),
    )
    dropped = candidates[: max(0, len(everything) - keep)]
    for path in dropped:
        shutil.rmtree(path)
    return dropped


def artifacts_before(record: RunRecord, step_index: int) -> dict[str, Artifact]:
    """Run 1's artifact index as it stood when step `step_index` began.

    Trimmed to the earlier steps rather than taken whole. The full index would
    let a step read run 1's *later* output under a name the step writes itself,
    which is not a read any run can make on its own, and the reference pipeline
    has two steps that write a name they also read.
    """
    return {a.name: a for step in record.steps[:step_index] for a in step.artifacts}


def execute_run(
    steps: list[Step],
    run: int,
    run_dir: Path,
    carried: dict[str, Artifact],
    reference: RunRecord | None = None,
) -> tuple[RunRecord, dict[str, Artifact]]:
    """One execution of the pipeline.

    `reference` is run 1's record and turns containment on: every read resolves
    against what run 1 had written by that point. Run 1 itself passes None, and
    so does every run under --no-containment.
    """
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
                upstream=None if reference is None else artifacts_before(reference, index),
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
                    uncontained_reads=sorted(ctx.uncontained_reads),
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


class UnknownArtifact(LookupError):
    """--key named an artifact this pipeline never writes."""


def _check_keys_named_something(keys: Mapping[str, Sequence[str]], reference: RunRecord) -> None:
    """A --key entry matching no artifact used to be dropped without a word.

    keys.get(name) returned None and the artifact was compared with the default
    key, so a typo produced a full, confident, ordinary report in which the flag
    had done nothing. The column case already exits 2 with a helpful message;
    this matches it, and it can only run after run 1 because that is when every
    artifact name is known.
    """
    written = {a.name for step in reference.steps for a in step.artifacts}
    unknown = sorted(set(keys) - written)
    if unknown:
        raise UnknownArtifact(
            f"--key named {', '.join(unknown)}, which this pipeline does not write. "
            f"It writes {', '.join(sorted(written)) or 'nothing'}"
        )


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


def measure(
    manifest: Manifest, keys: Mapping[str, Sequence[str]]
) -> list[StepMeasurement]:
    """Compare every later run against run 1, whether it just ran or was loaded.

    Split out so `judge` can re-derive a saved run's measurements from the same
    code path the run command used. Comparing artifacts on disk is a pure
    function of those artifacts, which is why a saved run can be re-scored at
    all.
    """
    reference = manifest.runs[0]
    _check_keys_named_something(keys, reference)
    measured = [
        StepMeasurement(
            index=s.index, name=s.name, comparisons=len(manifest.runs) - 1, terms=s.rows_read
        )
        for s in reference.steps
    ]
    for later in manifest.runs[1:]:
        found_here = compare_runs(reference, later, keys)
        for step, ran, found in zip(measured, later.steps, found_here, strict=True):
            step.observe(found)
            step.uncontained_reads.update(ran.uncontained_reads)
    return measured


def rejudge(
    manifest_path: Path, policy: Policy, keys: Mapping[str, Sequence[str]] | None = None
) -> Report:
    manifest = Manifest.load(manifest_path)
    if len(manifest.runs) < 2:
        raise PipelineError(f"{manifest_path} holds one run, so there is nothing to compare")
    missing = [
        a.path
        for run in manifest.runs
        for step in run.steps
        for a in step.artifacts
        if not Path(a.path).exists()
    ]
    if missing:
        raise PipelineError(
            f"{len(missing)} artifact(s) named in {manifest_path} are gone, starting with "
            f"{missing[0]}. Retention drops old run directories, so judge the current one or "
            f"rerun with --keep"
        )
    return Report(
        pipeline=manifest.pipeline,
        run_dir=str(manifest.root),
        runs=len(manifest.runs),
        environment=manifest.environment,
        steps=measure(manifest, keys or {}),
        seconds=sum(run.seconds for run in manifest.runs),
        policy=policy,
        contained=manifest.contained,
        keep=0,
    )


def run_pipeline(
    pipeline: Path,
    runs: int,
    parent: Path,
    keep: int = 1,
    keys: Mapping[str, Sequence[str]] | None = None,
    policy: Policy | None = None,
    contained: bool = True,
) -> tuple[Report, Manifest]:
    if runs < 2:
        raise PipelineError(f"--runs must be at least 2 to have anything to compare, got {runs}")

    steps = load_steps(pipeline)
    run_dir = new_run_dir(parent)
    marker = mark_running(run_dir)
    dropped = prune_run_dirs(parent, keep, current=run_dir)
    try:
        probe = duckdb.connect()
        environment = Environment.observe(probe)
        probe.close()

        manifest = Manifest(
            pipeline=str(pipeline), root=run_dir, environment=environment, contained=contained
        )
        began = time.perf_counter()

        carried: dict[str, Artifact] = {}
        reference: RunRecord | None = None
        for run in range(1, runs + 1):
            record, written = execute_run(
                steps,
                run,
                run_dir / f"run-{run:02d}",
                carried,
                reference=reference if contained else None,
            )
            manifest.runs.append(record)
            if run == 1:
                reference, reference_written = record, written
            # Contained, every run from 2 on carries run 1's state, so each is
            # the second run of the same pipeline rather than the next link in a
            # chain that has already drifted three times.
            carried = reference_written if contained else written

        measured = measure(manifest, keys or {})
        manifest.save()
        report = Report(
            pipeline=str(pipeline),
            run_dir=str(run_dir),
            runs=runs,
            environment=environment,
            steps=measured,
            seconds=time.perf_counter() - began,
            policy=policy or Policy(),
            contained=contained,
            pruned=len(dropped),
            keep=keep,
        )
    finally:
        marker.unlink(missing_ok=True)
    return report, manifest
