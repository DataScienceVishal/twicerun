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
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import NamedTuple

import duckdb

from twicerun.amplify import (
    AMPLIFIERS,
    PROBE_RUNS,
    Amplification,
    Attempt,
    NotApplicable,
    Substituted,
)
from twicerun.cause import BISECT_THREADS, Bisect
from twicerun.manifest import (
    AmplifiedRuns,
    Artifact,
    Environment,
    Manifest,
    RunRecord,
    StepRecord,
)
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


class Retention(NamedTuple):
    dropped: list[Path]
    live: int


class Repeated(NamedTuple):
    """What `repeat_steps` produced, and the state its run 1 left for later runs.

    The carried state comes back because amplification extends a sequence it
    already started: escalating from 3 runs to 5 executes runs 4 and 5 against
    the run 1 already on disk, and run 1 is where the state overlay was built.
    """

    records: list[RunRecord]
    carried: dict[str, Artifact]


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


def prune_run_dirs(parent: Path, keep: int, current: Path | None = None) -> Retention:
    """Drop all but the most recent `keep` run directories, `current` always among them.

    A five-run pass over the reference pipeline writes about 260 MB: 205 MB of
    generated inputs, held once per run because the comparison needs a copy per
    run, and 53 MB of single-threaded bisect artifacts. Without pruning,
    following the README a dozen times leaves three gigabytes behind and
    nothing ever reclaims it.

    The default keeps one directory, which is the current run. Nothing in the
    tool reads a previous run yet, so keeping more would be storing 260 MB
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

    The live count comes back with the dropped list because skipping those
    directories suspends the retention the header states as a fact. Three
    parallel invocations leave three directories and 778 MB, each report
    claiming to keep one, and a reader with eight CI jobs deserves to be told
    by the tool rather than by their disk.
    """
    if keep < 1:
        return Retention([], 0)
    everything = [p for p in parent.glob("run-*") if p.is_dir() and RUN_DIR_NAME.match(p.name)]
    others = [p for p in everything if p != current]
    candidates = sorted(
        (p for p in others if not is_running(p)), key=lambda p: (p.stat().st_mtime, p.name)
    )
    dropped = candidates[: max(0, len(everything) - keep)]
    for path in dropped:
        shutil.rmtree(path)
    return Retention(dropped, len(others) - len(candidates))


def artifacts_before(record: RunRecord, step_index: int) -> dict[str, Artifact]:
    """Run 1's artifact index as it stood when step `step_index` began.

    Trimmed to the earlier steps rather than taken whole. The full index would
    let a step read run 1's *later* output under a name the step writes itself,
    which is not a read any run can make on its own, and the reference pipeline
    has two steps that write a name they also read.

    Filtered on each step's own index rather than sliced by list position. The
    two agree for a record of a whole pipeline and stop agreeing for one built
    with `only`, where position 2 can be step 5, and the bisect builds exactly
    those.
    """
    return {
        a.name: a
        for step in record.steps
        if step.index < step_index
        for a in step.artifacts
    }


def execute_run(
    steps: list[Step],
    run: int,
    run_dir: Path,
    carried: dict[str, Artifact],
    upstream: Callable[[int], dict[str, Artifact]] | None = None,
    only: Collection[int] | None = None,
    threads: int | None = None,
) -> tuple[RunRecord, dict[str, Artifact]]:
    """One execution of the pipeline, or of the steps in `only`.

    `upstream` turns containment on: called with a step index, it answers with
    what that step's reads should resolve against. Run 1 of the main loop passes
    None, and so does every run under --no-containment.

    It is a callable rather than run 1's record because the two loops downstream
    of the main one want different answers out of it. The bisect wants run 1's
    artifacts as they stood before each step, which is what `artifacts_before`
    computes. Amplification wants a substituted set that has nothing to do with
    what any run wrote.

    `only` and `threads` are the bisect's. Executing one step out of the middle
    of a pipeline works for reads that go through `ctx.read` and `ctx.state`,
    because those resolve against run 1's artifacts and a skipped step's output
    is already on disk. It does not work for anything a skipped step left in
    the connection through `ctx.sql`: each run gets its own in-memory database,
    so a table another step created with CREATE TABLE is not there and a step
    depending on one will raise. `bisect_runs` is called in a way that survives
    that, because the bisect is downstream of an answer that is already
    complete.

    Setting threads here changes this execution and nothing else, for the same
    reason: the database is this run's.
    """
    con = duckdb.connect()
    if threads is not None:
        con.execute(f"SET threads={threads}")
    written: dict[str, Artifact] = {}
    record = RunRecord(run=run)
    began = time.perf_counter()
    try:
        for index, step in enumerate(steps):
            if only is not None and index not in only:
                continue
            ctx = StepContext(
                con,
                run=run,
                step_index=index,
                step_name=step.__name__,
                run_dir=run_dir,
                written=written,
                carried=carried,
                upstream=None if upstream is None else upstream(index),
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
                    reads=sorted(ctx.reads),
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


def bisect_runs(
    steps: list[Step], reference: RunRecord, divergent: Sequence[int], run_dir: Path, runs: int
) -> list[RunRecord]:
    """Re-execute the divergent steps `runs` times at threads=1, under containment.

    The same `runs` as the main loop, so the two fire rates share a denominator.
    A three-run bisect under a five-run loop would print two fractions that
    cannot be compared with each other, which is most of the value gone.

    Cost is `len(divergent) * runs` step executions and no more: the steps that
    never fired are not re-executed, because there is nothing about them for a
    thread count to explain. The steps that never fired go to amplification
    instead, which is the other half of this arithmetic.
    """
    return repeat_steps(
        steps,
        divergent,
        run_dir / f"threads-{BISECT_THREADS}",
        runs,
        upstream=partial(artifacts_before, reference),
        seed=artifacts_before(reference, len(steps)),
        threads=BISECT_THREADS,
    ).records


def repeat_steps(
    steps: list[Step],
    only: Sequence[int],
    into: Path,
    runs: int,
    upstream: Callable[[int], dict[str, Artifact]],
    seed: dict[str, Artifact],
    threads: int | None = None,
    start: int = 1,
    carried: dict[str, Artifact] | None = None,
) -> Repeated:
    """Execute runs `start` to `runs` of a subset of the pipeline, run 1 the reference.

    The main loop again, narrowed. Both the bisect and amplification want it,
    they want it with different reads resolved and different thread counts, and
    the state handling below is the part neither can get wrong twice.

    That state starts empty, exactly as it did for run 1 of the main loop, and
    every later run reads the first one's. Seeding it from the main loop's run 1
    instead was wrong in a way that took a reference-pipeline run to see:
    `append_audit_log` then read the same log in all five re-executions, agreed
    with itself five times, and got a label about parallelism on a bug that
    duplicates a log on rerun whatever the thread count.

    Which is why runs 2 to N carry `seed` underneath what run 1 produced rather
    than only what it produced. A re-executed step can carry state under a name
    that some skipped step wrote, and skipping the writer left the name missing
    so every later run fell back to `seed` and the same mislabelling came back
    through the other door. Names this loop re-produced take its own version;
    every other name takes the seed's.

    `start` above 1 continues a sequence rather than beginning one, which is
    what amplification does when an amplifier fires at 3 runs and has to reach
    the main loop's 5. Looping from 1 regardless re-executed runs 1 to 3 a
    second time and overwrote their Parquet, so the manifest kept the first
    call's records beside the second call's files and `judge` scored a rate
    against artifacts nothing described. It also cost 19 executions on the
    reference pipeline where the arithmetic above predicts 14.

    `carried` has to arrive with it, because the state overlay is built after
    run 1 and a continuation does not run one.
    """
    carried = dict(carried or {})
    records = []
    for run in range(start, runs + 1):
        record, written = execute_run(
            steps,
            run,
            into / f"run-{run:02d}",
            carried,
            upstream=upstream,
            only=set(only),
            threads=threads,
        )
        records.append(record)
        if run == 1:
            carried = {**seed, **written}
    return Repeated(records, carried)


def amplify_runs(
    steps: list[Step],
    reference: RunRecord,
    quiet: Sequence[int],
    run_dir: Path,
    runs: int,
    threads: int,
    keys: Mapping[str, Sequence[str]],
) -> list[AmplifiedRuns]:
    """Re-execute each step that never fired against an input built to make it fire.

    Only the quiet steps, because a step that already diverged has been answered
    and pushing its firing probability higher would cost executions to learn
    nothing. That is also what keeps the status of a step independent of the
    policy: which steps get amplified comes off the measured fire rate, so the
    same pipeline judged twice re-executes the same steps.

    Each amplifier gets three runs, which is two comparisons, and any that fires
    is extended to the main loop's N so the two rates share a denominator. The
    extension continues against the same amplified run 1 rather than starting
    over, so escalating costs `N - 3` executions rather than `N`.

    Below four runs the probe is clamped to `runs`, or the report prints an
    amplified `2 of 2` beside a main-loop `0 of 1` and invites a comparison the
    denominators do not support. The bisect refuses that case by construction
    and this had no way down.
    """
    amplified = []
    con = duckdb.connect()
    try:
        for index in quiet:
            reads = set(_step_record(reference, index).reads)
            inputs = {
                name: artifact
                for name, artifact in artifacts_before(reference, index).items()
                if name in reads
            }
            for amplifier, substitute in AMPLIFIERS:
                into = run_dir / "amplified" / f"step-{index:02d}" / _slug(amplifier)
                amplified.append(
                    _one_amplifier(
                        steps, reference, index, runs, keys,
                        amplifier=amplifier,
                        build=partial(substitute, con, into / "input", inputs, threads),
                        into=into,
                    )
                )
    finally:
        con.close()
    return amplified


def _one_amplifier(
    steps: list[Step],
    reference: RunRecord,
    index: int,
    runs: int,
    keys: Mapping[str, Sequence[str]],
    *,
    amplifier: str,
    build: Callable[[], Attempt],
    into: Path,
) -> AmplifiedRuns:
    """Execute one (step, amplifier) pair, or record why it did not run.

    The blanket catch is what AMPLIFICATION_FAILED is made of. An amplified
    input can make a step raise rather than diverge, most obviously by
    collapsing a column a downstream uniqueness constraint depends on, and the
    step is arbitrary user code so there is no narrower exception to name.
    Nothing is swallowed: the error is stored, printed against the amplifier
    that produced it, and it stops the step reaching a clean status. Falling
    back to green is the failure this whole feature exists to prevent.

    Building the substituted input is inside the catch, not evaluated as an
    argument outside it. A COPY that runs out of disk while materialising four
    million rows would otherwise take five finished runs, the bisect and the
    manifest with it and exit 3. The bisect already had that guard, added after
    exactly this bite; this loop did not inherit it.

    Whatever the probe measured survives the failure path. An amplifier that
    fired twice and then raised while escalating has still watched the step give
    two different answers, and discarding those records turned that into a
    status that does not gate a release.
    """
    attempt = None
    records: list[RunRecord] = []
    error = None
    probe = min(PROBE_RUNS, runs)
    try:
        attempt = build()
        if isinstance(attempt, NotApplicable):
            return AmplifiedRuns(index, amplifier, note=attempt.reason)
        repeat = partial(
            repeat_steps,
            steps,
            [index],
            into,
            upstream=lambda _: attempt.artifacts,
            seed=artifacts_before(reference, len(steps)),
            threads=attempt.threads,
        )
        started = repeat(runs=probe)
        records = started.records
        if runs > probe and _scored(records, keys)[0]:
            records = records + repeat(
                runs=runs, start=probe + 1, carried=started.carried
            ).records
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}".strip()
    # A failure while building the input leaves no note of its own, and the
    # error underneath it is the whole story anyway.
    note = attempt.note if isinstance(attempt, Substituted) else "while building the input"
    return AmplifiedRuns(index, amplifier, note=note, runs=records, error=error)


def _scored(
    records: Sequence[RunRecord], keys: Mapping[str, Sequence[str]]
) -> tuple[int, int]:
    """How many of runs 2 to N disagreed with run 1, and how many artifacts said so.

    The second figure is the guard the main loop and the bisect already carry: a
    re-executed step that wrote nothing produces `0 of 2` out of two comparisons
    of nothing, and that has to be distinguishable from two clean ones.
    """
    if len(records) < 2:
        return 0, 0
    reference, *later = records
    fired = compared = 0
    for run in later:
        found = [f for step in compare_runs(reference, run, keys) for f in step]
        compared += len(found)
        fired += any(f.diverged for f in found)
    return fired, compared


def attach_amplification(
    manifest: Manifest, measured: list[StepMeasurement], keys: Mapping[str, Sequence[str]]
) -> None:
    """Score the amplified runs against each other and hang each rate on its step.

    Run from `measure`'s two callers rather than from inside it, for the same
    reason the bisect is: the run command needs the main loop's fire rates
    before it knows which steps to amplify at all.
    """
    on = {step.index: step for step in measured}
    for entry in manifest.amplified:
        step = on.get(entry.step_index)
        if step is None:
            continue
        fired, compared = _scored(entry.runs, keys)
        step.amplifications.append(
            Amplification(
                amplifier=entry.amplifier,
                note=entry.note,
                comparisons=max(len(entry.runs) - 1, 0),
                fired=fired,
                artifacts_compared=compared,
                error=entry.error,
            )
        )


def _step_record(record: RunRecord, index: int) -> StepRecord:
    return next(step for step in record.steps if step.index == index)


def _slug(amplifier: str) -> str:
    return amplifier.replace(" ", "-")


def attach_bisect(
    manifest: Manifest, measured: list[StepMeasurement], keys: Mapping[str, Sequence[str]]
) -> None:
    """Score the single-threaded runs against each other and hang the rate on each step.

    Run in `measure`, so a saved run judged again gets the same cause line from
    the same comparison code rather than from a number copied into the manifest.
    """
    if len(manifest.bisect) < 2:
        return
    reference, *later = manifest.bisect
    fired = dict.fromkeys((s.index for s in reference.steps), 0)
    compared = dict.fromkeys((s.index for s in reference.steps), 0)
    for run in later:
        for step, found in zip(reference.steps, compare_runs(reference, run, keys), strict=True):
            compared[step.index] += len(found)
            if any(f.diverged for f in found):
                fired[step.index] += 1
    for step in measured:
        if step.index in fired:
            step.bisect = Bisect(
                comparisons=len(later),
                fired=fired[step.index],
                artifacts_compared=compared[step.index],
            )


def _amplified_runs(manifest: Manifest) -> list[RunRecord]:
    return [run for entry in manifest.amplified for run in entry.runs]


def measure(
    manifest: Manifest, keys: Mapping[str, Sequence[str]]
) -> list[StepMeasurement]:
    """Compare every later run against run 1, whether it just ran or was loaded.

    Split out so `judge` can re-derive a saved run's measurements from the same
    code path the run command used. Comparing artifacts on disk is a pure
    function of those artifacts, which is why a saved run can be re-scored at
    all.

    The main loop only. `attach_bisect` is a separate call because
    `run_pipeline` needs these fire rates before it knows which steps to
    bisect, so the two cannot be one function without measuring twice. Calling
    it from in here as well made a hidden no-op at run time and left a window
    where every step's `cause` was None for a reason nothing could distinguish
    from having no bisect. Both callers now do the two calls in order.
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
    # Every run this function will re-score, which is the main loop, the bisect
    # and the amplified sequences. The amplified ones were missing from this
    # list, so a pruned run directory reached attach_amplification and raised
    # out of a command whose contract is that it re-scores or explains itself.
    missing = [
        a.path
        for run in (*manifest.runs, *manifest.bisect, *_amplified_runs(manifest))
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
    measured = measure(manifest, keys or {})
    attach_bisect(manifest, measured, keys or {})
    attach_amplification(manifest, measured, keys or {})
    return Report(
        pipeline=manifest.pipeline,
        run_dir=str(manifest.root),
        runs=len(manifest.runs),
        environment=manifest.environment,
        steps=measured,
        seconds=None,
        policy=policy,
        contained=manifest.contained,
        amplified=bool(manifest.amplified),
        bisect_error=manifest.bisect_error,
    )


def run_pipeline(
    pipeline: Path,
    runs: int,
    parent: Path,
    keep: int = 1,
    keys: Mapping[str, Sequence[str]] | None = None,
    policy: Policy | None = None,
    contained: bool = True,
    amplify: bool = True,
) -> tuple[Report, Manifest]:
    if runs < 2:
        raise PipelineError(f"--runs must be at least 2 to have anything to compare, got {runs}")

    steps = load_steps(pipeline)
    run_dir = new_run_dir(parent)
    marker = mark_running(run_dir)
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
                upstream=partial(artifacts_before, reference) if contained and reference else None,
            )
            manifest.runs.append(record)
            if run == 1:
                reference, reference_written = record, written
            # Contained, every run from 2 on carries run 1's state, so each is
            # the second run of the same pipeline rather than the next link in a
            # chain that has already drifted three times.
            carried = reference_written if contained else written

        measured = measure(manifest, keys or {})
        # Which steps get bisected comes off the measured fire rate rather than
        # off the verdict, so the same pipeline under two policies re-executes
        # the same steps and produces the same artifacts.
        divergent = [step.index for step in measured if step.fired]
        if divergent:
            try:
                manifest.bisect = bisect_runs(steps, reference, divergent, run_dir, runs)
            except Exception as exc:  # noqa: BLE001
                # Blanket, and for the same reason as the other two: the
                # bisect is downstream of an answer that is already complete,
                # so a failure here has to become a reported outcome rather
                # than the end of the run. A step that re-executes badly alone,
                # most often because a skipped step created the table it reads
                # through ctx.sql, used to take five finished runs and their
                # manifest with it and report the whole thing as exit 3.
                # Nothing is swallowed: the report says the bisect did not run
                # and prints what raised.
                manifest.bisect_error = f"{type(exc).__name__}: {exc}".strip()
        attach_bisect(manifest, measured, keys or {})
        # The steps the bisect leaves alone are exactly the ones amplification
        # takes, so the two together re-execute every step once more and neither
        # covers a step twice.
        if amplify:
            quiet = [step.index for step in measured if not step.fired]
            manifest.amplified = amplify_runs(
                steps, reference, quiet, run_dir, runs, environment.threads, keys or {}
            )
            attach_amplification(manifest, measured, keys or {})
        manifest.save()
        # Pruning is last rather than first, which costs one run directory of
        # peak disk and buys the previous run surviving anything that goes
        # wrong in this one. --key is only checkable against artifacts that
        # exist, so a typo raises after five executions have finished, and
        # pruning up front had already deleted the run the user could have
        # judged instead. A crashing step lost it the same way.
        retention = prune_run_dirs(parent, keep, current=run_dir)
        report = Report(
            pipeline=str(pipeline),
            run_dir=str(run_dir),
            runs=runs,
            environment=environment,
            steps=measured,
            seconds=time.perf_counter() - began,
            policy=policy or Policy(),
            contained=contained,
            amplified=amplify,
            bisect_error=manifest.bisect_error,
            pruned=len(retention.dropped),
            live=retention.live,
            keep=keep,
        )
    finally:
        marker.unlink(missing_ok=True)
    return report, manifest
