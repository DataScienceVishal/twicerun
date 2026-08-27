#!/usr/bin/env python3
"""Ten full detection passes over the matched-pair reference set, four baselines, one table.

    uv run python scripts/eval.py               # the ten trials the spec fixed
    uv run python scripts/eval.py --trials 2    # a quicker read of the same shape
    uv run python scripts/eval.py --json out.json

Every threshold below was written into `specs/2026-08-26-rerun-determinism-spec.md`
before any of this project existed, including three that declare a part of it
unnecessary. They print with whatever they came out as, and a triggered
condition is a published result rather than a failed run, so this exits 0
either way. The one non-zero exit is 2, for an `--into` that already exists.
Which of them trigger moves between runs of the same code: the
README publishes four ten-trial runs and one of the four breached the spec's
sensitivity threshold.

A trial is three passes: the broken pipeline at the defaults, its matched twin at
the defaults, and the broken pipeline again with containment off. The first two
give sensitivity and specificity, the third is baseline 3. Baseline 1 rescores
the first pass's own runs 1 and 2 bit-exactly, and then rescores every
comparison the oracle got the same way, which is the row that separates a
difference in comparison method from a difference in run count. Baseline 4
rescores the same measurements with the amplifiers taken away, through the
shipped exit-code function. Only baseline 3 costs an extra pipeline pass, and
the run-matched row costs about 20 seconds a trial in comparisons.

Ten trials took 532 seconds on a ten-core laptop, a median of 53 a trial. Peak
disk measured at 1,292 MB, which is one trial's three run directories alive at
once. Each trial's are removed before the next one starts, so that is a peak
rather than a total. The first estimate here was 800 MB and came from arithmetic
rather than from watching it, which is the same mistake the cost section of the
README already records once.
"""

from __future__ import annotations

import argparse
import ast
import json
import platform
import re
import shutil
import statistics
import sys
import textwrap
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from twicerun.amplify import AMPLIFIERS, DIVERGENT, STABLE_ON_THIS_INPUT, Amplification
from twicerun.cli import exit_code
from twicerun.compare import compare as bit_exact
from twicerun.manifest import Manifest
from twicerun.measurement import StepMeasurement, name_classes
from twicerun.policy import HEADROOM_REQUIRED, REDUCTION_ORDER, DriftBound, Policy, judge
from twicerun.report import WIDTH
from twicerun.runner import amplify_runs, load_steps, run_pipeline, scored_amplification
from twicerun.tables import ARTIFACT_VERSION, EVAL, distribution, magnitudes, tally

HERE = Path(__file__).resolve().parent.parent
REFERENCE = HERE / "pipelines" / "reference.py"
TWINS = HERE / "pipelines" / "twins.py"

# The spine of the whole eval. A detector that fires on everything scores 50
# percent against a matched set, so every broken step here has a twin that
# differs from it by one line and must never fire.
PAIRS = (
    ("generate_inputs", "generate_inputs"),
    ("daily_revenue", "daily_revenue_decimal"),
    ("customer_keys", "customer_keys_tiebreak"),
    ("apply_price_updates", "apply_price_updates_deduped"),
    ("append_audit_log", "replace_audit_log"),
    ("sparse_customer_keys", "sparse_customer_keys_tiebreak"),
)

# The four the spec calls broken, plus the intermittent one, which is scored
# apart from them because its whole point is that it does not always fire.
BROKEN = ("daily_revenue", "customer_keys", "apply_price_updates", "append_audit_log")
INTERMITTENT = "sparse_customer_keys"
# Correct code that drifts. Baseline 1's count here is the argument for the
# oracle and the first pre-registered condition under which it is unnecessary.
BENIGN = "mean_basket"
# Downstream of a divergence and contained, so it is what baseline 3 moves.
DOWNSTREAM = "roll_up_keys"
# The append with no unique key, which is where containment changes a magnitude
# rather than a rate.
APPEND = "append_audit_log"

# The spec fixed ten minutes for the ten trials it also fixed, so the budget is
# a minute a trial and the condition scales with --trials. A flat 600 seconds
# against `--trials 2`, which is what the README recommends for a quick read, is
# five times the work's own budget and cannot fail.
BUDGET_SECONDS_PER_TRIAL = 60.0

# Where the trial directories go. Named in .gitignore, which is checked by a
# test, because an interrupted run leaves up to 1,292 MB of TLC-derived Parquet
# here and this repository commits no data bytes.
WORKSPACE = Path(".twicerun-eval")

NOT_MEASURED = "NOT MEASURED"


def say(line: str = "") -> None:
    print(line, flush=True)


def hang(label: str, body: str) -> None:
    """A label in the left column and a paragraph beside it that wraps under itself.

    The same shape `report.py` uses, and it is here for the same reason. Wrapping
    a labelled line at a flat indent puts the overflow under the label rather
    than under the text, so `not triggered  the naive baseline's ...` continued
    with `more machinery than the problem needs` in the verdict column and read
    as a second verdict.
    """
    wrapped = textwrap.wrap(body, width=max(WIDTH - len(label) - 4, 24))
    say(f"  {label}  {wrapped[0]}")
    for line in wrapped[1:]:
        say(f"  {' ' * len(label)}  {line}")


@dataclass(frozen=True)
class NaiveScore:
    """Baseline 1 on one step: what failed to pair on each side, out of what, over how many files.

    The two sides are separate for the reason `StepScore.observe` keeps them
    separate: they mean different things and one of them is often zero. They
    used to be summed into a single count that was then printed against the
    reference row count alone, so a step where 632 rows failed to pair each way
    reported `median 1,265 rows unmatched out of 1,000 on each side`, a
    magnitude larger than the ceiling on the very line asserting a ceiling
    cannot be beaten. `rows_compared` is both sides added up, which is what 1,265
    is actually out of.

    `artifacts` is the guard the other three loops already carry. A step that
    wrote nothing pairs nothing and reports zero unmatched, which is the same
    zero a step whose two runs agreed reports, and the first pre-registered
    condition reads it as grounds for calling the oracle unnecessary. It counts
    the reference run's artifacts, because a name that exists only in the second
    run is invisible to this pass, which is one of the ways it is deliberately
    naive.
    """

    unmatched_reference: int
    unmatched_candidate: int
    rows_compared: int
    artifacts: int

    @property
    def unmatched(self) -> int:
        return self.unmatched_reference + self.unmatched_candidate


@dataclass
class Trial:
    """One trial's measurements, kept after its artifacts are deleted.

    A `StepMeasurement` holds counts the oracle already computed rather than
    anything pointing at a Parquet file, which is why a trial survives its own
    run directory being removed. That matters: ten trials of live artifacts is
    four gigabytes.
    """

    reference: list[StepMeasurement]
    twins: list[StepMeasurement]
    uncontained: list[StepMeasurement]
    naive: dict[str, NaiveScore]
    # The same comparison as `naive` over the runs the oracle got, on both
    # pipelines, which is what tells a difference in method from a difference in
    # run count.
    matched: dict[str, tuple[int, int]]
    matched_twins: dict[str, tuple[int, int]]
    # Each amplifier's rate on the intermittent step, taken every trial rather
    # than only on the trials where the loop came back quiet.
    forced: dict[str, Amplification]
    exit_with_amplifiers: int
    exit_without: int
    seconds: float


@dataclass
class StepScore:
    """One step across every trial. Rates as a distribution, magnitudes as a median.

    Two kinds of number and they are printed differently on purpose. A fire rate
    out of four has five possible values, so every one of them gets a cell,
    including the ones that did not come up. The magnitudes are maxima over a
    thousand groups: their observed range grows with looking time by
    construction, so they get a median and an n and no bracket. That method
    changed in slice 4 after a published bracket was beaten for the fifth time.

    Printing the empty buckets is the refinement slice 5 forced, and it came out
    of this eval disagreeing with itself. Slice 4 published this step's rates
    over 40 passes as `4 of 4 x27, 3 x5, 2 x5, 1 x3` and called it complete
    because a rate out of four cannot leave the range. Two ten-trial runs of the
    eval then gave `apply_price_updates` a flat 0 of 4 in one and nothing below
    1 of 4 in the other. Omitting a bucket that did not come up reads as the
    value being impossible rather than unobserved, which is the same overclaim
    as a bracket in a different shape.
    """

    name: str
    index: int | None = None
    rates: Counter = field(default_factory=Counter)
    single_threaded: Counter = field(default_factory=Counter)
    statuses: Counter = field(default_factory=Counter)
    classes: Counter = field(default_factory=Counter)
    causes: Counter = field(default_factory=Counter)
    unmatched: list[int] = field(default_factory=list)
    extra: list[int] = field(default_factory=list)
    of_rows: list[int] = field(default_factory=list)
    ulps: list[int] = field(default_factory=list)
    relative: list[float] = field(default_factory=list)
    silent_trials: int = 0
    # Every denominator seen, not one of them. `--runs` moves it, and so does a
    # step that wrote nothing on one round of one trial, which is why this is a
    # Counter and not the int it used to be.
    denominators: Counter = field(default_factory=Counter)

    def observe(self, step: StepMeasurement) -> None:
        self.index = step.index
        if step.artifacts_compared == 0:
            # The guard the main loop, the bisect and the amplifiers all carry,
            # arriving in the fourth loop. `any([])` is False, so a step that
            # wrote nothing scores 0 of 4 out of four comparisons of nothing and
            # reads exactly like four clean ones. A trial like that is not
            # evidence about the step and is counted where it can be seen.
            self.silent_trials += 1
            return
        self.denominators[step.measured_comparisons] += 1
        self.rates[f"{step.fired} of {step.measured_comparisons}"] += 1
        self.statuses[step.status or "no status"] += 1
        if step.classes:
            self.classes[name_classes(step.classes)] += 1
        if step.cause:
            self.causes[step.cause] += 1
        if step.bisect is not None and step.bisect.measured:
            self.single_threaded[f"{step.bisect.fired} of {step.bisect.comparisons}"] += 1
        worst = step.worst
        if worst is not None and worst.unmatched_reference + worst.unmatched_candidate:
            # Both sides, because they mean different things and one of them is
            # always zero. A shuffled key loses rows on both sides in equal
            # numbers; an append loses none on the reference side and gains them
            # on the later one. Recording only the reference side reported the
            # append bug as a magnitude of zero next to a fire rate of 4 of 4.
            self.unmatched.append(worst.unmatched_reference)
            self.extra.append(worst.unmatched_candidate)
            self.of_rows.append(worst.reference_rows)
        if step.max_ulps is not None:
            self.ulps.append(step.max_ulps)
        if step.max_relative is not None:
            self.relative.append(step.max_relative)

    @property
    def trials(self) -> int:
        return sum(self.rates.values())

    @property
    def comparisons(self) -> int:
        """The denominator most of these rates are out of, and normally all of them."""
        return max(self.denominators, default=0)

    @property
    def fired_in(self) -> int:
        return sum(n for rate, n in self.rates.items() if not rate.startswith("0 "))

    def distribution(self) -> str:
        """The terminal's copy of the README's cell, from `twicerun.tables`.

        One implementation, because the two used to be separate and a rate that
        read `4 of 4 x27, 3 x5, 2 x5, 1 x3` in one place and something else in
        the other is the drift slice 7 is about, one layer below the tables.
        """
        return distribution(self.rates, self.comparisons)

    def detail(self) -> str:
        """Class, cause, the threads=1 rate, and how far the step moved.

        The magnitudes come from `twicerun.tables`, for the reason
        `distribution` above does. This half had its own copy and the two
        disagreed about which side of an unmatched pair to print: the docstring
        on `magnitudes` has the shape they disagreed on. The labels stay local,
        because the terminal wants `VALUE_DRIFT x8` where the table wants it in
        backticks, and that difference is deliberate.
        """
        parts = []
        if self.classes:
            parts.append(tally(self.classes))
        if self.causes:
            parts.append(f"cause {tally(self.causes)}")
        if self.single_threaded:
            parts.append(f"threads=1 {tally(self.single_threaded)}")
        parts += magnitudes(self.figures())
        return ", ".join(parts)

    def figures(self) -> dict:
        """The same counts `distribution()` and `detail()` render, before they become prose.

        Slice 7 turned the README's tables into something generated from a
        committed copy of this, so a figure in that file cannot be a figure
        somebody retyped. The generator needs the counts and not the sentences:
        parsing `median 491,520 of the 500,000 reference rows found no partner`
        back apart to put two numbers in two cells would be the retyping problem
        again with an extra step in it.
        """
        return {
            "name": self.name,
            "index": self.index,
            "trials": self.trials,
            "fired_in": self.fired_in,
            "comparisons": self.comparisons,
            "silent_trials": self.silent_trials,
            "rates": dict(self.rates),
            "single_threaded": dict(self.single_threaded),
            "statuses": dict(self.statuses),
            "classes": dict(self.classes),
            "causes": dict(self.causes),
            "unmatched_median": median_or_none(self.unmatched),
            "extra_median": median_or_none(self.extra),
            "of_rows_median": median_or_none(self.of_rows),
            "ulps_median": median_or_none(self.ulps),
            "relative_median": median_or_none(self.relative),
            "magnitudes_n": len(self.relative),
        }


def median_or_none(seen: list[int] | list[float]) -> float | None:
    """None where nothing was measured, because a median of nothing is not zero.

    Every table this feeds distinguishes the two. A step that never drifted and
    a step whose drift was zero print differently, and the second one does not
    happen.
    """
    return statistics.median(seen) if seen else None


def only(scored: dict[str, StepScore], name: str) -> StepScore:
    """The score for one step, or an empty one where no pass ever ran it.

    A name that is missing and a name whose every trial compared nothing are
    different things, but both have to reach the pre-registered conditions as
    zero measured trials rather than as a KeyError partway through printing the
    results.
    """
    found = scored.get(name)
    return StepScore(name) if found is None else found


def score(steps: list[StepMeasurement], into: dict[str, StepScore]) -> None:
    for step in steps:
        into.setdefault(step.name, StepScore(step.name)).observe(step)


def naive_pass(manifest: Manifest) -> dict[str, tuple[int, int]]:
    """Baseline 1: runs 1 and 2 of the same trial, compared as multisets of row hashes.

    Two runs, no tolerance, no classification, no repetition. What almost anyone
    would write first, and what the first version of this project's spec
    proposed. Pointed at the trial's own first two runs rather than at two fresh
    ones, so it sees the same bytes under the same containment.

    It does not see the same number of comparisons, and this docstring used to
    say the comparison was the only difference. It is not: the oracle gets runs
    2 to 5 against run 1 and this gets run 2 alone, so most of the published gap
    between them is a run count. `run_matched` below is the row that separates
    those two claims, and it should be read next to this one.

    Per step it returns the rows each side had that the other did not, the
    total rows the two runs put in front of it, and how many artifacts it
    looked at.
    """
    con = duckdb.connect()
    found: dict[str, NaiveScore] = {}
    try:
        reference, second = manifest.runs[0], manifest.runs[1]
        for ref_step, cand_step in zip(reference.steps, second.steps, strict=True):
            written = {a.name: a for a in cand_step.artifacts}
            missing = appeared = rows = 0
            for artifact in ref_step.artifacts:
                later = written.get(artifact.name)
                if later is None:
                    missing += artifact.rows
                    rows += artifact.rows
                    continue
                diff = bit_exact(con, artifact, later)
                missing += diff.only_in_reference
                appeared += diff.only_in_candidate
                rows += artifact.rows + later.rows
            found[ref_step.name] = NaiveScore(
                missing, appeared, rows, len(ref_step.artifacts)
            )
    finally:
        con.close()
    return found


def run_matched(manifest: Manifest) -> dict[str, tuple[int, int]]:
    """The same bit-exact comparison, over every comparison the oracle got.

    Baseline 1 gets one comparison and the oracle gets four, so the two answers
    differ by a run count as well as by a comparison and the eval published the
    difference as though only the comparison moved. This scores runs 2 to N
    against run 1 with `twicerun.compare`, which is multiset equality over row
    hashes and produces no class, no magnitude, no attributed column, no bound
    and no cause.

    Per step it returns the comparisons in which anything differed and the
    comparisons that looked at an artifact, which are `StepMeasurement.fired`
    and `measured_comparisons` computed the cheap way, so the two are
    comparable cell by cell.
    """
    con = duckdb.connect()
    found: dict[str, tuple[int, int]] = {}
    try:
        reference, *later = manifest.runs
        for run in later:
            for ref_step, cand_step in zip(reference.steps, run.steps, strict=True):
                written = {a.name: a for a in cand_step.artifacts}
                differed = False
                for artifact in ref_step.artifacts:
                    twin = written.get(artifact.name)
                    if twin is None:
                        differed = True
                        continue
                    diff = bit_exact(con, artifact, twin)
                    differed = differed or bool(diff.only_in_reference or diff.only_in_candidate)
                fired, compared = found.get(ref_step.name, (0, 0))
                looked = 1 if ref_step.artifacts else 0
                hit = 1 if differed and looked else 0
                found[ref_step.name] = (fired + hit, compared + looked)
    finally:
        con.close()
    return found


# Baseline 2. Written after the bugs were known, which is the strongest bias
# possible in a static checker's favour, and it is stated in the output because
# the interesting result is what it still cannot separate.
STATIC_CHECKS = (
    (
        "a MERGE whose source multiplicity is not visible here",
        re.compile(r"MERGE\s+INTO", re.IGNORECASE),
    ),
    (
        "a window ordered by one column, which may not be unique",
        re.compile(r"OVER\s*\(\s*ORDER BY\s+[A-Za-z_][A-Za-z_0-9]*\s*\)", re.IGNORECASE),
    ),
    (
        "a float aggregate whose reduction order is not pinned",
        re.compile(r"\b(sum|avg)\s*\(\s*[A-Za-z_][A-Za-z_0-9]*\s*\)", re.IGNORECASE),
    ),
    (
        "a step that writes a name it also reads, so a rerun appends",
        # ast.unparse below normalises every string literal to single quotes,
        # so a pattern spelled with double quotes matches the source file and
        # not the thing it is actually run against. That cost one silent miss.
        re.compile(
            r"ctx\.state\(\s*['\"](?P<name>\w+)['\"].*"
            r"ctx\.write\(\s*['\"](?P=name)['\"]",
            re.DOTALL,
        ),
    ),
)


def static_flags(pipeline: Path) -> dict[str, list[str]]:
    """Which of the four patterns hit which step, per function body.

    Parsed rather than grepped because the pairing is what the baseline is
    scored on and a file-level hit cannot be attributed to a step. Grep the
    twins file for the append pattern and it hits, because
    `apply_price_updates_deduped` reads and writes `prices`; a file-level check
    would report an append bug in the file whose entire purpose is that it has
    none.

    Docstrings are dropped on the way. On these two files that changes no hit,
    which was measured rather than assumed, but every bug here is described in
    words directly above its own code and leaving them in would make the
    patterns depend on how the prose is worded.
    """
    tree = ast.parse(pipeline.read_text(encoding="utf-8"))
    flagged = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        body = "\n".join(
            ast.unparse(statement)
            for statement in node.body
            if not (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant))
        )
        flagged[node.name] = [name for name, pattern in STATIC_CHECKS if pattern.search(body)]
    return flagged


def without_amplifiers(steps: list[StepMeasurement]) -> list[StepMeasurement]:
    """The same measurements as `--no-amplify` would have produced.

    The main loop is identical code either way, so this is not an approximation
    of the flag: dropping the amplifications is exactly what the flag does to
    what `measure` returns. The exit code then comes out of the shipped
    `exit_code`, so this baseline cannot drift away from the tool.
    """
    return [replace(step, amplifications=[]) for step in steps]


def forced_amplification(
    manifest: Manifest, runs: int, index: int
) -> dict[str, Amplification]:
    """Each amplifier's rate on one step, whether or not the main loop already caught it.

    `twicerun run` will not do this and should not: amplification only touches
    steps that came back quiet, which is the whole cost argument for it. But the
    gap it exists to close is only observable when the two rates sit side by
    side on the same step in the same trial, and waiting for a trial where the
    loop stays quiet throws away most of them.

    So this points the shipped amplifiers at one step directly, through the same
    `amplify_runs` the runner calls. It is what `amplification_gap.py` next door
    measures over 40 passes, folded into a trial that has already paid for the
    pipeline run underneath it.

    The rate is built by `scored_amplification`, which is the function the runner
    calls for the same job, so `measured` means here what it means in a report.
    """
    rates: dict[str, Amplification] = {}
    for entry in amplify_runs(
        load_steps(REFERENCE),
        manifest.runs[0],
        [index],
        Path(manifest.root),
        runs,
        manifest.environment.threads,
        {},
    ):
        rates[entry.amplifier] = scored_amplification(entry, {})
    return rates


def claim_workspace(into: Path) -> bool:
    """Create the scratch directory, refusing any path this invocation did not make.

    The trial directories are deleted when the run finishes, and the delete used
    to be `shutil.rmtree(args.into, ignore_errors=True)` pointed at whatever
    `--into` was handed. `--into .` therefore deleted the working tree, and
    `ignore_errors` meant a half-finished delete of the wrong tree said nothing
    at all. Everything this eval writes goes into a directory it created itself,
    so refusing one that already exists costs nothing and is the entire check.
    """
    try:
        into.mkdir(parents=True)
    except FileExistsError:
        print(
            f"eval.py: {into} already exists, and this deletes the directory it is given when "
            f"it finishes, so it will only use one it made itself. Pass --into somewhere that "
            f"does not exist, or remove that path yourself first.",
            file=sys.stderr,
        )
        return False
    return True


def one_trial(into: Path, runs: int) -> Trial:
    began = time.perf_counter()
    broken, manifest = run_pipeline(REFERENCE, runs=runs, parent=into / "reference", keep=1)
    naive = naive_pass(manifest)
    matched = run_matched(manifest)
    index = next(s.index for s in broken.steps if s.name == INTERMITTENT)
    amplified = forced_amplification(manifest, runs, index)
    twinned, twin_manifest = run_pipeline(TWINS, runs=runs, parent=into / "twins", keep=1)
    matched_twins = run_matched(twin_manifest)
    ablated, _ = run_pipeline(
        REFERENCE, runs=runs, parent=into / "uncontained", keep=1, contained=False
    )
    quiet = replace(broken, steps=without_amplifiers(broken.steps), amplified=False)
    trial = Trial(
        reference=broken.steps,
        twins=twinned.steps,
        uncontained=ablated.steps,
        naive=naive,
        matched=matched,
        matched_twins=matched_twins,
        forced=amplified,
        exit_with_amplifiers=exit_code(broken),
        exit_without=exit_code(quiet),
        seconds=time.perf_counter() - began,
    )
    shutil.rmtree(into)
    return trial


def environment() -> dict[str, str]:
    """The three things every number here is specific to, in one place.

    The spec asks for the DuckDB version, the thread count and the platform
    beside any figure quoted from this. They were printed and not written to
    `--json`, so the file slice 7 reads to regenerate the README's tables had no
    way to label them.
    """
    probe = duckdb.connect()
    threads = probe.execute("SELECT current_setting('threads')").fetchone()[0]
    probe.close()
    return {
        "duckdb": duckdb.__version__,
        "threads": str(threads),
        "platform": platform.platform(),
    }


def environment_lines(where: dict[str, str], runs: int, trials: int) -> list[str]:
    return [
        f"  reference  {REFERENCE.relative_to(HERE)}",
        f"  twins      {TWINS.relative_to(HERE)}",
        f"  trials     {trials}, each a full detection pass, a twin pass and an "
        f"uncontained pass",
        f"  runs       {runs} per pass, so {runs - 1} comparisons per step per pass",
        f"  duckdb     {where['duckdb']}, threads={where['threads']}",
        f"  platform   {where['platform']}",
    ]


WHAT_EACH_STEP_IS = {
    "generate_inputs": "the control, which must never fire",
    "daily_revenue": "bug 1, sum() over DOUBLE under parallel reduction",
    "customer_keys": "bug 2, row_number() over a non-unique sort, 500 rows per tie",
    "apply_price_updates": "bug 3, a MERGE whose source has two rows per target key",
    "append_audit_log": "bug 4, an append with no unique key",
    "mean_basket": "no bug: a correct float average that legitimately reassociates",
    "sparse_customer_keys": "bug 2 at 2 rows per tie, where it fires only sometimes",
    "roll_up_keys": "no bug of its own, and downstream of bug 2",
}


def report_sensitivity(scored: dict[str, StepScore], trials: int) -> None:
    """The results table. Two lines per step, because one line cannot hold both kinds of number.

    The rate line carries quantities with a real ceiling, printed as the whole
    distribution so there is nothing left to beat. The detail line carries
    quantities that are maxima over many groups, printed as a median with an n
    and no bracket, because the observed range of a maximum grows with looking
    time by construction and a bracket on one is a promise rather than a
    summary.
    """
    say(f"\nsensitivity, {trials} trials of the broken pipeline at the defaults")
    width = max(len(name) for name in scored)
    for name, description in WHAT_EACH_STEP_IS.items():
        step = scored.get(name)
        if step is None:
            continue
        say(
            f"  {name:<{width}}  fired in {step.fired_in:>2} of {trials}   "
            f"{step.distribution()}"
        )
        say(f"  {'':<{width}}  {description}")
        detail = step.detail()
        if detail:
            hang(" " * width, detail)
        if step.silent_trials:
            say(
                f"  {'':<{width}}  {step.silent_trials} trial(s) compared no artifact at all "
                f"and are not counted above"
            )
    denominators = sorted({step.comparisons for step in scored.values() if step.trials})
    if not denominators:
        say("  No step compared an artifact in any trial, so there is no rate above to read.")
        return
    of = denominators[-1]
    say("  A fire rate is the whole distribution rather than a range, so every value it could")
    say(f"  have taken gets a cell: out of {of} it has {of + 1} possible values. The magnitudes")
    say("  are maxima over a thousand groups, so they carry a median and an n and no bracket.")
    if len(denominators) > 1:
        say(f"  Not every step compared {of} times: the denominators above are {denominators}.")


def report_specificity(scored: dict[str, StepScore], trials: int) -> None:
    say("\nspecificity: the matched twins, one line of difference each, none of which may fire")
    width = max(len(twin) for _, twin in PAIRS)
    fires = compared = quiet = 0
    for _, twin in PAIRS:
        step = scored.get(twin)
        if step is None:
            continue
        hang(
            f"{twin:<{width}}",
            f"{step.fired_in} of {step.trials} trials fired   {step.distribution()}   "
            f"{', '.join(f'{s} x{n}' for s, n in step.statuses.most_common())}",
        )
        fires += step.fired_in
        compared += step.trials
        quiet += step.silent_trials
    say(
        f"  {fires} twin step-passes fired across the {compared} that compared anything, of "
        f"{len(PAIRS) * trials} attempted."
    )
    if quiet:
        # The guard report_sensitivity carries. Without it, sixty twin
        # step-passes that compared nothing print as `0 twin step-passes fired`
        # and read as the strongest specificity result in the file.
        say(f"  {quiet} of them compared no artifact at all and are not counted as clean.")
    shared = [twin for broken, twin in PAIRS if broken == twin]
    untwinned = [name for name in WHAT_EACH_STEP_IS if name not in {b for b, _ in PAIRS}]
    if shared:
        say(f"  {', '.join(shared)} is the same function in both files, so its share of that")
        say("  denominator is the sensitivity table's control row counted a second time.")
    if untwinned:
        say(f"  {', '.join(untwinned)} have no twin and are not in this table at all, which")
        say("  leaves out the correct-code step that fires in every trial above.")


def report_twin_comparisons(trials: list[Trial]) -> tuple[dict[str, int], list[dict]]:
    """How many comparisons the twins actually survived, counted rather than assumed.

    The spec predicted 4 main-loop plus 6 amplifier comparisons per twin per
    trial. Tie collapse declines more often than it applies, so the real
    amplified count is lower and printing the prediction would overstate the
    evidence by about a third.

    The main-loop figure was taken nominally, at runs minus one per step per
    trial, in the same function that filtered the amplified one on `measured`
    and under this docstring. That number is the specificity evidence the README
    quotes twice, so it is counted the same way as its neighbour now.
    """
    loop = sum(step.measured_comparisons for trial in trials for step in trial.twins)
    amplified = sum(
        a.comparisons
        for trial in trials
        for step in trial.twins
        for a in step.amplifications
        if a.measured
    )
    declined = sum(
        1
        for trial in trials
        for step in trial.twins
        for a in step.amplifications
        if not a.measured
    )
    say(f"  {loop} main-loop comparisons and {amplified} amplified ones on correct code.")
    say(f"  {declined} amplifier attempts declined and are not counted as clean.")
    return {"main_loop": loop, "amplified": amplified, "declined": declined}, twin_coverage(trials)


def twin_coverage(trials: list[Trial]) -> list[dict]:
    """Per amplifier: how many twin step-passes it took, and what it found on them.

    The totals above collapse the three amplifiers into one number, and the
    first column of this is the honest part. Tie collapse declines most of its
    chances on the twins because two of their columns are already past the
    density it targets, so a twin it never touched is not evidence that it is
    safe on that twin. Folding those into the zero made the table read twice as
    strong as it is.
    """
    every = [a for trial in trials for step in trial.twins for a in step.amplifications]
    # Every twin step-pass, including any that fired and so never reached an
    # amplifier at all. Counting only the attempts would put the denominator at
    # the mercy of the numerator.
    step_passes = sum(len(trial.twins) for trial in trials)
    rows = []
    for amplifier, _ in AMPLIFIERS:
        offered = [a for a in every if a.amplifier == amplifier]
        usable = [a for a in offered if a.measured]
        rows.append(
            {
                "amplifier": amplifier,
                "step_passes": step_passes,
                "offered": len(offered),
                "ran_on": len(usable),
                "comparisons": sum(a.comparisons for a in usable),
                "fired": sum(a.fired for a in usable),
                "raised": sum(1 for a in offered if a.error),
            }
        )
    return rows


def report_baseline_one(trials: list[Trial], scored: dict[str, StepScore]) -> dict[str, float]:
    say("\nbaseline 1: bit-exact multiset equality, no tolerance and no classes, at two run counts")
    say("  Runs 1 and 2 of each trial, rescored. Same bytes as the oracle saw.")
    benign = [n for n in (trial.naive.get(BENIGN) for trial in trials) if n and n.artifacts]
    fired = sum(1 for n in benign if n.unmatched)
    counts = [n.unmatched for n in benign]
    if benign:
        hang(
            f"{BENIGN:<22}",
            f"fired on {fired} of {len(benign)} trials, median "
            f"{statistics.median(counts):,.0f} of the "
            f"{statistics.median(n.rows_compared for n in benign):,.0f} rows compared found no "
            f"partner: {statistics.median(n.unmatched_reference for n in benign):,.0f} on the "
            f"reference side and "
            f"{statistics.median(n.unmatched_candidate for n in benign):,.0f} on the later run's",
        )
        if len(benign) < len(trials):
            hang(" " * 22, f"{len(trials) - len(benign)} trial(s) compared nothing here")
        say("  Nothing is wrong with that step. It is a correct float average and every one of")
        say("  those findings is the arithmetic behaving normally.")
    else:
        hang(f"{BENIGN:<22}", f"wrote no artifact to compare in any of the {len(trials)} trials")

    intermittent = [
        n for n in (trial.naive.get(INTERMITTENT) for trial in trials) if n and n.artifacts
    ]
    hang(
        f"{INTERMITTENT:<22}",
        f"fired on {sum(1 for n in intermittent if n.unmatched)} of {len(intermittent)} trials "
        f"at one comparison, against the five-run loop's "
        f"{scored[INTERMITTENT].fired_in} of {scored[INTERMITTENT].trials}",
    )
    figures = {
        # The median of both sides added up, which is the figure the README has
        # published since slice 5, now with the denominator it is out of.
        # None rather than 0.0 where nothing compared, on `median_or_none`'s
        # argument: the benign step's whole role in this eval is that its zero
        # is a real measurement, so an absence dressed as one is the mistake
        # three pre-registered conditions are guarded against making.
        "benign_median": median_or_none(counts),
        "benign_rows_compared": median_or_none([n.rows_compared for n in benign]),
        "benign_reference_side": median_or_none([n.unmatched_reference for n in benign]),
        "benign_later_side": median_or_none([n.unmatched_candidate for n in benign]),
        "benign_fired": fired,
        "benign_trials": len(benign),
    }
    # Both halves, always, and the run-matched half is why. It used to sit
    # behind an early return taken when the benign step compared nothing, which
    # deleted the one row in this eval that separates a difference in comparison
    # method from a difference in run count, and left `baseline_1` carrying
    # three of its ten keys for `twicerun report` to fail on.
    return {**figures, **report_run_matched(trials)}


def report_run_matched(trials: list[Trial]) -> dict[str, float]:
    """The same comparison over the same runs the oracle got, which is the honest control.

    Everything above this line hands the cheap comparison one comparison and the
    oracle four, then reports the difference as though the comparison method
    were the only thing that moved. It is not, and this is the row that says so:
    same runs, same bytes, same containment, and nothing but multiset equality.

    What the oracle produces that this cannot is the class, the ulp and relative
    magnitudes, the attributed column, the derived bound and the bisect cause.
    None of those appear in the sensitivity or specificity tables, which is why
    those two tables are the wrong place to look for evidence that the oracle
    earns its cost.
    """
    say("")
    say("  Run-matched: the same bit-exact comparison over all of the oracle's comparisons.")
    fires = {
        name: sum(1 for t in trials if t.matched.get(name, (0, 0))[0])
        for name in (*BROKEN, INTERMITTENT, BENIGN)
    }
    hang(
        f"{'run-matched':<22}",
        ", ".join(f"{name} {n} of {len(trials)}" for name, n in fires.items()),
    )
    cells = disagreed = 0
    for trial in trials:
        for step in trial.reference:
            found = trial.matched.get(step.name)
            if found is None or not found[1]:
                continue
            cells += 1
            disagreed += found[0] != step.fired
    twin_fires = sum(fired for t in trials for fired, _ in t.matched_twins.values())
    twin_passes = sum(1 for t in trials for _, compared in t.matched_twins.values() if compared)
    hang(
        " " * 22,
        f"disagreed with the oracle's fire count on {disagreed} of {cells} step-trials, and "
        f"fired on {twin_fires} of {twin_passes} twin step-passes",
    )
    if not cells or not twin_passes:
        say("  One side of that compared nothing, so this row is not a result either way.")
    elif not disagreed and not twin_fires:
        say("  So the sensitivity and specificity tables above hold no evidence for the oracle")
        say("  over multiset equality given the same runs. What it adds is the class, the")
        say("  magnitudes, the attributed column, the bound and the cause, and none of those")
        say("  are scored up there.")
    else:
        say("  Where those two disagree is the only place in this eval where the comparison")
        say("  method is doing the work rather than the run count.")
    return {
        "matched_disagreements": disagreed,
        "matched_cells": cells,
        "matched_twin_fires": twin_fires,
        "matched_twin_passes": twin_passes,
    }


def report_baseline_two() -> dict[str, int]:
    say("\nbaseline 2: a static pattern check, which is the answer to why not just read the code")
    say("  Four patterns, written after the bugs were known. That is the strongest bias a")
    say("  static checker can be given, and it is the point: what it still cannot do is the")
    say("  result.")
    broken_flags, twin_flags = static_flags(REFERENCE), static_flags(TWINS)
    width = max(len(a) + len(b) for a, b in PAIRS) + 4
    separated = 0
    for broken, twin in PAIRS:
        on_broken = set(broken_flags.get(broken, ()))
        on_twin = set(twin_flags.get(twin, ()))
        if broken == twin:
            verdict = "control, flagged on neither" if not on_broken else "flags the control"
        elif on_broken and not on_twin:
            separated += 1
            verdict = f"separated: {'; '.join(sorted(on_broken))}"
        elif on_broken and on_twin:
            verdict = f"flags both: {'; '.join(sorted(on_broken & on_twin))}"
        else:
            verdict = "misses the broken one"
        hang(f"{broken} / {twin}".ljust(width - 4), verdict)
    false_positives = [
        name
        for name, hits in broken_flags.items()
        if hits and name not in BROKEN and name != INTERMITTENT
    ]
    say(f"  {separated} of {len(PAIRS) - 1} pairs separated.")
    for name in false_positives:
        hang(
            "false positive",
            f"{name} has no bug and is flagged anyway: {'; '.join(broken_flags[name])}",
        )
    say("  The pair it cannot separate is the MERGE, and no pattern can: the fix is a GROUP BY")
    say("  in a different statement, so telling the two apart is data-flow analysis rather")
    say("  than a pattern. Three of these five bugs are properties of the data, not the text.")
    return {"pairs_separated": separated, "false_positives": len(false_positives)}


def report_baseline_three(
    trials: list[Trial], scored: dict[str, StepScore]
) -> tuple[dict[str, float], dict[str, StepScore]]:
    """Two quantities, because containment moves two different things.

    The first is the step that gets falsely reported divergent, which is the
    count the spec pre-registered. The second is the magnitude on a step that
    diverges either way, which containment does not stop but does correct.

    Counting steps-divergent-per-pass and subtracting was the first attempt and
    it was noise: two steps in this pipeline are intermittent, so the difference
    between a contained pass and an uncontained one is dominated by which of
    them happened to fire. The quantity has to be per step.
    """
    say("\nbaseline 3: no containment, which is the ablation that earns the Spot citation")
    ablated: dict[str, StepScore] = {}
    for trial in trials:
        score(trial.uncontained, ablated)
    width = max(len(name) for name in ablated)
    falsely = 0
    for name in ablated:
        with_it, without = scored.get(name), ablated[name]
        if with_it is None:
            continue
        moved = "" if with_it.fired_in == without.fired_in else "   MOVED"
        hang(
            f"{name:<{width}}",
            f"contained {with_it.distribution()}   /   "
            f"uncontained {without.distribution()}{moved}",
        )
        if name == DOWNSTREAM and without.fired_in:
            falsely = without.fired_in
    downstream = only(ablated, DOWNSTREAM)
    say(
        f"  {DOWNSTREAM} is the one with no bug of its own. Uncontained it fires on {falsely} "
        f"of the {downstream.trials} trials"
    )
    say(
        f"  that compared it, and is given cause {tally(downstream.causes) or 'none'}, "
        f"which is a confident wrong diagnosis:"
    )
    say("  it computes an integer minimum, and an integer minimum cannot reassociate into a")
    say("  different answer. A fire there means it was fed something different, never that it")
    say("  computed something different. The bisect gives it that label because a step cannot")
    say("  be re-executed alone without run 1's artifacts to read, so the ablation ablates the")
    say("  main loop and not the bisect.")
    say("  The other steps move too, and that is the intermittent pair rather than containment.")

    overstated = _append_magnitudes(trial.uncontained for trial in trials)
    true = _append_magnitudes(trial.reference for trial in trials)
    with_it, without = only(scored, APPEND), only(ablated, APPEND)
    # Four lines on one label column, because the pair below only means anything
    # against the pair above it: the magnitude moves and the rate does not.
    width = len(f"{APPEND} extra rows,")
    say("")
    for label, uncontained, contained in (
        (f"{APPEND} extra rows,", tally(overstated), tally(true)),
        ("fire rate,", without.distribution(), with_it.distribution()),
    ):
        say(f"  {label:>{width}} {'uncontained':>11}  {uncontained}")
        say(f"  {'':>{width}} {'contained':>11}  {contained}")
    say("  Four reruns' worth of duplication charged to one step, against what one rerun of it")
    if without.distribution() == with_it.distribution():
        say("  does. The rate did not move, so what containment bought here is the magnitude")
        say("  being a fact about the step rather than about how many times the tool ran.")
    else:
        say("  does, and the rate moved as well this time. This line asserted 4 of 4 either way")
        say("  for two slices without deriving it from anything, which is why it prints both.")
    return {
        "falsely_divergent_trials": falsely,
        # The denominator condition 4 is out of. Uncontained, this step reads a
        # diverging artifact and fires; if it compared nothing, a zero here is
        # not evidence that containment removed no false step.
        "downstream_trials": downstream.trials,
        "overstated": max((int(k.replace(",", "")) for k in overstated), default=0),
        "contained_magnitude": max((int(k.replace(",", "")) for k in true), default=0),
    }, ablated


def _append_magnitudes(passes: Iterable[list[StepMeasurement]]) -> Counter:
    """How many extra rows the append bug produced, per pass, as a distribution.

    The count is on the later side. An append with no key loses nothing from the
    reference run and gains rows in every run after it, so the reference-side
    figure is zero and reading it would report the loudest bug in the pipeline
    as a magnitude of nothing.
    """
    seen = Counter()
    for steps in passes:
        step = next((s for s in steps if s.name == APPEND), None)
        worst = None if step is None else step.worst
        if worst is not None:
            seen[f"{worst.unmatched_candidate:,}"] += 1
    return seen


def report_baseline_four(
    trials: list[Trial], scored: dict[str, StepScore]
) -> tuple[dict[str, int], list[dict]]:
    say("\nbaseline 4: no amplification, scored through the tool's own exit-code function")
    say("  The main loop is identical code either way, so this is the flag rather than a")
    say("  model of it: the same measurements with the amplifiers taken away.")
    went_green = [t for t in trials if t.exit_without == 0 and t.exit_with_amplifiers != 0]
    codes = Counter(f"{t.exit_with_amplifiers} to {t.exit_without}" for t in trials)
    say(f"  exit code with amplifiers to without   {tally(codes)}")
    say(
        f"  {len(went_green)} of {len(trials)} trials exit 0 without the amplifiers on a "
        f"pipeline that is broken."
    )

    # The trial-level gap is only observable where the plain loop had nothing to
    # say, because amplification deliberately only touches steps that came back
    # quiet. On a run where the loop caught the step every time, calling that a
    # zero gap reports the opposite of what happened. So the per-comparison gap
    # is taken every trial as well, by pointing the amplifiers at the step
    # whether or not the loop already had it.
    looked = [t for t in trials if _compared(t.reference, INTERMITTENT)]
    silent = [t for t in looked if _rate(t.reference, INTERMITTENT) == 0]
    caught = [t for t in silent if _status(t.reference, INTERMITTENT) == STABLE_ON_THIS_INPUT]
    stable = only(scored, INTERMITTENT).statuses.get(STABLE_ON_THIS_INPUT, 0)
    say(
        f"\n  The five-run loop reported nothing on {INTERMITTENT} in {len(silent)} of the "
        f"{len(looked)} trials it"
    )
    say(
        f"  compared, and an amplifier fired on {len(caught)} of those {len(silent)}. That is "
        f"the gap at the trial level."
    )

    say(f"\n  Per comparison on {INTERMITTENT}, with the amplifiers pointed at it every trial:")
    loop_fired = sum(_rate(t.reference, INTERMITTENT) for t in trials)
    loop_of = sum(
        s.measured_comparisons for t in trials for s in t.reference if s.name == INTERMITTENT
    )
    # A trial with nothing to report is three different things and they were one
    # column: the amplifier declined to build an input, or it built one and the
    # step wrote nothing to compare, or it compared and found the step clean.
    # Only the last of those is evidence about the step.
    rows = [
        (
            "the five-run loop",
            loop_fired,
            loop_of,
            len(silent),
            len(trials) - len(looked),
            0,
        )
    ]
    for amplifier, _ in AMPLIFIERS:
        seen = [t.forced[amplifier] for t in trials if amplifier in t.forced]
        usable = [a for a in seen if a.measured]
        rows.append(
            (
                amplifier,
                sum(a.fired for a in usable),
                sum(a.comparisons for a in usable),
                sum(1 for a in usable if not a.fired),
                sum(1 for a in seen if a.ran and not a.measured),
                sum(1 for a in seen if not a.ran),
            )
        )
    width = max(len(name) for name, *_ in rows)
    for name, fired, of, clean, nothing, declined in rows:
        rate = f"{fired} of {of}" if of else "nothing compared"
        say(
            f"    {name:<{width}}  {rate:>12}  {fired / max(of, 1):>5.2f}   "
            f"{clean} clean, {nothing} compared nothing, {declined} declined"
        )
    raised = sum(1 for t in trials for a in t.forced.values() if a.error)
    if raised:
        say(f"    {raised} of the attempts above raised while running and count as declined.")
    say("  A row with fewer comparisons than the others found nothing in its first two:")
    say("  an amplifier is only escalated to the full run count on a hit, which is the cost")
    say("  argument for it working.")
    amplified_gap = max(
        (fired / of for _, fired, of, *_ in rows[1:] if of), default=0.0
    )
    forced = [
        {
            "source": name,
            "fired": fired,
            "comparisons": of,
            "clean": clean,
            "compared_nothing": nothing,
            "declined": declined,
        }
        for name, fired, of, clean, nothing, declined in rows
    ]
    return {
        "false_negative_trials": len(went_green),
        "stable_trials": stable,
        "loop_looked_trials": len(looked),
        "loop_silent_trials": len(silent),
        "amplifier_caught": len(caught),
        "loop_rate": loop_fired / max(loop_of, 1),
        "best_amplifier_rate": amplified_gap,
        # Condition 2 compares two rates, so it needs both denominators. Either
        # of them at zero is an absence of measurement and not a zero gap.
        "loop_comparisons": loop_of,
        "amplifier_comparisons": sum(of for _, _, of, *_ in rows[1:]),
    }, forced


def _rate(steps: list[StepMeasurement], name: str) -> int:
    step = next((s for s in steps if s.name == name), None)
    return 0 if step is None else step.fired


def _compared(steps: list[StepMeasurement], name: str) -> bool:
    step = next((s for s in steps if s.name == name), None)
    return step is not None and step.artifacts_compared > 0


def _status(steps: list[StepMeasurement], name: str) -> str | None:
    step = next((s for s in steps if s.name == name), None)
    return None if step is None else step.status


def report_bounds(
    trials: list[Trial], policy: Policy
) -> tuple[dict[str, float], list[dict]]:
    say(f"\nthe pre-registered {HEADROOM_REQUIRED:,.0f}x headroom check, at both choices of n")
    loose_clear = tight_clear = passes = 0
    loose, tight = [], []
    per_step: dict[str, tuple[int, list[DriftBound]]] = {}
    for trial in trials:
        for step in trial.reference:
            bound = judge(step, policy).bound
            if bound is None or bound.observed <= 0:
                continue
            passes += 1
            loose.append(bound.ratio)
            tight.append(bound.tight_ratio)
            loose_clear += bound.ratio >= HEADROOM_REQUIRED
            tight_clear += bound.tight_ratio >= HEADROOM_REQUIRED
            per_step.setdefault(step.name, (step.index, []))[1].append(bound)
    if not passes:
        say("  no float step drifted in any trial, so there was nothing to check")
        return {}, []
    say(
        f"  n = rows read by the step      cleared on {loose_clear} of {passes} float "
        f"step-passes, median {statistics.median(loose):,.0f}x"
    )
    say(
        f"  n = terms per output row       cleared on {tight_clear} of {passes} float "
        f"step-passes, median {statistics.median(tight):,.0f}x"
    )
    say("  The first carries a factor of the output row count in slack and the second has")
    say("  none. The check as written is the first. The honest reading is the second.")
    # Multiplying that factor out is the part nobody did. relative_bound is
    # linear in terms in this regime, so the loose ratio is the tight one times
    # the output row count, and this pipeline groups into 1,000 days.
    slack = statistics.median(loose) / statistics.median(tight)
    say(f"  The first is the second times {slack:,.0f}, which is the pipeline's output row count.")
    if abs(slack - HEADROOM_REQUIRED) <= HEADROOM_REQUIRED * 0.05:
        say(f"  That is also {HEADROOM_REQUIRED:,.0f}, the multiplier this check asks for, so "
            f"clearing it at the loose n")
        say("  is clearing the honest n with no margin at all. A pipeline grouping into 100 days")
        say("  would fail the same check on the same drift.")
    else:
        say("  The multiplier this check asks for was fixed independently of that factor.")
    return {
        "loose_cleared": loose_clear,
        "tight_cleared": tight_clear,
        "step_passes": passes,
        "loose_median": statistics.median(loose),
        "tight_median": statistics.median(tight),
        "loose_over_tight": slack,
    }, [drift_row(name, index, seen) for name, (index, seen) in per_step.items()]


def drift_row(name: str, index: int, seen: list[DriftBound]) -> dict:
    """One float step's drift against both readings of the bound, over every trial it drifted in.

    The two bounds are properties of the term count rather than of the run, so
    they come back as single numbers. The drift is a maximum over a thousand
    groups and moves every trial, so it comes back as a median with the n it was
    taken over and no bracket. That split is the method the README arrived at
    after a bracket on a maximum had been beaten five times.
    """
    return {
        "step": name,
        "index": index,
        "terms": statistics.median(b.terms for b in seen),
        "output_rows": statistics.median(b.output_rows for b in seen),
        "tight_terms": statistics.median(b.tight_terms for b in seen),
        "bound_loose": statistics.median(b.bound for b in seen),
        "bound_tight": statistics.median(b.tight_bound for b in seen),
        "observed_median": statistics.median(b.observed for b in seen),
        "loose_ratio_median": statistics.median(b.ratio for b in seen),
        "tight_ratio_median": statistics.median(b.tight_ratio for b in seen),
        "loose_cleared": sum(1 for b in seen if b.ratio >= HEADROOM_REQUIRED),
        "tight_cleared": sum(1 for b in seen if b.tight_ratio >= HEADROOM_REQUIRED),
        "step_passes": len(seen),
    }


def report_policy_cost(trials: list[Trial]) -> tuple[dict[str, int], dict[str, dict]]:
    """What `--policy reduction-order` costs on the four broken steps.

    Everything else here runs strict, which is the default and the only policy
    the tables measure, so this figure appears nowhere above and the README
    explains the mechanism without ever printing its price. A step whose every
    comparison is downgraded does not gate a release, and one of the four is a
    float aggregate whose drift is exactly what the policy exists to downgrade.
    """
    say("\nsensitivity under --policy reduction-order, which no table above runs")
    lenient = Policy(name=REDUCTION_ORDER)
    lost = []
    passes = 0
    # Every step, not only the four the spec calls broken. The terminal keeps
    # the four, because the cost of the flag is a fact about broken steps and
    # the rest of that list is noise on a screen. The README's results table has
    # a column for it on all eight, so the whole map goes back to the caller.
    downgraded: dict[str, dict] = {}
    for name, gated, seen, rates in gate_under(trials, lenient):
        downgraded[name] = {"gated_in": gated, "of": seen, "rates": rates}
        if name not in BROKEN or not seen:
            continue
        passes += 1
        hang(f"{name:<22}", f"gates on {gated} of {seen} trials")
        if gated < seen:
            lost.append(name)
    if not passes:
        say("  Nothing compared, so there is no cost to report here.")
        return {"gating_steps": 0, "gating_of": 0}, downgraded
    say(f"  {passes - len(lost)} of the {passes} broken steps still gate a release under it.")
    if lost:
        say(f"  {', '.join(lost)} stops gating: float-only drift, inside the derived bound, and")
        say("  gone at threads=1, which is the conjunction the policy downgrades on. That is")
        say("  the trade the flag makes and it is not in any table above.")
    return {"gating_steps": passes - len(lost), "gating_of": passes}, downgraded


def gate_under(trials: list[Trial], policy: Policy) -> Iterable[tuple[str, int, int, dict]]:
    """Per step: how many trials still gate a release under `policy`, and at what rate.

    The rate is a distribution keyed the same way `StepScore.rates` is, so the
    two can sit in adjacent cells of one table without either being reformatted
    into the other's shape. A step is counted only in the trials where it
    compared something. Seven other functions in this file carry the same
    refusal, and this sentence said four for as long as there were four: nothing
    noticed the fifth, sixth, seventh or eighth arriving. `test_refusals.py`
    counts them now, which is `test_blanket_catches.py`'s argument one remove
    out.
    """
    ordered = dict.fromkeys(step.name for trial in trials for step in trial.reference)
    for name in ordered:
        gated = seen = 0
        rates: Counter = Counter()
        for trial in trials:
            step = next((s for s in trial.reference if s.name == name), None)
            if step is None or step.artifacts_compared == 0:
                continue
            seen += 1
            verdict = judge(step, policy)
            gated += verdict.fired > 0
            rates[f"{verdict.fired} of {step.measured_comparisons}"] += 1
        yield name, gated, seen, dict(rates)


def report_attribution(trials: list[Trial]) -> dict[str, int]:
    """Whether leave-one-out named the column the step invented, first, every time."""
    expected = {
        "customer_keys": "surrogate_id",
        "sparse_customer_keys": "surrogate_id",
        "apply_price_updates": "price_cents",
    }
    say("\nattribution: did leave-one-out name the right column first")
    right = seen = tied = 0
    for name, column in expected.items():
        hits = misses = ties = 0
        for trial in trials:
            step = next((s for s in trial.reference if s.name == name), None)
            worst = None if step is None else step.worst
            unstable = () if worst is None else worst.unstable_key_columns
            if not unstable:
                continue
            seen += 1
            # Where the two best columns leave the same count behind, nothing
            # in the counts chose between them and the sort key did. Both
            # descriptions of the divergence are true, and the tie-break prefers
            # the column the step invented, so scoring the answer against that
            # preference is scoring a sort key against itself.
            if len(unstable) > 1 and unstable[0].remaining == unstable[1].remaining:
                ties += 1
                tied += 1
            if unstable[0].column == column:
                right += 1
                hits += 1
            else:
                misses += 1
        if hits + misses:
            say(
                f"  {name:<22} {hits} right, {misses} wrong, out of {hits + misses} attributions, "
                f"{ties} decided by the tie-break"
            )
        else:
            say(f"  {name:<22} no attribution came out of any trial, so nothing to score here")
    if seen:
        say(f"  {right} of {seen} named the column the step invented rather than one it copied in.")
        say(f"  {tied} of those {seen} had two columns leaving the same count behind, so the")
        say("  counts chose nothing and the sort key chose, which is a preference this repo")
        say("  also asserts as a unit test. The rest is the arithmetic doing the work.")
    else:
        say("  Nothing was attributed in any trial, so there is no rate here rather than a zero.")
    say("  This is a rate over findings the oracle produced, so it cannot fall when the oracle")
    say("  stops finding anything: a detector that fires on nothing scores nothing here.")
    return {"attribution_right": right, "attribution_seen": seen, "attribution_tied": tied}


def report_conditions(
    trials: list[Trial],
    broken: dict[str, StepScore],
    twins: dict[str, StepScore],
    naive: dict[str, float],
    ablation: dict[str, float],
    amplification: dict[str, int],
    bounds: dict[str, float],
    seconds: float,
) -> list[tuple[str, str, str]]:
    """Every threshold the spec fixed before any code, with what it came out as.

    Each line is a failure condition, so the verdict is TRIGGERED or not. Three
    of them declare a piece of this project unnecessary if they fire, and they
    are printed either way, because a tool whose author cannot say what would
    have made it pointless has not tested the premise.

    The third verdict is NOT MEASURED and seven of the eight can reach it. A
    step that wrote no artifacts compares nothing, fires on none of the nothing
    it compared, and reads exactly like a step that agreed with itself: the
    naive baseline then has zero false positives, the amplification gap is two
    zero rates, and the uncontained pass removes no falsely divergent step.
    Those three are the conditions that call a piece of this project
    unnecessary, so the absence of a measurement published as a zero argues
    against the thing that was not measured. The one condition that cannot get
    here is the wall clock, which is measured whatever the pipeline did.
    """
    n = len(trials)
    budget = BUDGET_SECONDS_PER_TRIAL * n
    intermittent = only(broken, INTERMITTENT)
    downstream = only(broken, DOWNSTREAM)
    silent = amplification["loop_silent_trials"]
    # Condition 2 compares two rates, so both sides need a denominator. Either
    # of them at zero is no gap to see rather than a gap of zero.
    gap_seen = min(amplification["loop_comparisons"], amplification["amplifier_comparisons"])
    bounds_short = bounds["loose_cleared"] < bounds["step_passes"] if bounds else False
    sensitivity = [(name, only(broken, name)) for name in BROKEN]
    missed = [(name, step) for name, step in sensitivity if step.fired_in < step.trials]
    short = [(name, step) for name, step in sensitivity if step.trials < n]
    twin_fires = sum(only(twins, twin).fired_in for _, twin in PAIRS)
    twin_trials = sum(only(twins, twin).trials for _, twin in PAIRS)
    reached = intermittent.statuses.get(DIVERGENT, 0) + intermittent.statuses.get(
        STABLE_ON_THIS_INPUT, 0
    )

    naive_words = (
        f"it fired on {naive['benign_fired']:.0f} of {naive['benign_trials']:.0f} trials, median "
        f"{naive['benign_median']:,.0f} rows unmatched of the "
        f"{naive['benign_rows_compared']:,.0f} compared, on a step where nothing is wrong"
        if naive["benign_trials"]
        else f"{BENIGN} wrote no artifact for it to compare in any of the {n} trials"
    )
    gap_words = (
        f"per comparison the loop is {amplification['loop_rate']:.2f} over "
        f"{amplification['loop_comparisons']} and the best amplifier "
        f"{amplification['best_amplifier_rate']:.2f} over "
        f"{amplification['amplifier_comparisons']}; at the trial level the loop said nothing on "
        f"{silent} of the {amplification['loop_looked_trials']} trials that compared the step "
        f"and an amplifier fired on {amplification['amplifier_caught']} of those"
        if gap_seen
        else (
            f"the loop compared {amplification['loop_comparisons']} and the amplifiers "
            f"{amplification['amplifier_comparisons']}, so there is no pair of rates here"
        )
    )
    if gap_seen and amplification["loop_rate"] >= 1.0:
        # A maximum cannot exceed a saturated rate, so the condition is at its
        # ceiling and would trigger on a run where amplification worked
        # perfectly. That is a property of how it was written, and it was
        # written before any of this existed, so it prints rather than moves.
        gap_words += (
            "; the loop is at 1.00 and no amplifier can beat a rate that is already at its "
            "ceiling, so this condition triggers on saturation rather than on amplification"
        )
    containment_words = (
        f"{DOWNSTREAM} fires uncontained on {ablation['falsely_divergent_trials']:.0f} of "
        f"{ablation['downstream_trials']:.0f} trials that compared it, and contained on "
        f"{downstream.fired_in} of {downstream.trials}"
        if ablation["downstream_trials"]
        else f"{DOWNSTREAM} compared nothing uncontained in any of the {n} trials"
    )
    sensitivity_words = ", ".join(
        f"{name} {step.fired_in} of {step.trials}" for name, step in sensitivity
    )
    if short:
        sensitivity_words += "; nothing compared on " + ", ".join(
            f"{name} in {n - step.trials}" for name, step in short
        )
    twin_words = (
        f"{twin_fires} twin step-passes fired across {twin_trials} twin trials that compared "
        f"anything, out of {len(PAIRS) * n} attempted"
    )

    checked = [
        (
            "the naive baseline's false positives on correct code are zero, so the oracle is "
            "more machinery than the problem needs",
            _measured(naive["benign_trials"], naive["benign_fired"] == 0),
            naive_words,
        ),
        (
            "the amplification gap on the intermittent step is zero, so amplification is "
            "unmotivated on this evidence",
            _measured(
                gap_seen,
                amplification["best_amplifier_rate"] <= amplification["loop_rate"],
            ),
            gap_words,
        ),
        (
            f"observed drift is not {HEADROOM_REQUIRED:,.0f}x inside the computed bound",
            _measured(len(bounds), bounds_short),
            _bound_words(bounds),
        ),
        (
            "containment removes no falsely divergent step, so the Spot borrowing did not "
            "earn its place",
            _measured(ablation["downstream_trials"], ablation["falsely_divergent_trials"] == 0),
            containment_words,
        ),
        (
            f"sensitivity below {n} of {n} on the four broken steps",
            _observed(bool(missed), not short),
            sensitivity_words,
        ),
        (
            "any twin fired at all",
            _observed(bool(twin_fires), twin_trials == len(PAIRS) * n),
            twin_words,
        ),
        (
            f"the intermittent step reached neither {DIVERGENT} nor {STABLE_ON_THIS_INPUT} "
            f"in every trial",
            _observed(reached < intermittent.trials, intermittent.trials == n),
            f"{tally(intermittent.statuses)}, over {intermittent.trials} of {n} trials that "
            f"compared it",
        ),
        (
            f"the whole eval took longer than {budget / 60:.0f} minutes, which is the spec's "
            f"ten at the ten trials it fixed",
            _verdict(seconds > budget),
            f"{seconds:.0f}s over {n} trials, {seconds / max(n, 1):.0f}s each",
        ),
    ]
    say("\npre-registered conditions, every one written into the spec before any of this existed")
    say("  Each line is a failure condition. TRIGGERED is not a broken run, it is the answer,")
    say("  and three of these declare a part of this project unnecessary if they fire.")
    for condition, verdict, evidence in checked:
        say("")
        hang(f"{verdict:<13}", condition)
        hang(" " * 13, evidence)
    return checked


def _verdict(triggered: bool) -> str:
    return "TRIGGERED" if triggered else "not triggered"


def _measured(counted: int, triggered: bool) -> str:
    """The verdict, or NOT MEASURED where the count behind it came out at zero.

    For the four conditions whose TRIGGERED state is itself a zero. Nothing
    found and nothing looked at are the same number, and three of those four
    conclude that a piece of this project is unnecessary.
    """
    return NOT_MEASURED if not counted else _verdict(triggered)


def _observed(triggered: bool, complete: bool) -> str:
    """The verdict for the three conditions whose TRIGGERED state is a positive finding.

    A fire is evidence whatever else went unmeasured, so it wins over an
    incomplete sample. The other direction does not: 10 of 10 cannot be claimed
    off nine trials, so a short sample is NOT MEASURED rather than a pass.
    """
    if triggered:
        return _verdict(True)
    return _verdict(False) if complete else NOT_MEASURED


def _bound_words(bounds: dict[str, float]) -> str:
    if not bounds:
        return "no float step drifted, so the check had nothing to run on"
    return (
        f"cleared on {bounds['loose_cleared']:.0f} of {bounds['step_passes']:.0f} at n = rows "
        f"read, and {bounds['tight_cleared']:.0f} of {bounds['step_passes']:.0f} at the tight n"
    )


@dataclass(frozen=True)
class Figures:
    """One eval run, after every section has printed and before anything is written out.

    Two audiences share this object and they read different halves of it. The
    eight pre-registered conditions consult the scalars, which are the same
    plain dicts they have always been consulted through. The README's tables are
    generated from the per-step blocks beside them, which no condition looks at,
    so a table gaining a column cannot move a threshold.

    It also exists because the conditions test used to be a hand-copied replica
    of `main()`'s call sequence, and the copy went stale the first time a
    reporter had something more to hand back.
    """

    trials: list[Trial]
    steps: dict[str, StepScore]
    twins: dict[str, StepScore]
    uncontained: dict[str, StepScore]
    twin_comparisons: dict[str, int]
    twin_coverage: list[dict]
    naive: dict[str, float]
    static: dict[str, int]
    ablation: dict[str, float]
    amplification: dict[str, int]
    forced: list[dict]
    bounds: dict[str, float]
    drift: list[dict]
    policy_cost: dict[str, int]
    reduction_order: dict[str, dict]
    attribution: dict[str, int]

    def conditions(self, seconds: float) -> list[tuple[str, str, str]]:
        return report_conditions(
            self.trials,
            self.steps,
            self.twins,
            self.naive,
            self.ablation,
            self.amplification,
            self.bounds,
            seconds,
        )


def report_all(trials: list[Trial]) -> Figures:
    """Print every section, in the order the spec lists them, and keep what they measured."""
    broken: dict[str, StepScore] = {}
    twins: dict[str, StepScore] = {}
    for trial in trials:
        score(trial.reference, broken)
        score(trial.twins, twins)
    report_sensitivity(broken, len(trials))
    report_specificity(twins, len(trials))
    twin_comparisons, twin_coverage = report_twin_comparisons(trials)
    naive = report_baseline_one(trials, broken)
    static = report_baseline_two()
    ablation, uncontained = report_baseline_three(trials, broken)
    amplification, forced = report_baseline_four(trials, broken)
    bounds, drift = report_bounds(trials, Policy())
    policy_cost, reduction_order = report_policy_cost(trials)
    attribution = report_attribution(trials)
    return Figures(
        trials=trials,
        steps=broken,
        twins=twins,
        uncontained=uncontained,
        twin_comparisons=twin_comparisons,
        twin_coverage=twin_coverage,
        naive=naive,
        static=static,
        ablation=ablation,
        amplification=amplification,
        forced=forced,
        bounds=bounds,
        drift=drift,
        policy_cost=policy_cost,
        reduction_order=reduction_order,
        attribution=attribution,
    )


def as_artifact(
    figures: Figures,
    where: dict[str, str],
    runs: int,
    seconds: float,
    checked: list[tuple[str, str, str]],
) -> dict:
    """The whole run as one JSON document, which is what the README's tables are made of.

    Slice 7's argument is that a number nobody retypes is a number that cannot
    drift, and six corrections in this repository were all retyped figures. So
    the file has to carry everything a table needs and label what the figures
    are specific to: DuckDB pins the parallel behaviour, the thread count picks
    how the work is divided, and the platform decides both.

    `version` is here because `twicerun report` reads this back and a committed
    artifact outlives the shape it was written in. It refuses a number it does
    not know rather than generating a table out of a file it is guessing at.
    """
    return {
        "version": ARTIFACT_VERSION,
        "kind": EVAL,
        "generated": datetime.now(UTC).isoformat(timespec="seconds"),
        "trials": len(figures.trials),
        "runs": runs,
        "seconds": seconds,
        "seconds_per_trial": statistics.median(t.seconds for t in figures.trials),
        "environment": where,
        "steps": [
            {
                **step.figures(),
                "what": WHAT_EACH_STEP_IS.get(name, ""),
                "reduction_order": figures.reduction_order.get(name, {}),
                "uncontained": only(figures.uncontained, name).figures(),
            }
            for name, step in figures.steps.items()
        ],
        "twins": [step.figures() for step in figures.twins.values()],
        "twin_comparisons": figures.twin_comparisons,
        "twin_coverage": figures.twin_coverage,
        "baseline_1": figures.naive,
        "baseline_2": figures.static,
        "baseline_3": figures.ablation,
        "baseline_4": figures.amplification,
        "forced_amplifiers": figures.forced,
        "amplification_gap": [
            {name: asdict(a) for name, a in trial.forced.items()} for trial in figures.trials
        ],
        "bounds": figures.bounds,
        "drift": figures.drift,
        "policy_cost": figures.policy_cost,
        "attribution": figures.attribution,
        "conditions": [{"condition": c, "verdict": v, "evidence": e} for c, v, e in checked],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="eval.py",
        description="Ten full detection passes over the matched-pair reference set, "
        "four baselines, and every threshold the spec fixed before any code.",
    )
    parser.add_argument("--trials", type=int, default=10)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--into", type=Path, default=WORKSPACE)
    parser.add_argument("--json", type=Path, help="write the figures out as well as printing them")
    args = parser.parse_args(argv)
    if not claim_workspace(args.into):
        return 2

    began = time.perf_counter()
    where = environment()
    say("twicerun eval")
    for line in environment_lines(where, args.runs, args.trials):
        say(line)

    trials: list[Trial] = []
    for n in range(args.trials):
        trials.append(one_trial(args.into / f"trial-{n:02d}", args.runs))
        print(
            f"  trial {n + 1} of {args.trials}, {trials[-1].seconds:.0f}s",
            file=sys.stderr,
            flush=True,
        )
    shutil.rmtree(args.into)

    figures = report_all(trials)
    seconds = time.perf_counter() - began
    checked = figures.conditions(seconds)
    say(f"\n{args.trials} trials in {seconds:.0f}s, "
        f"median {statistics.median(t.seconds for t in trials):.0f}s each.")

    if args.json:
        args.json.write_text(
            json.dumps(as_artifact(figures, where, args.runs, seconds, checked), indent=2),
            encoding="utf-8",
        )
        say(f"figures written to {args.json}")

    # Nothing here exits non-zero on a triggered condition. A triggered
    # condition is a published result rather than a failed run, and a script
    # that failed on one would be a script with a reason to stop publishing it.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
