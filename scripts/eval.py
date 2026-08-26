#!/usr/bin/env python3
"""Ten full detection passes over the matched-pair reference set, four baselines, one table.

    uv run python scripts/eval.py               # the ten trials the spec fixed
    uv run python scripts/eval.py --trials 2    # a quicker read of the same shape
    uv run python scripts/eval.py --json out.json

Every threshold below was written into `specs/2026-08-26-rerun-determinism-spec.md`
before any of this project existed, including three that declare a part of it
unnecessary. They print with whatever they came out as, and a triggered
condition is a published result rather than a failed run, so this exits 0
either way. Which of them trigger moves between runs of the same code: the
README publishes four ten-trial runs and one of the four breached the spec's
sensitivity threshold.

A trial is three passes: the broken pipeline at the defaults, its matched twin at
the defaults, and the broken pipeline again with containment off. The first two
give sensitivity and specificity, the third is baseline 3. Baseline 1 rescores
the first pass's own runs 1 and 2 bit-exactly, so the only thing separating it
from the oracle is the comparison, and baseline 4 rescores the same measurements
with the amplifiers taken away, through the shipped exit-code function. Only
baseline 3 costs an extra pass.

Peak disk measured at 1,292 MB, which is one trial's three run directories alive
at once. Each trial's are removed before the next one starts, so that is a peak
rather than a total. The first estimate here was 800 MB and came from arithmetic
rather than from watching it, which is the same mistake the cost section of the
README already records once.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import shutil
import statistics
import sys
import textwrap
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path

import duckdb

from twicerun.amplify import AMPLIFIERS, DIVERGENT, STABLE_ON_THIS_INPUT
from twicerun.cli import exit_code
from twicerun.compare import compare as bit_exact
from twicerun.manifest import Manifest
from twicerun.measurement import StepMeasurement, name_classes
from twicerun.policy import HEADROOM_REQUIRED, Policy, judge
from twicerun.runner import amplify_runs, load_steps, run_pipeline, scored_runs

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

BUDGET_SECONDS = 600.0


# Two columns short of the 100 the linter allows, matching report.py, so a
# terminal at 100 does not add a wrap of its own on top of this one.
WIDTH = 98


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
    naive: dict[str, tuple[int, int]]
    # Each amplifier's rate on the intermittent step, taken every trial rather
    # than only on the trials where the loop came back quiet.
    forced: dict[str, tuple[int, int]]
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
    # The denominator every rate here is out of, taken from the first step
    # observed rather than assumed, because `--runs` moves it.
    comparisons: int = 0

    def observe(self, step: StepMeasurement) -> None:
        if step.artifacts_compared == 0:
            # The guard the main loop, the bisect and the amplifiers all carry,
            # arriving in the fourth loop. `any([])` is False, so a step that
            # wrote nothing scores 0 of 4 out of four comparisons of nothing and
            # reads exactly like four clean ones. A trial like that is not
            # evidence about the step and is counted where it can be seen.
            self.silent_trials += 1
            return
        self.comparisons = step.comparisons
        self.rates[f"{step.fired} of {step.comparisons}"] += 1
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
    def fired_in(self) -> int:
        return sum(n for rate, n in self.rates.items() if not rate.startswith("0 "))

    def distribution(self) -> str:
        """Every possible rate with its count, highest first, zeros included.

        Ordered by the rate rather than by the count so the shape is readable
        down a column and two steps compare line to line. `_render` is used
        everywhere else here and orders by count, which is right for a set of
        labels and wrong for a small integer support.

        A step that gave the same rate every trial collapses to `0 of 4 on all
        10`, which omits nothing: naming the trial count accounts for every
        trial, so there is no bucket left for a reader to wonder about. Spelling
        five buckets out to say a twin never fired put four x0 cells on every
        line of the specificity table and buried the one number in it.
        """
        if not self.rates:
            return "nothing compared"
        if len(self.rates) == 1:
            only, times = next(iter(self.rates.items()))
            return f"{only} on all {times}"
        return ", ".join(
            f"{k} of {self.comparisons} x{self.rates[f'{k} of {self.comparisons}']}"
            for k in range(self.comparisons, -1, -1)
        )

    def detail(self) -> str:
        """Class, cause, the threads=1 rate, and how far the step moved.

        The row counts print against the hard maximum they came out of, because
        a step comparing 500,000 rows cannot lose more than 500,000 of them and
        a ceiling cannot be beaten. The ulp and relative figures print as a
        median with an n and nothing else.
        """
        parts = []
        if self.classes:
            parts.append(_render(self.classes))
        if self.causes:
            parts.append(f"cause {_render(self.causes)}")
        if self.single_threaded:
            parts.append(f"threads=1 {_render(self.single_threaded)}")
        if any(self.unmatched):
            parts.append(
                f"median {statistics.median(self.unmatched):,.0f} of the "
                f"{statistics.median(self.of_rows):,.0f} reference rows found no partner"
            )
        if any(self.extra) and not any(self.unmatched):
            parts.append(
                f"median {statistics.median(self.extra):,.0f} extra rows against "
                f"{statistics.median(self.of_rows):,.0f} reference rows"
            )
        if self.relative:
            parts.append(
                f"median {statistics.median(self.ulps):g} ulp and "
                f"{statistics.median(self.relative):.1e} relative, n={len(self.relative)}"
            )
        return ", ".join(parts)


def score(steps: list[StepMeasurement], into: dict[str, StepScore]) -> None:
    for step in steps:
        into.setdefault(step.name, StepScore(step.name)).observe(step)


def naive_pass(manifest: Manifest) -> dict[str, tuple[int, int]]:
    """Baseline 1: runs 1 and 2 of the same trial, compared as multisets of row hashes.

    Two runs, no tolerance, no classification, no repetition. What almost anyone
    would write first, and what the first version of this project's spec
    proposed. Pointed at the trial's own first two runs rather than at two fresh
    ones, so the only difference between this and the oracle's answer is the
    comparison: same bytes, same containment, same everything else.

    Per step it returns the rows one side had that the other did not, and the
    reference row count they came out of.
    """
    con = duckdb.connect()
    found: dict[str, tuple[int, int]] = {}
    try:
        reference, second = manifest.runs[0], manifest.runs[1]
        for ref_step, cand_step in zip(reference.steps, second.steps, strict=True):
            written = {a.name: a for a in cand_step.artifacts}
            unmatched = rows = 0
            for artifact in ref_step.artifacts:
                later = written.get(artifact.name)
                if later is None:
                    unmatched += artifact.rows
                    rows += artifact.rows
                    continue
                diff = bit_exact(con, artifact, later)
                unmatched += diff.only_in_reference + diff.only_in_candidate
                rows += artifact.rows
            found[ref_step.name] = (unmatched, rows)
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
) -> dict[str, tuple[int, int]]:
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
    """
    rates: dict[str, tuple[int, int]] = {}
    for entry in amplify_runs(
        load_steps(REFERENCE),
        manifest.runs[0],
        [index],
        Path(manifest.root),
        runs,
        manifest.environment.threads,
        {},
    ):
        rates[entry.amplifier] = (scored_runs(entry.runs, {})[0], max(len(entry.runs) - 1, 0))
    return rates


def one_trial(into: Path, runs: int) -> Trial:
    began = time.perf_counter()
    broken, manifest = run_pipeline(REFERENCE, runs=runs, parent=into / "reference", keep=1)
    naive = naive_pass(manifest)
    index = next(s.index for s in broken.steps if s.name == INTERMITTENT)
    amplified = forced_amplification(manifest, runs, index)
    twinned, _ = run_pipeline(TWINS, runs=runs, parent=into / "twins", keep=1)
    ablated, _ = run_pipeline(
        REFERENCE, runs=runs, parent=into / "uncontained", keep=1, contained=False
    )
    quiet = replace(broken, steps=without_amplifiers(broken.steps), amplified=False)
    trial = Trial(
        reference=broken.steps,
        twins=twinned.steps,
        uncontained=ablated.steps,
        naive=naive,
        forced=amplified,
        exit_with_amplifiers=exit_code(broken),
        exit_without=exit_code(quiet),
        seconds=time.perf_counter() - began,
    )
    shutil.rmtree(into, ignore_errors=True)
    return trial


def environment_lines(runs: int, trials: int) -> list[str]:
    probe = duckdb.connect()
    threads = probe.execute("SELECT current_setting('threads')").fetchone()[0]
    probe.close()
    import platform as _platform

    return [
        f"  reference  {REFERENCE.relative_to(HERE)}",
        f"  twins      {TWINS.relative_to(HERE)}",
        f"  trials     {trials}, each a full detection pass, a twin pass and an "
        f"uncontained pass",
        f"  runs       {runs} per pass, so {runs - 1} comparisons per step per pass",
        f"  duckdb     {duckdb.__version__}, threads={threads}",
        f"  platform   {_platform.platform()}",
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
    say("  A fire rate is the whole distribution rather than a range: out of four it has five")
    say("  possible values, so the cell holds all of it. The magnitudes are maxima over a")
    say("  thousand groups, so they carry a median and an n and no bracket at all.")


def report_specificity(scored: dict[str, StepScore], trials: int) -> None:
    say("\nspecificity: the matched twins, one line of difference each, none of which may fire")
    width = max(len(twin) for _, twin in PAIRS)
    fires = 0
    for _, twin in PAIRS:
        step = scored.get(twin)
        if step is None:
            continue
        hang(
            f"{twin:<{width}}",
            f"{step.fired_in} of {trials} trials fired   {step.distribution()}   "
            f"{', '.join(f'{s} x{n}' for s, n in step.statuses.most_common())}",
        )
        fires += step.fired_in
    say(f"  {fires} twin step-passes fired across {trials} trials.")


def report_twin_comparisons(trials: list[Trial]) -> tuple[int, int]:
    """How many comparisons the twins actually survived, counted rather than assumed.

    The spec predicted 4 main-loop plus 6 amplifier comparisons per twin per
    trial. Tie collapse declines more often than it applies, so the real
    amplified count is lower and printing the prediction would overstate the
    evidence by about a third.
    """
    loop = sum(step.comparisons for trial in trials for step in trial.twins)
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
    return loop, amplified


def report_baseline_one(trials: list[Trial], scored: dict[str, StepScore]) -> dict[str, float]:
    say("\nbaseline 1: two runs, bit-exact multiset equality, no tolerance and no classes")
    say("  Runs 1 and 2 of each trial, rescored. Same bytes as the oracle saw.")
    benign = [trial.naive.get(BENIGN, (0, 0)) for trial in trials]
    fired = sum(1 for unmatched, _ in benign if unmatched)
    counts = [unmatched for unmatched, _ in benign]
    rows = benign[0][1] if benign else 0
    hang(
        f"{BENIGN:<22}",
        f"fired on {fired} of {len(trials)} trials, median "
        f"{statistics.median(counts):,.0f} rows unmatched out of {rows:,} on each side",
    )
    say("  Nothing is wrong with that step. It is a correct float average and every one of")
    say("  those findings is the arithmetic behaving normally.")

    missed = [trial.naive.get(INTERMITTENT, (0, 0))[0] for trial in trials]
    hang(
        f"{INTERMITTENT:<22}",
        f"fired on {sum(1 for n in missed if n)} of {len(trials)} trials at one comparison, "
        f"against the five-run loop's {scored[INTERMITTENT].fired_in} of {len(trials)}",
    )
    return {"benign_median": statistics.median(counts) if counts else 0.0, "benign_fired": fired}


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


def report_baseline_three(trials: list[Trial], scored: dict[str, StepScore]) -> dict[str, float]:
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
    say(
        f"  {DOWNSTREAM} is the one with no bug of its own. Uncontained it fires on "
        f"{falsely} of {len(trials)}"
    )
    say(
        f"  trials and is given cause {_render(ablated[DOWNSTREAM].causes) or 'none'}, which is a "
        f"confident wrong diagnosis:"
    )
    say("  it computes an integer minimum, and an integer minimum cannot reassociate into a")
    say("  different answer. A fire there means it was fed something different, never that it")
    say("  computed something different. The bisect gives it that label because a step cannot")
    say("  be re-executed alone without run 1's artifacts to read, so the ablation ablates the")
    say("  main loop and not the bisect.")
    say("  The other steps move too, and that is the intermittent pair rather than containment.")

    overstated = _append_magnitudes(trial.uncontained for trial in trials)
    true = _append_magnitudes(trial.reference for trial in trials)
    say(f"\n  append_audit_log extra rows, uncontained  {_render(overstated)}")
    say(f"  {'':<41}contained  {_render(true)}")
    say("  Four reruns' worth of duplication charged to one step, against what one rerun of it")
    say("  does. The fire rate is 4 of 4 either way, so what containment bought there is the")
    say("  magnitude being a fact about the step rather than about how many times the tool ran.")
    return {
        "falsely_divergent_trials": falsely,
        "overstated": max((int(k.replace(",", "")) for k in overstated), default=0),
        "contained_magnitude": max((int(k.replace(",", "")) for k in true), default=0),
    }


def _append_magnitudes(passes: Iterable[list[StepMeasurement]]) -> Counter:
    """How many extra rows the append bug produced, per pass, as a distribution.

    The count is on the later side. An append with no key loses nothing from the
    reference run and gains rows in every run after it, so the reference-side
    figure is zero and reading it would report the loudest bug in the pipeline
    as a magnitude of nothing.
    """
    seen = Counter()
    for steps in passes:
        step = next((s for s in steps if s.name == "append_audit_log"), None)
        worst = None if step is None else step.worst
        if worst is not None:
            seen[f"{worst.unmatched_candidate:,}"] += 1
    return seen


def report_baseline_four(trials: list[Trial], scored: dict[str, StepScore]) -> dict[str, int]:
    say("\nbaseline 4: no amplification, scored through the tool's own exit-code function")
    say("  The main loop is identical code either way, so this is the flag rather than a")
    say("  model of it: the same measurements with the amplifiers taken away.")
    went_green = [t for t in trials if t.exit_without == 0 and t.exit_with_amplifiers != 0]
    codes = Counter(f"{t.exit_with_amplifiers} to {t.exit_without}" for t in trials)
    say(f"  exit code with amplifiers to without   {_render(codes)}")
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
    silent = [t for t in trials if _rate(t.reference, INTERMITTENT) == 0]
    caught = [t for t in silent if _status(t.reference, INTERMITTENT) == STABLE_ON_THIS_INPUT]
    stable = scored[INTERMITTENT].statuses.get(STABLE_ON_THIS_INPUT, 0)
    say(
        f"\n  The five-run loop reported nothing on {INTERMITTENT} in {len(silent)} of "
        f"{len(trials)} trials, and an amplifier"
    )
    say(f"  fired on {len(caught)} of those {len(silent)}. That is the gap at the trial level.")

    say(f"\n  Per comparison on {INTERMITTENT}, with the amplifiers pointed at it every trial:")
    loop_fired = sum(_rate(t.reference, INTERMITTENT) for t in trials)
    loop_of = sum(
        s.comparisons for t in trials for s in t.reference if s.name == INTERMITTENT
    )
    rows = [("the five-run loop", loop_fired, loop_of, len(silent))]
    for amplifier, _ in AMPLIFIERS:
        seen = [t.forced[amplifier] for t in trials if amplifier in t.forced]
        rows.append(
            (
                amplifier,
                sum(fired for fired, _ in seen),
                sum(of for _, of in seen),
                sum(1 for fired, _ in seen if not fired),
            )
        )
    width = max(len(name) for name, *_ in rows)
    for name, fired, of, blank in rows:
        rate = f"{fired} of {of}" if of else "nothing compared"
        say(
            f"    {name:<{width}}  {rate:>12}  {fired / max(of, 1):>5.2f}   "
            f"{blank} of {len(trials)} trials with nothing at all"
        )
    say("  A row with fewer comparisons than the others found nothing in its first two:")
    say("  an amplifier is only escalated to the full run count on a hit, which is the cost")
    say("  argument for it working.")
    amplified_gap = max(
        (fired / max(of, 1) for name, fired, of, _ in rows[1:] if of), default=0.0
    )
    return {
        "false_negative_trials": len(went_green),
        "stable_trials": stable,
        "loop_silent_trials": len(silent),
        "amplifier_caught": len(caught),
        "loop_rate": loop_fired / max(loop_of, 1),
        "best_amplifier_rate": amplified_gap,
    }


def _rate(steps: list[StepMeasurement], name: str) -> int:
    step = next((s for s in steps if s.name == name), None)
    return 0 if step is None else step.fired


def _status(steps: list[StepMeasurement], name: str) -> str | None:
    step = next((s for s in steps if s.name == name), None)
    return None if step is None else step.status


def report_bounds(trials: list[Trial], policy: Policy) -> dict[str, float]:
    say("\nthe pre-registered 1000x headroom check, at both choices of n")
    loose_clear = tight_clear = passes = 0
    loose, tight = [], []
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
    if not passes:
        say("  no float step drifted in any trial, so there was nothing to check")
        return {}
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
    return {
        "loose_cleared": loose_clear,
        "tight_cleared": tight_clear,
        "step_passes": passes,
        "loose_median": statistics.median(loose),
        "tight_median": statistics.median(tight),
    }


def report_attribution(trials: list[Trial]) -> dict[str, int]:
    """Whether leave-one-out named the column the step invented, first, every time."""
    expected = {
        "customer_keys": "surrogate_id",
        "sparse_customer_keys": "surrogate_id",
        "apply_price_updates": "price_cents",
    }
    say("\nattribution: did leave-one-out name the right column first")
    right = seen = 0
    for name, column in expected.items():
        hits = misses = 0
        for trial in trials:
            step = next((s for s in trial.reference if s.name == name), None)
            worst = None if step is None else step.worst
            unstable = () if worst is None else worst.unstable_key_columns
            if not unstable:
                continue
            seen += 1
            if unstable[0].column == column:
                right += 1
                hits += 1
            else:
                misses += 1
        say(f"  {name:<22} {hits} right, {misses} wrong, out of {hits + misses} attributions")
    say(f"  {right} of {seen} named the column the step invented rather than one it copied in.")
    return {"attribution_right": right, "attribution_seen": seen}


def report_conditions(
    trials: list[Trial],
    scored: dict[str, StepScore],
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

    The third verdict is NOT MEASURED and it exists for one condition. The
    amplification gap can only be seen on trials where the plain loop said
    nothing, since amplification deliberately only touches steps that came back
    quiet. On a run where the loop caught the step every time there is no gap to
    observe, and calling that a zero gap would report the opposite of what
    happened.
    """
    n = len(trials)
    silent = amplification["loop_silent_trials"]
    checked = [
        (
            "the naive baseline's false positives on correct code are zero, so the oracle is "
            "more machinery than the problem needs",
            _verdict(naive["benign_fired"] == 0),
            f"it fired on {naive['benign_fired']:.0f} of {n} trials, median "
            f"{naive['benign_median']:,.0f} rows unmatched on a step where nothing is wrong",
        ),
        (
            "the amplification gap on the intermittent step is zero, so amplification is "
            "unmotivated on this evidence",
            _verdict(amplification["best_amplifier_rate"] <= amplification["loop_rate"]),
            f"per comparison the loop is {amplification['loop_rate']:.2f} and the best "
            f"amplifier {amplification['best_amplifier_rate']:.2f}; at the trial level the "
            f"loop said nothing on {silent} of {n} and an amplifier fired on "
            f"{amplification['amplifier_caught']} of those",
        ),
        (
            f"observed drift is not {HEADROOM_REQUIRED:,.0f}x inside the computed bound",
            "NOT MEASURED" if not bounds else _verdict(
                bounds["loose_cleared"] < bounds["step_passes"]
            ),
            _bound_words(bounds),
        ),
        (
            "containment removes no falsely divergent step, so the Spot borrowing did not "
            "earn its place",
            _verdict(ablation["falsely_divergent_trials"] == 0),
            f"{DOWNSTREAM} fires uncontained on "
            f"{ablation['falsely_divergent_trials']:.0f} of {n} trials and never contained",
        ),
        (
            f"sensitivity below {n} of {n} on the four broken steps",
            _verdict(any(scored[name].fired_in < n for name in BROKEN)),
            ", ".join(f"{name} {scored[name].fired_in} of {n}" for name in BROKEN),
        ),
        (
            "any twin fired at all",
            _verdict(any(scored[twin].fired_in for _, twin in PAIRS if twin in scored)),
            f"{sum(scored[twin].fired_in for _, twin in PAIRS if twin in scored)} twin "
            f"step-passes fired",
        ),
        (
            f"the intermittent step reached neither {DIVERGENT} nor {STABLE_ON_THIS_INPUT} "
            f"in every trial",
            _verdict(
                scored[INTERMITTENT].statuses.get(DIVERGENT, 0)
                + scored[INTERMITTENT].statuses.get(STABLE_ON_THIS_INPUT, 0)
                < n
            ),
            _render(scored[INTERMITTENT].statuses),
        ),
        (
            f"the whole eval took longer than {BUDGET_SECONDS / 60:.0f} minutes",
            _verdict(seconds > BUDGET_SECONDS),
            f"{seconds:.0f}s over {n} trials",
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


def _bound_words(bounds: dict[str, float]) -> str:
    if not bounds:
        return "no float step drifted, so the check had nothing to run on"
    return (
        f"cleared on {bounds['loose_cleared']:.0f} of {bounds['step_passes']:.0f} at n = rows "
        f"read, and {bounds['tight_cleared']:.0f} of {bounds['step_passes']:.0f} at the tight n"
    )


def _render(counted: Counter) -> str:
    if not counted:
        return "nothing seen"
    return ", ".join(f"{k} x{n}" if n > 1 else f"{k}" for k, n in counted.most_common())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="eval.py",
        description="Ten full detection passes over the matched-pair reference set, "
        "four baselines, and every threshold the spec fixed before any code.",
    )
    parser.add_argument("--trials", type=int, default=10)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--into", type=Path, default=Path(".twicerun-eval"))
    parser.add_argument("--json", type=Path, help="write the figures out as well as printing them")
    args = parser.parse_args(argv)

    began = time.perf_counter()
    say("twicerun eval")
    for line in environment_lines(args.runs, args.trials):
        say(line)

    trials: list[Trial] = []
    for n in range(args.trials):
        trials.append(one_trial(args.into / f"trial-{n:02d}", args.runs))
        print(
            f"  trial {n + 1} of {args.trials}, {trials[-1].seconds:.0f}s",
            file=sys.stderr,
            flush=True,
        )
    shutil.rmtree(args.into, ignore_errors=True)

    broken_scores: dict[str, StepScore] = {}
    twin_scores: dict[str, StepScore] = {}
    for trial in trials:
        score(trial.reference, broken_scores)
        score(trial.twins, twin_scores)
    scored = {**twin_scores, **broken_scores}

    report_sensitivity(broken_scores, len(trials))
    report_specificity(twin_scores, len(trials))
    report_twin_comparisons(trials)
    naive = report_baseline_one(trials, broken_scores)
    static = report_baseline_two()
    ablation = report_baseline_three(trials, broken_scores)
    amplification = report_baseline_four(trials, broken_scores)
    bounds = report_bounds(trials, Policy())
    attribution = report_attribution(trials)

    seconds = time.perf_counter() - began
    checked = report_conditions(
        trials, scored, naive, ablation, amplification, bounds, seconds
    )
    say(f"\n{args.trials} trials in {seconds:.0f}s, "
        f"median {statistics.median(t.seconds for t in trials):.0f}s each.")

    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "trials": args.trials,
                    "runs": args.runs,
                    "duckdb": duckdb.__version__,
                    "seconds": seconds,
                    "sensitivity": {
                        name: dict(step.rates) for name, step in broken_scores.items()
                    },
                    "statuses": {
                        name: dict(step.statuses) for name, step in broken_scores.items()
                    },
                    "specificity": {name: step.fired_in for name, step in twin_scores.items()},
                    "baseline_1": naive,
                    "baseline_2": static,
                    "baseline_3": ablation,
                    "baseline_4": amplification,
                    "amplification_gap": [trial.forced for trial in trials],
                    "bounds": bounds,
                    "attribution": attribution,
                    "conditions": [
                        {"condition": c, "verdict": v, "evidence": e} for c, v, e in checked
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        say(f"figures written to {args.json}")

    # Nothing here exits non-zero on a triggered condition. A triggered
    # condition is a published result rather than a failed run, and a script
    # that failed on one would be a script with a reason to stop publishing it.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
