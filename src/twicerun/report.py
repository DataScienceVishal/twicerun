"""What the run command prints, and the boundary between measuring and judging.

`StepMeasurement` holds everything the oracle found and nothing about whether
any of it is acceptable. That separation is the point rather than tidiness: the
numbers in a report must not move when the policy does, or the tolerance is
deciding the measurement and every figure underneath it is negotiable. Run the
same pipeline under --policy strict and --policy reduction-order and the ulp
counts, the relative magnitudes and the row counts are identical. Only the
verdict changes.

Two rules the format follows. Every header carries the DuckDB version and the
thread count, because every number underneath is specific to both and a table
missing them is not reproducible. And a step that never diverged prints `0 of
4`, never a word like stable or deterministic: four clean comparisons rule out
very little and saying otherwise would be the exact mistake this tool exists to
catch.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field

from twicerun.amplify import (
    AMPLIFICATION_FAILED,
    NO_DIVERGENCE_OBSERVED,
    NOT_VARIED,
    PROBE_RUNS,
    STABLE_ON_THIS_INPUT,
    Amplification,
)
from twicerun.cause import (
    BISECT_THREADS,
    CONFIDENCE,
    PARALLEL_ORDER,
    PERSISTS_SINGLE_THREADED,
    upper_bound,
)
from twicerun.manifest import Environment
from twicerun.measurement import StepMeasurement, name_classes
from twicerun.oracle import ArtifactFindings, KeyEffect
from twicerun.policy import HEADROOM_REQUIRED, DriftBound, Policy, StepVerdict, judge

# Two columns short of the 100 the linter allows, so a terminal at 100 does not
# add a wrap of its own on top of this one.
WIDTH = 98


def _plural(n: int, singular: str, plural: str | None = None) -> str:
    return f"{n} {singular}" if n == 1 else f"{n} {plural or singular + 's'}"


def _wrap(body: str, indent: str = "") -> list[str]:
    """Fill a sentence that has step names and amplifier names in it.

    The fixed prose elsewhere in this file is wrapped by hand, which is fine
    while the words never change. These sentences carry data, so a step called
    `apply_price_updates` moves every line break after it and hand-wrapping
    would be wrong the first time a pipeline used longer names.
    """
    return textwrap.wrap(body, width=WIDTH, initial_indent=indent, subsequent_indent=indent)


@dataclass
class Report:
    pipeline: str
    run_dir: str
    runs: int
    environment: Environment
    steps: list[StepMeasurement]
    # Both None when a saved run is being judged, because judging executes
    # nothing and deletes nothing. Printing the run command's words over
    # figures that are not true of the run is worse than printing neither: the
    # seconds were out by about 8x and the retention read as "keeping
    # everything", which is also how 0 is spelled for the flag.
    seconds: float | None
    policy: Policy = field(default_factory=Policy)
    contained: bool = True
    amplified: bool = True
    bisect_error: str | None = None
    pruned: int = 0
    live: int = 0
    keep: int | None = None

    @property
    def verdicts(self) -> list[StepVerdict]:
        return [judge(step, self.policy) for step in self.steps]

    def render(self) -> str:
        judged = self.verdicts
        sections = [
            self._header(),
            self._steps(judged),
            self._causes(),
            self._amplification(),
            self._statuses(),
            self._bounds(judged),
            self._footer(judged),
        ]
        return "\n".join(line for section in sections for line in section)

    def _header(self) -> list[str]:
        env = self.environment
        return [
            f"pipeline   {self.pipeline}",
            f"runs       {self.runs}, run 1 is the reference, so "
            f"{_plural(self.runs - 1, 'comparison')} per step",
            f"contained  {self._containment()}",
            f"policy     {self.policy.describe()}",
            f"duckdb     {env.duckdb_version}, threads={env.threads}",
            f"platform   {env.platform}",
            f"artifacts  {self.run_dir}",
            *self._retention(),
            "",
        ]

    def _retention(self) -> list[str]:
        if self.keep is None:
            return []
        kept = (
            "everything"
            if self.keep == 0
            else _plural(self.keep, "run directory", "run directories")
        )
        line = f"retention  keeping {kept}" + (f", dropped {self.pruned}" if self.pruned else "")
        if not self.live:
            return [line]
        # Retention is per invocation while another one is running, because a
        # live directory is never a deletion candidate. Saying "keeping 1"
        # over three directories on disk would be the tool misreporting itself.
        return [
            f"{line}, and left {_plural(self.live, 'directory', 'directories')} alone that "
            f"another invocation is still writing"
        ]

    def _containment(self) -> str:
        if self.contained:
            return (
                f"on, so runs 2 to {self.runs} read run 1's artifacts and a divergence at "
                f"one step cannot reach the next"
            )
        return (
            "off (--no-containment), so every run reads what it wrote itself and one "
            "divergence can be counted again by every step below it"
        )

    def _steps(self, verdicts: list[StepVerdict]) -> list[str]:
        width = max((len(f"{s.index} {s.name}") for s in self.steps), default=10)
        lines = []
        for verdict in verdicts:
            step = verdict.step
            label = f"{step.index} {step.name}".ljust(width)
            rate = f"{verdict.fired} of {step.comparisons}"
            # Two spaces between the groups, because `MULTIPLICITY
            # PERSISTS_SINGLE_THREADED` under one space reads as two classes,
            # and the cause is a different axis entirely.
            tail = "  ".join(t for t in (step.status or "", name_classes(step.classes)) if t)
            if step.cause:
                tail = f"{tail}  cause {step.cause}"
            if verdict.tolerated:
                tail = (
                    f"{tail}  TOLERATED on {verdict.tolerated} of {step.comparisons} "
                    f"by {' and '.join(verdict.routes)}"
                )
            lines.append(f"  {label}  {rate:>8}  {tail}".rstrip())
            if step.artifacts_compared == 0:
                lines.append("      wrote no artifacts, so nothing was compared")
                continue
            worst = step.worst
            if worst is not None:
                lines.append(f"      {worst.describe()}")
                lines.extend(f"      {line}" for line in _matched_on(worst))
                lines.extend(f"      {line}" for line in _attribution(worst))
            lines.extend(f"      {line}" for line in _magnitudes(step))
            lines.extend(f"      {hint}" for hint in step.hints)
            lines.extend(f"      {line}" for line in _fallbacks(step))
            if verdict.bound_refusal:
                lines.append(f"      {verdict.bound_refusal}")
        return lines

    def _causes(self) -> list[str]:
        """The two matched fire rates per divergent step, then what each label is worth.

        The rows come first and the prose after them. It used to sit between
        the heading and the rows, which put five lines of caveat in front of
        the thing a reader opened the section for.

        Each label explains itself only when it appears. PARALLEL_ORDER is
        guessable from the two rates beside it; PERSISTS_SINGLE_THREADED is
        not, and the sentence that makes it mean anything was in the README and
        never in the terminal, which is where a stranger meets it.
        """
        bisected = [s for s in self.steps if s.fired and s.bisect is not None]
        if self.bisect_error:
            return self._bisect_failed()
        if not bisected:
            return []
        width = max(len(f"{s.index} {s.name}") for s in bisected)
        rows = []
        for step in bisected:
            label = f"  {f'{step.index} {step.name}'.ljust(width)}  "
            if step.cause is None:
                rows.append(
                    f"{label}{'no rate':<24}  re-executed and wrote no artifacts, so there "
                    f"was nothing to compare"
                )
                continue
            rows.append(
                f"{label}{step.cause:<24}  "
                f"{step.fired} of {step.comparisons} at threads={self.environment.threads}, "
                f"{step.bisect.fired} of {step.bisect.comparisons} at "
                f"threads={step.bisect.threads}"
            )
        return [
            "",
            f"cause, from re-executing each divergent step {self.runs} times at "
            f"threads={BISECT_THREADS}:",
            *rows,
            "",
            *self._what_the_labels_mean(bisected),
        ]

    def _bisect_failed(self) -> list[str]:
        # The error prints as it was raised, wrapping and all, because an
        # edited error message is a worse thing to hand someone than an untidy
        # one.
        raised = [f"  {line}" for line in (self.bisect_error or "").splitlines()]
        return [
            "",
            "cause: the bisect did not run, so no step has one.",
            *raised,
            f"  The {_plural(self.runs, 'run')} above completed and their comparison is "
            f"unaffected. A step often cannot be",
            "  re-executed on its own because a step the bisect skips created the table it "
            "reads through",
            "  ctx.sql, which no artifact holds.",
        ]

    def _what_the_labels_mean(self, bisected: list[StepMeasurement]) -> list[str]:
        shown = {step.cause for step in bisected}
        comparisons = bisected[0].bisect.comparisons
        lines = [
            f"  Both rates are out of {comparisons}, which is what lets them be read against "
            f"each other."
        ]
        if PARALLEL_ORDER in shown:
            lines += [
                f"  {PARALLEL_ORDER} means the step stopped diverging with one thread. That is "
                f"{_plural(comparisons, 'clean comparison')} and no",
                f"  more than that: the {CONFIDENCE * 100:.0f} percent one-sided upper bound it "
                f"leaves on the per-comparison rate is",
                f"  {upper_bound(comparisons) * 100:.0f} percent. The bound also assumes an "
                f"independence these runs do not have, since they",
                "  share a process, a page cache and a machine.",
            ]
        if PERSISTS_SINGLE_THREADED in shown:
            lines += [
                f"  {PERSISTS_SINGLE_THREADED} means the thread count is not the explanation. "
                f"The tool stops there rather",
                "  than guessing between a clock read, a data-dependent branch, appended state "
                "and something",
                "  outside the pipeline.",
            ]
        if not self.contained:
            lines += [
                "  Containment is off for the loop above but not here: a step cannot be "
                "re-executed on its own",
                "  without run 1's artifacts to read, so a step that only inherited a divergence "
                f"can read as {PARALLEL_ORDER}.",
            ]
        return lines

    def _amplification(self) -> list[str]:
        """What each amplifier did to each step that never fired, and to what effect.

        Every attempted pair prints, including the pairs where the amplifier
        declined. An amplifier with no input to substitute is an axis that was
        not varied, and leaving it out would turn this into a list of clean
        results with the caveats deleted.
        """
        if not self.amplified:
            return self._amplification_off()
        covered = [step for step in self.steps if step.amplifications]
        if not covered:
            return []
        rows = []
        for step in covered:
            rows.append(f"  {step.index} {step.name}")
            rows += [f"      {_attempt_line(a)}".rstrip() for a in step.amplifications]
        return [
            "",
            f"amplification, {PROBE_RUNS} runs per amplifier on the "
            f"{_plural(len(covered), 'step')} that never fired:",
            *rows,
            "",
            *_wrap(
                "Tie collapse leaves each artifact's most distinct column alone, so a correct "
                "tiebreak survives it, and every value it substitutes is another real value from "
                "the same column."
                + (
                    f" Anything that fires here is re-run at {self.runs} runs so its rate can be "
                    f"read against the one above it."
                    if self.runs > PROBE_RUNS
                    else ""
                ),
                indent="  ",
            ),
        ]

    def _amplification_off(self) -> list[str]:
        """What a zero above is worth without amplification, said rather than left out.

        A flag that switches off a check has to switch off the claim the check
        supported, or the report keeps the confident half and drops the
        evidence. None of the four statuses prints under --no-amplify: three of
        them are defined by what an amplifier did, and the fourth,
        NO_DIVERGENCE_OBSERVED, needs a zero under every amplifier.
        """
        quiet = [s for s in self.steps if not s.fired and s.artifacts_compared]
        if not quiet:
            return []
        comparisons = self.runs - 1
        return [
            "",
            "amplification is off (--no-amplify), so no status is printed for the "
            f"{_plural(len(quiet), 'step')} that never fired:",
            f"  {_named(quiet)}",
            *_wrap(
                f"Each of those is {_plural(comparisons, 'comparison')} on the one input this "
                f"pipeline was given and nothing else. {_bound_words(comparisons)} "
                f"{NO_DIVERGENCE_OBSERVED} needs a zero under every amplifier as well, so it is "
                f"not claimed, and neither is anything weaker.",
                indent="  ",
            ),
        ]

    def _statuses(self) -> list[str]:
        """The three statuses that need a sentence, each printed only where it appears.

        DIVERGENT does not need one: the class, the cause and the rate on its
        own line say what it means. The other three are claims about something
        that was not seen, and a claim about something not seen is worth exactly
        the list of things that were tried.
        """
        by_status: dict[str, list[StepMeasurement]] = {}
        for step in self.steps:
            if step.status is not None:
                by_status.setdefault(step.status, []).append(step)

        lines = []
        for status, explain in (
            (STABLE_ON_THIS_INPUT, self._stable_on_this_input),
            (AMPLIFICATION_FAILED, self._amplification_failed),
            (NO_DIVERGENCE_OBSERVED, self._no_divergence_observed),
        ):
            here = by_status.get(status)
            if here:
                lines += ["", *explain(here)]
        return lines

    def _stable_on_this_input(self, steps: list[StepMeasurement]) -> list[str]:
        """The status the whole slice exists for, and the one most able to overclaim.

        It has to mean that the step did not fire under these specific stresses,
        which are named on the lines above it. It must never be read as the step
        being reproducible, so the sentence saying so is printed every time
        rather than left in a README nobody has open.
        """
        hits = [
            f"  {step.index} {step.name}: "
            + ", ".join(f"{a.fired} of {a.comparisons} under {a.amplifier}" for a in fired)
            for step in steps
            if (fired := step.amplified_by)
        ]
        return [
            *_wrap(
                f"{STABLE_ON_THIS_INPUT} on {_named(steps)}. Each of those reproduced on the "
                f"input this pipeline was given, {self.runs - 1} of {self.runs - 1} clean, and "
                f"stopped reproducing once the input was stressed:"
            ),
            *hits,
            *_wrap(
                f"That is the case a plain {self.runs}-run loop reports as a clean zero, which is "
                f"the case this tool exists for. It is not a milder DIVERGENT and it is not a "
                f"pass. It says the step did not fire under these particular stresses, the ones "
                f"named above. It does not say the step is reproducible, and no number of runs "
                f"could say that.",
                indent="  ",
            ),
        ]

    def _amplification_failed(self, steps: list[StepMeasurement]) -> list[str]:
        raised = [
            f"  {step.index} {step.name}, {a.amplifier}: {a.error}"
            for step in steps
            for a in step.amplifications
            if a.error
        ]
        return [
            *_wrap(
                f"{AMPLIFICATION_FAILED} on {_named(steps)}. An amplified input made the step "
                f"raise rather than diverge, so the amplifier answered nothing and the step gets "
                f"no clean status:"
            ),
            *raised,
            *_wrap(
                "The commonest cause is an amplifier violating something downstream depends on, "
                "such as a uniqueness constraint on a column tie collapse has just made "
                "non-unique. That is worth knowing either way, and it is not a reason to report "
                "the step as quiet.",
                indent="  ",
            ),
        ]

    def _no_divergence_observed(self, steps: list[StepMeasurement]) -> list[str]:
        """The rate, the bound in words, and the two lists. Never fewer than all three.

        The pair of lists is the part that cannot be dropped. "Never fired in
        four comparisons" and "cannot fire" are different sentences, and naming
        what was moved against what was not is the only version of that
        difference a black-box tester can produce.

        The lists are per step because they differ per step. A generator step
        has no input for two of the three amplifiers to touch, and folding that
        into one summary put the same amplifier in both lists at once.
        """
        smallest = min(
            (a.comparisons for step in steps for a in step.amplifications if a.measured),
            default=0,
        )
        lines = [
            *_wrap(
                f"{NO_DIVERGENCE_OBSERVED} on {_named(steps)}. It is a fire rate and two lists, "
                f"and it is not a verdict about the code."
            ),
            *_wrap(
                f"0 of {self.runs - 1} on the real input"
                + (f", and 0 of {smallest} under each amplifier that ran. " if smallest else ". ")
                + _bound_words(self.runs - 1)
                + (
                    f" An amplifier's {smallest} rule out one above "
                    f"{upper_bound(smallest) * 100:.0f} percent."
                    if smallest and smallest != self.runs - 1
                    else ""
                )
                + " That assumes an independence these runs do not have, since they share a "
                "process, a page cache and a machine.",
                indent="  ",
            ),
        ]
        for step in steps:
            lines.append(f"  {step.index} {step.name}")
            varied = ["how many times the pipeline ran"]
            varied += [a.amplifier for a in step.amplifications if a.measured]
            lines += _wrap("varied: " + ", ".join(varied), indent="      ")
            missed = [
                f"{a.amplifier} ({_why_not(a)})"
                for a in step.amplifications
                if not a.measured
            ]
            if missed:
                lines += _wrap("not varied: " + ", ".join(missed), indent="      ")
        lines += _wrap("Not varied anywhere: " + ", ".join(NOT_VARIED) + ".", indent="  ")
        return lines

    def _bounds(self, verdicts: list[StepVerdict]) -> list[str]:
        drifting = [v for v in verdicts if v.bound is not None]
        if not drifting:
            return []
        lines = [
            "",
            "reassociation bound, computed rather than picked:",
            f"  {HEADROOM_REQUIRED:,.0f}x of headroom was fixed before any of this was "
            f"written and has not moved since.",
            "  Below is that one check at two choices of n. The first is the count the spec "
            "settled on and carries a",
            "  factor of the output row count in slack. The second is the terms behind one "
            "output value, has no slack",
            "  in it, so it is normally the one that fails: it cleared on 1 of 40 step-passes "
            "here, at 1,229x. Both",
            "  print so the slack is visible rather than described.",
        ]
        for verdict in drifting:
            lines.append(f"  {verdict.step.index} {verdict.step.name}")
            lines.extend(f"      {line}" for line in _bound_lines(verdict.bound))
        return lines

    def _footer(self, verdicts: list[StepVerdict]) -> list[str]:
        fired = sum(1 for v in verdicts if v.fired)
        tolerated = sum(v.tolerated for v in verdicts)
        silent = [s.name for s in self.steps if s.artifacts_compared == 0]
        took = "" if self.seconds is None else f" in {self.seconds:.1f}s"
        lines = [
            "",
            f"{fired} of {len(self.steps)} steps diverged{took}"
            + (
                f", with {_plural(tolerated, "further comparison")} measured "
                f"and downgraded to TOLERATED."
                if tolerated
                else "."
            ),
        ]
        if silent:
            lines += [
                "",
                f"{_plural(len(silent), 'step')} wrote nothing and "
                f"{'was' if len(silent) == 1 else 'were'} therefore not checked at all: "
                f"{', '.join(silent)}.",
                "A 0 of 4 from a step with no artifacts is not evidence about that step.",
            ]
        return lines


def _named(steps: list[StepMeasurement]) -> str:
    return ", ".join(f"{s.index} {s.name}" for s in steps)


def _bound_words(comparisons: int) -> str:
    """The sentence a zero has to carry, spelled out rather than left as a fraction.

    A reader who does not already know the arithmetic reads `0 of 4` as a pass.
    The number that stops that is 53 percent, and it belongs next to the zero
    rather than in a README.
    """
    verb = "rules" if comparisons == 1 else "rule"
    return (
        f"{_plural(comparisons, 'clean comparison')} {verb} out a per-comparison divergence "
        f"probability above {upper_bound(comparisons) * 100:.0f} percent, "
        f"{CONFIDENCE * 100:.0f} percent one-sided."
    )


def _attempt_line(attempt: Amplification) -> str:
    if attempt.error:
        return f"{attempt.amplifier:<19} raised: {attempt.error}"
    if not attempt.ran:
        return f"{attempt.amplifier:<19} not run: {attempt.note}"
    rate = f"{attempt.fired} of {attempt.comparisons}"
    if not attempt.measured:
        return f"{attempt.amplifier:<19} {rate}, {_why_not(attempt)}"
    return f"{attempt.amplifier:<19} {rate}, {attempt.note}"


def _why_not(attempt: Amplification) -> str:
    """Why an amplifier's answer does not count, in the three ways it can fail to.

    The middle one is the guard the main loop and the bisect already carry.
    `0 of 2` from two comparisons of nothing reads exactly like two clean ones,
    and this is the third loop where that could have been let through.
    """
    if attempt.error:
        return "it raised"
    if attempt.ran:
        return "over no artifact: the step wrote nothing when re-executed on its own"
    return attempt.note


def _magnitudes(step: StepMeasurement) -> list[str]:
    """The furthest a float moved anywhere in the step, and the pair that did it.

    A verdict is what a build gate needs. Someone reconciling a figure that did
    not add up needs the two values that disagreed, so they are printed rather
    than summarised. All three figures come off one ColumnDrift, so the
    magnitude here and the one the bound is checked against cannot drift apart.
    """
    furthest = step.furthest_drift
    if furthest is None:
        return []
    scope = _plural(step.comparisons, "comparison")
    line = (
        f"furthest move over {scope}: {furthest.column}, "
        f"{furthest.max_ulps} ulp and {furthest.max_relative:.2e} relative"
    )
    if furthest.example[0] is None:
        return [line]
    return [line, f"{furthest.example[0]} against {furthest.example[1]}"]


def _fallbacks(step: StepMeasurement) -> list[str]:
    """Where the header's containment line does not hold, named rather than left implied.

    A read only falls back when run 1 never wrote the artifact, which is itself
    a divergence and is reported against the step that wrote it. The line matters
    anyway: without it the header says every read went to run 1 and one of them
    did not.
    """
    if not step.uncontained_reads:
        return []
    names = sorted(step.uncontained_reads)
    return [
        f"containment did not cover {', '.join(names)}: run 1 never wrote "
        f"{'it' if len(names) == 1 else 'them'}, so this run read its own copy"
    ]


def _matched_on(findings: ArtifactFindings) -> list[str]:
    """Which columns decided that two rows were the same row.

    The README claimed the tool reported this and it did not, which mattered
    most in the case the README was describing: an artifact with no exact
    columns has no key at all, sorts as one group and pairs rows by order
    alone. That is the weakest comparison here and it was invisible.

    Printed only when it is not the default, because on a normal run it is
    every exact column and saying so on every line is noise.
    """
    if findings.schema_note is not None:
        return []
    if not findings.key:
        return ["matched on no key: this artifact has no exact column, so rows pair by sort order"]
    return []


def _attribution(findings: ArtifactFindings) -> list[str]:
    """The line that turns a large unmatched count into the name of a column."""
    unstable = findings.unstable_key_columns
    if not unstable:
        if findings.attribution:
            return ["no key column explains it: dropping each one in turn changed nothing"]
        return []

    named, before = unstable[0], findings.unmatched_reference
    lines = [
        f"dropping {named.column} from the key takes unmatched reference rows "
        f"from {before:,} to {named.unmatched_reference:,}"
    ]
    lines.extend(_runners_up(named, unstable[1:]))
    if findings.attribution_capped:
        lines.append(
            f"{_plural(findings.attribution_capped, 'further key column')} "
            f"{'was' if findings.attribution_capped == 1 else 'were'} not tested: attribution "
            f"runs over the {len(findings.attribution)} with the most distinct values"
        )
    return lines


def _runners_up(named: KeyEffect, rest: list[KeyEffect]) -> list[str]:
    """What else shrank the unmatched count, and what actually broke a tie.

    The sort is (remaining, from_input, column), so the reason a column came
    first depends on which of the three decided it. Printing the input-based
    reason unconditionally was wrong whenever the tied columns shared
    `from_input`, which is every tie in a pipeline whose inputs arrive through
    ctx.sql, since input_columns is then empty and no column is carried. It
    read as `zeta does the same, so alpha is named first because it is the one
    no input to this step carries`, over a tie that the letter a decided.

    Near-ties are disclosed too. A runner-up taking 491,520 down to 4 also
    explains the divergence, and on real data those will be commoner than exact
    ties. No threshold is involved: the runner-up's own figure is printed and
    the reader judges it.
    """
    if not rest:
        return []
    tied = [e for e in rest if e.remaining == named.remaining]
    if tied:
        columns = ", ".join(e.column for e in tied)
        broke_the_tie = any(e.from_input != named.from_input for e in tied)
        because = (
            "it is the one no input to this step carries"
            if broke_the_tie
            else "nothing separates them but the sort order"
        )
        return [f"{columns} does the same, so {named.column} is named first because {because}"]
    runner = rest[0]
    return [
        f"{runner.column} also shrinks it, to {runner.unmatched_reference:,}, "
        f"so more than one column moved"
    ]


def _bound_lines(bound: DriftBound) -> list[str]:
    """The check that was pre-registered before any of this existed, run and reported.

    Two ratios, because they answer different questions. The first uses the n
    the spec settled on and is the check as written. The second uses the terms
    behind one output value, which is the version with no slack in it, and if
    the two disagree the margin in the first came from the slack rather than
    from the drift being small.

    Both verdicts read CLEARS or FAILS in the same column, and both ratios use
    the same fixed-point format. They did not: the second printed lowercase at
    the end of the longest sentence in the block, and switched to scientific
    notation in exactly the case where it cleared, so the block read as one
    assertion breaking rather than as two answers to the same question.
    """
    return [
        f"observed furthest relative drift {bound.observed:.4e}",
        f"n = {bound.terms:,} rows read by the step, bound {bound.bound:.4e}, "
        f"headroom {bound.ratio:,.0f}x  {_verdict(bound.ratio)}",
        f"n = {bound.tight_terms:,} terms per output row, bound {bound.tight_bound:.4e}, "
        f"headroom {bound.tight_ratio:,.0f}x  {_verdict(bound.tight_ratio)}",
    ]


def _verdict(headroom: float) -> str:
    return "CLEARS" if headroom >= HEADROOM_REQUIRED else "FAILS"
