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

from dataclasses import dataclass, field

from twicerun.manifest import Environment
from twicerun.measurement import StepMeasurement, name_classes
from twicerun.oracle import ArtifactFindings, KeyEffect
from twicerun.policy import HEADROOM_REQUIRED, DriftBound, Policy, StepVerdict, judge


def _plural(n: int, singular: str, plural: str | None = None) -> str:
    return f"{n} {singular}" if n == 1 else f"{n} {plural or singular + 's'}"


@dataclass
class Report:
    pipeline: str
    run_dir: str
    runs: int
    environment: Environment
    steps: list[StepMeasurement]
    seconds: float
    policy: Policy = field(default_factory=Policy)
    pruned: int = 0
    keep: int = 1

    @property
    def verdicts(self) -> list[StepVerdict]:
        return [judge(step, self.policy) for step in self.steps]

    def render(self) -> str:
        judged = self.verdicts
        sections = [
            self._header(judged),
            self._steps(judged),
            self._bounds(judged),
            self._footer(judged),
        ]
        return "\n".join(line for section in sections for line in section)

    def _header(self, verdicts: list[StepVerdict]) -> list[str]:
        env = self.environment
        unverified = dict.fromkeys(note for v in verdicts for note in v.unverified)
        return [
            f"pipeline   {self.pipeline}",
            f"runs       {self.runs}, run 1 is the reference, so "
            f"{_plural(self.runs - 1, 'comparison')} per step",
            f"policy     {self.policy.describe()}",
            *(f"           {note}" for note in unverified),
            f"duckdb     {env.duckdb_version}, threads={env.threads}",
            f"platform   {env.platform}",
            f"artifacts  {self.run_dir}",
            f"retention  keeping {self.keep} run "
            + ("directory" if self.keep == 1 else "directories")
            + (f", dropped {self.pruned}" if self.pruned else ""),
            "",
        ]

    def _steps(self, verdicts: list[StepVerdict]) -> list[str]:
        width = max((len(f"{s.index} {s.name}") for s in self.steps), default=10)
        lines = []
        for verdict in verdicts:
            step = verdict.step
            label = f"{step.index} {step.name}".ljust(width)
            rate = f"{verdict.fired} of {step.comparisons}"
            tail = name_classes(step.classes)
            if verdict.tolerated:
                tail = f"{tail} TOLERATED on {verdict.tolerated} of {step.comparisons}"
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
            if verdict.blocked:
                lines.append(f"      {verdict.blocked}")
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
            "  in it, and is expected to fail. Both print so the slack is visible rather than "
            "described.",
        ]
        for verdict in drifting:
            lines.append(f"  {verdict.step.index} {verdict.step.name}")
            lines.extend(f"      {line}" for line in _bound_lines(verdict.bound))
        return lines

    def _footer(self, verdicts: list[StepVerdict]) -> list[str]:
        fired = sum(1 for v in verdicts if v.fired)
        tolerated = sum(v.tolerated for v in verdicts)
        silent = [s.name for s in self.steps if s.artifacts_compared == 0]
        lines = [
            "",
            f"{fired} of {len(self.steps)} steps diverged in {self.seconds:.1f}s"
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
