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
from twicerun.oracle import ArtifactFindings
from twicerun.policy import HEADROOM_REQUIRED, DriftBound, Policy, StepVerdict, judge


def _plural(n: int) -> str:
    return f"{n} comparison" if n == 1 else f"{n} comparisons"


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
            f"runs       {self.runs}, run 1 is the reference, so {_plural(self.runs - 1)} "
            f"per step",
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
                lines.extend(f"      {line}" for line in _magnitudes(worst))
                lines.extend(f"      {line}" for line in _attribution(worst))
            lines.extend(f"      {hint}" for hint in step.hints)
            if verdict.blocked:
                lines.append(f"      {verdict.blocked}")
        return lines

    def _bounds(self, verdicts: list[StepVerdict]) -> list[str]:
        drifting = [v for v in verdicts if v.bound is not None]
        if not drifting:
            return []
        lines = ["", "reassociation bound, computed rather than picked:"]
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
                f", with {tolerated} further comparison(s) measured and downgraded to TOLERATED."
                if tolerated
                else "."
            ),
        ]
        if silent:
            lines += [
                "",
                f"{len(silent)} step(s) wrote nothing and were therefore not checked at all: "
                f"{', '.join(silent)}.",
                "A 0 of 4 from a step with no artifacts is not evidence about that step.",
            ]
        return lines


def _magnitudes(findings: ArtifactFindings) -> list[str]:
    """The two numbers next to each other, for someone tracing a total by hand.

    A verdict is what a build gate needs. Someone reconciling a figure that did
    not add up needs the pair of values that disagreed, so the pair is printed
    rather than summarised.
    """
    worst = findings.worst_drift
    if worst is None or worst.example[0] is None:
        return []
    return [f"worst pair on {worst.column}: {worst.example[0]} against {worst.example[1]}"]


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
    tied = [e.column for e in unstable[1:] if e.remaining == named.remaining]
    if tied:
        reason = (
            "it is the one no input to this step carries"
            if not named.from_input
            else "it sorts first"
        )
        lines.append(
            f"{', '.join(tied)} does the same, so {named.column} is named first because {reason}"
        )
    if findings.attribution_capped:
        lines.append(
            f"{findings.attribution_capped} further key column(s) were not tested: "
            f"attribution runs over the {len(findings.attribution)} with the most distinct values"
        )
    return lines


def _bound_lines(bound: DriftBound) -> list[str]:
    """The check that was pre-registered before any of this existed, run and reported.

    Two ratios, because they answer different questions. The first uses the n
    the spec settled on and is the check as written. The second uses the terms
    behind one output value, which is the version with no slack in it, and if
    the two disagree the margin in the first one came from the slack rather
    than from the drift being small.
    """
    verdict = "clears" if bound.ratio >= HEADROOM_REQUIRED else "FAILS"
    tight = "clears" if bound.tight_ratio >= HEADROOM_REQUIRED else "fails"
    return [
        f"n = {bound.terms:,} rows read by the step, so the bound is "
        f"{bound.bound:.4e} relative",
        f"observed max relative drift {bound.observed:.4e}, which is "
        f"{bound.ratio:.3g}x inside the bound",
        f"pre-registered check wanted 1000x of headroom and {verdict} it",
        f"at n = {bound.tight_terms:,} terms per output row the bound is "
        f"{bound.tight_bound:.4e} and the headroom is {bound.tight_ratio:.3g}x, which {tight}",
    ]
