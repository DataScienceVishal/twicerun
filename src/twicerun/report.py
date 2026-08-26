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
from twicerun.oracle import ArtifactFindings, Divergence

CLASS_ORDER = [
    Divergence.SCHEMA,
    Divergence.ROW_MISSING,
    Divergence.ROW_EXTRA,
    Divergence.MULTIPLICITY,
    Divergence.VALUE_DRIFT,
]


@dataclass
class StepMeasurement:
    """Every comparison of one step against the reference run, policy-free."""

    index: int
    name: str
    comparisons: int
    terms: int = 0
    rounds: list[list[ArtifactFindings]] = field(default_factory=list)

    def observe(self, findings: list[ArtifactFindings]) -> None:
        self.rounds.append(findings)

    @property
    def artifacts_compared(self) -> int:
        return sum(len(round_) for round_ in self.rounds)

    @property
    def diverged(self) -> list[list[ArtifactFindings]]:
        return [[f for f in round_ if f.diverged] for round_ in self.rounds]

    @property
    def fired(self) -> int:
        """Comparisons in which the oracle found anything at all.

        A measurement, not a verdict. The policy layer reports its own count
        beside this one and never overwrites it.
        """
        return sum(1 for round_ in self.diverged if round_)

    @property
    def classes(self) -> frozenset[Divergence]:
        seen = (f.classes for round_ in self.rounds for f in round_)
        return frozenset().union(*seen, frozenset())

    @property
    def worst(self) -> ArtifactFindings | None:
        every = [f for round_ in self.rounds for f in round_ if f.diverged]
        return max(every, key=_rank, default=None)

    @property
    def max_relative(self) -> float | None:
        seen = [
            d.max_relative
            for round_ in self.rounds
            for f in round_
            for d in f.drift
            if d.max_relative is not None
        ]
        return max(seen) if seen else None

    @property
    def max_ulps(self) -> int | None:
        seen = [
            d.max_ulps
            for round_ in self.rounds
            for f in round_
            for d in f.drift
            if d.max_ulps is not None
        ]
        return max(seen) if seen else None

    @property
    def hints(self) -> list[str]:
        ordered = dict.fromkeys(h for round_ in self.rounds for f in round_ for h in f.hints)
        return list(ordered)


def _plural(n: int) -> str:
    return f"{n} comparison" if n == 1 else f"{n} comparisons"


def _rank(findings: ArtifactFindings) -> tuple[bool, int, int]:
    """How interesting one finding is, given only one of them gets printed.

    A schema change comes first regardless of size. It carries no row counts at
    all, because the row comparison is skipped when the columns do not line up,
    so ranking on volume alone sorted the one class that is never noise below a
    two-row drift on a sibling artifact and dropped it out of the report.
    """
    return (
        findings.schema_note is not None,
        findings.unmatched_reference + findings.unmatched_candidate,
        findings.drift_rows,
    )


def name_classes(classes: frozenset[Divergence]) -> str:
    return " ".join(c.value for c in CLASS_ORDER if c in classes)


@dataclass
class Report:
    pipeline: str
    run_dir: str
    runs: int
    environment: Environment
    steps: list[StepMeasurement]
    seconds: float
    pruned: int = 0
    keep: int = 1

    def render(self) -> str:
        return "\n".join([*self._header(), *self._steps(), *self._footer()])

    def _header(self) -> list[str]:
        env = self.environment
        return [
            f"pipeline   {self.pipeline}",
            f"runs       {self.runs}, run 1 is the reference, so {_plural(self.runs - 1)} "
            f"per step",
            f"duckdb     {env.duckdb_version}, threads={env.threads}",
            f"platform   {env.platform}",
            f"artifacts  {self.run_dir}",
            f"retention  keeping {self.keep} run "
            + ("directory" if self.keep == 1 else "directories")
            + (f", dropped {self.pruned}" if self.pruned else ""),
            "",
        ]

    def _steps(self) -> list[str]:
        width = max((len(f"{s.index} {s.name}") for s in self.steps), default=10)
        lines = []
        for step in self.steps:
            label = f"{step.index} {step.name}".ljust(width)
            rate = f"{step.fired} of {step.comparisons}"
            lines.append(f"  {label}  {rate:>8}  {name_classes(step.classes)}".rstrip())
            if step.artifacts_compared == 0:
                lines.append("      wrote no artifacts, so nothing was compared")
                continue
            worst = step.worst
            if worst is not None:
                lines.append(f"      {worst.describe()}")
                lines.extend(f"      {line}" for line in _magnitudes(worst))
            lines.extend(f"      {hint}" for hint in step.hints)
        return lines

    def _footer(self) -> list[str]:
        fired = sum(1 for s in self.steps if s.fired)
        silent = [s.name for s in self.steps if s.artifacts_compared == 0]
        lines = [
            "",
            f"{fired} of {len(self.steps)} steps diverged in {self.seconds:.1f}s.",
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
