"""One step, every comparison of it, and nothing about whether any of it is acceptable.

This is the boundary object between the three stages. The oracle fills it in,
the policy layer reads it and returns a verdict, and the report prints both. The
separation is what makes it checkable that a tolerance cannot move a measured
number: `policy.py` imports this and never writes to it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from twicerun.cause import Bisect
from twicerun.oracle import ArtifactFindings, ColumnDrift, Divergence

# Ordered so a report lists the classes the same way every time, loudest first.
CLASS_ORDER = [
    Divergence.SCHEMA,
    Divergence.ROW_MISSING,
    Divergence.ROW_EXTRA,
    Divergence.MULTIPLICITY,
    Divergence.VALUE_DRIFT,
]


def name_classes(classes: frozenset[Divergence]) -> str:
    return " ".join(c.value for c in CLASS_ORDER if c in classes)


@dataclass
class StepMeasurement:
    """Every comparison of one step against the reference run, policy-free."""

    index: int
    name: str
    comparisons: int
    terms: int = 0
    rounds: list[list[ArtifactFindings]] = field(default_factory=list)
    uncontained_reads: set[str] = field(default_factory=set)
    bisect: Bisect | None = None

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
    def cause(self) -> str | None:
        """What the single-threaded re-execution made of this step's divergence.

        None where there is nothing to explain, which is a step that never
        fired; where nothing was measured, which is a manifest written before
        the bisect existed or a bisect that failed; and where the re-execution
        wrote no artifacts, which is a rate out of nothing.
        """
        if self.bisect is None or not self.fired:
            return None
        return self.bisect.label

    @property
    def classes(self) -> frozenset[Divergence]:
        seen = (f.classes for round_ in self.rounds for f in round_)
        return frozenset().union(*seen, frozenset())

    @property
    def worst(self) -> ArtifactFindings | None:
        every = [f for round_ in self.rounds for f in round_ if f.diverged]
        return max(every, key=_rank, default=None)

    @property
    def furthest_drift(self) -> ColumnDrift | None:
        """The single pair that moved furthest, anywhere in this step.

        One object rather than a set of independent maxima, so the magnitude on
        the step line, the magnitude in the bound section and the pair of values
        printed underneath are all the same observation. They were not: the step
        line showed the comparison with the most drifting rows and the bound
        section the largest magnitude across comparisons, both labelled as
        maxima, and on the reference pipeline they disagreed on about one float
        step-pass in six. Always understating, so a reader tracing a total saw
        less drift than had occurred.
        """
        every = [
            d
            for round_ in self.rounds
            for f in round_
            for d in f.drift
            if d.max_relative is not None
        ]
        return max(every, key=lambda d: d.max_relative, default=None)

    @property
    def max_relative(self) -> float | None:
        furthest = self.furthest_drift
        return None if furthest is None else furthest.max_relative

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
