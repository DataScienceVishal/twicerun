"""Comparing two artifacts by pairing rows rather than by hashing them.

The bit-exact multiset comparison in `compare.py` answers one question well and
one question badly. It cannot say that two rows are nearly the same, so a float
aggregate that reassociates under parallelism arrives as a pair of unmatched
rows and gets counted exactly as a wrong answer does. On the reference
pipeline's correct float average that is around 600 findings on 1,000 groups.

The fix is to pair rows up first. Every column that cannot reassociate becomes
the join key, every column that can becomes a value, and the two sides are
joined on the key plus an ordinal inside the key group:

    row_number() OVER (PARTITION BY <key> ORDER BY <values>)

The ordinal is what preserves multiplicity. A key group holding three rows in
the reference run and five in a later one pairs ordinals 1 to 3 and leaves 4 and
5 unpaired, which is the INSERT-without-a-key case exactly. Joining on the key
alone would fan out to fifteen pairs and report nothing.

Sorting by the value columns to assign that ordinal is the one place this
imposes an order, and it imposes the same one on both sides. With a single float
column, sorted-to-sorted pairing minimises total absolute difference, so it is
optimal. With several, the lexicographic sort is a heuristic that can mispair
two rows inside one key group, which can only understate a difference. That
false negative is bounded to within-key-group permutations of float-only
differences and it goes in the README rather than being left to be discovered.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path

import duckdb

from twicerun.columns import ColumnPlan, Partition, clock_columns, plan, schema_difference
from twicerun.manifest import Artifact
from twicerun.sql import census_sql, comparison_sql, distinct_ratio_sql


class Divergence(StrEnum):
    SCHEMA = "SCHEMA"
    ROW_MISSING = "ROW_MISSING"
    ROW_EXTRA = "ROW_EXTRA"
    MULTIPLICITY = "MULTIPLICITY"
    VALUE_DRIFT = "VALUE_DRIFT"


@dataclass(frozen=True)
class ColumnDrift:
    """One value column that moved between two runs, with both magnitudes.

    Neither magnitude decides anything on its own and both are printed. The ULP
    distance says whether a difference is last-bit noise; the relative
    difference is the one a domain user can judge. An exact column can only
    reach here through --key, and carries no magnitudes because there is no
    scale on which a VARCHAR is nearly right.
    """

    column: str
    approximate: bool
    rows: int
    max_ulps: int | None
    max_relative: float | None
    example: tuple[str | None, str | None]


@dataclass(frozen=True)
class KeyEffect:
    """What happens to the unmatched count when one column leaves the key.

    `from_input` is the tie-break, and it is needed rather than decorative. On
    the reference pipeline's row_number bug, dropping `surrogate_id` and
    dropping `event_id` both take the unmatched count from 491,520 to 0,
    because the two columns are a bijection whose pairing moved and either
    description of that is true. The storage interface knows `event_id` arrived
    from an artifact this step read and `surrogate_id` did not, so the column
    the step invented gets named first.
    """

    column: str
    unmatched_reference: int
    unmatched_candidate: int
    from_input: bool

    @property
    def remaining(self) -> int:
        return self.unmatched_reference + self.unmatched_candidate


@dataclass(frozen=True)
class ArtifactFindings:
    name: str
    key: tuple[str, ...]
    reference_rows: int
    candidate_rows: int
    matched: int = 0
    row_missing: int = 0
    row_extra: int = 0
    multiplicity_reference: int = 0
    multiplicity_candidate: int = 0
    drift_rows: int = 0
    drift: tuple[ColumnDrift, ...] = ()
    attribution: tuple[KeyEffect, ...] = ()
    attribution_capped: int = 0
    schema_note: str | None = None
    hints: tuple[str, ...] = ()

    @property
    def multiplicity(self) -> int:
        return self.multiplicity_reference + self.multiplicity_candidate

    @property
    def unmatched_reference(self) -> int:
        return self.row_missing + self.multiplicity_reference

    @property
    def unmatched_candidate(self) -> int:
        return self.row_extra + self.multiplicity_candidate

    @property
    def classes(self) -> frozenset[Divergence]:
        if self.schema_note is not None:
            return frozenset({Divergence.SCHEMA})
        seen = set()
        if self.row_missing:
            seen.add(Divergence.ROW_MISSING)
        if self.row_extra:
            seen.add(Divergence.ROW_EXTRA)
        if self.multiplicity:
            seen.add(Divergence.MULTIPLICITY)
        if self.drift_rows:
            seen.add(Divergence.VALUE_DRIFT)
        return frozenset(seen)

    @property
    def diverged(self) -> bool:
        return bool(self.classes)

    @property
    def worst_drift(self) -> ColumnDrift | None:
        return max(self.drift, key=lambda d: (d.rows, d.max_relative or 0.0), default=None)

    @property
    def unstable_key_columns(self) -> tuple[KeyEffect, ...]:
        """The columns whose removal from the key actually shrank the unmatched count.

        Reported in full rather than as a winner, because more than one column
        can explain the same divergence and hiding that would be a stronger
        claim than the arithmetic supports.
        """
        before = self.unmatched_reference + self.unmatched_candidate
        return tuple(e for e in self.attribution if e.remaining < before)

    def describe(self) -> str:
        if self.schema_note is not None:
            return f"{self.name}: {self.schema_note}"
        parts = []
        if self.unmatched_reference and self.unmatched_candidate:
            parts.append(
                f"{self.unmatched_reference:,} of {self.reference_rows:,} reference rows "
                f"and {self.unmatched_candidate:,} later rows found no partner"
            )
        elif self.unmatched_reference:
            parts.append(
                f"{self.unmatched_reference:,} of {self.reference_rows:,} reference rows "
                f"found no partner"
            )
        elif self.unmatched_candidate:
            parts.append(
                f"{self.unmatched_candidate:,} later rows found no partner, "
                f"against {self.reference_rows:,} reference rows"
            )
        # Row counts only. This describes one comparison, and a magnitude here
        # read as a maximum over the step while the bound section printed a real
        # one, so the two disagreed by design about one float step-pass in six.
        worst = self.worst_drift
        if worst is not None:
            parts.append(f"{worst.rows:,} of {self.matched:,} paired rows moved on {worst.column}")
        if not parts:
            return f"{self.name}: {self.matched:,} rows paired, none differing"
        return f"{self.name}: " + ", ".join(parts)


def compare(
    con: duckdb.DuckDBPyConnection,
    reference: Artifact,
    candidate: Artifact,
    key_override: Sequence[str] | None = None,
    input_columns: Collection[str] = (),
) -> ArtifactFindings:
    """Pair one artifact against another and count what did not line up.

    The schema check comes first and stops everything else. Comparing rows
    across a column set that moved produces numbers about the wrong thing, so
    the report says the schema changed and stops there.
    """
    note = schema_difference(reference.columns, candidate.columns)
    if note is not None:
        return ArtifactFindings(
            name=reference.name,
            key=(),
            reference_rows=reference.rows,
            candidate_rows=candidate.rows,
            schema_note=note,
        )

    columns = plan(reference.columns, key_override)
    measured = con.execute(
        comparison_sql(columns, Path(reference.path), Path(candidate.path))
    ).fetchone()
    missing, extra, mult_reference, mult_candidate, matched = (int(n) for n in measured[:5])
    drift_rows = int(measured[5])

    drift = tuple(
        moved
        for moved in (
            _column_drift(columns, column, measured[6 + 5 * i : 11 + 5 * i])
            for i, column in enumerate(columns.values)
        )
        if moved is not None
    )
    # Attribution is aimed at key instability, which shows up as rows lost on
    # both sides at once. Rows that only went missing did not move, they went.
    attribution, capped = ((), 0)
    if missing and extra:
        attribution, capped = attribute(
            con, columns, Path(reference.path), Path(candidate.path), input_columns
        )

    return ArtifactFindings(
        name=reference.name,
        key=columns.key,
        reference_rows=reference.rows,
        candidate_rows=candidate.rows,
        matched=matched,
        row_missing=missing,
        row_extra=extra,
        multiplicity_reference=mult_reference,
        multiplicity_candidate=mult_candidate,
        drift_rows=drift_rows,
        drift=drift,
        attribution=attribution,
        attribution_capped=capped,
        hints=_hints(columns, drift),
    )


ATTRIBUTION_CAP = 12


def attribute(
    con: duckdb.DuckDBPyConnection,
    columns: ColumnPlan,
    left: Path,
    right: Path,
    input_columns: Collection[str],
) -> tuple[tuple[KeyEffect, ...], int]:
    """Drop each key column in turn and recount what failed to pair.

    This is what turns 491,520 unmatched rows into the name of one column. The
    counts alone do not always single one out, so the ordering falls back to
    whether the step could have invented the column at all.

    Skipped below two key columns, because dropping the only one leaves an
    empty key that matches everything by construction and says nothing.
    """
    if len(columns.key) < 2:
        return (), 0

    considered, capped = columns.key, 0
    if len(considered) > ATTRIBUTION_CAP:
        considered = _most_key_like(con, left, columns.key)[:ATTRIBUTION_CAP]
        capped = len(columns.key) - ATTRIBUTION_CAP

    effects = []
    for column in considered:
        without = replace(columns, key=tuple(c for c in columns.key if c != column))
        counted = con.execute(census_sql(without, left, right)).fetchone()
        missing, extra, mult_reference, mult_candidate, _ = (int(n) for n in counted)
        effects.append(
            KeyEffect(
                column=column,
                unmatched_reference=missing + mult_reference,
                unmatched_candidate=extra + mult_candidate,
                from_input=column in input_columns,
            )
        )
    effects.sort(key=lambda e: (e.remaining, e.from_input, e.column))
    return tuple(effects), capped


def _most_key_like(
    con: duckdb.DuckDBPyConnection, path: Path, key: Sequence[str]
) -> tuple[str, ...]:
    """Key columns ordered by distinct values per row, highest first."""
    measured = con.execute(distinct_ratio_sql(path, key)).fetchone()
    return tuple(c for _, c in sorted(zip(measured, key, strict=True), reverse=True))


def _column_drift(columns: ColumnPlan, column: str, cells: Sequence) -> ColumnDrift | None:
    rows, ulps, relative, left, right = cells
    if not rows:
        return None
    return ColumnDrift(
        column=column,
        approximate=columns.partition[column] is Partition.APPROX,
        rows=int(rows),
        max_ulps=None if ulps is None else int(ulps),
        max_relative=None if relative is None else float(relative),
        example=(left, right),
    )


def _hints(columns: ColumnPlan, drift: Sequence[ColumnDrift]) -> tuple[str, ...]:
    """A timestamp that moved is almost always the wall clock, and naming it costs a line.

    It is the commonest reason a step never reproduces, and without the hint it
    arrives as an anonymous VALUE_DRIFT on a column called something like
    loaded_at.
    """
    moved = clock_columns(columns.types, (d.column for d in drift))
    if not moved:
        return ()
    return (f"WALL_CLOCK: {', '.join(moved)} carries a clock type and its values moved",)
