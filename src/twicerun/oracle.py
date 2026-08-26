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

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import duckdb

from twicerun.columns import ColumnPlan, Partition, clock_columns, plan, schema_difference
from twicerun.manifest import Artifact
from twicerun.storage import quote


class Divergence(StrEnum):
    SCHEMA = "SCHEMA"
    ROW_MISSING = "ROW_MISSING"
    ROW_EXTRA = "ROW_EXTRA"
    MULTIPLICITY = "MULTIPLICITY"
    VALUE_DRIFT = "VALUE_DRIFT"


def identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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
        worst = self.worst_drift
        if worst is not None:
            parts.append(
                f"{worst.rows:,} of {self.matched:,} paired rows moved on {worst.column}"
                + (
                    f", up to {worst.max_ulps} ulp and {worst.max_relative:.2e} relative"
                    if worst.approximate
                    else ""
                )
            )
        if not parts:
            return f"{self.name}: {self.matched:,} rows paired, none differing"
        return f"{self.name}: " + ", ".join(parts)


def compare(
    con: duckdb.DuckDBPyConnection,
    reference: Artifact,
    candidate: Artifact,
    key_override: Sequence[str] | None = None,
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
        hints=_hints(columns, drift),
    )


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


def side_sql(columns: ColumnPlan, path: Path, *, ordered: bool, with_values: bool = False) -> str:
    """One side of the join, with every user column renamed out of the way.

    Aliasing to k0, v0 and so on rather than keeping the original names means a
    column called `ord` cannot collide with the ordinal, and the join condition
    below stays readable at any column count.

    `ordered` drops the ORDER BY inside the window. Leave-one-out attribution
    only needs the count of rows that failed to pair, and that count is
    |group size on one side minus the other| whichever rows inside the group
    happen to pair up, so the sort is pure cost there.
    """
    projected = [f"{identifier(c)} AS k{i}" for i, c in enumerate(columns.key)]
    if with_values:
        projected += [f"{identifier(c)} AS v{i}" for i, c in enumerate(columns.values)]

    partition = ", ".join(identifier(c) for c in columns.key)
    order = ", ".join(identifier(c) for c in columns.ordinal_order())
    window = " ".join(
        piece
        for piece in (
            f"PARTITION BY {partition}" if partition else "",
            f"ORDER BY {order}" if order and ordered else "",
        )
        if piece
    )
    projected.append(f"row_number() OVER ({window}) AS ord")
    return f"SELECT {', '.join(projected)} FROM read_parquet({quote(path)})"


def paired_sql(
    columns: ColumnPlan,
    left: Path,
    right: Path,
    *,
    ordered: bool = True,
    with_values: bool = False,
) -> str:
    """The FULL OUTER JOIN itself, as a pair of CTEs ending in `paired`.

    IS NOT DISTINCT FROM rather than = so that a NULL in a key column matches a
    NULL on the other side. With plain equality every NULL-keyed row would fail
    to pair with itself and the report would fill with rows that never moved.

    The same operator decides whether a value column moved, and on a DOUBLE it
    gives exactly the equality the spec asks for without a special case: DuckDB
    puts floats in a total order where NaN equals NaN and -0.0 equals 0.0, and
    IS NOT DISTINCT FROM adds NULL equals NULL on top.
    """
    on = [f"l.k{i} IS NOT DISTINCT FROM r.k{i}" for i in range(len(columns.key))]
    on.append("l.ord = r.ord")
    selected = [
        "l.ord IS NOT NULL AS in_reference",
        "r.ord IS NOT NULL AS in_candidate",
        *(f"coalesce(l.k{i}, r.k{i}) AS k{i}" for i in range(len(columns.key))),
    ]
    if with_values:
        for i in range(len(columns.values)):
            selected += [
                f"l.v{i} AS lv{i}",
                f"r.v{i} AS rv{i}",
                f"NOT (l.v{i} IS NOT DISTINCT FROM r.v{i}) AS moved{i}",
            ]
    return (
        f"WITH l AS ({side_sql(columns, left, ordered=ordered, with_values=with_values)}),\n"
        f"     r AS ({side_sql(columns, right, ordered=ordered, with_values=with_values)}),\n"
        f"     paired AS MATERIALIZED (\n"
        f"       SELECT {', '.join(selected)}\n"
        f"       FROM l FULL OUTER JOIN r ON {' AND '.join(on)}\n"
        f"     )"
    )


CENSUS_TOTALS = """
       coalesce(sum(CASE WHEN candidate_n = 0 THEN reference_only ELSE 0 END), 0) AS row_missing,
       coalesce(sum(CASE WHEN reference_n = 0 THEN candidate_only ELSE 0 END), 0) AS row_extra,
       coalesce(sum(CASE WHEN candidate_n > 0 THEN reference_only ELSE 0 END), 0) AS mult_reference,
       coalesce(sum(CASE WHEN reference_n > 0 THEN candidate_only ELSE 0 END), 0) AS mult_candidate,
       coalesce(sum(least(reference_n, candidate_n)), 0) AS matched"""


def per_key_sql(columns: ColumnPlan) -> str:
    """Group sizes on both sides, which is what separates ROW_MISSING from MULTIPLICITY.

    A row that failed to pair is ROW_MISSING when its key is absent from the
    other side altogether and MULTIPLICITY when the key is there with a
    different row count, so the totals aggregate twice: once to here, once
    across this.
    """
    by_key = ", ".join(f"k{i}" for i in range(len(columns.key)))
    return f"""
     per_key AS (
       SELECT count(*) FILTER (WHERE in_reference) AS reference_n,
              count(*) FILTER (WHERE in_candidate) AS candidate_n,
              count(*) FILTER (WHERE in_reference AND NOT in_candidate) AS reference_only,
              count(*) FILTER (WHERE in_candidate AND NOT in_reference) AS candidate_only
       FROM paired {f"GROUP BY {by_key}" if by_key else ""}
     )"""


def census_sql(columns: ColumnPlan, left: Path, right: Path, *, ordered: bool = True) -> str:
    return (
        f"{paired_sql(columns, left, right, ordered=ordered)},"
        f"{per_key_sql(columns)}\n"
        f"SELECT{CENSUS_TOTALS}\nFROM per_key\n"
    )


def ordered_int_sql(value: str, width: int) -> str:
    """IEEE-754 bits reinterpreted so that integer distance is ULP distance.

    DuckDB casts a float to BIT giving the raw layout, and BIT to a signed
    integer of the same width giving the two's-complement reading of those
    bits. That is only half the job: the layout is sign-magnitude, so the
    negatives run backwards. Reflecting them about the minimum turns the whole
    range into one monotone integer ordering, and it drops -0.0 onto +0.0,
    which is the equality IEEE-754 already gives them.

    Checked against struct.unpack across zeros, subnormals, both infinities and
    a NaN rather than taken on trust.
    """
    kind, floor = ("BIGINT", "(-9223372036854775807)::HUGEINT - 1") if width == 64 else (
        "INTEGER", "(-2147483648)::HUGEINT"
    )
    bits = f"({value})::BIT::{kind}"
    return f"CASE WHEN {bits} >= 0 THEN ({bits})::HUGEINT ELSE {floor} - ({bits})::HUGEINT END"


def relative_sql(left: str, right: str) -> str:
    """|a - b| / max(|a|, |b|), with the cases where that ratio means nothing sent to infinity.

    Only reached for a pair that already differs, so both NaN and both infinite
    with the same sign are already excluded. What is left is a NULL against a
    value, a NaN against a number, or an infinity against a finite number, and
    for all three the relative difference is either undefined or 1. Reporting
    infinity keeps them out of every tolerance rather than letting a NaN
    poison the max.
    """
    a, b = f"({left})::DOUBLE", f"({right})::DOUBLE"
    return (
        f"CASE WHEN {a} IS NULL OR {b} IS NULL OR isnan({a}) OR isnan({b}) "
        f"OR isinf({a}) OR isinf({b}) THEN 'inf'::DOUBLE "
        f"ELSE abs({a} - {b}) / greatest(abs({a}), abs({b})) END"
    )


def drift_sql(columns: ColumnPlan) -> str:
    """Per value column, how many pairs moved and by how much.

    Restricted to pairs that moved before any of the magnitude arithmetic runs.
    On the reference pipeline's float average that takes the ULP transform from
    two million rows to about six hundred.
    """
    values = columns.values
    if not values:
        return "     drift AS (SELECT 0 AS drift_rows)"

    moved = " OR ".join(f"moved{i}" for i in range(len(values)))
    aggregates = ["count(*) AS drift_rows"]
    for i, column in enumerate(values):
        rows = f"count(*) FILTER (WHERE moved{i}) AS d{i}_rows"
        example = (
            f"any_value(lv{i}::VARCHAR) FILTER (WHERE moved{i}) AS d{i}_left, "
            f"any_value(rv{i}::VARCHAR) FILTER (WHERE moved{i}) AS d{i}_right"
        )
        if columns.partition[column] is not Partition.APPROX:
            aggregates += [rows, f"NULL AS d{i}_ulps, NULL AS d{i}_rel", example]
            continue
        ulps = (
            f"abs({ordered_int_sql(f'lv{i}', columns.float_width(column))} "
            f"- {ordered_int_sql(f'rv{i}', columns.float_width(column))})"
        )
        relative = relative_sql(f"lv{i}", f"rv{i}")
        aggregates += [
            rows,
            f"max({ulps}) FILTER (WHERE moved{i}) AS d{i}_ulps",
            f"max({relative}) FILTER (WHERE moved{i}) AS d{i}_rel",
            f"arg_max(lv{i}::VARCHAR, {relative}) FILTER (WHERE moved{i}) AS d{i}_left, "
            f"arg_max(rv{i}::VARCHAR, {relative}) FILTER (WHERE moved{i}) AS d{i}_right",
        ]
    return (
        f"     drift AS (\n"
        f"       SELECT {', '.join(aggregates)}\n"
        f"       FROM paired WHERE in_reference AND in_candidate AND ({moved})\n"
        f"     )"
    )


def comparison_sql(columns: ColumnPlan, left: Path, right: Path) -> str:
    """Both halves off one join: what failed to pair, and what paired but moved."""
    return (
        f"{paired_sql(columns, left, right, with_values=True)},"
        f"{per_key_sql(columns)},\n"
        f"     census AS (SELECT{CENSUS_TOTALS} FROM per_key),\n"
        f"{drift_sql(columns)}\n"
        f"SELECT * FROM census, drift\n"
    )
