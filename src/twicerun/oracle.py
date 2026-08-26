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

from twicerun.columns import ColumnPlan, plan, schema_difference
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
    schema_note: str | None = None

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
        return frozenset(seen)

    @property
    def diverged(self) -> bool:
        return bool(self.classes)

    def describe(self) -> str:
        if self.schema_note is not None:
            return f"{self.name}: {self.schema_note}"
        parts = []
        if self.unmatched_reference:
            parts.append(
                f"{self.unmatched_reference:,} of {self.reference_rows:,} reference rows unmatched"
            )
        if self.unmatched_candidate:
            parts.append(f"{self.unmatched_candidate:,} rows in the later run with no partner")
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
    counted = con.execute(
        census_sql(columns, Path(reference.path), Path(candidate.path))
    ).fetchone()
    missing, extra, mult_reference, mult_candidate, matched = (int(n) for n in counted)

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
    )


def side_sql(columns: ColumnPlan, path: Path, *, ordered: bool) -> str:
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


def paired_sql(columns: ColumnPlan, left: Path, right: Path, *, ordered: bool = True) -> str:
    """The FULL OUTER JOIN itself, as a pair of CTEs ending in `paired`.

    IS NOT DISTINCT FROM rather than = so that a NULL in a key column matches a
    NULL on the other side. With plain equality every NULL-keyed row would fail
    to pair with itself and the report would fill with rows that never moved.
    """
    on = [f"l.k{i} IS NOT DISTINCT FROM r.k{i}" for i in range(len(columns.key))]
    on.append("l.ord = r.ord")
    grouped = [f"coalesce(l.k{i}, r.k{i}) AS k{i}" for i in range(len(columns.key))]
    selected = [
        "l.ord IS NOT NULL AS in_reference",
        "r.ord IS NOT NULL AS in_candidate",
        *grouped,
    ]
    return (
        f"WITH l AS ({side_sql(columns, left, ordered=ordered)}),\n"
        f"     r AS ({side_sql(columns, right, ordered=ordered)}),\n"
        f"     paired AS MATERIALIZED (\n"
        f"       SELECT {', '.join(selected)}\n"
        f"       FROM l FULL OUTER JOIN r ON {' AND '.join(on)}\n"
        f"     )"
    )


def census_sql(columns: ColumnPlan, left: Path, right: Path, *, ordered: bool = True) -> str:
    """How many rows fell into each class, counted per key group.

    A row that failed to pair is ROW_MISSING when its key is absent from the
    other side altogether and MULTIPLICITY when the key is there with a
    different row count. Telling those apart needs the group sizes, which is
    why this aggregates twice: once to per_key, once across it.
    """
    by_key = ", ".join(f"k{i}" for i in range(len(columns.key)))
    group = f"GROUP BY {by_key}" if by_key else ""
    return f"""
{paired_sql(columns, left, right, ordered=ordered)},
     per_key AS (
       SELECT count(*) FILTER (WHERE in_reference) AS reference_n,
              count(*) FILTER (WHERE in_candidate) AS candidate_n,
              count(*) FILTER (WHERE in_reference AND NOT in_candidate) AS reference_only,
              count(*) FILTER (WHERE in_candidate AND NOT in_reference) AS candidate_only
       FROM paired {group}
     )
SELECT coalesce(sum(CASE WHEN candidate_n = 0 THEN reference_only ELSE 0 END), 0),
       coalesce(sum(CASE WHEN reference_n = 0 THEN candidate_only ELSE 0 END), 0),
       coalesce(sum(CASE WHEN candidate_n > 0 THEN reference_only ELSE 0 END), 0),
       coalesce(sum(CASE WHEN reference_n > 0 THEN candidate_only ELSE 0 END), 0),
       coalesce(sum(least(reference_n, candidate_n)), 0)
FROM per_key
"""
