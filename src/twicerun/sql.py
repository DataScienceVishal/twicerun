"""The comparison, written out as DuckDB.

Everything here builds a query string and nothing runs one. `comparison_sql`
produces the whole thing in one statement so the FULL OUTER JOIN happens once
and both halves of the answer, what failed to pair and what paired but moved,
come off it together.

The piece worth reading is `ordered_int_sql`. ULP distance needs the IEEE-754
bit pattern, and pulling both sides of a 500,000-row artifact into Python to get
it is not a comparison that finishes. DuckDB will cast a DOUBLE to BIT, which gives the
raw layout, and BIT to BIGINT, which gives the two's-complement reading of those
bits. That is half the job: the layout is sign-magnitude, so the negative half
runs backwards, and reflecting it about the minimum turns the whole range into
one monotone ordering where integer distance is ulp distance. It also drops
-0.0 onto +0.0, which is the equality IEEE-754 already gives them.

Every user column is aliased to k0, v0 and so on before the join. That is not
cosmetic: a column genuinely called `ord` would otherwise collide with the
ordinal, and the join condition stays readable at any column count.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from twicerun.columns import ColumnPlan, Partition
from twicerun.storage import quote


def identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def distinct_ratio_sql(path: Path, key: Sequence[str]) -> str:
    """Distinct values per row for each key column, in one pass.

    Only needed past the attribution cap, where the question is which twelve
    columns are worth spending a join on. One value per row is the shape a
    surrogate key has.
    """
    ratios = ", ".join(
        f"count(DISTINCT {identifier(c)})::DOUBLE / greatest(count(*), 1)" for c in key
    )
    return f"SELECT {ratios} FROM read_parquet({quote(path)})"


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


def census_sql(columns: ColumnPlan, left: Path, right: Path) -> str:
    """Row counts per key group and nothing else, which is all leave-one-out needs.

    Unordered, and that is not a choice a caller gets: this exists for the
    attribution pass, which drops one key column at a time and asks how many
    rows failed to pair. `side_sql` explains why the sort is pure cost for that
    question. The parameter was here to let a caller ask for the sort and the
    one caller has always passed False.
    """
    return (
        f"{paired_sql(columns, left, right, ordered=False)},"
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
    the artifact's 1,000 paired rows down to the six hundred or so that moved.
    The step reads two million rows to produce those 1,000, and this query never
    sees them.
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
