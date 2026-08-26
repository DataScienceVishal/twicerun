from __future__ import annotations

import pytest

from twicerun.columns import (
    ColumnPlan,
    FloatInKey,
    Partition,
    UnknownKeyColumn,
    UnsupportedColumn,
    classify,
    clock_columns,
    plan,
)


@pytest.mark.parametrize(
    "sql_type",
    ["TINYINT", "INTEGER", "BIGINT", "HUGEINT", "UBIGINT", "BOOLEAN", "DATE",
     "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "VARCHAR", "BLOB", "UUID",
     "DECIMAL(18,4)", "ENUM('a', 'b')", "INTERVAL"],
)
def test_types_that_cannot_reassociate_are_exact(sql_type):
    assert classify(sql_type) is Partition.EXACT


@pytest.mark.parametrize("sql_type", ["FLOAT", "DOUBLE", "REAL", "double"])
def test_the_two_float_widths_are_approximate(sql_type):
    assert classify(sql_type) is Partition.APPROX


@pytest.mark.parametrize(
    "sql_type",
    ["INTEGER[]", "DOUBLE[3]", "STRUCT(a INTEGER)", "MAP(VARCHAR, INTEGER)",
     "UNION(n INTEGER, s VARCHAR)"],
)
def test_nested_types_are_refused(sql_type):
    assert classify(sql_type) is Partition.UNSUPPORTED


def test_a_type_the_classifier_has_never_seen_is_refused_rather_than_guessed():
    """The branch that matters when a future DuckDB adds a type.

    Falling through to exact would compare it, silently, with whatever
    equality DuckDB happens to give the new type.
    """
    assert classify("GEOMETRY") is Partition.UNSUPPORTED


def test_refusing_names_the_column_and_its_type():
    with pytest.raises(UnsupportedColumn) as raised:
        plan({"id": "BIGINT", "tags": "VARCHAR[]"})
    assert "tags (VARCHAR[])" in str(raised.value)


def test_every_exact_column_is_in_the_key_by_default():
    made = plan({"day": "INTEGER", "label": "VARCHAR", "revenue": "DOUBLE"})
    assert made.key == ("day", "label")
    assert made.approx_values() == ("revenue",)
    assert made.exact_values() == ()


def test_a_float_never_lands_in_the_key_on_its_own():
    made = plan({"amount": "DOUBLE", "fee": "FLOAT"})
    assert made.key == ()
    assert made.approx_values() == ("amount", "fee")


def test_key_override_pushes_the_other_exact_columns_into_the_value_side():
    made = plan({"day": "INTEGER", "label": "VARCHAR", "revenue": "DOUBLE"}, key_override=["day"])
    assert made.key == ("day",)
    assert made.exact_values() == ("label",)
    assert made.approx_values() == ("revenue",)


def test_key_override_cannot_put_a_float_in_the_key():
    """It is not a preference. It would delete the tool's own falsifiable check.

    A float in the key is matched with bit equality, so one ulp of
    reassociation arrives as a missing row plus an extra row, the step reports
    no drift, and the reassociation bound section including the pre-registered
    headroom check drops out of the report with no warning anywhere.
    """
    with pytest.raises(FloatInKey) as raised:
        plan({"day": "INTEGER", "revenue": "DOUBLE"}, key_override=["day", "revenue"])
    assert "revenue (DOUBLE)" in str(raised.value)


def test_key_override_naming_a_column_that_is_not_there_says_what_is():
    with pytest.raises(UnknownKeyColumn) as raised:
        plan({"day": "INTEGER"}, key_override=["dya"])
    assert "It has day" in str(raised.value)


def test_the_ordinal_sorts_by_floats_first_then_by_exact_leftovers():
    made = plan(
        {"day": "INTEGER", "label": "VARCHAR", "revenue": "DOUBLE"}, key_override=["day"]
    )
    assert made.ordinal_order() == ("revenue", "label")


def test_timestamps_are_flagged_for_the_wall_clock_hint_but_stay_exact():
    columns = {"seen_at": "TIMESTAMP", "day": "DATE", "n": "BIGINT"}
    assert clock_columns(columns, columns) == ("seen_at", "day")
    assert plan(columns).key == ("seen_at", "day", "n")


def test_the_plan_covers_every_type_duckdb_gives_the_reference_pipeline(con):
    """Guards against the classifier and DuckDB's DESCRIBE drifting apart.

    Every type below is one the pipeline or its fixtures actually produce, and
    a rename in a DuckDB release would otherwise turn into an UnsupportedColumn
    at run time rather than a test failure here.
    """
    described = con.execute(
        "DESCRIBE SELECT 1::INTEGER AS a, 1::BIGINT AS b, 1.0::DOUBLE AS c, "
        "'x' AS d, DATE '2026-01-01' AS e, 1.0::DECIMAL(18,4) AS f"
    ).fetchall()
    columns = {name: kind for name, kind, *_ in described}
    made = plan(columns)
    assert made.key == ("a", "b", "d", "e", "f")
    assert made.approx_values() == ("c",)


def test_a_plan_with_no_key_still_reports_its_value_columns():
    made = ColumnPlan(types={"v": "DOUBLE"}, partition={"v": Partition.APPROX}, key=())
    assert made.values == ("v",)


def test_duckdb_still_spells_a_list_type_with_brackets(con):
    """The classifier reads the type string, so the string is the contract."""
    kind = con.execute("DESCRIBE SELECT [1, 2] AS xs").fetchall()[0][1]
    assert kind.endswith("]")
    assert classify(kind) is Partition.UNSUPPORTED
