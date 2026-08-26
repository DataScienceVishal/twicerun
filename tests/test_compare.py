from __future__ import annotations

from twicerun.compare import compare, row_digest

ROWS = "SELECT i AS id, (i % 7)::VARCHAR AS label, (i * 1.5)::DOUBLE AS score "\
       "FROM range(500) AS s(i)"


def test_identical_artifacts_do_not_diverge(con, make_artifact):
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact("t", ROWS, run=2)
    assert compare(con, left, right).diverged is False


def test_row_order_is_not_part_of_the_answer(con, make_artifact):
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact("t", f"SELECT * FROM ({ROWS}) ORDER BY id DESC", run=2)
    assert compare(con, left, right).diverged is False


def test_one_changed_row_shows_on_both_sides(con, make_artifact):
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact(
        "t",
        f"SELECT id, label, CASE WHEN id = 3 THEN 0.0 ELSE score END AS score FROM ({ROWS})",
        run=2,
    )
    diff = compare(con, left, right)
    assert (diff.only_in_reference, diff.only_in_candidate) == (1, 1)


def test_a_duplicated_row_is_extra_not_changed(con, make_artifact):
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact("t", f"SELECT * FROM ({ROWS}) UNION ALL SELECT * FROM ({ROWS})", run=2)
    diff = compare(con, left, right)
    assert (diff.only_in_reference, diff.only_in_candidate) == (0, 500)


def test_missing_rows_are_attributed_to_the_reference_side(con, make_artifact):
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact("t", f"SELECT * FROM ({ROWS}) WHERE id >= 10", run=2)
    diff = compare(con, left, right)
    assert (diff.only_in_reference, diff.only_in_candidate) == (10, 0)


def test_nulls_do_not_shift_between_columns(con, make_artifact):
    left = make_artifact("t", "SELECT 'a' AS x, NULL::VARCHAR AS y", run=1)
    right = make_artifact("t", "SELECT NULL::VARCHAR AS x, 'a' AS y", run=2)
    assert compare(con, left, right).diverged is True


def test_a_null_is_distinct_from_the_text_it_would_print_as(con, make_artifact):
    left = make_artifact("t", "SELECT NULL::VARCHAR AS x", run=1)
    right = make_artifact("t", "SELECT '~' AS x", run=2)
    assert compare(con, left, right).diverged is True


def test_negative_zero_is_not_zero(con, make_artifact):
    """Bit-exact means bit-exact. -0.0 == 0.0 in IEEE-754 and they differ here.

    Written as a multiplication because DuckDB's parser folds the literal
    (-0.0)::DOUBLE to positive zero, so the obvious spelling tests nothing.
    """
    left = make_artifact("t", "SELECT (-1.0::DOUBLE * 0.0::DOUBLE) AS v", run=1)
    right = make_artifact("t", "SELECT (0.0)::DOUBLE AS v", run=2)
    assert compare(con, left, right).diverged is True


def test_one_ulp_of_drift_counts_as_divergence(con, make_artifact):
    """The false positive this slice exists to produce, in its smallest form."""
    left = make_artifact("t", "SELECT (1.0)::DOUBLE AS v", run=1)
    right = make_artifact("t", "SELECT (1.0 + 2.220446049250313e-16)::DOUBLE AS v", run=2)
    assert compare(con, left, right).diverged is True


def test_nan_matches_nan(con, make_artifact):
    left = make_artifact("t", "SELECT ('nan')::DOUBLE AS v", run=1)
    right = make_artifact("t", "SELECT ('nan')::DOUBLE AS v", run=2)
    assert compare(con, left, right).diverged is False


def test_a_new_column_stops_the_row_comparison(con, make_artifact):
    left = make_artifact("t", "SELECT 1 AS a", run=1)
    right = make_artifact("t", "SELECT 1 AS a, 2 AS b", run=2)
    diff = compare(con, left, right)
    assert diff.diverged is True
    assert "added ['b']" in diff.schema_note
    assert (diff.only_in_reference, diff.only_in_candidate) == (0, 0)


def test_a_widened_column_is_reported_as_a_type_change(con, make_artifact):
    left = make_artifact("t", "SELECT 1::INTEGER AS a", run=1)
    right = make_artifact("t", "SELECT 1::BIGINT AS a", run=2)
    diff = compare(con, left, right)
    assert diff.schema_note == "column types changed: a INTEGER to BIGINT"


def test_digest_sql_quotes_awkward_column_names():
    assert '"order by"' in row_digest(["order by"])
