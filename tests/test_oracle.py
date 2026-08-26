"""Pure tests over hand-built artifact pairs.

Nothing here involves DuckDB's parallelism. Every divergence below is
constructed, so the class the oracle assigns is checkable against an answer
worked out by hand rather than against whatever a race happened to produce.
"""

from __future__ import annotations

import pytest

from twicerun.columns import UnknownKeyColumn, UnsupportedColumn
from twicerun.oracle import Divergence, compare

ROWS = "SELECT i AS id, (i % 7)::VARCHAR AS label, (i * 1.5)::DOUBLE AS score "\
       "FROM range(500) AS s(i)"


def test_identical_artifacts_pair_every_row(con, make_artifact):
    found = compare(con, make_artifact("t", ROWS, run=1), make_artifact("t", ROWS, run=2))
    assert found.classes == frozenset()
    assert found.matched == 500


def test_row_order_is_still_not_part_of_the_answer(con, make_artifact):
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact("t", f"SELECT * FROM ({ROWS}) ORDER BY id DESC", run=2)
    assert compare(con, left, right).diverged is False


def test_a_key_present_in_one_run_only_is_missing_and_extra(con, make_artifact):
    """The row_number shape: an exact column moved, so both sides lose a row."""
    left = make_artifact("t", "SELECT 1 AS id, 10 AS sid", run=1)
    right = make_artifact("t", "SELECT 1 AS id, 11 AS sid", run=2)
    found = compare(con, left, right)
    assert found.classes == {Divergence.ROW_MISSING, Divergence.ROW_EXTRA}
    assert (found.row_missing, found.row_extra, found.multiplicity) == (1, 1, 0)


def test_dropped_rows_are_missing_and_nothing_else(con, make_artifact):
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact("t", f"SELECT * FROM ({ROWS}) WHERE id >= 10", run=2)
    found = compare(con, left, right)
    assert found.classes == {Divergence.ROW_MISSING}
    assert found.row_missing == 10
    assert found.matched == 490


def test_a_duplicated_row_is_multiplicity_not_an_extra_key(con, make_artifact):
    """The INSERT-without-a-key case. Every key is still there; the counts moved.

    A join on the key alone would fan out to 1,000 pairs here and report
    nothing wrong, which is why the ordinal is in the join condition.
    """
    left = make_artifact("t", ROWS, run=1)
    right = make_artifact("t", f"SELECT * FROM ({ROWS}) UNION ALL SELECT * FROM ({ROWS})", run=2)
    found = compare(con, left, right)
    assert found.classes == {Divergence.MULTIPLICITY}
    assert found.multiplicity == 500
    assert (found.row_missing, found.row_extra) == (0, 0)


def test_multiplicity_is_counted_on_whichever_side_lost_rows(con, make_artifact):
    left = make_artifact("t", "SELECT 1 AS id FROM range(5)", run=1)
    right = make_artifact("t", "SELECT 1 AS id FROM range(2)", run=2)
    found = compare(con, left, right)
    assert (found.multiplicity_reference, found.multiplicity_candidate) == (3, 0)
    assert found.matched == 2


def test_every_row_lands_in_exactly_one_bucket(con, make_artifact):
    """The accounting invariant. Without it a class could double count silently."""
    left = make_artifact("t", "SELECT i % 3 AS g FROM range(30) AS s(i)", run=1)
    right = make_artifact(
        "t", "SELECT i % 4 AS g FROM range(20) AS s(i) UNION ALL SELECT 0", run=2
    )
    found = compare(con, left, right)
    assert found.matched + found.unmatched_reference == found.reference_rows
    assert found.matched + found.unmatched_candidate == found.candidate_rows


def test_a_null_key_still_pairs_with_itself(con, make_artifact):
    """Plain equality would leave every NULL-keyed row unmatched against a copy.

    That is a row that did not move being reported as two divergences, which is
    why the join uses IS NOT DISTINCT FROM.
    """
    both = "SELECT NULL::VARCHAR AS label, 1.0::DOUBLE AS v"
    left, right = make_artifact("t", both, run=1), make_artifact("t", both, run=2)
    assert compare(con, left, right).matched == 1


def test_a_schema_change_stops_the_row_comparison(con, make_artifact):
    left = make_artifact("t", "SELECT 1 AS a", run=1)
    right = make_artifact("t", "SELECT 1 AS a, 2 AS b", run=2)
    found = compare(con, left, right)
    assert found.classes == {Divergence.SCHEMA}
    assert found.schema_note == "schema changed: columns added b"
    assert found.matched == 0


def test_a_nested_column_is_refused_by_name(con, make_artifact):
    left = make_artifact("t", "SELECT 1 AS id, [1, 2] AS xs", run=1)
    right = make_artifact("t", "SELECT 1 AS id, [1, 3] AS xs", run=2)
    with pytest.raises(UnsupportedColumn, match="xs"):
        compare(con, left, right)


def test_key_override_narrows_what_counts_as_the_same_row(con, make_artifact):
    """With sid in the key these are two different rows; with only id, one row."""
    left = make_artifact("t", "SELECT 1 AS id, 10 AS sid", run=1)
    right = make_artifact("t", "SELECT 1 AS id, 11 AS sid", run=2)
    assert compare(con, left, right, key_override=["id"]).classes == frozenset()
    assert compare(con, left, right).classes == {Divergence.ROW_MISSING, Divergence.ROW_EXTRA}


def test_key_override_that_names_nothing_real_is_refused(con, make_artifact):
    left = make_artifact("t", "SELECT 1 AS id", run=1)
    right = make_artifact("t", "SELECT 1 AS id", run=2)
    with pytest.raises(UnknownKeyColumn, match="idd"):
        compare(con, left, right, key_override=["idd"])


def test_an_all_float_artifact_pairs_by_sorted_order(con, make_artifact):
    """No exact column means no key, so the whole artifact is one group.

    Sorted-to-sorted pairing is what is left, and it is the right answer here:
    the same three values in a different row order have not diverged.
    """
    left = make_artifact("t", "SELECT unnest([1.0, 2.0, 3.0])::DOUBLE AS v", run=1)
    right = make_artifact("t", "SELECT unnest([3.0, 1.0, 2.0])::DOUBLE AS v", run=2)
    found = compare(con, left, right)
    assert found.key == ()
    assert found.matched == 3
