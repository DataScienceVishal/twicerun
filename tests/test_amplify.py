"""What each amplifier does to an input, asserted without waiting on a race.

An amplifier is a pure function from one Parquet file to another, so the part
that has to hold every time can be checked every time: row count preserved,
types preserved, distinct count actually lower, and the column a correct query
would break the tie on left alone. The probabilistic half, whether the fire rate
went up, is measured by running the reference pipeline and lives in the README.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from twicerun.amplify import (
    NotApplicable,
    Substituted,
    amplified_threads,
    row_multiplication,
    thread_count,
    tie_collapse,
)


@pytest.fixture
def sparse(make_artifact):
    """500,000 rows, two per value of cust, every event_id unique.

    The configuration the surrogate-key bug is intermittent at, and the one the
    tie-collapse amplifier exists to move.
    """
    return make_artifact(
        "sparse_customers",
        "SELECT (i % 250000)::INTEGER AS cust, i AS event_id FROM range(500000) AS s(i)",
    )


def counted(con: duckdb.DuckDBPyConnection, path: str, column: str) -> tuple[int, int]:
    return con.execute(
        f'SELECT count(*), count(DISTINCT "{column}") FROM read_parquet(\'{path}\')'
    ).fetchone()


def test_tie_collapse_lowers_the_distinct_count_of_the_column_a_sort_would_tie_on(
    con, tmp_path, sparse
):
    attempt = tie_collapse(con, tmp_path / "amp", {"sparse_customers": sparse}, threads=1)
    assert isinstance(attempt, Substituted)

    rows, distinct = counted(con, attempt.artifacts["sparse_customers"].path, "cust")
    assert rows == sparse.rows
    assert distinct == 1000
    assert "cust into 1,000 buckets, from 2 to 500 rows per value" in attempt.note


def test_tie_collapse_leaves_the_most_distinct_column_alone_so_a_tiebreak_survives(
    con, tmp_path, sparse
):
    """The property that separates an amplifier from a chaos generator.

    `ORDER BY cust, event_id` is only stable while `event_id` is unique. If this
    amplifier collapsed it too, the correct version of the surrogate-key step
    would start diverging and the tool would be measuring its own damage.
    """
    attempt = tie_collapse(con, tmp_path / "amp", {"sparse_customers": sparse}, threads=1)
    amplified = attempt.artifacts["sparse_customers"]

    rows, distinct = counted(con, amplified.path, "event_id")
    assert distinct == rows == sparse.rows

    tied = con.execute(
        f"SELECT count(*) FROM (SELECT cust, event_id FROM read_parquet('{amplified.path}') "
        f"GROUP BY 1, 2 HAVING count(*) > 1)"
    ).fetchone()[0]
    assert tied == 0


def test_tie_collapse_keeps_the_column_type_it_was_given(con, tmp_path, sparse):
    """A widened column would arrive at the comparison as a SCHEMA divergence.

    That would be the amplifier reporting itself. INTEGER out of a UBIGINT hash
    is the case that needs the bucket representative rather than a cast.
    """
    attempt = tie_collapse(con, tmp_path / "amp", {"sparse_customers": sparse}, threads=1)
    assert attempt.artifacts["sparse_customers"].columns == sparse.columns


def test_tie_collapse_refuses_an_artifact_whose_ties_are_already_denser_than_it_targets(
    con, tmp_path, make_artifact
):
    dense = make_artifact(
        "orders",
        "SELECT (i % 10)::INTEGER AS day, i AS order_id FROM range(100000) AS s(i)",
    )
    attempt = tie_collapse(con, tmp_path / "amp", {"orders": dense}, threads=1)
    assert isinstance(attempt, NotApplicable)
    assert "already at or past 500 rows per value" in attempt.reason


def test_tie_collapse_refuses_when_collapsing_would_leave_no_tiebreak(
    con, tmp_path, make_artifact
):
    only_one = make_artifact("ids", "SELECT i AS event_id FROM range(1000) AS s(i)")
    attempt = tie_collapse(con, tmp_path / "amp", {"ids": only_one}, threads=1)
    assert isinstance(attempt, NotApplicable)
    assert "no exact column to collapse that is not its only one" in attempt.reason


def test_tie_collapse_reports_the_artifacts_it_skipped_beside_the_one_it_changed(
    con, tmp_path, make_artifact, sparse
):
    """A step reading two artifacts where only one can be collapsed.

    The note has to carry both halves or the reader cannot tell an amplifier
    that touched everything from one that touched a corner.
    """
    dense = make_artifact(
        "orders", "SELECT (i % 10)::INTEGER AS day, i AS order_id FROM range(100000) AS s(i)"
    )
    attempt = tie_collapse(
        con, tmp_path / "amp", {"sparse_customers": sparse, "orders": dense}, threads=1
    )
    assert isinstance(attempt, Substituted)
    assert "sparse_customers.cust" in attempt.note
    assert "orders is already at or past" in attempt.note


def test_row_multiplication_doubles_the_rows_and_keeps_the_schema(con, tmp_path, sparse):
    attempt = row_multiplication(con, tmp_path / "amp", {"sparse_customers": sparse}, threads=1)
    amplified = attempt.artifacts["sparse_customers"]
    assert amplified.rows == sparse.rows * 2
    assert amplified.columns == sparse.columns


def test_row_multiplication_refuses_an_empty_input(con, tmp_path, make_artifact):
    empty = make_artifact("nothing", "SELECT 1 AS i WHERE false")
    attempt = row_multiplication(con, tmp_path / "amp", {"nothing": empty}, threads=1)
    assert isinstance(attempt, NotApplicable)
    assert "duplicating rows is a no-op" in attempt.reason


@pytest.mark.parametrize("amplifier", [tie_collapse, row_multiplication])
def test_the_two_data_amplifiers_refuse_a_step_that_reads_nothing(con, tmp_path, amplifier):
    """A generator step is the realistic case, and it is the control step here.

    It has to come out as an axis that was not varied rather than as an axis
    that came back quiet, because the difference between those two is the whole
    NO_DIVERGENCE_OBSERVED report.
    """
    attempt = amplifier(con, tmp_path / "amp", {}, threads=1)
    assert isinstance(attempt, NotApplicable)
    assert "reads no artifact" in attempt.reason


def test_thread_count_is_the_one_amplifier_a_step_reading_nothing_still_gets(con, tmp_path):
    attempt = thread_count(con, tmp_path / "amp", {}, threads=2)
    assert isinstance(attempt, Substituted)
    assert attempt.threads == 4


def test_thread_count_refuses_rather_than_re_running_at_the_count_it_already_had(con, tmp_path):
    attempt = thread_count(con, tmp_path / "amp", {}, threads=64)
    assert isinstance(attempt, NotApplicable)
    assert "already 64" in attempt.reason


@pytest.mark.parametrize(
    ("default", "raised"),
    [(1, 4), (2, 4), (4, 8), (10, 20), (40, 64), (64, 64)],
)
def test_the_thread_floor_clears_where_the_mechanism_switches_on(default, raised):
    """Doubling a default of 1 or 2 lands short of where the float sum starts moving.

    Measured on this machine: 1 of 8 comparisons at threads=2, 8 of 8 at
    threads=4. A floor of 4 is the difference between an amplifier and a second
    look at the same thing.
    """
    assert amplified_threads(default) == raised


def test_an_amplified_input_is_a_file_so_all_three_runs_read_the_same_bytes(
    con, tmp_path, sparse
):
    """Regenerating the input per run would put the amplifier's own noise in the answer.

    Nothing in the collapse is order-dependent, `min()` over an exact column
    being the reason, but the runs read one path rather than re-deriving it, and
    that is what the assertion below pins.
    """
    first = tie_collapse(con, tmp_path / "a", {"sparse_customers": sparse}, threads=1)
    second = tie_collapse(con, tmp_path / "b", {"sparse_customers": sparse}, threads=1)
    left = Path(first.artifacts["sparse_customers"].path)
    right = Path(second.artifacts["sparse_customers"].path)

    differing = con.execute(
        f"SELECT count(*) FROM ("
        f"  (SELECT * FROM read_parquet('{left}') EXCEPT ALL SELECT * FROM read_parquet('{right}'))"
        f"  UNION ALL"
        f"  (SELECT * FROM read_parquet('{right}') EXCEPT ALL SELECT * FROM read_parquet('{left}'))"
        f")"
    ).fetchone()[0]
    assert differing == 0
