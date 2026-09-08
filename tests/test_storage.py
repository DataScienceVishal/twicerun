from __future__ import annotations

from pathlib import Path

import pytest

from twicerun.manifest import Artifact
from twicerun.storage import MissingArtifact, StepContext, quote


def context(
    con, tmp_path, *, run=1, index=0, name="step", written=None, carried=None, upstream=None
):
    return StepContext(
        con,
        run=run,
        step_index=index,
        step_name=name,
        run_dir=tmp_path / f"run-{run:02d}",
        written=written if written is not None else {},
        carried=carried if carried is not None else {},
        upstream=upstream,
    )


def test_write_records_rows_schema_and_a_path_under_the_step(con, tmp_path):
    ctx = context(con, tmp_path, index=3, name="load")
    artifact = ctx.write("sales", "SELECT i AS id, i::VARCHAR AS tag FROM range(40) AS s(i)")

    assert artifact.rows == 40
    assert artifact.columns == {"id": "BIGINT", "tag": "VARCHAR"}
    assert Path(artifact.path).parent.name == "step-03-load"
    assert Path(artifact.path).exists()
    assert artifact.size_bytes > 0


def test_read_brings_an_earlier_artifact_into_scope_by_name(con, tmp_path):
    written: dict[str, Artifact] = {}
    first = context(con, tmp_path, index=0, name="load", written=written)
    first.write("sales", "SELECT 7 AS id")

    second = context(con, tmp_path, index=1, name="total", written=written)
    second.read("sales")
    assert con.execute("SELECT id FROM sales").fetchone() == (7,)
    assert second.rows_read == 1


def test_a_contained_read_takes_run_ones_copy_over_this_runs_own(con, tmp_path):
    """Both exist and containment decides. This is the whole mechanism.

    The name resolves to two different files, so the later run computes from
    what run 1 produced rather than from what it produced itself one step ago.
    """
    mine: dict[str, Artifact] = {}
    context(con, tmp_path, run=2, written=mine).write("sales", "SELECT 2 AS id")
    theirs: dict[str, Artifact] = {}
    context(con, tmp_path, run=1, written=theirs).write("sales", "SELECT 1 AS id")

    contained = context(con, tmp_path, run=2, index=1, written=mine, upstream=theirs)
    contained.read("sales")
    assert con.execute("SELECT id FROM sales").fetchone() == (1,)
    assert contained.uncontained_reads == set()

    context(con, tmp_path, run=2, index=1, written=mine).read("sales")
    assert con.execute("SELECT id FROM sales").fetchone() == (2,)


def test_a_contained_read_run_one_cannot_answer_falls_back_and_records_it(con, tmp_path):
    mine: dict[str, Artifact] = {}
    context(con, tmp_path, run=2, written=mine).write("rejects", "SELECT 9 AS id")

    ctx = context(con, tmp_path, run=2, index=1, written=mine, upstream={})
    ctx.read("rejects")
    assert con.execute("SELECT id FROM rejects").fetchone() == (9,)
    assert ctx.uncontained_reads == {"rejects"}


def test_reading_something_nobody_wrote_names_what_is_available(con, tmp_path):
    written: dict[str, Artifact] = {}
    context(con, tmp_path, written=written).write("sales", "SELECT 1 AS id")
    ctx = context(con, tmp_path, index=1, name="total", written=written)

    with pytest.raises(MissingArtifact) as raised:
        ctx.read("saels")
    assert "Available: sales" in str(raised.value)


def test_state_falls_back_to_the_seed_on_the_first_run(con, tmp_path):
    ctx = context(con, tmp_path, run=1)
    ctx.state("ledger", "SELECT 0 AS id WHERE false")
    assert con.execute("SELECT count(*) FROM ledger").fetchone() == (0,)
    assert ctx.rows_read == 0


def test_state_picks_up_what_the_previous_run_left(con, tmp_path):
    first = context(con, tmp_path, run=1)
    carried = {"ledger": first.write("ledger", "SELECT i AS id FROM range(5) AS s(i)")}

    second = context(con, tmp_path, run=2, carried=carried)
    second.state("ledger", "SELECT 0 AS id WHERE false")
    assert con.execute("SELECT count(*) FROM ledger").fetchone() == (5,)
    assert second.rows_read == 5


def test_appending_to_carried_state_is_what_makes_a_rerun_different(con, tmp_path):
    """The INSERT-without-a-key bug, in miniature.

    A checker that starts every run from an empty directory cannot see this,
    which is why the interface carries state forward at all.
    """
    first = context(con, tmp_path, run=1)
    grow = "SELECT id FROM ledger UNION ALL SELECT i AS id FROM range(5) AS s(i)"
    first.state("ledger", "SELECT 0 AS id WHERE false")
    carried = {"ledger": first.write("ledger", grow)}

    second = context(con, tmp_path, run=2, carried=carried)
    second.state("ledger", "SELECT 0 AS id WHERE false")
    assert second.write("ledger", grow).rows == 10


def test_rows_read_accumulates_across_several_reads(con, tmp_path):
    written: dict[str, Artifact] = {}
    setup = context(con, tmp_path, written=written)
    setup.write("a", "SELECT i FROM range(3) AS s(i)")
    setup.write("b", "SELECT i FROM range(11) AS s(i)")

    ctx = context(con, tmp_path, index=1, written=written)
    ctx.read("a")
    ctx.read("b")
    assert ctx.rows_read == 14


def test_quote_survives_an_apostrophe_in_the_path():
    """A single quote in a path is what breaks a hand-built SQL string, so it is the fixture.

    Doubling is DuckDB's escape and there is no second one to check. The path
    that used to be here had a name in it, which read as decoration next to the
    apostrophe that is the whole test.
    """
    assert quote(Path("/tmp/it's here/a.parquet")) == "'/tmp/it''s here/a.parquet'"


def test_sql_runs_ddl_and_returns_nothing_for_it(con, tmp_path):
    ctx = context(con, tmp_path)
    assert ctx.sql("CREATE TABLE staged AS SELECT i FROM range(3) AS s(i)") is None
    assert con.execute("SELECT count(*) FROM staged").fetchone() == (3,)


def test_sql_hands_back_a_relation_for_a_select(con, tmp_path):
    ctx = context(con, tmp_path)
    assert ctx.sql("SELECT 41 + 1 AS answer").fetchone() == (42,)


def test_sql_lets_a_step_build_something_write_can_then_capture(con, tmp_path):
    """The reason the escape hatch exists: MERGE is not a single SELECT."""
    ctx = context(con, tmp_path)
    ctx.sql("CREATE TABLE target AS SELECT 1 AS id, 10 AS v")
    ctx.sql("CREATE TABLE source AS SELECT 1 AS id, 99 AS v")
    ctx.sql(
        "MERGE INTO target t USING source s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )
    assert ctx.write("merged", "SELECT * FROM target").rows == 1
    assert con.execute("SELECT v FROM target").fetchone() == (99,)


def test_rows_read_does_not_see_anything_pulled_in_through_sql(con, tmp_path):
    """The documented hole in rows_read, asserted rather than left as a comment.

    The reassociation bound in `policy.py` consumes this number, so the gap
    needs to be visible in the suite rather than discovered by whoever writes
    that bound.
    """
    ctx = context(con, tmp_path)
    ctx.sql("CREATE TABLE staged AS SELECT i FROM range(5000) AS s(i)")
    assert ctx.rows_read == 0
