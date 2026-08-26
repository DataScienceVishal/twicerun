from __future__ import annotations

from pathlib import Path

import pytest

from twicerun.manifest import Artifact
from twicerun.storage import MissingArtifact, StepContext, quote


def context(con, tmp_path, *, run=1, index=0, name="step", written=None, carried=None):
    return StepContext(
        con,
        run=run,
        step_index=index,
        step_name=name,
        run_dir=tmp_path / f"run-{run:02d}",
        written=written if written is not None else {},
        carried=carried if carried is not None else {},
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
    assert quote(Path("/tmp/vishal's runs/a.parquet")) == "'/tmp/vishal''s runs/a.parquet'"
