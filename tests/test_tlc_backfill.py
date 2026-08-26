"""The TLC backfill, run offline against three partitions built from the checked-in schema.

No TLC bytes are committed, for the licence reason in `scripts/fetch_tlc.py`,
and the suite runs under `--disable-socket` so it could not fetch them if it
wanted to. `pipelines/tlc_green_schema.sql` is what stands in: the real column
lists as DuckDB reads the real Parquet, at the twenty-column shape and the
twenty-one-column shape, so the one property the backfill is about survives.

What this cannot check is the parallel float reduction in `zone_revenue`. That
needs several hundred thousand rows before DuckDB divides the work at all, and
a suite that generated those would take a minute and still only fire sometimes.
`tests/test_runner.py` covers the loop deterministically with a fake wobbling
step; the real thing is measured in the README and by `scripts/measure_duckdb.py`.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pytest

from twicerun.cli import main

SCHEMA = Path(__file__).resolve().parent.parent / "pipelines" / "tlc_green_schema.sql"
PIPELINE = Path(__file__).resolve().parent.parent / "pipelines" / "tlc_backfill.py"

ROWS_PER_PARTITION = 900


def build_partitions(into: Path, rows: int = ROWS_PER_PARTITION) -> Path:
    """Three monthly Parquet files, the oldest one a column short, as TLC publishes them.

    Values come from the row index rather than from `random()`, so every run of
    the pipeline reads byte-identical input and a divergence is the pipeline's.
    """
    into.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(SCHEMA.read_text(encoding="utf-8"))

    for month, table in (
        ("2024-12", "green_tripdata_before_2025"),
        ("2025-01", "green_tripdata_from_2025"),
        ("2025-02", "green_tripdata_from_2025"),
    ):
        columns = [
            name for name, *_ in con.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
        ]
        projected = ", ".join(_generated(column, month) for column in columns)
        target = into / f"green_tripdata_{month}.parquet"
        con.execute(
            f"COPY (SELECT {projected} FROM range({rows}) AS s(i)) "
            f"TO '{target}' (FORMAT PARQUET)"
        )
    con.close()
    return into


def _generated(column: str, month: str) -> str:
    """One column's values, typed to match the schema file rather than to be interesting."""
    if column.endswith("_datetime"):
        return f"(TIMESTAMP '{month}-01 00:00:00' + INTERVAL (i % 600) MINUTE) AS {column}"
    if column == "store_and_fwd_flag":
        return f"CASE WHEN i % 50 = 0 THEN 'Y' ELSE 'N' END AS {column}"
    if column.endswith("LocationID"):
        return f"((i % 30) + 1)::INTEGER AS {column}"
    if column == "VendorID":
        return f"((i % 2) + 1)::INTEGER AS {column}"
    if column in {"RatecodeID", "passenger_count", "payment_type", "trip_type"}:
        return f"((i % 5) + 1)::BIGINT AS {column}"
    if column == "cbd_congestion_fee":
        # NULL on a quarter of them, because the real 2025 partitions have it
        # NULL on trips that never entered the zone: 91,123 of 94,947 rows are
        # populated. That matters to the assertion below, where the union makes
        # "this column did not exist yet" and "this trip was not charged"
        # indistinguishable.
        return f"CASE WHEN i % 4 = 0 THEN NULL ELSE (i % 4 * 0.75)::DOUBLE END AS {column}"
    return f"((i % 97) + 1)::DOUBLE / 7 AS {column}"


@pytest.fixture
def backfilled(tmp_path, monkeypatch, capsys):
    """One pass of the pipeline over generated partitions, with its report and run directory."""
    monkeypatch.setenv("TWICERUN_TLC_DIR", str(build_partitions(tmp_path / "tlc")))
    code = main(["run", str(PIPELINE), "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    return code, capsys.readouterr().out, tmp_path / "artifacts"


def step_line(printed: str, step: str) -> str:
    line = next((s for s in printed.splitlines() if re.match(rf"\s+\d+ {step}\b", s)), None)
    assert line is not None, f"no line for {step} in:\n{printed}"
    return line


def test_the_backfill_catches_the_reloaded_ledger(backfilled):
    code, printed, _ = backfilled
    assert code == 1
    assert "MULTIPLICITY" in step_line(printed, "revenue_ledger")
    assert "2 of 2" in step_line(printed, "revenue_ledger")


def test_the_landing_step_is_a_control_and_does_not_fire(backfilled):
    _, printed, _ = backfilled
    assert "0 of 2" in step_line(printed, "land_partitions")
    assert "0 of 2" in step_line(printed, "unify_partitions")


def test_the_2024_partition_lands_without_the_column_the_2025_ones_have(backfilled):
    """The whole reason this pipeline reads TLC data rather than generated data.

    Asserted on the artifacts rather than on the report, because the report says
    nothing about a schema that held still, and holding still is the thing being
    checked.
    """
    _, _, run_dir = backfilled
    con = duckdb.connect()
    landed = {
        month: {
            name
            for name, *_ in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{_artifact(run_dir, month)}')"
            ).fetchall()
        }
        for month in ("2024_12", "2025_01", "2025_02")
    }
    con.close()
    assert "cbd_congestion_fee" not in landed["2024_12"]
    assert "cbd_congestion_fee" in landed["2025_01"]
    assert landed["2025_01"] == landed["2025_02"]
    assert landed["2024_12"] | {"cbd_congestion_fee"} == landed["2025_01"]


def test_the_union_fills_the_missing_column_with_null_for_exactly_that_partition(backfilled):
    _, _, run_dir = backfilled
    unified = next(run_dir.glob("run-*/run-01/step-01-unify_partitions/trips.parquet"))
    con = duckdb.connect()
    per_month = con.execute(
        f"SELECT partition_month, count(*), count(cbd_congestion_fee) "
        f"FROM read_parquet('{unified}') GROUP BY 1 ORDER BY 1"
    ).fetchall()
    con.close()
    # The 2024 zero is the schema change. The 2025 shortfall is trips that were
    # never charged, and after the union nothing distinguishes the two.
    assert per_month == [
        ("2024-12", ROWS_PER_PARTITION, 0),
        ("2025-01", ROWS_PER_PARTITION, 675),
        ("2025-02", ROWS_PER_PARTITION, 675),
    ]


def test_a_missing_partition_names_the_script_that_fetches_it(tmp_path, monkeypatch, capsys):
    """Exit 3, because the data being absent is a crash rather than a divergence.

    Worth pinning: 1 is the code a release gate keys on, and a run that never
    started must not reach it.
    """
    monkeypatch.setenv("TWICERUN_TLC_DIR", str(tmp_path / "nothing-here"))
    code = main(["run", str(PIPELINE), "--runs", "2", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 3
    assert "scripts/fetch_tlc.py" in capsys.readouterr().err


def test_two_taxis_in_one_directory_is_refused(tmp_path, monkeypatch, capsys):
    into = build_partitions(tmp_path / "tlc")
    for month in ("2024-12", "2025-01", "2025-02"):
        (into / f"yellow_tripdata_{month}.parquet").write_bytes(
            (into / f"green_tripdata_{month}.parquet").read_bytes()
        )
    monkeypatch.setenv("TWICERUN_TLC_DIR", str(into))
    code = main(["run", str(PIPELINE), "--runs", "2", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 3
    assert "takes one taxi" in capsys.readouterr().err


def _artifact(run_dir: Path, month: str) -> Path:
    return next(run_dir.glob(f"run-*/run-01/step-00-land_partitions/trips_{month}.parquet"))
