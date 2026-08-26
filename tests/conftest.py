from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from twicerun.manifest import Artifact
from twicerun.storage import StepContext


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """Single-threaded on purpose.

    These tests are about the comparison logic, not about DuckDB's parallelism.
    Letting the thread pool loose here would make a suite that fails now and
    then for reasons unrelated to whatever it is asserting.
    """
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    yield connection
    connection.close()


@pytest.fixture
def make_artifact(con: duckdb.DuckDBPyConnection, tmp_path: Path):
    written: dict[str, Artifact] = {}

    def build(name: str, query: str, *, run: int = 1, step: int = 0) -> Artifact:
        ctx = StepContext(
            con,
            run=run,
            step_index=step,
            step_name="fixture",
            run_dir=tmp_path / f"run-{run:02d}",
            written=written,
            carried={},
        )
        return ctx.write(name, query)

    return build
