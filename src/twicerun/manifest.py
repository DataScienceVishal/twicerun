"""What each run wrote, where, and under what conditions.

Every DuckDB number this project reports depends on the version and the thread
count, so both are recorded next to the artifacts rather than left to whoever
reads the report to remember.

`StepRecord.rows_read` is here for slice 2's reassociation bound, which is a
function of the term count, and the storage interface is the only thing that
sees it. Two things about it are worth stating before slice 2 leans on it.

It is a lower bound, not a count of rows scanned. Only `ctx.read` and
`ctx.state` add to it, so anything a step pulls in through `ctx.sql` is
invisible: `apply_price_updates` records 300,000 while its two CREATE TABLE AS
statements scan at least 300,000 more.

And "brought into scope" is not "scanned" even where it is counted.
`daily_revenue` records 2,000,000 for a step whose query reads `orders` once,
and would record the same 2,000,000 if the query read it twice. The bound needs
terms per aggregate, which is a narrower quantity than either. Slice 2 has to
derive that rather than assume this field already is it.
"""

from __future__ import annotations

import dataclasses
import json
import platform
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from twicerun import __version__


@dataclass(frozen=True)
class Environment:
    duckdb_version: str
    threads: int
    platform: str
    python: str
    twicerun: str = __version__

    @classmethod
    def observe(cls, con: duckdb.DuckDBPyConnection) -> Environment:
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        return cls(
            duckdb_version=duckdb.__version__,
            threads=int(threads),
            platform=platform.platform(),
            python=".".join(str(n) for n in sys.version_info[:3]),
        )


@dataclass(frozen=True)
class Artifact:
    """One Parquet file, addressed by (run, step index, name)."""

    name: str
    run: int
    step_index: int
    step: str
    path: str
    rows: int
    columns: dict[str, str]
    size_bytes: int


@dataclass
class StepRecord:
    index: int
    name: str
    seconds: float
    rows_read: int
    input_columns: list[str] = field(default_factory=list)
    artifacts: list[Artifact] = field(default_factory=list)


@dataclass
class RunRecord:
    run: int
    seconds: float = 0.0
    steps: list[StepRecord] = field(default_factory=list)


@dataclass
class Manifest:
    pipeline: str
    root: Path
    environment: Environment
    started: str = field(default_factory=lambda: datetime.now(UTC).isoformat(timespec="seconds"))
    runs: list[RunRecord] = field(default_factory=list)

    def save(self) -> Path:
        where = self.root / "manifest.json"
        body = dataclasses.asdict(self)
        body["root"] = str(self.root)
        where.write_text(json.dumps(body, indent=2), encoding="utf-8")
        return where
