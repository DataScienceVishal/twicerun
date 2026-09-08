"""What each run wrote, where, and under what conditions.

Every DuckDB number this project reports depends on the version and the thread
count, so both are recorded next to the artifacts rather than left to whoever
reads the report to remember.

`StepRecord.rows_read` feeds the reassociation bound in `policy.py`, which is a
function of the term count. This file recorded it and flagged two limits before
anything read it. The bound is the consumer that had to decide what to do about
them, and it kept the number as it is. Here is what that costs in each
direction.

It is a lower bound on rows scanned, not a count of them. Only `ctx.read` and
`ctx.state` add to it, so anything a step pulls in through `ctx.sql` is
invisible: `apply_price_updates` records 300,000 while its two CREATE TABLE AS
statements scan at least 300,000 more. That makes n too small, the bound too
tight, and the tool report a difference reassociation could in fact explain. A
false positive, which is the direction to err in.

And "brought into scope" is not "terms behind one output value", which is what
the bound actually wants. `daily_revenue` records 2,000,000 for a step whose
1,000 output rows each sum about 2,000 terms, so n is too large by roughly the
output row count, the bound too loose, and a difference reassociation cannot
explain could be tolerated. That is the unsafe direction and for a group-by it
dominates the other one.

Getting the quantity the bound wants needs the query plan, which is out of
scope. Deriving it as rows_read divided by the output row count is only correct
for a single-input aggregate over equal-sized groups. So the number stays as it
is and every report prints what the headroom would be at the narrower count, so
the slack is visible rather than described.

`StepRecord.input_columns` is the other consumer: leave-one-out attribution uses
it to tell a column the step invented from one it copied in.
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
    # What the step pulled in through ctx.read. Amplification substitutes these
    # and nothing else: the artifacts written before a step are everything the
    # pipeline has produced by then, 42 MB of Parquet on this pipeline, where
    # `roll_up_keys` reads one 5 MB artifact out of that. Rewriting the lot per
    # amplifier attempt is disk nobody asked for, and the figure here said
    # 205 MB until someone measured a run directory.
    reads: list[str] = field(default_factory=list)
    # Names this step had to read from its own run because run 1 never wrote
    # them. Empty on every run of a pipeline whose steps write the same
    # artifacts every time, which is why it is worth recording: when it is not
    # empty, the report's containment line would otherwise be claiming more
    # than the run did.
    uncontained_reads: list[str] = field(default_factory=list)
    artifacts: list[Artifact] = field(default_factory=list)


@dataclass
class RunRecord:
    run: int
    seconds: float = 0.0
    steps: list[StepRecord] = field(default_factory=list)


@dataclass
class AmplifiedRuns:
    """One amplifier's re-executions of one step, kept rather than summarised.

    Same reasoning as the bisect below: `judge` re-derives the fire rate from
    these artifacts through the comparison code the run used, instead of reading
    back a number this file could have got wrong. `note` is the exception,
    because what the amplifier did to the input is not recoverable from the
    output and there is nowhere else to put it.
    """

    step_index: int
    amplifier: str
    note: str
    runs: list[RunRecord] = field(default_factory=list)
    error: str | None = None


@dataclass
class Manifest:
    pipeline: str
    root: Path
    environment: Environment
    contained: bool = True
    started: str = field(default_factory=lambda: datetime.now(UTC).isoformat(timespec="seconds"))
    runs: list[RunRecord] = field(default_factory=list)
    # The single-threaded re-executions, holding only the steps that diverged.
    # Recorded rather than summarised into a fire rate, so `judge` re-derives
    # that rate from the artifacts through the same comparison code the run
    # used instead of trusting a number this file could have got wrong.
    bisect: list[RunRecord] = field(default_factory=list)
    # The amplified re-executions, one entry per (step, amplifier) pair that was
    # attempted, including the pairs where the amplifier declined to change
    # anything. Those carry their reason and no runs, and they are the half of
    # the report that says which axes were not varied.
    amplified: list[AmplifiedRuns] = field(default_factory=list)
    # What stopped the bisect, where something did. It is kept because a run
    # whose bisect failed is not a run whose steps have no cause, and judging
    # the saved manifest later has no other way to tell those apart.
    bisect_error: str | None = None

    def save(self) -> Path:
        where = self.root / "manifest.json"
        body = dataclasses.asdict(self)
        body["root"] = str(self.root)
        where.write_text(json.dumps(body, indent=2), encoding="utf-8")
        return where

    @classmethod
    def load(cls, where: Path) -> Manifest:
        """Read a saved run back, so it can be judged again without re-running it.

        This exists because the claim that a policy cannot move a measured
        number was only checkable by reading a test. Two invocations of the run
        command execute the pipeline twice and the figures move between them for
        the reason the whole project is about, so the two documented commands
        looked like they refuted the paragraph describing them.
        """
        body = json.loads(where.read_text(encoding="utf-8"))
        return cls(
            pipeline=body["pipeline"],
            root=Path(body["root"]),
            environment=Environment(**body["environment"]),
            # A manifest without the key was written before containment existed,
            # so False is not a fallback here, it is the truth about that run.
            contained=body.get("contained", False),
            started=body["started"],
            runs=[_run_record(run) for run in body["runs"]],
            bisect=[_run_record(run) for run in body.get("bisect", [])],
            bisect_error=body.get("bisect_error"),
            amplified=[
                AmplifiedRuns(
                    step_index=entry["step_index"],
                    amplifier=entry["amplifier"],
                    note=entry["note"],
                    runs=[_run_record(run) for run in entry["runs"]],
                    error=entry.get("error"),
                )
                for entry in body.get("amplified", [])
            ],
        )


def _run_record(body: dict) -> RunRecord:
    return RunRecord(
        run=body["run"],
        seconds=body["seconds"],
        steps=[
            StepRecord(
                index=step["index"],
                name=step["name"],
                seconds=step["seconds"],
                rows_read=step["rows_read"],
                input_columns=step["input_columns"],
                reads=step.get("reads", []),
                uncontained_reads=step.get("uncontained_reads", []),
                artifacts=[Artifact(**a) for a in step["artifacts"]],
            )
            for step in body["steps"]
        ],
    )
