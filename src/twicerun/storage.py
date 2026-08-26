"""The interface a pipeline has to be written against for twicerun to see it.

There is no way to observe an arbitrary pipeline's writes without either a
kernel block-layer wrapper or system-call interception, and both are ruled out
here. What is left is an injected interface the pipeline reads and writes
through, which is portable, needs no root, and only sees pipelines that opted
in. That last part is the project's first limitation, not a footnote.

What the interface buys back is that one abstraction carries four jobs: it is
where artifacts get captured, where reads get redirected for containment, where
input rows get counted for the tolerance bound, and where amplified inputs will
be substituted. A step calling `ctx.write()` gets all four. A step calling
`duckdb.execute("COPY ... TO ...")` behind its back gets none.

`ctx.sql` is the seam in that argument and it is deliberate rather than
overlooked. A step needs to run DDL and statements like MERGE that are not a
single SELECT, so the escape hatch has to exist. What it costs is that anything
`ctx.sql` scans is uncounted and anything it writes outside `ctx.write` is
uncaptured. The reference pipeline uses it three times, all inside
`apply_price_updates`, and that step's `rows_read` is short by at least 300,000
as a result. Narrowing this is slice 2's problem, since slice 2 is the first
thing that consumes the number.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from twicerun.manifest import Artifact


class MissingArtifact(LookupError):
    """A step read something no earlier step in this run wrote."""


def quote(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


class StepContext:
    """Handed to one step of one run. Not reused across steps.

    `written` is the live index of artifacts this run has produced, keyed by
    name, and `carried` is the same index from the run before. Two dictionaries
    rather than a store object because artifact resolution is the thing slice 3
    changes to implement containment, and keeping it as plain lookups here means
    that change lands in one place.
    """

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        run: int,
        step_index: int,
        step_name: str,
        run_dir: Path,
        written: dict[str, Artifact],
        carried: dict[str, Artifact],
    ) -> None:
        self._con = con
        self._run = run
        self._step_index = step_index
        self._step_name = step_name
        self._step_dir = run_dir / f"step-{step_index:02d}-{step_name}"
        self._written = written
        self._carried = carried
        self.rows_read = 0
        # Attribution needs to tell a column this step invented from one it
        # copied in, because on the row_number bug two columns explain the
        # divergence equally well and only one of them is the step's doing.
        self.input_columns: set[str] = set()

    def read(self, name: str) -> duckdb.DuckDBPyRelation:
        """Bring an artifact written earlier in this run into scope as a view."""
        artifact = self._written.get(name)
        if artifact is None:
            known = ", ".join(sorted(self._written)) or "nothing yet"
            raise MissingArtifact(
                f"step {self._step_index} ({self._step_name}) read {name!r}, "
                f"which no earlier step wrote. Available: {known}"
            )
        return self._view_over(name, artifact)

    def state(self, name: str, initial: str) -> duckdb.DuckDBPyRelation:
        """Bring the *previous run's* copy of an artifact into scope.

        This is what separates a pipeline rerun from calling a function twice.
        A step that appends to a table is reading state its own last execution
        left behind, and a checker that starts every run from an empty directory
        can never see the duplication that causes. On the first run there is no
        previous copy, so `initial` builds one.

        The reference pipeline's `append_audit_log` is where this is genuinely
        load-bearing: without it that step cannot diverge at all. Its
        `apply_price_updates` reads carried state too, and 25,000 of its 125,000
        rows survive each merge untouched, but that step would still diverge if
        the seed were rebuilt from scratch every run. Worth keeping the two
        apart, because only the first is an argument for this method existing.
        """
        artifact = self._carried.get(name)
        if artifact is None:
            self._con.execute(f'CREATE OR REPLACE VIEW "{name}" AS {initial}')
            self.rows_read += self._count(name)
            self.input_columns.update(self._columns(name))
            return self._con.table(name)
        return self._view_over(name, artifact)

    def sql(self, query: str) -> duckdb.DuckDBPyRelation | None:
        """Run a statement the other three methods cannot express, such as MERGE.

        Returns None for anything without a result set, which is what DuckDB
        does. Nothing here is counted or captured; see the module docstring.
        """
        return self._con.sql(query)

    def write(self, name: str, query: str) -> Artifact:
        self._step_dir.mkdir(parents=True, exist_ok=True)
        parquet = self._step_dir / f"{name}.parquet"
        self._con.execute(f"COPY ({query}) TO {quote(parquet)} (FORMAT PARQUET)")

        schema = self._con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({quote(parquet)})"
        ).fetchall()
        rows = self._con.execute(
            f"SELECT count(*) FROM read_parquet({quote(parquet)})"
        ).fetchone()[0]

        artifact = Artifact(
            name=name,
            run=self._run,
            step_index=self._step_index,
            step=self._step_name,
            path=str(parquet),
            rows=rows,
            columns={column: kind for column, kind, *_ in schema},
            size_bytes=parquet.stat().st_size,
        )
        self._written[name] = artifact
        return artifact

    def _view_over(self, name: str, artifact: Artifact) -> duckdb.DuckDBPyRelation:
        self._con.execute(
            f'CREATE OR REPLACE VIEW "{name}" AS '
            f"SELECT * FROM read_parquet({quote(Path(artifact.path))})"
        )
        self.rows_read += artifact.rows
        self.input_columns.update(artifact.columns)
        return self._con.table(name)

    def _count(self, view: str) -> int:
        return self._con.execute(f'SELECT count(*) FROM "{view}"').fetchone()[0]

    def _columns(self, view: str) -> list[str]:
        described = self._con.execute(f'DESCRIBE SELECT * FROM "{view}"').fetchall()
        return [name for name, *_ in described]
