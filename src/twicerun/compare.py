"""Bit-exact multiset comparison of two artifacts.

Row order is not part of a pipeline's answer. DuckDB will hand back the same
group-by in a different order between two runs and nothing is wrong, so the
comparison is over multisets: hash each row into a canonical digest, count the
digests on both sides, and report how many rows one side has that the other
does not.

Bit-exact is the whole method here and it is knowingly the wrong answer for
floats. `sum()` over a DOUBLE column reassociates under parallelism and comes
back different in the last bits, so this comparison reports hundreds of
differences on arithmetic that is not wrong. Slice 2 replaces the float half of
this with a typed oracle. The count it reports first is the number that
justifies building one.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from twicerun.manifest import Artifact
from twicerun.storage import quote

FIELD_SEPARATOR = 31  # ASCII unit separator


def row_digest(columns: Iterable[str]) -> str:
    """SQL for a canonical 128-bit digest of a row.

    DuckDB casts a DOUBLE to the shortest decimal string that reads back as the
    same double, so the text is injective over the bit patterns: 0.0 and -0.0
    encode differently, and so do two doubles one ULP apart. That is what makes
    this bit-exact rather than approximately exact.

    Every field is prefixed with '=' before being joined, because concat_ws
    silently drops NULL arguments and ('a', NULL) would otherwise digest the
    same as ('a'). The prefix means no real value can collide with the NULL
    marker.
    """
    fields = ", ".join(f"coalesce('=' || \"{column}\"::VARCHAR, '~')" for column in columns)
    return f"md5(concat_ws(chr({FIELD_SEPARATOR}), {fields}))"


@dataclass(frozen=True)
class ArtifactDiff:
    name: str
    reference_rows: int
    candidate_rows: int
    only_in_reference: int
    only_in_candidate: int
    schema_note: str | None = None

    @property
    def diverged(self) -> bool:
        return bool(self.schema_note) or self.only_in_reference > 0 or self.only_in_candidate > 0

    def describe(self) -> str:
        if self.schema_note:
            return f"{self.name}: {self.schema_note}"
        return (
            f"{self.name}: {self.only_in_reference:,} rows only in the reference run, "
            f"{self.only_in_candidate:,} only in the later run, of {self.reference_rows:,}"
        )


def compare(
    con: duckdb.DuckDBPyConnection, reference: Artifact, candidate: Artifact
) -> ArtifactDiff:
    if reference.columns != candidate.columns:
        note = _schema_note(reference, candidate)
        return ArtifactDiff(
            name=reference.name,
            reference_rows=reference.rows,
            candidate_rows=candidate.rows,
            only_in_reference=0,
            only_in_candidate=0,
            schema_note=note,
        )

    digest = row_digest(reference.columns)
    left, right = quote(Path(reference.path)), quote(Path(candidate.path))
    unmatched = con.execute(
        f"""
        WITH reference AS (
            SELECT {digest} AS digest, count(*) AS n
            FROM read_parquet({left}) GROUP BY 1
        ), candidate AS (
            SELECT {digest} AS digest, count(*) AS n
            FROM read_parquet({right}) GROUP BY 1
        )
        SELECT
            coalesce(sum(greatest(coalesce(reference.n, 0) - coalesce(candidate.n, 0), 0)), 0),
            coalesce(sum(greatest(coalesce(candidate.n, 0) - coalesce(reference.n, 0), 0)), 0)
        FROM reference FULL OUTER JOIN candidate USING (digest)
        """
    ).fetchone()

    return ArtifactDiff(
        name=reference.name,
        reference_rows=reference.rows,
        candidate_rows=candidate.rows,
        only_in_reference=int(unmatched[0]),
        only_in_candidate=int(unmatched[1]),
    )


def _schema_note(reference: Artifact, candidate: Artifact) -> str:
    gone = sorted(set(reference.columns) - set(candidate.columns))
    added = sorted(set(candidate.columns) - set(reference.columns))
    if gone or added:
        return f"schema changed, columns dropped {gone or 'none'}, added {added or 'none'}"
    changed = [
        f"{column} {reference.columns[column]} to {candidate.columns[column]}"
        for column in reference.columns
        if reference.columns[column] != candidate.columns[column]
    ]
    return f"column types changed: {', '.join(changed)}"
