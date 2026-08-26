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


def row_digest(columns: Iterable[str]) -> str:
    """SQL for a canonical 128-bit digest of a row.

    Each field is hashed on its own and the fixed-width hashes are concatenated,
    rather than joining the raw values with a separator and hashing once. A
    separator is not safe here: any byte chosen as one can occur inside a
    VARCHAR, so ('x', 'y<sep>=z') and ('x<sep>=y', 'z') join to the same string
    and digest identically. Both spellings were run and the collision is real.
    Fixed-width hashes cannot span a field boundary, so the ambiguity goes away
    at the cost of one md5 per column.

    Every field is prefixed with '=' before hashing, because a NULL has to be
    distinguishable from the text a value would print as, and '~' cannot be
    produced by any non-NULL value once every one of them starts with '='.

    On the encoding itself: DuckDB casts a DOUBLE to the shortest decimal string
    that reads back as the same double, so distinct finite doubles get distinct
    text. 0.0 and -0.0 differ, and so do two doubles one ULP apart. The one
    exception is NaN. Sign survives, as 'nan' against '-nan', but the payload
    does not: 7ff8000000000000 and 7ff8000000000001 both render 'nan' and this
    digest cannot tell them apart, before or after a Parquet round trip.
    """
    fields = " || ".join(
        f"md5(coalesce('=' || \"{column}\"::VARCHAR, '~'))" for column in columns
    )
    return f"md5({fields})"


@dataclass(frozen=True)
class ArtifactDiff:
    name: str
    reference_rows: int
    candidate_rows: int
    only_in_reference: int
    only_in_candidate: int
    note: str | None = None

    @property
    def diverged(self) -> bool:
        return bool(self.note) or self.only_in_reference > 0 or self.only_in_candidate > 0

    def describe(self) -> str:
        if self.note:
            return f"{self.name}: {self.note}"
        return (
            f"{self.name}: {self.only_in_reference:,} rows only in the reference run, "
            f"{self.only_in_candidate:,} only in the later run, of {self.reference_rows:,}"
        )


def compare(
    con: duckdb.DuckDBPyConnection, reference: Artifact, candidate: Artifact
) -> ArtifactDiff:
    if reference.columns != candidate.columns:
        note = _schema_change(reference, candidate)
        return ArtifactDiff(
            name=reference.name,
            reference_rows=reference.rows,
            candidate_rows=candidate.rows,
            only_in_reference=0,
            only_in_candidate=0,
            note=note,
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


def _schema_change(reference: Artifact, candidate: Artifact) -> str:
    """Every way the two schemas differ, not the first one found.

    A run that drops one column and widens another has done two things and the
    report should say both. Only reached when the column dictionaries differ, so
    at least one of the three clauses always fires.
    """
    gone = sorted(set(reference.columns) - set(candidate.columns))
    added = sorted(set(candidate.columns) - set(reference.columns))
    retyped = [
        f"{column} {reference.columns[column]} to {candidate.columns[column]}"
        for column in reference.columns
        if column in candidate.columns and reference.columns[column] != candidate.columns[column]
    ]

    parts = []
    if gone:
        parts.append(f"columns dropped {', '.join(gone)}")
    if added:
        parts.append(f"columns added {', '.join(added)}")
    if retyped:
        parts.append(f"types changed {', '.join(retyped)}")
    return "schema changed: " + "; ".join(parts)
