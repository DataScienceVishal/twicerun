"""Changing a step's input so that a broken step fires more often than it otherwise would.

Running the pipeline more times lowers the chance of missing a step that
diverges with probability p. It does not touch p. Amplification touches p, by
handing the step an input built to make the mechanism fire, and that is the
division of labour between the two. At p = 0.2 five runs still miss 41 percent
of the time, and twenty runs leave a 15 percent upper bound on p when nothing
fires, so no practical run count settles the question on its own.

Substituting the input is legitimate because this tool is not checking that the
answer is right. Reproducibility is a property of the code rather than of the
data, so an input with the same types and the same row count is a fair question
to ask of a step, and a step that only reproduces on the data it happened to be
given is fragile.

What keeps this a detector rather than a chaos generator is one property that
has to hold for every amplifier here: the bug's matched twin has to survive it.
Tie collapse must not break `ORDER BY cust, event_id`, and row multiplication
must not break `CREATE OR REPLACE TABLE`. An amplifier that makes correct code
fail is measuring its own violence and nothing else. `pipelines/twins.py` is the
one-line fix for every bug in the reference pipeline, and the README publishes
what each amplifier did to it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

import duckdb

from twicerun.columns import Partition, classify
from twicerun.manifest import Artifact
from twicerun.sql import identifier
from twicerun.storage import quote

DIVERGENT = "DIVERGENT"
STABLE_ON_THIS_INPUT = "STABLE_ON_THIS_INPUT"
NO_DIVERGENCE_OBSERVED = "NO_DIVERGENCE_OBSERVED"
AMPLIFICATION_FAILED = "AMPLIFICATION_FAILED"

# Three runs give two comparisons, which is enough while an amplifier is doing
# its job of pushing p towards 1. Anything that fires is re-run at the main
# loop's N so the two rates can be read against each other, so the cost only
# rises where something was found.
PROBE_RUNS = 3

# Rows per distinct value that tie collapse aims for. The surrogate-key bug was
# measured at 2 rows per tie group and at 500: standalone, 500 fired on every
# attempt while 2 fired on roughly a third of them.
TARGET_ROWS_PER_VALUE = 500

# One duplicate is the smallest change that manufactures a repeated key where
# there was none, which is what the MERGE bug needs, and it doubles the artifact
# rather than tripling the disk.
DUPLICATION = 2

THREAD_FLOOR = 4
# DuckDB gives every thread its own buffers, and the measurement behind the floor
# is flat from 4 threads upward, so there is nothing above this to buy.
THREAD_CEILING = 64

# The axes this tool does not touch. Printed under every NO_DIVERGENCE_OBSERVED
# next to the ones it does, because that pair of lists is the only honest
# version of "never fired" against "cannot fire" a black-box tester can give.
NOT_VARIED = (
    "input distribution beyond what the amplifiers above changed",
    "memory limit and spill behaviour",
    "DuckDB version",
    "wall clock",
    "filesystem",
)


@dataclass(frozen=True)
class Substituted:
    """The inputs one amplifier wants a step re-executed against."""

    artifacts: dict[str, Artifact]
    note: str
    threads: int | None = None


@dataclass(frozen=True)
class NotApplicable:
    reason: str


Attempt = Substituted | NotApplicable


@dataclass(frozen=True)
class Amplification:
    """One amplifier's attempt on one step, scored.

    `note` carries what the amplifier did when it ran and why it did not when it
    could not, because both belong in the same column of the report. A reader
    deciding what a zero is worth needs the reason as much as the rate.
    """

    amplifier: str
    note: str
    comparisons: int = 0
    fired: int = 0
    artifacts_compared: int = 0
    error: str | None = None

    @property
    def ran(self) -> bool:
        """Whether any comparison happened, whatever went wrong afterwards.

        `error` is deliberately not part of this. An amplifier can fire on its
        first two comparisons and then raise on the fourth, and an error that
        arrives after evidence does not delete the evidence. Treating the two as
        exclusive turned a step the tool had watched give two different answers
        into AMPLIFICATION_FAILED, which is the one status that does not gate a
        release, so a crash during escalation returned green on exactly the
        failure this feature exists to catch.
        """
        return self.comparisons > 0

    @property
    def measured(self) -> bool:
        """Whether the re-execution compared any artifact at all.

        A step re-executed on its own can write nothing, most often by checking
        for a table a skipped step would have created. Its rate then comes out
        0 of 2 from two comparisons of nothing and reads exactly like two clean
        ones. The main loop and the bisect both refuse to call that clean and
        this is the third loop, so it refuses too: an amplifier that compared
        nothing is an axis that was not varied, not an axis that came back
        quiet.
        """
        return self.ran and self.artifacts_compared > 0


def amplified_threads(default: int) -> int:
    """Where the thread-count amplifier sets `threads`.

    Twice the machine's default, with a floor of 4 that is the measured part.
    The float aggregate over 2,000,000 rows fired on 1 of 8 comparisons at
    threads=2 and on 8 of 8 at threads=4, so doubling a default of 1 or 2 lands
    short of where the mechanism switches on. Above 4 that measurement is flat,
    8 of 8 at 4, 10, 20 and 40 alike, which is why this amplifier finds nothing
    on a laptop whose default is already 10 and why the README says so rather
    than implying all three amplifiers earn their place everywhere.

    The ceiling is where the doubling stops being worth its memory, and it is
    also what makes the refusal below reachable: a machine already running 64
    threads gets told this amplifier has nothing to add rather than being sent
    to 128.
    """
    return min(THREAD_CEILING, max(THREAD_FLOOR, default * 2))


def tie_collapse(
    con: duckdb.DuckDBPyConnection, into: Path, inputs: Mapping[str, Artifact], threads: int
) -> Attempt:
    """Hash a column's values into buckets so that many rows come to share one value.

    Aimed at the surrogate-key bug. `row_number() OVER (ORDER BY cust)` cannot
    disagree with itself unless `cust` has ties, and firing probability climbs
    with tie density: 2 rows per group over 500,000 rows fired on 10 of 12
    comparisons here, 500 rows per group on 11 of 12, and the standalone
    measurement behind the spec fired at 500 on every attempt.

    The column with the most distinct values is left alone, and that is the
    twin-survival property rather than an implementation detail. `ORDER BY cust,
    event_id` is stable only while `event_id` breaks the tie, so an amplifier
    that collapsed every column would break correct code.

    Each non-NULL value is replaced by another real value from the same column,
    the minimum of its bucket, so the type, the row count, the NULL count and
    the value domain all survive. Casting a hash back to the column's type would
    have worked for integers and not for DATE or DECIMAL.

    NULLs needed the special case in `_collapse_sql` rather than falling out.
    `hash(NULL)` is an ordinary non-NULL constant, so a NULL row lands in a
    bucket with real values and takes their representative: 300 NULLs in 3,000
    rows came out as zero NULLs, on a transformation whose report line claims
    the domain survives. Row count and type did survive, which is why it read as
    fine. A step that branches on NULL was being asked a different question from
    the one printed.
    """
    if not inputs:
        return NotApplicable("this step reads no artifact, so there is no input to substitute")

    collapsed: dict[str, Artifact] = {}
    notes, refusals = [], []
    for name, artifact in inputs.items():
        exact = [c for c, kind in artifact.columns.items() if classify(kind) is Partition.EXACT]
        if len(exact) < 2:
            refusals.append(f"{name} has no exact column to collapse that is not its only one")
            continue
        counts = _distinct_counts(con, Path(artifact.path), exact)
        buckets = max(1, artifact.rows // TARGET_ROWS_PER_VALUE)
        tiebreak, *rest = sorted(exact, key=lambda c: (-counts[c], c))
        collapsing = [c for c in rest if counts[c] > buckets]
        if not collapsing:
            refusals.append(
                f"{name} is already at or past {TARGET_ROWS_PER_VALUE:,} rows per value on every "
                f"column but {tiebreak}"
            )
            continue
        query = _collapse_sql(Path(artifact.path), list(artifact.columns), collapsing, buckets)
        collapsed[name] = _rewrite(con, artifact, into / f"{name}.parquet", query)
        was = min(artifact.rows / max(counts[c], 1) for c in collapsing)
        notes.append(
            f"{name}.{', '.join(collapsing)} into {buckets:,} "
            f"bucket{'' if buckets == 1 else 's'}, from {was:,.0f} to "
            f"{artifact.rows / buckets:,.0f} rows per value"
        )

    if not collapsed:
        return NotApplicable("; ".join(refusals))
    return Substituted({**inputs, **collapsed}, note="; ".join(notes + refusals))


def row_multiplication(
    con: duckdb.DuckDBPyConnection, into: Path, inputs: Mapping[str, Artifact], threads: int
) -> Attempt:
    """Duplicate every input row, so a source that held one row per key now holds two.

    Aimed at the append and MERGE bugs. The MERGE only disagrees with itself
    when the source carries two candidate rows for one target key, and nothing
    in the statement decides which wins; duplication manufactures that where the
    real input did not have it. An append with no unique key duplicates whatever
    it is fed, so more rows means a louder finding rather than a different one.

    `CREATE OR REPLACE TABLE` and a MERGE over a deduplicated source both give
    the same answer on twice the rows, which is the twin-survival half.
    """
    if not inputs:
        return NotApplicable("this step reads no artifact, so there is no input to substitute")
    if not any(a.rows for a in inputs.values()):
        return NotApplicable(
            "every artifact this step reads is empty, so duplicating rows is a no-op"
        )

    multiplied, notes = {}, []
    for name, artifact in inputs.items():
        query = (
            f"SELECT src.* FROM read_parquet({quote(Path(artifact.path))}) AS src "
            f"CROSS JOIN range({DUPLICATION})"
        )
        multiplied[name] = _rewrite(con, artifact, into / f"{name}.parquet", query)
        notes.append(f"{name} {artifact.rows:,} rows to {multiplied[name].rows:,}")
    return Substituted({**inputs, **multiplied}, note="; ".join(notes))


def thread_count(
    con: duckdb.DuckDBPyConnection, into: Path, inputs: Mapping[str, Artifact], threads: int
) -> Attempt:
    """Re-execute on the same input with the thread pool raised.

    The only amplifier that changes nothing about the data, which is why it is
    the one that can be applied to a step reading no artifact at all. It is also
    the one whose worth depends entirely on the machine: on a box where DuckDB
    already defaults to more threads than the mechanism needs, raising it finds
    nothing, and the report prints both counts so a reader can see which case
    they are in.
    """
    raised = amplified_threads(threads)
    if raised <= threads:
        return NotApplicable(f"threads is already {threads}, which is at or above the {raised} "
                             f"this would set")
    return Substituted(
        dict(inputs),
        note=f"threads raised to {raised} from the {threads} in the loop above",
        threads=raised,
    )


# Ordered as the spec lists them, so a report puts the three in the same order
# every time and the one aimed at the loudest measured bug comes first.
AMPLIFIERS = (
    ("tie collapse", tie_collapse),
    ("thread count", thread_count),
    ("row multiplication", row_multiplication),
)


def _distinct_counts(
    con: duckdb.DuckDBPyConnection, path: Path, columns: Sequence[str]
) -> dict[str, int]:
    counted = ", ".join(f"count(DISTINCT {identifier(c)})" for c in columns)
    measured = con.execute(f"SELECT {counted} FROM read_parquet({quote(path)})").fetchone()
    return {column: int(n) for column, n in zip(columns, measured, strict=True)}


def _collapse_sql(
    path: Path, columns: Sequence[str], collapsing: Sequence[str], buckets: int
) -> str:
    """One pass per collapsed column to pick its bucket representatives, then one join each.

    The representative is `min()` over an exact column, which is the same value
    however the scan is divided, so the amplified input is a file rather than a
    thing that has to be regenerated identically for each of the three runs.
    """
    bucket_of = {c: identifier(f"__bucket_{c}") for c in collapsing}
    reps = ", ".join(
        f"{bucket_of[c]} AS (SELECT hash({identifier(c)}) % {buckets} AS bucket, "
        f"min({identifier(c)}) AS rep FROM src GROUP BY 1)"
        for c in collapsing
    )
    projected = ", ".join(
        f"CASE WHEN src.{identifier(c)} IS NULL THEN NULL ELSE {bucket_of[c]}.rep END "
        f"AS {identifier(c)}"
        if c in collapsing
        else f"src.{identifier(c)}"
        for c in columns
    )
    joins = " ".join(
        f"JOIN {bucket_of[c]} ON hash(src.{identifier(c)}) % {buckets} = {bucket_of[c]}.bucket"
        for c in collapsing
    )
    return (
        f"WITH src AS (SELECT * FROM read_parquet({quote(path)})), {reps} "
        f"SELECT {projected} FROM src {joins}"
    )


def _rewrite(
    con: duckdb.DuckDBPyConnection, source: Artifact, into: Path, query: str
) -> Artifact:
    """Materialise an amplified input, keeping everything about it that is not the point.

    Run 0 is not a run. These files are derived from run 1's artifacts and no
    execution of the pipeline produced them, so numbering them as one would put
    a run in the manifest that never happened.
    """
    into.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {quote(into)} (FORMAT PARQUET)")
    described = con.execute(f"DESCRIBE SELECT * FROM read_parquet({quote(into)})").fetchall()
    rows = con.execute(f"SELECT count(*) FROM read_parquet({quote(into)})").fetchone()[0]
    return replace(
        source,
        run=0,
        path=str(into),
        rows=rows,
        columns={column: kind for column, kind, *_ in described},
        size_bytes=into.stat().st_size,
    )
