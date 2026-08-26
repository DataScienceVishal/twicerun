#!/usr/bin/env python3
"""Re-derive every DuckDB number the twicerun spec quotes, on the machine quoting it.

    uv run python scripts/measure_duckdb.py

The counts move between runs, because the thing being measured is itself
non-deterministic. What has to hold every time: the parallel figures are large,
the threads=1 figures are zero, and count() never moves. Those three are
asserted at the end and the script exits non-zero if any of them breaks.

Printing is unbuffered on purpose. The first attempt at the spec's version of
this script printed nothing for ten minutes, and there was no way to tell a slow
measurement from a hung one.

Every row-level comparison happens in SQL rather than by fetching rows into
Python. That is not tidiness. Pulling the 500,000-row window result back through
`fetchall()` stalled twice in four runs of this script, once for seven and a half
minutes on a query that executes in six milliseconds, spinning inside
`BatchedBufferedData::ExecuteTaskInternal`. Comparing in SQL removes the fetch
path, and it is also what twicerun itself does.
"""

from __future__ import annotations

import platform
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb

FIXED_THREADS = 8  # what the spec's numbers were taken at
DEFAULT_THREADS = duckdb.connect().execute("SELECT current_setting('threads')").fetchone()[0]


@contextmanager
def session(threads: int):
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    try:
        yield con
    finally:
        con.close()


def say(line: str) -> None:
    print(line, flush=True)


def float_group_sums(threads: int, rows: int = 2_000_000, groups: int = 1_000) -> int:
    """Groups whose sum changes between two identical queries in one session."""
    with session(threads) as con:
        con.execute(
            f"CREATE TABLE t AS SELECT (i % {groups}) AS g, random() AS v "
            f"FROM range({rows}) AS s(i)"
        )
        q = "SELECT g, sum(v) AS s FROM t GROUP BY g ORDER BY g"
        first, second = con.execute(q).fetchall(), con.execute(q).fetchall()
    return sum(1 for (_, x), (_, y) in zip(first, second, strict=True) if x != y)


def float_group_sums_across_parquet(
    threads: int, rows: int = 2_000_000, groups: int = 1_000
) -> int:
    """The same measurement through a Parquet file, one fresh connection per query.

    Not in the spec, and it is the measurement twicerun's design actually rests
    on: the tool compares separate runs reading artifacts off disk, so if the
    divergence were an artifact of reusing one session the N-run loop would be
    measuring nothing. Values come from hash(i) rather than random() so both
    connections read identical bytes.

    Note what is not used here: an inline `SELECT ... FROM range(n)` source.
    DuckDB splits a range deterministically across threads, so that formulation
    reports 0 differing groups and looks like a clean bill of health. The
    divergence needs a real scan, either a Parquet file or a materialised table.
    """
    values = "hash(i)::DOUBLE / 18446744073709551615.0"
    with tempfile.TemporaryDirectory() as scratch:
        parquet = Path(scratch) / "source.parquet"
        with session(threads) as con:
            con.execute(
                f"COPY (SELECT (i % {groups}) AS g, {values} AS v FROM range({rows}) AS s(i)) "
                f"TO '{parquet}' (FORMAT PARQUET)"
            )
        q = f"SELECT g, sum(v) AS s FROM read_parquet('{parquet}') GROUP BY g ORDER BY g"
        sums = []
        for _ in range(2):
            with session(threads) as con:
                sums.append(con.execute(q).fetchall())
    return sum(1 for (_, x), (_, y) in zip(*sums, strict=True) if x != y)


def global_sum_spread(threads: int, trials: int = 5) -> int:
    """Distinct values a single global sum() takes across identical runs."""
    with session(threads) as con:
        con.execute("CREATE TABLE t AS SELECT random() AS v FROM range(2_000_000)")
        seen = {con.execute("SELECT sum(v) FROM t").fetchone()[0] for _ in range(trials)}
    return len(seen)


def count_stability(threads: int, trials: int = 3) -> int:
    """The control. Integer arithmetic cannot reassociate wrongly, so this is always 1."""
    with session(threads) as con:
        con.execute("CREATE TABLE t AS SELECT (i % 1000) AS g FROM range(2_000_000) AS s(i)")
        q = "SELECT g, count(*) FROM t GROUP BY g ORDER BY g"
        seen = {tuple(con.execute(q).fetchall()) for _ in range(trials)}
    return len(seen)


def surrogate_keys(threads: int, rows: int, tie_groups: int, order_by: str = "cust") -> int:
    """Rows assigned a different row_number() across two identical runs.

    Tie density is the variable. Sorting on a column with few duplicates leaves
    the parallel merge little freedom; at 500 rows per distinct value it has a
    great deal. Each run is materialised into its own table so the optimiser
    cannot notice the two queries are identical and evaluate them once.
    """
    window = f"SELECT id, row_number() OVER (ORDER BY {order_by}) AS rn FROM k"
    with session(threads) as con:
        con.execute(
            f"CREATE TABLE k AS SELECT (i % {tie_groups}) AS cust, i AS id "
            f"FROM range({rows}) AS s(i)"
        )
        con.execute(f"CREATE TABLE first AS {window}")
        con.execute(f"CREATE TABLE second AS {window}")
        moved = con.execute(
            "SELECT count(*) FROM first JOIN second USING (id) WHERE first.rn <> second.rn"
        ).fetchone()[0]
    return moved


def merge_answers(threads: int, targets: int, runs: int, dedupe: bool = False) -> int:
    """Distinct answers a MERGE gives across `runs` fresh connections.

    Two source rows carry a different value for every target key, so whichever
    candidate the MERGE picks is visible in the answer. Nothing in the statement
    pins the choice. `dedupe` collapses the source first, which is the twin.

    Each run gets its own connection and rebuilds both tables. An earlier version
    reused one connection and refilled the target with DELETE then INSERT, which
    made run 1 disagree with runs 2 to 4 every single time. That is repeatable,
    so it is not this bug; it is the physical row order left behind by a delete.
    """
    source_side = "(SELECT id, max(val) AS val FROM source GROUP BY id)" if dedupe else "source"
    seen = set()
    for _ in range(runs):
        with session(threads) as con:
            con.execute(
                f"CREATE TABLE target AS SELECT i AS id, i * 10 AS val "
                f"FROM range({targets}) AS s(i)"
            )
            con.execute(
                f"CREATE TABLE source AS SELECT (i % {targets}) AS id, 1000000 + i AS val "
                f"FROM range({targets * 2}) AS s(i)"
            )
            con.execute(
                f"MERGE INTO target t USING {source_side} s ON t.id = s.id "
                f"WHEN MATCHED THEN UPDATE SET val = s.val"
            )
            # Integer sum over row hashes: order-independent and, unlike a float
            # sum, incapable of reassociating into a different answer.
            seen.add(con.execute("SELECT sum(hash(id, val)::HUGEINT) FROM target").fetchone()[0])
    return len(seen)


def main() -> int:
    say(f"duckdb {duckdb.__version__} on {platform.platform()}")
    say(f"python {platform.python_version()}, default threads {DEFAULT_THREADS}\n")

    started = time.time()

    say("Float aggregation, 2,000,000 rows in 1,000 groups, two identical queries")
    parallel_groups = float_group_sums(FIXED_THREADS)
    serial_groups = float_group_sums(1)
    say(f"  threads={FIXED_THREADS}   group sums differing: {parallel_groups}/1000")
    say(f"  threads=1   group sums differing: {serial_groups}/1000")
    say(f"  threads={DEFAULT_THREADS} (default)  group sums differing: "
        f"{float_group_sums(DEFAULT_THREADS)}/1000")
    across = float_group_sums_across_parquet(DEFAULT_THREADS)
    say(f"  threads={DEFAULT_THREADS}  via Parquet, fresh connection per query: {across}/1000")
    say(f"  threads={FIXED_THREADS}   distinct global sum() values over 5 runs: "
        f"{global_sum_spread(FIXED_THREADS)}")
    stable = count_stability(FIXED_THREADS)
    say(f"  threads={FIXED_THREADS}   distinct count() results over 3 runs: {stable} "
        f"(1 means stable)\n")

    say("Surrogate keys, row_number() OVER (ORDER BY <non-unique>), 500,000 rows")
    dense = surrogate_keys(FIXED_THREADS, 500_000, 1_000)
    say(f"  threads={FIXED_THREADS}   500 rows per tie group: moved {dense:,}/500,000")
    dense_serial = surrogate_keys(1, 500_000, 1_000)
    say(f"  threads=1   500 rows per tie group: moved {dense_serial:,}/500,000")
    tiebroken = surrogate_keys(FIXED_THREADS, 500_000, 1_000, order_by="cust, id")
    say(f"  threads={FIXED_THREADS}   500 rows per tie group, ORDER BY cust, id: "
        f"moved {tiebroken:,}/500,000")

    # One observation of an intermittent phenomenon is worth nothing, and the
    # spec's claim about this case rests on two. Six is not many either, but it
    # is enough to show the spread rather than a single number.
    sparse = [surrogate_keys(FIXED_THREADS, 500_000, 250_000) for _ in range(6)]
    say(f"  threads={FIXED_THREADS}   2 rows per tie group, six attempts: "
        f"{', '.join(f'{n:,}' for n in sparse)}")
    say(f"  threads=1   2 rows per tie group: "
        f"moved {surrogate_keys(1, 500_000, 250_000):,}/500,000")

    say("\nSurrogate keys, 2,000,000 rows, 1,000 rows per tie group")
    big = surrogate_keys(FIXED_THREADS, 2_000_000, 2_000)
    say(f"  threads={FIXED_THREADS}   moved {big:,}/2,000,000")

    say("\nMERGE with two source rows per target key, distinct answers over 10 fresh connections")
    by_scale = {n: merge_answers(FIXED_THREADS, n, 10) for n in (1_000, 50_000, 100_000)}
    for targets, answers in by_scale.items():
        say(f"  threads={FIXED_THREADS}   {targets:>7,} target rows: {answers} of 10")
    merge_parallel = by_scale[100_000]
    merge_serial = merge_answers(1, 100_000, 10)
    say(f"  threads=1   100,000 target rows: {merge_serial} of 10")
    merge_twin = merge_answers(FIXED_THREADS, 100_000, 10, dedupe=True)
    say(f"  threads={FIXED_THREADS}   100,000 target rows, source deduplicated first: "
        f"{merge_twin} of 10")

    say(f"\nelapsed {time.time() - started:.1f}s")

    broken = []
    if parallel_groups == 0:
        broken.append("float group sums did not diverge at threads=8")
    if across == 0:
        broken.append(
            "float group sums did not diverge across connections, so the N-run loop is blind"
        )
    if serial_groups != 0:
        broken.append(f"float group sums diverged at threads=1 ({serial_groups}/1000)")
    if stable != 1:
        broken.append(f"count() was not stable ({stable} distinct results)")
    if dense == 0:
        broken.append("row_number() at 500 rows per tie group did not diverge")
    if dense_serial != 0:
        broken.append(f"row_number() diverged at threads=1 ({dense_serial} rows)")
    if tiebroken != 0:
        broken.append(f"the unique tiebreak did not fix row_number() ({tiebroken} rows)")
    if merge_parallel == 1:
        broken.append("MERGE gave one answer at threads=8, so bug 3 is gone from this machine")
    if merge_serial != 1:
        broken.append(
            f"MERGE diverged at threads=1 ({merge_serial} answers), which the spec expected "
            f"and this machine has never once shown"
        )
    if merge_twin != 1:
        broken.append(f"deduplicating the source did not fix MERGE ({merge_twin} answers)")

    if broken:
        say("\nInvariants that failed:")
        for line in broken:
            say(f"  {line}")
        return 1
    say("invariants hold: parallel large, serial zero, count() stable, tiebreak clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
