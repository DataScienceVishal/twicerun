#!/usr/bin/env python3
"""What the cbd_congestion_fee schema change does to a backfill, measured on the real files.

    uv run python scripts/fetch_tlc.py
    uv run python scripts/tlc_schema.py

Three facts, and the middle one is the reason this script exists rather than a
paragraph in the README. Handed a list of Parquet files whose schemas differ,
DuckDB takes the column set from the first file and drops the rest without a
word. Put the 2024 partition first, which is the order a backfill walks its
months in, and `cbd_congestion_fee` disappears. Reverse the list and it is back.
No error, no warning, and a revenue column silently absent from the answer.

twicerun cannot catch that, and saying so is the point of running this next to
it. It is wrong in exactly the same way on every run, so five runs agree with
each other perfectly. A rerun checker is blind to a deterministic wrong answer
by construction, which is the sharpest limitation this project has and the one
worth demonstrating on data nobody here chose.

The three facts are asserted, so this exits non-zero if any of them stops being
true of the files on disk rather than printing something that reads fine.
"""

from __future__ import annotations

import os
import platform
import sys
from pathlib import Path

import duckdb

from twicerun.storage import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch_tlc import MONTHS  # noqa: E402

DATA = Path(os.environ.get("TWICERUN_TLC_DIR") or Path(__file__).resolve().parent.parent
            / "data" / "tlc")
NEW_IN_2025 = "cbd_congestion_fee"


def say(line: str = "") -> None:
    print(line, flush=True)


def partitions() -> list[Path]:
    found = [next(iter(sorted(DATA.glob(f"*_tripdata_{m}.parquet"))), None) for m in MONTHS]
    if any(p is None for p in found):
        raise SystemExit(
            f"no TLC partitions under {DATA}. Run: uv run python scripts/fetch_tlc.py"
        )
    return found


def columns_of(con: duckdb.DuckDBPyConnection, source: str) -> list[str]:
    return [name for name, *_ in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()]


def file_list(paths: list[Path]) -> str:
    return "[" + ", ".join(quote(p) for p in paths) + "]"


def main() -> int:
    files = partitions()
    con = duckdb.connect()

    # The column-drop below is DuckDB's behaviour rather than the files', so the
    # version belongs beside it for the same reason every report header carries it.
    say(f"duckdb {duckdb.__version__} on {platform.platform()}")
    say(f"three partitions from {DATA}\n")
    per_file = {}
    for path, month in zip(files, MONTHS, strict=True):
        held = columns_of(con, f"read_parquet({quote(path)})")
        per_file[month] = held
        rows = con.execute(f"SELECT count(*) FROM read_parquet({quote(path)})").fetchone()[0]
        say(f"  {path.name:<32} {rows:>10,} rows  {len(held)} columns  "
            f"{NEW_IN_2025}: {'present' if NEW_IN_2025 in held else 'absent'}")

    oldest_first = columns_of(con, f"read_parquet({file_list(files)})")
    newest_first = columns_of(con, f"read_parquet({file_list(files[::-1])})")
    say("\nread_parquet over the three as one list, which is what a backfill hands it:")
    say(f"  oldest partition first  {len(oldest_first)} columns  "
        f"{NEW_IN_2025}: {'present' if NEW_IN_2025 in oldest_first else 'DROPPED, silently'}")
    say(f"  newest partition first  {len(newest_first)} columns  "
        f"{NEW_IN_2025}: {'present' if NEW_IN_2025 in newest_first else 'dropped'}")

    say("\nthe two spellings that do not do that:")
    con.execute(f"CREATE TABLE target AS SELECT * FROM read_parquet({quote(files[0])})")
    try:
        con.execute(f"INSERT INTO target SELECT * FROM read_parquet({quote(files[1])})")
        insert_raised = ""
    except duckdb.BinderException as exc:
        insert_raised = str(exc).splitlines()[0]
    say(f"  INSERT INTO a target built from {MONTHS[0]}: "
        f"{insert_raised or 'accepted, which it should not have been'}")

    # The partition label has to be carried in, because the union is where the
    # provenance of a row stops being recoverable and that is the whole point.
    unioned = " UNION ALL BY NAME ".join(
        f"SELECT '{month}' AS partition_month, * FROM read_parquet({quote(path)})"
        for path, month in zip(files, MONTHS, strict=True)
    )
    con.execute(f"CREATE TABLE unified AS {unioned}")
    say(f"  UNION ALL BY NAME: {len(columns_of(con, 'unified')) - 1} columns, "
        f"{NEW_IN_2025} NULL where the partition had no such column")

    pickup = next(c for c in columns_of(con, "unified") if c.endswith("_pickup_datetime"))
    say("\nafter the union, per source partition:")
    for month, rows, charged, elsewhere in con.execute(
        f"SELECT partition_month, count(*), count({NEW_IN_2025}), "
        f"count(*) FILTER (strftime({pickup}, '%Y-%m') <> partition_month) "
        f"FROM unified GROUP BY 1 ORDER BY 1"
    ).fetchall():
        say(f"  {month}  {rows:>10,} rows  {charged:>10,} with a fee recorded  "
            f"{elsewhere:>6,} picked up in another month")
    say("  A 2024 NULL means the column did not exist. A 2025 NULL means the trip was not")
    say("  charged. After the union nothing tells those two apart, which is the cost of")
    say("  handling the schema change the correct way.")
    say("  The last column is a different problem and is real data being real: TLC files a")
    say("  trip by when it was reported, so a few dozen sit in a partition they were not")
    say("  picked up in. A backfill keyed on pickup month rather than on the partition puts")
    say("  them in the wrong month, and in a different wrong month once a neighbour reloads.")

    broken = []
    if NEW_IN_2025 in oldest_first:
        broken.append(
            f"read_parquet kept {NEW_IN_2025} with the 2024 partition first, so the silent "
            f"drop this script is about is not happening on these files"
        )
    if NEW_IN_2025 not in newest_first:
        broken.append(f"reversing the list did not bring {NEW_IN_2025} back either, so the "
                      f"column set is not being taken from the first file at all")
    if not insert_raised:
        broken.append("INSERT of a 21-column partition into a 20-column target was accepted")
    if per_file[MONTHS[0]] == per_file[MONTHS[1]]:
        broken.append(
            f"{MONTHS[0]} and {MONTHS[1]} have the same columns, so TLC has restated one of "
            f"them and this backfill no longer spans a schema change"
        )

    if broken:
        say("\nInvariants that failed:")
        for line in broken:
            say(f"  {line}")
        return 1
    say(f"\ninvariants hold: {NEW_IN_2025} absent from the 2024 partition, dropped without a "
        f"word when that partition leads the list, back when it does not.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
