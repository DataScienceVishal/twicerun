#!/usr/bin/env python3
"""Download the three NYC TLC monthly partitions the backfill pipeline runs on.

This is the only file in the repository that opens a socket. The test suite runs
under `pytest-socket --disable-socket`, so nothing in `tests/` can reach the
network even by accident. `tests/test_fetch_tlc.py` does import seven names from
here and calls `download()` in none of them, which is the distinction that
matters: pytest-socket installs its guards in `pytest_runtest_setup`, so
collection is the one window where the flag is not in force, and nothing at
module level here may open anything. Run it once by hand and the pipeline has
its input.

    uv run python scripts/fetch_tlc.py              # green, 3.6 MB
    uv run python scripts/fetch_tlc.py --taxi yellow  # yellow, 181 MB
    uv run python scripts/fetch_tlc.py --check      # verify what is on disk

Green is the default because it is fifty times smaller and the schema change is
identical in both. Yellow is there because the parallel float reduction this
project is about needs more rows than a month of green taxi trips contains, and
docs/data.md publishes the row count where that switches on.

Licence, which is the reason no Parquet is committed here. TLC publishes no
licence for the trip records. Searching the trip record page for the word finds
"base license number", which is a field in the data. What it carries instead is:

    The trip data was not created by the TLC, and TLC makes no representations
    as to the accuracy of these data.

That disclaims responsibility. It grants nothing and says nothing about
redistribution either way, so this repository ships the checksums and the
address and never the bytes.

The digests below were taken on 2026-08-26 from the files this script fetches.
They are here so a reader can tell "TLC republished the month" apart from "the
download was truncated", which are the same missing bytes to everything else.
TLC does restate months: the yellow 2024-12 file this script names was last
modified 2025-02-21, two months after the period it covers.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.error
import urllib.request
from pathlib import Path

# CloudFront, which is where the trip record page's own links point.
BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SOURCE_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Three consecutive months either side of 2025-01-05, when the Congestion Relief
# Zone charge took effect and `cbd_congestion_fee` appeared in the schema. One
# month before, two after, so the backfill spans a column that exists for two of
# its three partitions.
MONTHS = ("2024-12", "2025-01", "2025-02")

DIGESTS = {
    "green": {
        "2024-12": "1d708f58d29b0795a54ca11a36db6df61262986d122985f0fb52a56acc064bac",
        "2025-01": "84f3a121667157efcbf012c3566a6065df6f8e0312c678cb2f29cd72cc9c0f10",
        "2025-02": "b3ffe16a96ba0c68c8a8f8b10f049294ae6eb8d21f2e13b3fdbd452fa2e3498e",
    },
    "yellow": {
        "2024-12": "41ebf7db80bebde60c58e5143c14cdf38ad04a0f3e3ff44215b3e240d55f6c78",
        "2025-01": "9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a",
        "2025-02": "037cba555a73663f3a51a2c27816e40e3feb364769942bdf122b9da31e377bd3",
    },
}

HERE = Path(__file__).resolve().parent.parent
DEFAULT_DIR = HERE / "data" / "tlc"


class ChecksumMismatch(ValueError):
    """The bytes on disk are not the bytes the digest above was taken from."""


def partition_name(taxi: str, month: str) -> str:
    return f"{taxi}_tripdata_{month}.parquet"


def digest_of(path: Path) -> str:
    """SHA-256 of a file, read in chunks so a 60 MB partition does not go in memory."""
    running = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            running.update(chunk)
    return running.hexdigest()


def verify(path: Path, expected: str) -> None:
    found = digest_of(path)
    if found != expected:
        raise ChecksumMismatch(
            f"{path.name} hashes to {found}, not the {expected} recorded on 2026-08-26. "
            f"Either the download was cut short or TLC has restated the month. Delete the "
            f"file and rerun to tell those apart: a truncated download will not repeat."
        )


def download(url: str, into: Path) -> None:
    """Fetch to a sibling temporary file and rename, so a killed run leaves nothing half-written.

    A partial Parquet is worse than no Parquet here: `read_parquet` raises
    `Invalid Input Error: No magic bytes found at end of file` on one, and the
    pipeline that hits it reports a crash rather than a missing input.
    """
    into.parent.mkdir(parents=True, exist_ok=True)
    partial = into.with_suffix(".parquet.partial")
    with urllib.request.urlopen(url, timeout=120) as response, partial.open("wb") as handle:
        while chunk := response.read(1 << 20):
            handle.write(chunk)
    partial.replace(into)


def fetch(taxi: str, into: Path, *, check_only: bool = False) -> int:
    expected = DIGESTS[taxi]
    missing = 0
    for month in MONTHS:
        name = partition_name(taxi, month)
        path = into / name
        if path.exists():
            verify(path, expected[month])
            print(f"  {name}  {path.stat().st_size / 1e6:>7.1f} MB  digest matches")
            continue
        if check_only:
            print(f"  {name}  not here")
            missing += 1
            continue
        url = f"{BASE}/{name}"
        print(f"  {name}  fetching from {url}", flush=True)
        download(url, path)
        verify(path, expected[month])
        print(f"  {name}  {path.stat().st_size / 1e6:>7.1f} MB  digest matches")
    return missing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="fetch_tlc.py",
        description="Fetch three NYC TLC monthly Parquet partitions and verify their SHA-256.",
        epilog=f"Source and licence position: {SOURCE_PAGE}. TLC publishes no licence, only a "
        f"disclaimer that it did not create the data, so nothing fetched here is committed.",
    )
    parser.add_argument("--taxi", choices=sorted(DIGESTS), default="green")
    parser.add_argument("--into", type=Path, default=DEFAULT_DIR)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the digests of what is already on disk and fetch nothing",
    )
    args = parser.parse_args(argv)

    print(f"{args.taxi} taxi, {', '.join(MONTHS)}, into {args.into}")
    try:
        missing = fetch(args.taxi, args.into, check_only=args.check)
    except ChecksumMismatch as exc:
        print(f"fetch_tlc: {exc}", file=sys.stderr)
        return 1
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"fetch_tlc: {BASE} did not answer: {exc}", file=sys.stderr)
        return 2

    if missing:
        print(
            f"\n{missing} of {len(MONTHS)} partitions are absent. Rerun without --check to "
            f"fetch them, or point --into at wherever you keep them."
        )
        return 1
    print(f"\nAll {len(MONTHS)} partitions verified. {SOURCE_PAGE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
