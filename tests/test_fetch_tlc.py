"""The download script's checks, run without ever reaching the network.

`pytest-socket` is on for the whole suite, so if anything here called
`download()` the test would fail with a socket error rather than quietly making
a request. Nothing does: `--check` is the mode that only reads the disk, and it
is the mode worth testing, because a digest that never refuses is a digest
nobody should trust.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from fetch_tlc import (  # noqa: E402
    DIGESTS,
    MONTHS,
    ChecksumMismatch,
    digest_of,
    main,
    partition_name,
    verify,
)


def plant(into: Path, taxi: str, month: str, body: bytes) -> Path:
    into.mkdir(parents=True, exist_ok=True)
    where = into / partition_name(taxi, month)
    where.write_bytes(body)
    return where


def test_the_digest_is_the_one_shasum_gives(tmp_path):
    where = tmp_path / "partition.parquet"
    where.write_bytes(b"PAR1" + bytes(range(256)) * 40)
    assert digest_of(where) == hashlib.sha256(where.read_bytes()).hexdigest()


def test_a_file_that_hashes_wrong_is_refused(tmp_path):
    """The failing case, constructed and watched to fail.

    Everything else here asserts that a matching file passes, which a `verify`
    that returned unconditionally would also satisfy.
    """
    where = tmp_path / "partition.parquet"
    where.write_bytes(b"not the bytes TLC served")
    with pytest.raises(ChecksumMismatch) as raised:
        verify(where, DIGESTS["green"]["2024-12"])
    assert digest_of(where) in str(raised.value)
    assert DIGESTS["green"]["2024-12"] in str(raised.value)


def test_check_passes_on_files_planted_with_their_own_digests(tmp_path, capsys, monkeypatch):
    """A whole --check pass over three partitions, with the digest table swapped.

    Real TLC bytes are 3.6 MB and are not committed, so the table is pointed at
    three files this test wrote. What is under test is the walk over the months
    and the exit code, not the constants.
    """
    bodies = {month: f"partition for {month}".encode() for month in MONTHS}
    monkeypatch.setitem(
        DIGESTS,
        "green",
        {month: hashlib.sha256(body).hexdigest() for month, body in bodies.items()},
    )
    for month, body in bodies.items():
        plant(tmp_path, "green", month, body)

    assert main(["--check", "--into", str(tmp_path)]) == 0
    printed = capsys.readouterr().out
    assert printed.count("digest matches") == len(MONTHS)


def test_check_reports_what_is_absent_rather_than_fetching_it(tmp_path, capsys):
    assert main(["--check", "--into", str(tmp_path)]) == 1
    absent = {
        line.split()[0] for line in capsys.readouterr().out.splitlines() if "not here" in line
    }
    assert absent == {partition_name("green", month) for month in MONTHS}


def test_one_corrupt_partition_fails_the_whole_check(tmp_path, capsys, monkeypatch):
    bodies = {month: f"partition for {month}".encode() for month in MONTHS}
    monkeypatch.setitem(
        DIGESTS,
        "green",
        {month: hashlib.sha256(body).hexdigest() for month, body in bodies.items()},
    )
    for month, body in bodies.items():
        plant(tmp_path, "green", month, body)
    truncated = tmp_path / partition_name("green", MONTHS[-1])
    truncated.write_bytes(truncated.read_bytes()[:-4])

    assert main(["--check", "--into", str(tmp_path)]) == 1
    assert "hashes to" in capsys.readouterr().err


def test_both_taxis_carry_a_digest_for_every_month():
    """Adding a month to MONTHS without adding its digests would be caught here.

    `verify` is only reached through `DIGESTS[taxi][month]`, so a missing entry
    raises KeyError in the middle of a download rather than before one starts.
    """
    for taxi, recorded in DIGESTS.items():
        assert sorted(recorded) == sorted(MONTHS), taxi
        assert all(len(sha) == 64 for sha in recorded.values()), taxi
