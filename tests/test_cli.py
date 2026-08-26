from __future__ import annotations

from pathlib import Path

import pytest

from twicerun.cli import KeySyntaxError, main, parse_keys

CLEAN = '''
def only_step(ctx):
    ctx.write("rows", "SELECT i FROM range(4) AS s(i)")


STEPS = [only_step]
'''

DIVERGES = '''
CALLS = {"n": 0}


def wobble(ctx):
    CALLS["n"] += 1
    ctx.write("rows", f"SELECT {CALLS['n']} AS attempt")


STEPS = [wobble]
'''


def pipeline(tmp_path: Path, body: str) -> Path:
    where = tmp_path / "p.py"
    where.write_text(body, encoding="utf-8")
    return where


def test_a_pipeline_that_never_diverged_exits_zero(tmp_path, capsys):
    code = main(["run", str(pipeline(tmp_path, CLEAN)),
                 "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 0
    assert "0 of 2" in capsys.readouterr().out


def test_divergence_exits_one_so_it_can_gate_a_release(tmp_path, capsys):
    code = main(["run", str(pipeline(tmp_path, DIVERGES)),
                 "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 1
    assert "2 of 2" in capsys.readouterr().out


def test_the_header_carries_the_version_and_the_thread_count(tmp_path, capsys):
    main(["run", str(pipeline(tmp_path, CLEAN)),
          "--runs", "2", "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out
    assert "duckdb " in printed and "threads=" in printed


def test_nothing_in_the_report_claims_a_step_is_deterministic(tmp_path, capsys):
    main(["run", str(pipeline(tmp_path, CLEAN)),
          "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out.lower()
    for forbidden in ("deterministic", "stable", "reproducible", "passed"):
        assert forbidden not in printed


def test_the_header_does_not_say_one_comparisons(tmp_path, capsys):
    main(["run", str(pipeline(tmp_path, CLEAN)), "--runs", "2",
          "--run-dir", str(tmp_path / "artifacts")])
    assert "so 1 comparison per step" in capsys.readouterr().out


def test_a_crashing_step_exits_three_not_one(tmp_path, capsys):
    """1 has to mean divergence and nothing else, or a gate cannot trust it."""
    body = (
        "def broken(ctx):\n"
        "    raise ValueError('the pipeline itself is wrong')\n\n\n"
        "STEPS = [broken]\n"
    )
    code = main(["run", str(pipeline(tmp_path, body)), "--run-dir", str(tmp_path / "artifacts")])
    assert code == 3
    printed = capsys.readouterr().err
    assert "the pipeline itself is wrong" in printed
    assert "Exit 3 is a crash" in printed


def test_two_invocations_in_the_same_second_do_not_collide(tmp_path, capsys):
    """Second-resolution stamps used to raise FileExistsError out of the CLI.

    --keep 0 because pruning would otherwise delete the evidence: the point here
    is that three directories got three distinct names, not that three survive.
    """
    where = pipeline(tmp_path, CLEAN)
    codes = [
        main(["run", str(where), "--runs", "2", "--keep", "0",
              "--run-dir", str(tmp_path / "artifacts")])
        for _ in range(3)
    ]
    assert codes == [0, 0, 0]
    assert len(list((tmp_path / "artifacts").iterdir())) == 3


def test_a_missing_pipeline_file_exits_two(tmp_path, capsys):
    code = main(["run", str(tmp_path / "absent.py"), "--run-dir", str(tmp_path / "artifacts")])
    assert code == 2
    assert "twicerun:" in capsys.readouterr().err


def test_key_is_parsed_per_artifact(tmp_path):
    assert parse_keys(["a=x,y", "b = z "]) == {"a": ("x", "y"), "b": ("z",)}


def test_key_without_an_artifact_says_what_the_spelling_is(tmp_path):
    with pytest.raises(KeySyntaxError, match="artifact=column"):
        parse_keys(["day"])


def test_key_narrows_what_the_report_calls_a_divergence(tmp_path, capsys):
    """Two runs disagreeing on one column: unmatched rows, or a drift on that column.

    Same measurement either way. --key decides which question was asked.
    """
    body = (
        "CALLS = {'n': 0}\n\n\n"
        "def wobble(ctx):\n"
        "    CALLS['n'] += 1\n"
        "    ctx.write('rows', f\"SELECT 1 AS id, {CALLS['n']} AS attempt\")\n\n\n"
        "STEPS = [wobble]\n"
    )
    where = pipeline(tmp_path, body)
    main(["run", str(where), "--runs", "2", "--run-dir", str(tmp_path / "a")])
    assert "ROW_MISSING ROW_EXTRA" in capsys.readouterr().out

    main(["run", str(where), "--runs", "2", "--key", "rows=id", "--run-dir", str(tmp_path / "b")])
    assert "VALUE_DRIFT" in capsys.readouterr().out


def test_a_key_naming_a_column_that_is_not_there_exits_two(tmp_path, capsys):
    code = main(["run", str(pipeline(tmp_path, CLEAN)), "--runs", "2",
                 "--key", "rows=nope", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 2
    assert "It has i" in capsys.readouterr().err
