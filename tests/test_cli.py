from __future__ import annotations

from pathlib import Path

from twicerun.cli import main

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


def test_a_missing_pipeline_file_exits_two(tmp_path, capsys):
    code = main(["run", str(tmp_path / "absent.py"), "--run-dir", str(tmp_path / "artifacts")])
    assert code == 2
    assert "twicerun:" in capsys.readouterr().err
