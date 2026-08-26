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


TOLERABLE_DRIFT = '''
CALLS = {"n": 0}


def seed(ctx):
    ctx.write("source", "SELECT i FROM range(100) AS s(i)")


def average(ctx):
    CALLS["n"] += 1
    ctx.read("source")
    nudge = "+ 1.1641532182693481e-10" if CALLS["n"] > 1 else ""
    ctx.write("total", f"SELECT 1 AS g, (1000000.0 {nudge})::DOUBLE AS v FROM source LIMIT 1")


STEPS = [seed, average]
'''


def test_the_same_drift_exits_one_under_strict_and_zero_under_reduction_order(tmp_path, capsys):
    """The slice's deliverable, in one pair of invocations.

    A float that moved by one ulp on a step that read 100 rows. Under the
    default that is a divergence, because the question asked was whether the
    pipeline gave the same answer twice and it did not. Under reduction-order
    it is measured, printed with its size, and downgraded.
    """
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    strict = main(["run", str(where), "--runs", "3", "--run-dir", str(tmp_path / "a")])
    printed = capsys.readouterr().out
    assert strict == 1
    assert "2 of 2  VALUE_DRIFT" in printed

    tolerant = main(["run", str(where), "--runs", "3", "--policy", "reduction-order",
                     "--run-dir", str(tmp_path / "b")])
    downgraded = capsys.readouterr().out
    assert tolerant == 0
    assert "0 of 2  VALUE_DRIFT TOLERATED on 2 of 2" in downgraded

    # The measured size is the same string in both, which is the whole argument
    # for splitting measurement from policy.
    magnitude = "1 ulp and 1.16e-16 relative"
    assert magnitude in printed and magnitude in downgraded


def test_a_report_that_downgrades_says_which_condition_is_missing(tmp_path, capsys):
    main(["run", str(pipeline(tmp_path, TOLERABLE_DRIFT)), "--runs", "3",
          "--policy", "reduction-order", "--run-dir", str(tmp_path / "artifacts")])
    assert "threads=1" in capsys.readouterr().out


def test_a_manual_tolerance_appears_in_the_header_so_a_reader_knows(tmp_path, capsys):
    main(["run", str(pipeline(tmp_path, TOLERABLE_DRIFT)), "--runs", "2",
          "--tolerance-ulps", "2", "--run-dir", str(tmp_path / "artifacts")])
    assert "--tolerance-ulps 2" in capsys.readouterr().out


def test_no_key_spelling_can_delete_the_pre_registered_check(tmp_path, capsys):
    """The defect this is here to stop coming back.

    --key daily_revenue=day,revenue used to be accepted. It moved the float
    into the key, where bit equality turned one ulp of reassociation into a
    missing row and an extra row, the step reported no drift at all, and the
    whole reassociation bound section went with it, including the headroom
    figure that fails on nearly every pass. No warning anywhere.
    """
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    main(["run", str(where), "--runs", "3", "--run-dir", str(tmp_path / "a")])
    assert "pre-registered check" in capsys.readouterr().out

    code = main(["run", str(where), "--runs", "3", "--key", "total=g,v",
                 "--run-dir", str(tmp_path / "b")])
    printed = capsys.readouterr()
    assert code == 2
    assert "v (DOUBLE)" in printed.err
    assert "ROW_MISSING" not in printed.out


def test_a_key_that_leaves_the_float_out_still_measures_it(tmp_path, capsys):
    """The half of --key that is allowed, and the check survives it."""
    code = main(["run", str(pipeline(tmp_path, TOLERABLE_DRIFT)), "--runs", "3",
                 "--key", "total=g", "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out
    assert code == 1
    assert "VALUE_DRIFT" in printed
    assert "pre-registered check" in printed
