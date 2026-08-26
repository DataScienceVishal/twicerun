from __future__ import annotations

import re
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


CASCADES = '''
CALLS = {"n": 0}


def wobble(ctx):
    CALLS["n"] += 1
    ctx.write("rows", f"SELECT {CALLS['n']} AS attempt")


def copy_it(ctx):
    ctx.read("rows")
    ctx.write("echo", "SELECT attempt FROM rows")


STEPS = [wobble, copy_it]
'''


def test_the_ablation_is_a_flag_a_reader_can_run_rather_than_a_claim(tmp_path, capsys):
    """--no-containment, and the two reports it produces side by side.

    Two steps, one bug. The default reports it once and the ablation reports it
    twice, which is the number the Spot borrowing has to earn.
    """
    where = pipeline(tmp_path, CASCADES)
    main(["run", str(where), "--runs", "3", "--run-dir", str(tmp_path / "on")])
    contained = capsys.readouterr().out
    main(["run", str(where), "--runs", "3", "--no-containment",
          "--run-dir", str(tmp_path / "off")])
    ablated = capsys.readouterr().out

    assert "1 of 2 steps diverged" in contained
    assert "2 of 2 steps diverged" in ablated
    assert "contained  on, so runs 2 to 3 read run 1's artifacts" in contained
    assert "contained  off (--no-containment)" in ablated


def test_judge_reports_the_containment_the_run_actually_used(tmp_path, capsys):
    """A saved run is scored again, and how it executed is not a flag on judge."""
    main(["run", str(pipeline(tmp_path, CASCADES)), "--runs", "2", "--no-containment",
          "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    capsys.readouterr()

    main(["judge", str(run_dir)])
    assert "contained  off (--no-containment)" in capsys.readouterr().out


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
    assert "0 of 2  VALUE_DRIFT PARALLEL_ORDER TOLERATED on 2 of 2" in downgraded

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
    assert "of headroom was fixed" in capsys.readouterr().out

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
    assert "of headroom was fixed" in printed


def test_the_headroom_threshold_is_printed_from_the_constant_that_enforces_it(tmp_path, capsys):
    """A project whose argument is that the threshold has not moved cannot print it twice."""
    from twicerun.policy import HEADROOM_REQUIRED

    main(["run", str(pipeline(tmp_path, TOLERABLE_DRIFT)), "--runs", "2",
          "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out
    assert f"{HEADROOM_REQUIRED:,.0f}x of headroom was fixed" in printed
    assert "CLEARS" in printed or "FAILS" in printed


def test_a_key_naming_an_artifact_that_does_not_exist_exits_two(tmp_path, capsys):
    """It used to produce a full, confident, ordinary report with the flag ignored."""
    code = main(["run", str(pipeline(tmp_path, CLEAN)), "--runs", "2",
                 "--key", "no_such_artifact=i", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 2
    assert "It writes rows" in capsys.readouterr().err


@pytest.mark.parametrize("spelling", ["rows=,", "rows= , ", "rows=", "=i", "rows"])
def test_a_key_that_names_no_column_is_refused(spelling):
    """`rows=,` passed the old raw-string check and left an empty key.

    An empty key is not a refusal. It puts the whole artifact in one group and
    pairs rows by sort order, which is the weakest comparison here.
    """
    with pytest.raises(KeySyntaxError):
        parse_keys([spelling])


def test_a_negative_keep_is_a_typo_not_a_stronger_zero(tmp_path, capsys):
    """It used to print "keeping -5 run directories" and quietly keep everything."""
    code = main(["run", str(pipeline(tmp_path, CLEAN)), "--runs", "2",
                 "--keep", "-5", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 2
    assert "Use 0 to keep everything" in capsys.readouterr().err


def test_an_artifact_with_no_exact_column_says_it_paired_by_sort_order(tmp_path, capsys):
    """The weakest comparison in the tool, and it used to be invisible.

    The README claimed the key was reported and nothing printed it. This is the
    case that claim existed for: no exact column means no key, so the whole
    artifact sorts as one group and rows pair by order alone.
    """
    body = (
        "CALLS = {'n': 0}\n\n\n"
        "def floats_only(ctx):\n"
        "    CALLS['n'] += 1\n"
        "    ctx.write('v', f\"SELECT ({CALLS['n']}.5)::DOUBLE AS v\")\n\n\n"
        "STEPS = [floats_only]\n"
    )
    main(["run", str(pipeline(tmp_path, body)), "--runs", "2",
          "--run-dir", str(tmp_path / "artifacts")])
    assert "rows pair by sort order" in capsys.readouterr().out


def measured_lines(printed: str) -> list[str]:
    """The indented detail lines, which are the measurements rather than the verdicts.

    The step line carries the fire rate and is meant to move with the policy.
    Everything under it is what the oracle found and must not.
    """
    return [ln for ln in printed.splitlines() if re.match(r"^ {6}\S", ln)]


def test_judging_one_saved_run_twice_shows_the_measurement_holding_still(tmp_path, capsys):
    """The project's central claim, made observable instead of only tested.

    Two invocations of `run` execute the pipeline twice, so the figures move
    between them for exactly the reason this tool exists, and the README
    paragraph describing the property looked like it was contradicted by the
    two commands printed beside it. Judging one saved run under two policies
    re-runs nothing.
    """
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    main(["run", str(where), "--runs", "3", "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    capsys.readouterr()

    assert main(["judge", str(run_dir)]) == 1
    strict = capsys.readouterr().out
    assert main(["judge", str(run_dir), "--policy", "reduction-order"]) == 0
    tolerant = capsys.readouterr().out

    assert "2 of 2  VALUE_DRIFT" in strict
    assert "0 of 2  VALUE_DRIFT PARALLEL_ORDER TOLERATED on 2 of 2" in tolerant
    assert measured_lines(strict) == measured_lines(tolerant)
    assert measured_lines(strict), "the comparison would be vacuous with nothing measured"


def test_judge_takes_the_manifest_file_as_well_as_the_directory(tmp_path):
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    main(["run", str(where), "--runs", "2", "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    assert main(["judge", str(run_dir / "manifest.json")]) == 1


def test_judging_a_run_whose_artifacts_were_pruned_says_so(tmp_path, capsys):
    """Retention drops old run directories, so a stale manifest is the normal mistake."""
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    main(["run", str(where), "--runs", "2", "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    saved = tmp_path / "manifest.json"
    saved.write_text((run_dir / "manifest.json").read_text(), encoding="utf-8")
    main(["run", str(where), "--runs", "2", "--run-dir", str(tmp_path / "rd")])

    assert main(["judge", str(saved)]) == 2
    assert "are gone" in capsys.readouterr().err


def test_judge_on_a_path_with_no_manifest_exits_two(tmp_path, capsys):
    assert main(["judge", str(tmp_path / "nowhere")]) == 2
    assert "no manifest at" in capsys.readouterr().err
