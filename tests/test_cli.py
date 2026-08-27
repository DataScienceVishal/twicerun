from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

from test_statuses import BREAKS_ON_A_DUPLICATE_KEY, ONLY_ON_TIED_INPUT
from twicerun import cli
from twicerun.amplify import (
    AMPLIFICATION_FAILED,
    DIVERGENT,
    NO_DIVERGENCE_OBSERVED,
    STABLE_ON_THIS_INPUT,
)
from twicerun.cli import KeySyntaxError, main, parse_keys
from twicerun.runner import RUNNING

STATUSES = (DIVERGENT, STABLE_ON_THIS_INPUT, NO_DIVERGENCE_OBSERVED, AMPLIFICATION_FAILED)

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


def test_real_input_divergence_outranks_an_amplified_one(tmp_path, capsys):
    """A gate keyed on `== 1` has to keep seeing 1 when there is real divergence.

    The pipeline below has one step of each kind, so the codes are competing.
    """
    both = ONLY_ON_TIED_INPUT.replace(
        "STEPS = [generate, sensitive]",
        "def wobble(ctx):\n"
        "    CALLS['n'] += 1\n"
        "    ctx.write('rows', f\"SELECT {CALLS['n']} AS attempt\")\n\n\n"
        "STEPS = [generate, sensitive, wobble]",
    )
    code = main(["run", str(pipeline(tmp_path, both)), "--runs", "3",
                 "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out

    assert code == 1
    assert STABLE_ON_THIS_INPUT in printed and DIVERGENT in printed


def test_the_header_carries_the_version_and_the_thread_count(tmp_path, capsys):
    main(["run", str(pipeline(tmp_path, CLEAN)),
          "--runs", "2", "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out
    assert "duckdb " in printed and "threads=" in printed


@pytest.mark.parametrize("body", [CLEAN, DIVERGES, ONLY_ON_TIED_INPUT])
def test_nothing_in_the_report_claims_a_step_is_deterministic(tmp_path, capsys, body):
    """Three shapes of report, because they do not print the same prose.

    A report that found something also prints the cause section, and a sentence
    about what a zero at threads=1 rules out is exactly where one of these
    words would have got in. The third pipeline reaches STABLE_ON_THIS_INPUT,
    which is the one status with a banned word inside it: the token is allowed
    and the bare word is not, so the token comes out of the text first and what
    is left has to be clean.
    """
    main(["run", str(pipeline(tmp_path, body)),
          "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out
    without_the_token = printed.replace(STABLE_ON_THIS_INPUT, "").lower()
    for forbidden in ("deterministic", "stable", "reproducible", "passed"):
        assert forbidden not in without_the_token


def test_a_step_only_an_amplifier_could_move_gets_its_own_exit_code(tmp_path, capsys):
    """Non-zero, because 0 rebuilds this tool's own complaint at the exit code.

    Not 1, because 1 already means the pipeline gave two answers on the user's
    data and this one gave two answers on an input twicerun fabricated. Those
    are different claims, and every other pair of claims in this codebase gets
    two names.
    """
    code = main(["run", str(pipeline(tmp_path, ONLY_ON_TIED_INPUT)),
                 "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out

    assert code == 4
    assert "0 of 2  STABLE_ON_THIS_INPUT" in printed


def test_an_amplifier_that_raised_is_not_a_divergence_and_is_not_a_clean_run_either(
    tmp_path, capsys
):
    """Nothing was seen giving two answers, so 1 would be wrong. So would 0.

    Zero would carry two meanings at once: found nothing, and could not look.
    Those are further apart than 1 and 4 are, because this one is silent. The
    step is left at 0 of 4 with a 53 percent upper bound and the only thing that
    tightens it did not run.
    """
    code = main(["run", str(pipeline(tmp_path, BREAKS_ON_A_DUPLICATE_KEY)),
                 "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    assert code == 5
    assert AMPLIFICATION_FAILED in capsys.readouterr().out


def test_an_amplified_divergence_outranks_an_amplifier_that_only_raised(tmp_path, capsys):
    """A divergence you can reproduce is worth more than a check that did not happen."""
    both = ONLY_ON_TIED_INPUT.replace(
        "STEPS = [generate, sensitive]",
        "def insists(ctx):\n"
        "    ctx.read('src')\n"
        "    ctx.sql('CREATE TABLE unique_keys (k INTEGER PRIMARY KEY)')\n"
        "    ctx.sql('INSERT INTO unique_keys SELECT k FROM src')\n"
        "    ctx.write('keys', 'SELECT k FROM unique_keys')\n\n\n"
        "STEPS = [generate, sensitive, insists]",
    )
    code = main(["run", str(pipeline(tmp_path, both)), "--runs", "3",
                 "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out

    assert code == 4
    assert STABLE_ON_THIS_INPUT in printed and AMPLIFICATION_FAILED in printed


def test_the_one_status_with_a_banned_word_in_it_carries_its_own_disclaimer(tmp_path, capsys):
    """STABLE_ON_THIS_INPUT is the obvious place for this tool to start overclaiming.

    It has to mean the step did not fire under the stresses named beside it,
    never that the step is reproducible, and the sentence saying so belongs in
    the terminal rather than in a README the reader does not have open.
    """
    main(["run", str(pipeline(tmp_path, ONLY_ON_TIED_INPUT)),
          "--runs", "3", "--run-dir", str(tmp_path / "artifacts")])
    printed = " ".join(capsys.readouterr().out.split())

    assert STABLE_ON_THIS_INPUT in printed
    assert "it is not this tool saying the step is fine" in printed
    assert "did not fire under these particular stresses, the ones named above" in printed


def test_no_amplify_removes_the_status_and_says_what_it_is_no_longer_claiming(tmp_path, capsys):
    """The audit two flags failed before this one: what disappears when it is set.

    --key with a float once deleted the whole reassociation bound section
    including the pre-registered check that is allowed to fail in it, and
    --tolerance once deleted the sentence explaining why the mechanism story did
    not hold. A flag that quietly removes the tool's own falsifiable claim is
    worse than no flag.

    So this one removes a status that amplification is what earns, and it has to
    replace it with the bound the status was resting on rather than with
    silence. The step reads 0 of 2 either way and means something weaker without
    the amplifiers.
    """
    where = pipeline(tmp_path, ONLY_ON_TIED_INPUT)
    main(["run", str(where), "--runs", "3", "--run-dir", str(tmp_path / "on")])
    amplified = capsys.readouterr().out
    main(["run", str(where), "--runs", "3", "--no-amplify", "--run-dir", str(tmp_path / "off")])
    plain = capsys.readouterr().out

    on_the_steps = [ln for ln in plain.splitlines() if re.match(r"^  \d \w+ ", ln)]
    assert on_the_steps
    assert not any(status in ln for ln in on_the_steps for status in STATUSES)
    assert any(STABLE_ON_THIS_INPUT in ln for ln in amplified.splitlines())

    flattened = " ".join(plain.split())
    assert "amplification is off (--no-amplify), so no status is printed" in flattened
    assert f"{NO_DIVERGENCE_OBSERVED} needs a zero under every amplifier as well" in flattened

    bound = "rule out a per-comparison divergence probability above 78 percent"
    assert bound in " ".join(amplified.split()), "the amplified report states the bound"
    assert bound in flattened, "and the flag must not delete it on the way out"


def test_judge_re_derives_the_statuses_from_the_saved_artifacts(tmp_path, capsys):
    """The amplified runs are kept rather than summarised, for the bisect's reason.

    A fire rate copied into the manifest is a number this tool could have got
    wrong. Re-scoring one goes back through the same comparison code the run
    used.
    """
    main(["run", str(pipeline(tmp_path, ONLY_ON_TIED_INPUT)), "--runs", "3",
          "--run-dir", str(tmp_path / "rd")])
    from_the_run = capsys.readouterr().out
    main(["judge", str(next((tmp_path / "rd").glob("run-*")))])
    from_the_judge = capsys.readouterr().out

    def about_the_step(text: str) -> list[str]:
        return [ln for ln in text.splitlines() if "sensitive" in ln]

    assert about_the_step(from_the_run) == about_the_step(from_the_judge)
    assert any(STABLE_ON_THIS_INPUT in ln for ln in about_the_step(from_the_judge))


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
    assert "2 of 2  DIVERGENT  VALUE_DRIFT" in printed

    tolerant = main(["run", str(where), "--runs", "3", "--policy", "reduction-order",
                     "--run-dir", str(tmp_path / "b")])
    downgraded = capsys.readouterr().out
    assert tolerant == 0
    assert "0 of 2  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER  TOLERATED on 2 of 2" in downgraded

    # The measured size is the same string in both, which is the whole argument
    # for splitting measurement from policy.
    magnitude = "1 ulp and 1.16e-16 relative"
    assert magnitude in printed and magnitude in downgraded


def test_the_step_line_names_which_route_downgraded_it(tmp_path, capsys):
    """A mechanism-backed downgrade and a user's threshold are different claims.

    They used to render identically as `TOLERATED on 2 of 2`, with the route
    collapsed to one boolean in the header over the whole report.
    """
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    main(["run", str(where), "--runs", "3", "--policy", "reduction-order",
          "--run-dir", str(tmp_path / "a")])
    assert "TOLERATED on 2 of 2 by the derived reassociation bound" in capsys.readouterr().out

    main(["run", str(where), "--runs", "3", "--tolerance-rel", "1e-6",
          "--run-dir", str(tmp_path / "b")])
    assert "TOLERATED on 2 of 2 by a --tolerance threshold" in capsys.readouterr().out


def test_a_downgrade_rests_on_a_measured_rate_rather_than_on_a_disclosure(tmp_path, capsys):
    """What slice 3 changed about a TOLERATED, in the one report that shows it.

    Every header that downgraded anything used to carry a note saying condition
    2 of 3 was not implemented. It is now, so the note is gone and the evidence
    it stood in for is printed instead: the same step's fire rate at threads=1,
    out of the same denominator as the rate above it.
    """
    main(["run", str(pipeline(tmp_path, TOLERABLE_DRIFT)), "--runs", "3",
          "--policy", "reduction-order", "--run-dir", str(tmp_path / "artifacts")])
    printed = capsys.readouterr().out

    assert "TOLERATED on 2 of 2" in printed
    assert "0 of 2 at threads=1" in printed
    assert "is not implemented" not in printed
    assert "rests on the other two" not in printed


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


def test_the_header_says_when_a_live_directory_suspended_retention(tmp_path, capsys):
    """Three parallel invocations leave three directories, each claiming to keep one.

    A run still writing is never a deletion candidate, deliberately, because
    deleting it would pull the Parquet out from under another process. The
    header stated retention as a fact anyway, so eight CI jobs was 2 GB and a
    report insisting on 260 MB.
    """
    parent = tmp_path / "artifacts"
    parent.mkdir()
    live = parent / "run-20260101-000001"
    live.mkdir()
    # pid 1 exists on every unix and is not this process.
    (live / RUNNING).write_text("1", encoding="utf-8")

    main(["run", str(pipeline(tmp_path, CLEAN)), "--runs", "2", "--run-dir", str(parent)])
    printed = capsys.readouterr().out

    assert "left 1 directory alone that another invocation is still writing" in printed
    assert live.is_dir()


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

    assert "2 of 2  DIVERGENT  VALUE_DRIFT" in strict
    assert "0 of 2  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER  TOLERATED on 2 of 2" in tolerant
    assert measured_lines(strict) == measured_lines(tolerant)
    assert measured_lines(strict), "the comparison would be vacuous with nothing measured"


def status_column(printed: str) -> list[str | None]:
    """The status off each step line, None where the report claimed none.

    Matched on the fire rate rather than on the leading index, because the
    amplification section and the axes block also start with a step index and
    neither is the step table.
    """
    return [
        next((token for token in STATUSES if token in line), None)
        for line in printed.splitlines()
        if re.match(r"^  \d+ \S+ +\d+ of \d+", line)
    ]


def test_the_status_column_does_not_move_when_the_policy_does(tmp_path, capsys):
    """The rule the fire rate already follows, extended to the thing slice 4 added.

    A policy decides whether a difference matters. Letting it decide whether one
    happened would put the status in the same negotiable pile as everything a
    tolerance touches, so it is read off the measurement and this judges one
    saved run under three policies to say so.
    """
    main(["run", str(pipeline(tmp_path, TOLERABLE_DRIFT)), "--runs", "3",
          "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    capsys.readouterr()

    rendered = []
    for flags in ([], ["--policy", "reduction-order"], ["--tolerance-ulps", "4"]):
        main(["judge", str(run_dir), *flags])
        rendered.append(capsys.readouterr().out)

    columns = [status_column(text) for text in rendered]
    assert columns[0] == [NO_DIVERGENCE_OBSERVED, DIVERGENT]
    assert columns[0] == columns[1] == columns[2]
    assert len({text for text in rendered}) == 3, "the three policies did print different reports"
    assert len({tuple(measured_lines(text)) for text in rendered}) == 1


def test_judge_does_not_print_figures_that_are_not_true_of_the_run(tmp_path, capsys):
    """It executed nothing and deleted nothing, so it claims neither.

    Retention read `keeping 0 run directories`, which is also how the flag
    spells keep everything, over a command that prunes nothing at all. The
    duration was the saved pipeline's five executions printed in the words the
    run command uses for its whole pass, which under-reported by about 8x.
    """
    main(["run", str(pipeline(tmp_path, TOLERABLE_DRIFT)), "--runs", "2",
          "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    printed_by_run = capsys.readouterr().out

    main(["judge", str(run_dir)])
    printed_by_judge = capsys.readouterr().out

    assert "retention  keeping 1 run directory" in printed_by_run
    assert "retention" not in printed_by_judge
    assert "steps diverged in " in printed_by_run
    assert "steps diverged." in printed_by_judge


def test_judge_takes_a_glob_that_matched_more_than_one_run(tmp_path, capsys):
    """The documented command against the state the documented flag creates.

    `judge .twicerun/run-*` works only while retention is 1, and the README
    suggests --keep 0 two paragraphs later. With two directories the shell
    passed both and argparse answered with a usage message that never mentioned
    run directories.
    """
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    parent = tmp_path / "rd"
    for _ in range(2):
        main(["run", str(where), "--runs", "2", "--keep", "0", "--run-dir", str(parent)])
    matched = sorted(parent.glob("run-*"))
    assert len(matched) == 2
    capsys.readouterr()

    assert main(["judge", *[str(p) for p in matched]]) == 1
    printed = capsys.readouterr()
    newest = max(matched, key=lambda p: p.stat().st_mtime)
    assert "judging the newest" in printed.err
    assert str(newest) in printed.out


def test_judge_works_from_a_different_directory_than_the_run(tmp_path, monkeypatch):
    """--run-dir defaults to a relative path and a manifest outlives one cwd.

    Artifact paths were stored as given, so a run made in one directory and
    judged from another resolved every path against the wrong root and reported
    all of them gone, blaming retention for files that were there.
    """
    where = pipeline(tmp_path, TOLERABLE_DRIFT)
    monkeypatch.chdir(tmp_path)
    main(["run", str(where), "--runs", "2", "--run-dir", ".twicerun"])
    run_dir = next((tmp_path / ".twicerun").glob("run-*")).resolve()

    monkeypatch.chdir(tmp_path.parent)
    assert main(["judge", str(run_dir)]) == 1


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


def test_judging_a_run_whose_amplified_artifacts_are_gone_says_so(tmp_path, capsys):
    """The existence check listed the main loop and the bisect and not the third loop.

    So a pruned directory reached attach_amplification, which read files that
    were not there and raised out of a command whose contract is that it
    re-scores or explains itself.
    """
    main(["run", str(pipeline(tmp_path, ONLY_ON_TIED_INPUT)), "--runs", "3",
          "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    shutil.rmtree(run_dir / "amplified")
    capsys.readouterr()

    code = main(["judge", str(run_dir)])
    assert code == 2
    assert "artifact(s) named in" in capsys.readouterr().err


def test_a_crash_inside_judge_exits_three_like_every_other_crash(tmp_path, capsys):
    """`judge` was dispatched outside the try block that every crash path relies on.

    An unhandled traceback leaves a shell exit of 1, and 1 is what this
    program's epilog defines as divergence, so a gate recorded a broken read as
    a pipeline that gave two answers.
    """
    main(["run", str(pipeline(tmp_path, CLEAN)), "--runs", "2",
          "--run-dir", str(tmp_path / "rd")])
    run_dir = next((tmp_path / "rd").glob("run-*"))
    capsys.readouterr()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(cli, "rejudge", _explode)
        code = main(["judge", str(run_dir)])

    assert code == 3
    assert "Exit 3 is a crash, not a divergence" in capsys.readouterr().err


def _explode(*args, **kwargs):
    raise MemoryError("the comparison ran out of memory")
