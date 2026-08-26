"""The eval's own guards, which are the ones the other three loops already carry.

There are four loops in this repository now: the main one, the single-threaded
bisect, amplification, and this. Three times a guard written for one of them
failed to reach its twins, so this file enumerates what the main loop refuses to
do and checks the eval refuses the same. It does not re-measure
`pipelines/reference.py`, which takes six minutes and belongs in
`scripts/eval.py`. One test runs a fifty-row pipeline of its own, because
baseline 1's arithmetic is over real artifacts and a hand-built score would
assert nothing but the fixture.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from twicerun.amplify import DIVERGENT, STABLE_ON_THIS_INPUT, Amplification
from twicerun.cause import Bisect
from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings
from twicerun.runner import run_pipeline

EVAL = Path(__file__).resolve().parent.parent / "scripts" / "eval.py"

sys.path.insert(0, str(EVAL.parent))

from eval import (  # noqa: E402
    BROKEN,
    INTERMITTENT,
    PAIRS,
    REFERENCE,
    STATIC_CHECKS,
    TWINS,
    StepScore,
    claim_workspace,
    main,
    naive_pass,
    static_flags,
    without_amplifiers,
)

# Fifty rows, every one of them a different id on the second run, so both sides
# of the comparison lose all fifty.
BOTH_SIDES_MOVE = """
CALLS = {"n": 0}


def shifts(ctx):
    CALLS["n"] += 1
    ctx.write("rows", f"SELECT i + {CALLS['n']} * 100 AS id FROM range(50) AS s(i)")


STEPS = [shifts]
"""


def measured(name: str, *, fired: int = 0, compared: int = 1) -> StepMeasurement:
    """A step with `compared` artifacts looked at, of which `fired` rounds diverged."""
    step = StepMeasurement(index=0, name=name, comparisons=4, terms=1000)
    for round_ in range(4):
        step.observe(
            [
                ArtifactFindings(
                    name="rows",
                    key=("id",),
                    reference_rows=100,
                    candidate_rows=100,
                    row_missing=7 if round_ < fired else 0,
                )
                for _ in range(compared)
            ]
        )
    return step


def test_a_step_that_compared_nothing_is_not_counted_as_a_clean_trial():
    """The guard that has now been written four times, once per loop.

    `any([])` is False, so a step that wrote no artifacts scores 0 of 4 out of
    four comparisons of nothing and reads exactly like four clean ones. The
    failing case is constructed here rather than assumed: without the check in
    `StepScore.observe` this step lands in `rates` as a zero and `trials` comes
    back 1.
    """
    scored = StepScore("wrote_nothing")
    scored.observe(measured("wrote_nothing", compared=0))
    assert scored.trials == 0
    assert scored.silent_trials == 1
    assert scored.distribution() == "nothing compared"


def test_a_step_that_compared_something_is_counted():
    scored = StepScore("wrote_something")
    scored.observe(measured("wrote_something", fired=3))
    assert scored.trials == 1
    assert scored.silent_trials == 0
    assert scored.fired_in == 1
    assert scored.distribution() == "3 of 4 on all 1"


def test_every_rate_prints_with_its_denominator_and_the_empty_buckets_too():
    """A rate that never came up has to print as x0 rather than be left out.

    Slice 4 published this step's 40-pass rates as `4 of 4 x27, 3 x5, 2 x5, 1 x3`
    and called the cell complete on the grounds that a rate out of four cannot
    leave the range. Two ten-trial runs of the eval then disagreed about whether
    0 of 4 happens. An omitted bucket reads as impossible rather than
    unobserved, so every bucket prints.
    """
    scored = StepScore("mixed")
    for fired in (4, 4, 2):
        scored.observe(measured("mixed", fired=fired))
    assert scored.distribution() == "4 of 4 x2, 3 of 4 x0, 2 of 4 x1, 1 of 4 x0, 0 of 4 x0"

    uniform = StepScore("uniform")
    for _ in range(3):
        uniform.observe(measured("uniform", fired=0))
    assert uniform.distribution() == "0 of 4 on all 3"


def test_the_append_magnitude_is_not_reported_as_zero():
    """Rows lost on the later side only, which is what an append with no key does.

    Recording the reference side alone printed `median 0 of the 3,953 rows`
    beside a fire rate of 4 of 4, which is the one shape of number this project
    exists to stop publishing.
    """
    step = StepMeasurement(index=0, name="appends", comparisons=4)
    step.observe(
        [
            ArtifactFindings(
                name="log", key=("id",), reference_rows=3953, candidate_rows=7906, row_extra=3953
            )
        ]
    )
    scored = StepScore("appends")
    scored.observe(step)
    assert "3,953 extra rows against 3,953 reference rows" in scored.detail()


def test_dropping_the_amplifiers_is_what_the_flag_does_and_nothing_more():
    """Baseline 4 has to be the flag rather than a model of it.

    The main loop is identical code with and without `--no-amplify`, so the fire
    rate, the classes and the single-threaded rate all have to survive, and only
    the status may move.
    """
    step = measured("quiet", fired=0)
    step.bisect = Bisect(comparisons=4, fired=0, artifacts_compared=4)
    step.amplifications = [
        Amplification(amplifier="tie collapse", note="", comparisons=2, fired=2,
                      artifacts_compared=2)
    ]
    assert step.status == STABLE_ON_THIS_INPUT

    without = without_amplifiers([step])[0]
    assert without.status is None
    assert without.fired == step.fired
    assert without.comparisons == step.comparisons
    assert without.bisect is step.bisect
    assert step.amplifications, "the original must not be emptied in place"


def test_the_static_baseline_attributes_a_hit_to_a_step_rather_than_to_a_file():
    """Per step, because a matched pair cannot be scored at file granularity.

    Grep `twins.py` and the append pattern hits, so a file-level check reports
    an append bug in the file whose whole purpose is that it has none. It is
    `apply_price_updates_deduped` that reads and writes `prices`, and saying so
    is the difference between a baseline that can be compared against the oracle
    and one that cannot.
    """
    append = next(pattern for name, pattern in STATIC_CHECKS if name.startswith("a step that"))
    assert append.search(TWINS.read_text(encoding="utf-8")), "the file-level grep hits"

    flagged = static_flags(TWINS)
    hit = [name for name, hits in flagged.items() if any(h.startswith("a step that") for h in hits)]
    assert hit == ["apply_price_updates_deduped"]
    assert not flagged["replace_audit_log"]


def test_the_static_baseline_flags_correct_code_too():
    """The false positive is the finding, so it has to be visible rather than tuned away."""
    flagged = static_flags(REFERENCE)
    assert flagged["mean_basket"], "a float aggregate on correct code should still be flagged"
    assert not flagged["generate_inputs"], "the control writes no aggregate"


def test_the_pairs_name_steps_that_exist_in_both_pipelines():
    """A typo here would score a twin as silently quiet, which is the eval's whole claim."""
    broken = set(static_flags(REFERENCE))
    twins = set(static_flags(TWINS))
    for left, right in PAIRS:
        assert left in broken, left
        assert right in twins, right
    for name in (*BROKEN, INTERMITTENT):
        assert name in broken, name


def test_baseline_one_counts_two_sides_against_a_total_that_can_hold_them(tmp_path):
    """A ceiling cannot be beaten, and baseline 1 beat its own by a factor of 1.27.

    Both sides went into the numerator while the denominator took the reference
    row count alone, so ten trials published `median 1,265 rows unmatched out of
    1,000 on each side`. Fifty rows that all move is 50 unmatched each way out
    of the 100 rows the two runs put in front of it; the old arithmetic made
    that 100 out of 50.

    This one runs a pipeline, unlike the rest of the file, because the
    arithmetic being checked is over real artifacts and a hand-built
    `NaiveScore` would only assert the fixture.
    """
    where = tmp_path / "shifts.py"
    where.write_text(BOTH_SIDES_MOVE, encoding="utf-8")
    _, manifest = run_pipeline(where, runs=2, parent=tmp_path / "artifacts", keep=1)

    scored = naive_pass(manifest)["shifts"]
    assert (scored.unmatched_reference, scored.unmatched_candidate) == (50, 50)
    assert scored.rows_compared == 100
    assert scored.unmatched <= scored.rows_compared


def test_the_eval_refuses_an_into_that_already_exists(tmp_path):
    """`--into .` deleted the working tree, and `ignore_errors=True` hid half of it.

    The cleanup at the end of the run was `shutil.rmtree(args.into,
    ignore_errors=True)` against whatever path was passed, so the destructive
    case was reachable from the README's own command line with one argument
    changed. The refusal has to come before the first trial starts, which is
    what this asserts by handing it a directory with something in it and
    checking the something survives.
    """
    theirs = tmp_path / "someone-elses-work"
    theirs.mkdir()
    (theirs / "keep.sql").write_text("select 1", encoding="utf-8")

    assert main(["--into", str(theirs), "--trials", "1"]) == 2
    assert (theirs / "keep.sql").read_text(encoding="utf-8") == "select 1"


def test_the_eval_makes_the_directory_it_is_going_to_delete(tmp_path):
    mine = tmp_path / "fresh"
    assert claim_workspace(mine)
    assert mine.is_dir()
    assert not claim_workspace(mine), "the second call is the refusal"


def printed_prose() -> str:
    """Every string literal in the eval that is prose rather than an identifier.

    Inclusive rather than call by call, which is the correction. The first
    version of this collected constants inside `say(...)` and nothing else, so
    it reached none of the eight condition texts, none of the eight evidence
    lines, the specificity table or baseline 3's per-step lines, which all go
    through `hang`, and nothing a helper composes and returns. Three mutations
    putting a forbidden word where the eval really prints it left every test in
    this file green.

    Dict keys and subscript keys come back out, because they are identifiers
    rather than words and one of them is `stable_trials`, which goes to the JSON
    file and is never printed. A test that fails on a dict key is a test that
    gets deleted. Docstrings come out because nothing prints them, and the
    runtime half of this check is in `test_eval_conditions.py`, which reads the
    terminal output itself.
    """
    tree = ast.parse(EVAL.read_text(encoding="utf-8"))
    not_prose = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            not_prose.update(k for k in node.keys if isinstance(k, ast.Constant))
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            not_prose.add(node.slice)
        elif isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef):
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                not_prose.add(first.value)
    return "\n".join(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node not in not_prose
    )


@pytest.mark.parametrize("word", ["deterministic", "stable", "reproducible", "passed"])
def test_the_eval_never_claims_a_step_is_deterministic(word):
    """The same four words `test_cli.py` holds the report to, on the other thing this repo prints.

    `stable` is in the list here where the CLI report has to strip a token out
    of its output first, because the eval spells no status as text at all: it
    interpolates the constant. The test below is what keeps that true.
    """
    assert word not in printed_prose().lower()


@pytest.mark.parametrize("status", [DIVERGENT, STABLE_ON_THIS_INPUT])
def test_no_status_reaches_the_terminal_as_typed_text(status):
    """Which is what makes the word check above a check rather than a spelling accident.

    This used to count occurrences of the identifier in the source and assert
    there were at least three, which is satisfied by any three lines that
    mention it. Hardcoding the status as a literal string in the one place the
    conditions print it left the old assertion green, so what it measured was
    that the file imports the constant, not that it never types the text.
    """
    assert status not in printed_prose()


def test_the_statuses_are_referenced_by_name_somewhere_in_the_eval():
    """The other half of the check above, which is otherwise satisfied by never printing them."""
    used = {
        node.id
        for node in ast.walk(ast.parse(EVAL.read_text(encoding="utf-8")))
        if isinstance(node, ast.Name)
    }
    assert {"DIVERGENT", "STABLE_ON_THIS_INPUT"} <= used
