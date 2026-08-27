"""The eval's own guards, which are the ones the other three loops already carry.

There are four loops in this repository now: the main one, the single-threaded
bisect, amplification, and this. Six times a guard written for one of them failed
to reach its twins, three while the loops were being built and three found in one
pass over the eval afterwards, so this file enumerates what the main loop refuses
to do and checks the eval refuses the same. It does not re-measure
`pipelines/reference.py`, which takes nine minutes and belongs in
`scripts/eval.py`. One test runs a fifty-row pipeline of its own, because
baseline 1's arithmetic is over real artifacts and a hand-built score would
assert nothing but the fixture.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from eval import (
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
from test_printed_words import printed_prose
from twicerun.amplify import DIVERGENT, STABLE_ON_THIS_INPUT, Amplification
from twicerun.cause import Bisect
from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings
from twicerun.runner import run_pipeline
from twicerun.tables import _moved

# The file itself, not the module, because two tests below parse it rather than
# call it. The eval's printed prose is held to the four forbidden words in
# `test_printed_words.py`, alongside every other script the README publishes;
# what stays here is the half specific to this file, that no status reaches the
# terminal as typed text.
EVAL = Path(__file__).resolve().parent.parent / "scripts" / "eval.py"

# Fifty rows, every one of them a different id on the second run, so both sides
# of the comparison lose all fifty.
BOTH_SIDES_MOVE = """
CALLS = {"n": 0}


def shifts(ctx):
    CALLS["n"] += 1
    ctx.write("rows", f"SELECT i + {CALLS['n']} * 100 AS id FROM range(50) AS s(i)")


STEPS = [shifts]
"""


def measured(
    name: str, *, fired: int = 0, compared: int = 1, blank: int = 0
) -> StepMeasurement:
    """A step with `compared` artifacts looked at, of which `fired` rounds diverged.

    `blank` rounds at the end compared nothing, which is what a step that wrote
    no artifact that time round leaves behind. They are the reason the measured
    denominator can differ from the nominal four.
    """
    step = StepMeasurement(index=0, name=name, comparisons=4, terms=1000)
    for round_ in range(4):
        looked_at = 0 if round_ >= 4 - blank else compared
        step.observe(
            [
                ArtifactFindings(
                    name="rows",
                    key=("id",),
                    reference_rows=100,
                    candidate_rows=100,
                    row_missing=7 if round_ < fired else 0,
                )
                for _ in range(looked_at)
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


def test_a_trial_whose_rate_is_out_of_a_smaller_denominator_still_gets_a_cell():
    """Ten trials cannot print as a distribution that accounts for one of them.

    The denominator was a single int overwritten by every observation from the
    last step seen, and the buckets were keyed on it, so the nine trials at 4 of
    4 disappeared out of the line the moment one trial came back 2 of 3, while
    `fired in 10 of 10` went on counting them. The sum of the counts printed has
    to be the number of trials.
    """
    scored = StepScore("mixed_denominators")
    for _ in range(9):
        scored.observe(measured("mixed_denominators", fired=4))
    scored.observe(measured("mixed_denominators", fired=2, blank=1))

    printed = scored.distribution()
    assert sum(int(n) for n in re.findall(r"x(\d+)", printed)) == scored.trials == 10
    assert "4 of 4 x9" in printed and "2 of 3 x1" in printed


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


def an_append(*, missing: int) -> StepMeasurement:
    """One comparison of the append step, with `missing` rows also gone from the reference side."""
    step = StepMeasurement(index=4, name="appends", comparisons=4)
    step.observe(
        [
            ArtifactFindings(
                name="log",
                key=("id",),
                reference_rows=3953,
                candidate_rows=7906,
                row_missing=missing,
                row_extra=3953,
            )
        ]
    )
    return step


def test_the_terminal_and_the_table_pick_the_same_side_of_an_unmatched_pair():
    """Six pure appends and four that also lost a row, which is where the two copies parted.

    The reference-side median over that mix is 0 and the later side's is 3,953.
    The terminal asked whether any trial had a reference-side count, which is
    true here, and then printed the median, so it published `median 0 of the
    3,953 reference rows found no partner` and suppressed the clause that
    carried the real number. The docstring on the test above calls that shape
    the one this project exists to stop publishing.
    """
    scored = StepScore("appends")
    for _ in range(6):
        scored.observe(an_append(missing=0))
    for _ in range(4):
        scored.observe(an_append(missing=17))

    assert scored.unmatched == [0] * 6 + [17] * 4, "the mix the two disagreed on"
    printed = scored.detail()
    assert "median 0 of the" not in printed
    assert "median 3,953 extra rows against 3,953 reference rows" in printed
    # One implementation, so the table's cell has to come out the same. The
    # labels differ on purpose: markdown wants them in backticks.
    cell = _moved({**scored.figures(), "what": "unused", "trials": 10})
    assert printed.split(", median")[1] == cell.split(", median")[1]


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


def test_every_twin_but_the_merge_loses_a_pattern_its_partner_carries():
    """A twin quietly reverted to its partner's shape would fail nothing in here.

    The specificity table rests entirely on `pipelines/twins.py` being the
    one-line fix for each bug, so a twin that stopped being one would print zero
    fires for the wrong reason and read as the strongest result in the eval.
    Sabotaging a twin left all 305 tests green.

    This is the shape check and not the behaviour: three of the four patterns
    are visible in the text, and `apply_price_updates_deduped` is the exception
    because no pattern separates a `MERGE` from its own fix, which is baseline
    2's headline. A twin broken in a way the patterns cannot see, say a
    tie-break on a second column that is also not unique, still gets past this,
    and the eval's specificity table is what would catch that.
    """
    broken_flags, twin_flags = static_flags(REFERENCE), static_flags(TWINS)
    unseparated = [
        broken
        for broken, twin in PAIRS
        if broken != twin and not set(broken_flags[broken]) - set(twin_flags[twin])
    ]
    assert unseparated == ["apply_price_updates"]


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


@pytest.mark.parametrize("status", [DIVERGENT, STABLE_ON_THIS_INPUT])
def test_no_status_reaches_the_terminal_as_typed_text(status):
    """Which is what makes the word check in `test_printed_words.py` a check.

    That file strips `STABLE_ON_THIS_INPUT` out before looking for `stable`, so
    the eval could satisfy it by typing the status as a literal and the word
    would go unnoticed. It does not: it interpolates the constant, and this is
    what keeps that true.

    This used to count occurrences of the identifier in the source and assert
    there were at least three, which is satisfied by any three lines that
    mention it. Hardcoding the status as a literal string in the one place the
    conditions print it left the old assertion green, so what it measured was
    that the file imports the constant, not that it never types the text.
    """
    assert status not in printed_prose(EVAL)


def test_the_statuses_are_referenced_by_name_somewhere_in_the_eval():
    """The other half of the check above, which is otherwise satisfied by never printing them."""
    used = {
        node.id
        for node in ast.walk(ast.parse(EVAL.read_text(encoding="utf-8")))
        if isinstance(node, ast.Name)
    }
    assert {"DIVERGENT", "STABLE_ON_THIS_INPUT"} <= used
