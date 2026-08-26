"""The eval's own guards, which are the ones the other three loops already carry.

There are four loops in this repository now: the main one, the single-threaded
bisect, amplification, and this. Three times a guard written for one of them
failed to reach its twins, so this file enumerates what the main loop refuses to
do and checks the eval refuses the same. It does not re-measure the pipelines,
which takes six minutes and belongs in `scripts/eval.py`.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from twicerun.amplify import STABLE_ON_THIS_INPUT, Amplification
from twicerun.cause import Bisect
from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from eval import (  # noqa: E402
    BROKEN,
    INTERMITTENT,
    PAIRS,
    REFERENCE,
    STATIC_CHECKS,
    TWINS,
    StepScore,
    static_flags,
    without_amplifiers,
)


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


def printed_prose() -> str:
    """Every literal the eval hands to `say`, and nothing else in the file.

    Walked rather than grepped. Scanning lines that look like strings picked up
    the JSON key "stable_trials", which is written to a file and never printed,
    and a test that fails on a dict key is a test that gets deleted.
    """
    tree = ast.parse((Path(__file__).resolve().parent.parent / "scripts" / "eval.py")
                     .read_text(encoding="utf-8"))
    said = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "say"):
            continue
        for piece in ast.walk(node):
            if isinstance(piece, ast.Constant) and isinstance(piece.value, str):
                said.append(piece.value)
    return "\n".join(said)


@pytest.mark.parametrize("word", ["deterministic", "stable", "reproducible", "passed"])
def test_the_eval_never_claims_a_step_is_deterministic(word):
    """The same check the CLI report carries, on the other thing this repo prints.

    `stable` is in the list here where the CLI has to strip a token out first,
    because the eval never spells STABLE_ON_THIS_INPUT as text: it interpolates
    the constant, so the literals below hold none of it.
    """
    assert word not in printed_prose().lower()


def test_the_status_reaches_the_terminal_by_its_name_rather_than_as_text():
    """Which is what makes the word check above a check rather than a spelling accident."""
    source = (Path(__file__).resolve().parent.parent / "scripts" / "eval.py").read_text(
        encoding="utf-8"
    )
    assert f'"{STABLE_ON_THIS_INPUT}"' not in source
    assert source.count(STABLE_ON_THIS_INPUT) >= 3
