"""What the eight pre-registered conditions say when nothing was measured.

Three of them declare a piece of this project unnecessary if they fire, and all
three fired on an absence: a step that wrote no artifacts compares nothing,
fires on none of the nothing it compared, and arrives at the conditions as a
clean zero. `mean_basket fired on 0 of 10 trials, median 0 rows unmatched out of
0 on each side` is a real line from a real run, printed under a verdict of
TRIGGERED next to the sentence "the oracle is more machinery than the problem
needs".

The trials here are built out of `StepMeasurement` rather than measured, because
the conditions read counts the oracle already computed and nothing that points
at a Parquet file. Ten real trials take nine minutes; these take milliseconds and
can be given the shape that never happens on a healthy laptop.

Having the whole report on tap for the price of a fixture is also what lets the
last two tests read the terminal output rather than the source, which is where
`test_eval.py` cannot follow: text a helper composes at runtime is not a literal
anywhere.
"""

from __future__ import annotations

import sys
from pathlib import Path

from twicerun.amplify import STABLE_ON_THIS_INPUT, Amplification
from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings
from twicerun.policy import Policy

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from eval import (  # noqa: E402
    BENIGN,
    INTERMITTENT,
    PAIRS,
    WIDTH,
    NaiveScore,
    Trial,
    report_attribution,
    report_baseline_four,
    report_baseline_one,
    report_baseline_three,
    report_baseline_two,
    report_bounds,
    report_conditions,
    report_sensitivity,
    report_specificity,
    report_twin_comparisons,
    score,
)

# The reference pipeline's eight steps and what each one does on a healthy
# laptop, so a fixture that departs from this is departing from something.
FIRES = {
    "generate_inputs": 0,
    "daily_revenue": 4,
    "customer_keys": 4,
    "apply_price_updates": 4,
    "append_audit_log": 4,
    "mean_basket": 4,
    "sparse_customer_keys": 2,
    "roll_up_keys": 0,
}


def a_step(index: int, name: str, *, fired: int = 0, artifacts: int = 1) -> StepMeasurement:
    """Four comparisons of `artifacts` artifacts each, of which `fired` disagreed.

    `artifacts=0` is the case the whole file is about: four rounds that compared
    nothing, which is what a step that wrote no Parquet leaves behind.
    """
    step = StepMeasurement(index=index, name=name, comparisons=4, terms=1000)
    for round_ in range(4):
        step.observe(
            [
                ArtifactFindings(
                    name=f"{name}_rows",
                    key=("id",),
                    reference_rows=1000,
                    candidate_rows=1000,
                    row_missing=9 if round_ < fired else 0,
                )
                for _ in range(artifacts)
            ]
        )
    return step


def a_trial(*, artifacts: int = 1, downstream_uncontained: int = 3, fires: dict | None = None):
    fires = FIRES if fires is None else {**FIRES, **fires}
    reference = [
        a_step(i, name, fired=fires[name], artifacts=artifacts)
        for i, name in enumerate(fires)
    ]
    uncontained = [
        a_step(
            i,
            name,
            fired=downstream_uncontained if name == "roll_up_keys" else fires[name],
            artifacts=artifacts,
        )
        for i, name in enumerate(fires)
    ]
    twins = []
    for i, (_, twin) in enumerate(PAIRS):
        step = a_step(i, twin, fired=0, artifacts=artifacts)
        step.amplifications = [
            Amplification(
                amplifier="tie collapse",
                note="one key group collapsed",
                comparisons=2,
                fired=0,
                artifacts_compared=2 if artifacts else 0,
            )
        ]
        twins.append(step)
    return Trial(
        reference=reference,
        twins=twins,
        uncontained=uncontained,
        naive={
            BENIGN: NaiveScore(632, 633, 2000, artifacts),
            INTERMITTENT: NaiveScore(0, 0, 2000, artifacts),
        },
        # The cheap comparison agreeing with the oracle cell for cell, which is
        # what it does on the real pipeline.
        matched={name: (fired if artifacts else 0, 4 if artifacts else 0)
                 for name, fired in fires.items()},
        matched_twins={twin: (0, 4 if artifacts else 0) for _, twin in PAIRS},
        forced={
            name: Amplification(
                amplifier=name,
                note="one key group collapsed",
                comparisons=4 if artifacts else 0,
                fired=fired if artifacts else 0,
                artifacts_compared=4 if artifacts else 0,
            )
            for name, fired in (
                ("tie collapse", 4),
                ("thread count", 3),
                ("row multiplication", 4),
            )
        },
        exit_with_amplifiers=1,
        exit_without=1,
        seconds=36.0,
    )


def verdicts(trials: list[Trial], seconds: float = 300.0) -> dict[str, str]:
    """Every report main() runs, in the order it runs them, keyed by condition."""
    broken: dict = {}
    twins: dict = {}
    for trial in trials:
        score(trial.reference, broken)
        score(trial.twins, twins)
    report_sensitivity(broken, len(trials))
    report_specificity(twins, len(trials))
    report_twin_comparisons(trials)
    naive = report_baseline_one(trials, broken)
    report_baseline_two()
    ablation = report_baseline_three(trials, broken)
    amplification = report_baseline_four(trials, broken)
    bounds = report_bounds(trials, Policy())
    report_attribution(trials)
    checked = report_conditions(
        trials, broken, twins, naive, ablation, amplification, bounds, seconds
    )
    return {condition: verdict for condition, verdict, _ in checked}


def verdict_for(seen: dict[str, str], needle: str) -> str:
    matched = [v for condition, v in seen.items() if needle in condition]
    assert len(matched) == 1, f"{needle} matched {len(matched)} conditions"
    return matched[0]


# The wording each condition is found by here, and what it concludes if it
# fires. The first three are the ones that call a piece of this project
# unnecessary.
UNNECESSARY = ("more machinery than the problem needs", "amplification is unmotivated", "Spot")
DERIVABLE = (
    *UNNECESSARY,
    "sensitivity below",
    "any twin fired",
    "reached neither",
)


def test_nothing_is_concluded_from_ten_trials_that_compared_nothing():
    seen = verdicts([a_trial(artifacts=0) for _ in range(10)])
    for needle in DERIVABLE:
        assert verdict_for(seen, needle) == "NOT MEASURED", needle
    assert verdict_for(seen, "inside the computed bound") == "NOT MEASURED"
    assert verdict_for(seen, "took longer than") == "not triggered", "the clock is always measured"


def test_the_conditions_still_decide_when_there_is_something_to_read():
    """The control. A guard that answers NOT MEASURED to everything is not a guard."""
    seen = verdicts([a_trial() for _ in range(10)])
    for needle in DERIVABLE:
        assert verdict_for(seen, needle) == "not triggered", needle
    # Nothing in these fixtures drifts by a float, so the headroom check has
    # nothing to run on and says so. That is the guard this file copied.
    assert verdict_for(seen, "inside the computed bound") == "NOT MEASURED"


def test_a_broken_step_that_misses_triggers_rather_than_going_unmeasured():
    ten = [a_trial() for _ in range(9)] + [a_trial(fires={"daily_revenue": 0})]
    assert verdict_for(verdicts(ten), "sensitivity below") == "TRIGGERED"


def test_a_broken_step_that_went_quiet_for_one_trial_cannot_be_called_ten_of_ten():
    """9 of 9 fired is not 10 of 10, and the spec fixed 10 of 10."""
    quiet = a_trial()
    quiet.reference[1] = a_step(1, "daily_revenue", artifacts=0)
    ten = [a_trial() for _ in range(9)] + [quiet]
    assert verdict_for(verdicts(ten), "sensitivity below") == "NOT MEASURED"


def test_a_twin_that_fires_beats_an_incomplete_twin_sample():
    """A fire is evidence whatever else was missed, so it outranks the gap."""
    loud = a_trial()
    loud.twins[2] = a_step(2, PAIRS[2][1], fired=1)
    ten = [a_trial(artifacts=0) for _ in range(9)] + [loud]
    assert verdict_for(verdicts(ten), "any twin fired") == "TRIGGERED"


def test_nothing_the_eval_prints_claims_a_step_is_deterministic(capsys):
    """The four words `test_cli.py` holds the report to, applied to the eval's output.

    Both fixtures, because the silent one reaches six sentences the healthy one
    never prints and "compared nothing" prose is exactly where one of these
    words gets in. The status token comes out of the text first, the same way
    the CLI's version of this test does it, because STABLE_ON_THIS_INPUT is
    allowed and the bare word is not.
    """
    verdicts([a_trial() for _ in range(10)])
    verdicts([a_trial(artifacts=0) for _ in range(10)])
    printed = capsys.readouterr().out.replace(STABLE_ON_THIS_INPUT, "").lower()
    for forbidden in ("deterministic", "stable", "reproducible", "passed"):
        assert forbidden not in printed


def test_no_printed_line_runs_past_the_width_the_eval_wraps_to(capsys):
    """Every long line in the eval is hand-broken, and hand-broken lines rot.

    Two of them went to 106 and 115 characters within an hour of being written,
    both because a denominator picked up a clause. This holds at the ten trials
    and five runs the eval defaults to: a fire rate out of nine buckets, which
    is what --runs 10 would print, is wider than anything measured here.
    """
    verdicts([a_trial() for _ in range(10)])
    verdicts([a_trial(artifacts=0) for _ in range(10)])
    over = [line for line in capsys.readouterr().out.splitlines() if len(line) > WIDTH]
    assert not over, over[:3]


def test_the_run_matched_row_does_not_claim_agreement_from_nothing(capsys):
    """The strongest sentence in the eval, and it must not come out of an absence.

    "No evidence for the oracle over multiset equality" is a conclusion about
    the project's own centrepiece. Ten trials that compared nothing produce zero
    disagreements and zero twin fires, which is the same pair of zeros a perfect
    agreement produces.
    """
    verdicts([a_trial(artifacts=0) for _ in range(10)])
    assert "no evidence for the oracle" not in capsys.readouterr().out

    verdicts([a_trial() for _ in range(10)])
    assert "no evidence for the oracle" in capsys.readouterr().out


def test_a_cheap_comparison_that_misses_a_step_is_counted_as_a_disagreement(capsys):
    """The control on the row above, which is a row of zeros on a healthy run.

    The oracle catches daily_revenue in all ten trials of this fixture, so a
    run-matched pass that missed it ten times has to print ten disagreements out
    of the eighty step-trials and lose the sentence that follows.
    """
    ten = [a_trial() for _ in range(10)]
    for trial in ten:
        trial.matched["daily_revenue"] = (0, 4)
    verdicts(ten)

    printed = capsys.readouterr().out
    assert "disagreed with the oracle's fire count on 10 of 80" in printed
    assert "no evidence for the oracle" not in printed
