"""Tests for the stage that decides, over measurements built by hand.

No DuckDB here at all. The point of the policy layer is that it is a pure
function of what the oracle measured, so it can be tested by handing it
measurements rather than by running a pipeline and hoping the right one turns up.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from twicerun.cause import Bisect
from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings, ColumnDrift
from twicerun.policy import (
    BY_BOUND,
    BY_THRESHOLD,
    REDUCTION_ORDER,
    STRICT,
    UNIT_ROUNDOFF,
    DriftBound,
    Policy,
    gamma,
    judge,
    relative_bound,
)


def drifting(
    relative: float, ulps: int | None = 3, *, approximate: bool = True
) -> ArtifactFindings:
    return ArtifactFindings(
        name="totals",
        key=("day",),
        reference_rows=1000,
        candidate_rows=1000,
        matched=1000,
        drift_rows=600,
        drift=(
            ColumnDrift(
                column="revenue",
                approximate=approximate,
                rows=600,
                max_ulps=ulps,
                max_relative=relative,
                example=("1.0", "1.0000000000000002"),
            ),
        ),
    )


def losing_rows() -> ArtifactFindings:
    return ArtifactFindings(
        name="keys",
        key=("id",),
        reference_rows=1000,
        candidate_rows=1000,
        matched=900,
        row_missing=100,
        row_extra=100,
    )


def step(
    *rounds: list[ArtifactFindings],
    terms: int = 2_000_000,
    single_threaded: int | None = 0,
    bisect_artifacts: int | None = None,
) -> StepMeasurement:
    """A step's measurements, carrying by default the bisect condition 2 needs.

    `single_threaded` is the bisect's own fire rate. 0 is a step whose drift
    went away at one thread, which is what reduction-order asks for. None is a
    step that was never bisected, which is no evidence rather than good news.
    `bisect_artifacts` defaults to one per comparison and is set to 0 for the
    step that re-executed and wrote nothing.
    """
    measured = StepMeasurement(
        index=1,
        name="daily_revenue",
        comparisons=len(rounds),
        terms=terms,
        bisect=(
            None
            if single_threaded is None
            else Bisect(
                comparisons=len(rounds),
                fired=single_threaded,
                artifacts_compared=len(rounds) if bisect_artifacts is None else bisect_artifacts,
            )
        ),
    )
    for round_ in rounds:
        measured.observe(round_)
    return measured


def test_gamma_is_the_textbook_formula():
    assert gamma(1) == pytest.approx(UNIT_ROUNDOFF / (1 - UNIT_ROUNDOFF))
    assert gamma(2_000_000) == pytest.approx(2.2204e-10, rel=1e-4)


def test_a_term_count_past_two_to_the_fifty_three_has_no_bound():
    """n*u >= 1 makes the denominator zero or negative, so there is nothing to compare against."""
    assert gamma(2**54) == float("inf")


def test_the_magnitude_cancels_out_of_the_bound():
    """The spec writes an absolute bound; the tool measures a relative difference.

    2 * gamma_n * max(|a|,|b|) compared against |a - b| is the same test as
    2 * gamma_n compared against |a - b| / max(|a|,|b|), so no magnitudes have
    to be carried around to apply it.
    """
    assert relative_bound(2_000_000) == pytest.approx(2 * gamma(2_000_000))


def test_strict_tolerates_nothing_however_small():
    verdict = judge(step([drifting(1e-18)], [drifting(1e-18)]), Policy(STRICT))
    assert (verdict.fired, verdict.tolerated) == (2, 0)


def test_reduction_order_downgrades_drift_inside_the_bound():
    verdict = judge(step([drifting(4.5e-16)], [drifting(4.5e-16)]), Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (0, 2)


def test_reduction_order_refuses_a_step_that_also_lost_rows():
    """Condition 1, and it is step-wide rather than per comparison.

    A step that lost a row in one comparison is not a step that merely
    reassociated in the others.
    """
    verdict = judge(step([drifting(4.5e-16)], [losing_rows()]), Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (2, 0)
    assert "ROW_MISSING ROW_EXTRA" in verdict.bound_refusal


def test_reduction_order_refuses_a_difference_too_large_to_be_reassociation():
    """Catastrophic cancellation and a genuinely different set of terms both land here.

    Vanishing at threads=1 is not enough on its own, which is why the
    conjunction has a magnitude condition in it at all.
    """
    verdict = judge(step([drifting(1e-6)]), Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (1, 0)
    assert "outside the reassociation bound" in verdict.bound_refusal


def test_drift_on_an_exact_column_is_never_reassociation():
    """A DECIMAL sum is fixed point. If it moved, the answer is wrong, not reordered."""
    verdict = judge(step([drifting(1e-30, approximate=False)]), Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (1, 0)


def test_the_measurement_does_not_move_when_the_policy_does():
    """The property the whole two-stage split exists to guarantee.

    If a tolerance could change a reported number, every number downstream of
    it would be negotiable and none of them would be worth printing.
    """
    measured = step([drifting(4.5e-16)], [drifting(4.5e-16)])
    strict, tolerant = judge(measured, Policy(STRICT)), judge(measured, Policy(REDUCTION_ORDER))

    assert strict.fired != tolerant.fired
    assert strict.step.max_relative == tolerant.step.max_relative
    assert strict.step.max_ulps == tolerant.step.max_ulps
    assert strict.step.fired == tolerant.step.fired == 2
    assert strict.bound.observed == tolerant.bound.observed


def test_drift_that_survives_one_thread_is_not_reassociation_however_small():
    """Condition 2, and the reason it is worth the cost of the bisect.

    Before it existed, a TOLERATED said the drift was small enough to be
    reduction order. With it, the drift has to actually disappear when the
    parallelism does. This step keeps moving at threads=1, so whatever moved it
    is not the order a parallel reduction added its terms in.
    """
    verdict = judge(step([drifting(4.5e-16)], single_threaded=1), Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (1, 0)
    assert "still diverges at threads=1, 1 of 1" in verdict.bound_refusal


def test_a_step_that_was_never_bisected_has_nothing_to_downgrade_on():
    """No evidence is not the same as evidence, and the old code treated it as if it were.

    A manifest written before the bisect existed lands here, and so would any
    future path that skipped it. The refusal is the safe direction: it reports
    a difference reassociation might well explain.
    """
    verdict = judge(step([drifting(4.5e-16)], single_threaded=None), Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (1, 0)
    assert "was not re-executed at threads=1" in verdict.bound_refusal


def test_a_threshold_downgrading_everything_still_reports_why_the_bound_did_not():
    """The all-tolerated half of a hole that had already been widened once.

    The reason was computed only where something still fired, so a threshold
    wide enough to cover every comparison deleted it. This step diverges 4 of 4
    at threads=1 and moves five orders of magnitude outside the bound, and it
    exited 0 with the tool saying nothing about either. A flag that quietly
    removes a falsifiable check is the defect class this project keeps finding.
    """
    far_out = step(*[[drifting(4.0e-07, ulps=9)] for _ in range(4)], single_threaded=4)
    verdict = judge(far_out, Policy(REDUCTION_ORDER, tolerance_relative=1e-6))

    assert (verdict.fired, verdict.tolerated) == (0, 4)
    assert "still diverges at threads=1, 4 of 4" in verdict.bound_refusal


def test_a_step_the_bound_does_cover_has_no_refusal_to_report():
    """The other side of it, or the line above would print on every clean downgrade."""
    assert judge(step([drifting(4.5e-16)]), Policy(REDUCTION_ORDER)).bound_refusal is None


def test_a_bisect_that_compared_nothing_is_not_a_clean_bisect():
    """`any([])` is False, so a rate out of no artifacts reads like four clean ones.

    A step can write nothing when it is re-executed alone, most often by
    checking for a table a skipped step would have created. The main loop
    already refuses to read an empty comparison as evidence about a step and
    the bisect has to match it, or reduction-order downgrades on a zero that
    counted nothing.
    """
    empty = step([drifting(4.5e-16)], single_threaded=0, bisect_artifacts=0)
    verdict = judge(empty, Policy(REDUCTION_ORDER))

    assert (verdict.fired, verdict.tolerated) == (1, 0)
    assert "out of nothing" in verdict.bound_refusal
    assert empty.cause is None


def test_a_manual_threshold_does_not_need_the_bisect_to_agree_with_it():
    """The two routes clear different conditions and that is deliberate.

    --tolerance-rel is a user saying a difference of that size does not matter
    in their domain. That claim is theirs to make and does not rest on any
    mechanism, so a step that still moves at threads=1 can still take it.
    """
    verdict = judge(step([drifting(1e-9)], single_threaded=1), Policy(STRICT, 1e-6))
    assert verdict.tolerated == 1
    assert verdict.routes == (BY_THRESHOLD,)


def test_no_route_is_recorded_when_nothing_was_downgraded():
    assert judge(step([losing_rows()]), Policy(REDUCTION_ORDER)).routes == ()


def test_a_manual_downgrade_under_reduction_order_does_not_claim_the_bound():
    """The note has to follow the route that downgraded, not the policy name.

    --policy reduction-order --tolerance-rel 0.001 sends a difference thousands
    of times outside the bound down the manual route. Gating the note on the
    policy name printed "every TOLERATED rests on conditions 1 and 3" over a
    downgrade that had cleared neither, in a report that stated the magnitude
    was outside the bound nine lines further down.
    """
    outside = step([drifting(1e-6)])
    verdict = judge(outside, Policy(REDUCTION_ORDER, tolerance_relative=1e-3))
    assert verdict.tolerated == 1
    assert verdict.routes == (BY_THRESHOLD,)


def test_a_step_using_both_routes_records_both():
    """One comparison the bound explains, one only the manual threshold does."""
    mixed = step([drifting(4.5e-16)], [drifting(1e-6)])
    verdict = judge(mixed, Policy(REDUCTION_ORDER, tolerance_relative=1e-3))
    assert verdict.tolerated == 2
    assert verdict.routes == (BY_BOUND, BY_THRESHOLD)


def test_the_bound_is_the_reason_on_record_when_it_can_explain_the_difference():
    """Order matters. A threshold wide enough to cover everything used to hide it."""
    wide = Policy(REDUCTION_ORDER, tolerance_relative=1.0, tolerance_ulps=10**9)
    assert judge(step([drifting(4.5e-16)]), wide).routes == (BY_BOUND,)


def test_a_manual_tolerance_works_under_strict_because_it_is_a_domain_claim():
    """--tolerance-ulps is someone saying they know their data, not a mechanism claim.

    So it does not need the reduction-order conjunction. It does still need the
    difference to be a float difference and nothing else.
    """
    lenient = Policy(STRICT, tolerance_ulps=4)
    assert judge(step([drifting(1e-6, ulps=3)]), lenient).tolerated == 1


def test_a_manual_tolerance_does_not_excuse_a_missing_row():
    lenient = Policy(STRICT, tolerance_ulps=1_000_000, tolerance_relative=1.0)
    assert judge(step([losing_rows()]), lenient).fired == 1


def test_setting_both_thresholds_means_a_difference_has_to_clear_both():
    both = Policy(STRICT, tolerance_ulps=4, tolerance_relative=1e-30)
    assert judge(step([drifting(1e-6, ulps=3)]), both).fired == 1


def test_the_bound_reports_the_headroom_at_the_looser_and_the_tighter_term_count():
    """Both, because only the tighter one can fail.

    n is the step's whole input row count, which for a group-by overstates the
    terms behind any one output value by roughly the group count. Clearing
    1000x on a bound loose by a factor of a thousand proves very little, so the
    report prints the version with the slack taken out next to it.
    """
    bound = DriftBound(terms=2_000_000, output_rows=1_000, observed=4.5e-16)
    assert bound.tight_terms == 2_000
    assert bound.ratio == pytest.approx(bound.tight_ratio * 1000, rel=1e-3)
    assert bound.ratio > 1000 > bound.tight_ratio


def test_a_step_that_read_nothing_still_gets_a_bound_of_one_rounding():
    """rows_read is 0 for a step that only used ctx.sql, and 0 terms is no bound at all.

    One addition is one rounding, so the floor is one term rather than zero.
    That is the strict direction: it reports a difference reassociation could
    in fact explain, rather than tolerating one it could not.
    """
    verdict = judge(step([drifting(1e-9)], terms=0), Policy(REDUCTION_ORDER))
    assert verdict.bound.bound == pytest.approx(2 * gamma(1))
    assert verdict.fired == 1


def test_a_step_with_no_drift_has_no_bound_to_report():
    assert judge(step([losing_rows()]), Policy(REDUCTION_ORDER)).bound is None


def test_replacing_the_policy_on_a_report_does_not_touch_the_findings():
    measured = step([drifting(4.5e-16)])
    before = replace(measured.rounds[0][0])
    judge(measured, Policy(REDUCTION_ORDER))
    assert measured.rounds[0][0] == before


def test_an_unmeasurable_ulp_distance_refuses_the_ulp_threshold():
    """Every float replaced by NULL, which is as wrong as an artifact gets.

    ULP distance comes back NULL because abs(NULL - x) is NULL and max() over
    only NULLs is NULL. Folding that to zero told --tolerance-ulps the values
    were nought last-bit steps apart and the tool exited 0. The relative figure
    is infinite in that case and says so, but with only --tolerance-ulps set it
    was never consulted.
    """
    nulled = step([drifting(float("inf"), ulps=None)])
    assert judge(nulled, Policy(STRICT, tolerance_ulps=4)).fired == 1
    assert judge(nulled, Policy(STRICT, tolerance_ulps=10**18)).fired == 1


def test_an_unmeasurable_ulp_distance_does_not_block_a_relative_threshold():
    """The two thresholds test different figures, so one missing is not both."""
    lenient = Policy(STRICT, tolerance_relative=1.0)
    assert judge(step([drifting(0.5, ulps=None)]), lenient).tolerated == 1


def test_the_step_line_and_the_bound_quote_the_same_observation():
    """Two maxima under one word was worth about a 25 percent understatement.

    The step line printed the comparison with the most drifting rows and the
    bound section the largest magnitude across every comparison, both called a
    maximum. Here the loudest comparison by row count is not the one that moved
    furthest, which is the shape that used to disagree.
    """
    loudest = replace(drifting(3.6e-16), drift_rows=900)
    loudest = replace(loudest, drift=(replace(loudest.drift[0], rows=900),))
    furthest = replace(drifting(4.6e-16), drift_rows=100)
    furthest = replace(furthest, drift=(replace(furthest.drift[0], rows=100),))

    measured = step([loudest], [furthest])
    assert measured.worst.drift_rows == 900
    assert measured.furthest_drift.max_relative == 4.6e-16
    assert judge(measured, Policy(STRICT)).bound.observed == measured.furthest_drift.max_relative


def test_a_step_with_some_comparisons_tolerated_still_explains_the_ones_that_fired():
    """It used to require that nothing at all had been downgraded.

    A step drifting inside the bound on one comparison and far outside it on
    another reported a fire rate with no reason attached to it.
    """
    mixed = step([drifting(4.5e-16)], [drifting(1e-6)])
    verdict = judge(mixed, Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (1, 1)
    assert "outside the reassociation bound" in verdict.bound_refusal
