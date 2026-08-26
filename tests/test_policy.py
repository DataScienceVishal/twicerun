"""Tests for the stage that decides, over measurements built by hand.

No DuckDB here at all. The point of the policy layer is that it is a pure
function of what the oracle measured, so it can be tested by handing it
measurements rather than by running a pipeline and hoping the right one turns up.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings, ColumnDrift
from twicerun.policy import (
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


def step(*rounds: list[ArtifactFindings], terms: int = 2_000_000) -> StepMeasurement:
    measured = StepMeasurement(index=1, name="daily_revenue", comparisons=len(rounds), terms=terms)
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
    assert "ROW_MISSING ROW_EXTRA" in verdict.blocked


def test_reduction_order_refuses_a_difference_too_large_to_be_reassociation():
    """Catastrophic cancellation and a genuinely different set of terms both land here.

    Vanishing at threads=1 is not enough on its own, which is why the
    conjunction has a magnitude condition in it at all.
    """
    verdict = judge(step([drifting(1e-6)]), Policy(REDUCTION_ORDER))
    assert (verdict.fired, verdict.tolerated) == (1, 0)
    assert "outside the reassociation bound" in verdict.blocked


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


def test_a_downgrade_names_the_condition_that_does_not_exist_yet():
    """The threads=1 half of the conjunction arrives in slice 3.

    Until then a TOLERATED rests on two conditions of three, and a report that
    did not say so would be claiming more than the code checked.
    """
    verdict = judge(step([drifting(4.5e-16)]), Policy(REDUCTION_ORDER))
    assert verdict.tolerated == 1
    assert any("threads=1" in note for note in verdict.unverified)


def test_nothing_is_unverified_when_nothing_was_downgraded():
    assert judge(step([losing_rows()]), Policy(REDUCTION_ORDER)).unverified == ()


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
    assert verdict.unverified == ()


def test_a_step_using_both_routes_still_names_the_missing_condition():
    """One comparison the bound explains, one only the manual threshold does."""
    mixed = step([drifting(4.5e-16)], [drifting(1e-6)])
    verdict = judge(mixed, Policy(REDUCTION_ORDER, tolerance_relative=1e-3))
    assert verdict.tolerated == 2
    assert any("threads=1" in note for note in verdict.unverified)


def test_the_bound_is_the_reason_on_record_when_it_can_explain_the_difference():
    """Order matters. A threshold wide enough to cover everything used to hide it."""
    wide = Policy(REDUCTION_ORDER, tolerance_relative=1.0, tolerance_ulps=10**9)
    assert judge(step([drifting(4.5e-16)]), wide).unverified != ()


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
