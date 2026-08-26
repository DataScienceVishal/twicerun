"""Deciding whether a measured difference counts, without touching the measurement.

This is a separate stage from the oracle on purpose, and it is the part of the
project most worth arguing about. A tolerance that can move the reported numbers
is a tolerance that was tuned until the demo passed, and any figure downstream
of it is negotiable. So nothing here writes back: the ulp counts, the relative
magnitudes and the row counts are identical under every policy, and only the
verdict moves.

`strict` is the default and it stays the default. Asked what difference between
two runs of the same total he would have accepted in a pipeline he shipped,
Vishal picked zero, and asked how he found out a pipeline had gone wrong, he
picked a downstream count that did not reconcile. Those two answers fit
together: if breakage surfaces as an exact reconciliation failing, a tool that
quietly absorbs a difference is a tool that hides the thing you would have used
to find the bug.

`reduction-order` is the opt-in that says a difference is float reassociation
rather than a wrong answer, and it has to clear three conditions at once. Only
two of them exist today; see `MISSING_CONDITION`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from twicerun.measurement import StepMeasurement, name_classes
from twicerun.oracle import ArtifactFindings, Divergence

STRICT = "strict"
REDUCTION_ORDER = "reduction-order"

# The unit roundoff for float64. Half an ulp at 1.0.
UNIT_ROUNDOFF = 2.0**-53

# Condition 2 of the reduction-order conjunction needs the step's fire rate at
# threads=1, and the single-threaded bisect arrives in slice 3. It is named in
# every report that downgrades anything, because a downgrade resting on two
# conditions of three is not the same claim as one resting on three, and
# quietly counting the missing one as satisfied is how a tolerance stops
# meaning anything.
MISSING_CONDITION = (
    "condition 2 of 3, that the step does not diverge at threads=1, is not "
    "implemented until slice 3. Every TOLERATED below rests on the other two"
)


def gamma(terms: int) -> float:
    """The classical bound factor for summing `terms` float64 values in some order.

    Reassociating a sum of n terms moves the result by at most gamma_n times
    the sum of the magnitudes, with gamma_n = n*u / (1 - n*u). Two different
    orderings differ by at most twice that.
    """
    if terms <= 0:
        return 0.0
    product = terms * UNIT_ROUNDOFF
    if product >= 1.0:
        # Past 2^53 terms the bound stops being a bound at all. Nothing is
        # tolerated rather than a negative or infinite threshold being used.
        return float("inf")
    return product / (1.0 - product)


def relative_bound(terms: int) -> float:
    """The reassociation bound expressed as a relative difference.

    The spec writes it as an absolute bound, `2 * gamma_n * max(|a|, |b|)`, and
    the tool measures relative difference as `|a - b| / max(|a|, |b|)`. The
    magnitude is the same quantity on both sides and cancels exactly, so the
    test reduces to a comparison against `2 * gamma_n` and no magnitudes have to
    be carried around to apply it.
    """
    return 2.0 * gamma(max(terms, 1))


@dataclass(frozen=True)
class Policy:
    name: str = STRICT
    tolerance_relative: float | None = None
    tolerance_ulps: int | None = None

    @property
    def manual(self) -> bool:
        return self.tolerance_relative is not None or self.tolerance_ulps is not None

    def describe(self) -> str:
        if self.name == STRICT and not self.manual:
            return "strict, so any difference at all is a divergence"
        parts = []
        if self.name == REDUCTION_ORDER:
            parts.append("reduction-order, so drift inside the reassociation bound is TOLERATED")
        else:
            parts.append("strict")
        if self.tolerance_relative is not None:
            parts.append(f"--tolerance-rel {self.tolerance_relative:g}")
        if self.tolerance_ulps is not None:
            parts.append(f"--tolerance-ulps {self.tolerance_ulps}")
        return ", ".join(parts)


@dataclass(frozen=True)
class DriftBound:
    """The bound, what was actually observed against it, and how much slack that leaves.

    Two figures, not one. `terms` is the step's `rows_read`, which is what the
    spec says to use and is loose in the permissive direction: for a group-by it
    is the whole input rather than the terms behind any one output value, so it
    overstates n by roughly the output row count. `tight_terms` divides it back
    down by that count, which is the number of terms an aggregate over
    equal-sized groups actually sums.

    Both ratios are printed because the pre-registered check only bites at the
    tighter one. Clearing 1000x on a bound that is loose by a factor of a
    thousand proves very little, and hiding that would be exactly the kind of
    result this project exists not to publish.
    """

    terms: int
    output_rows: int
    observed: float

    @property
    def bound(self) -> float:
        return relative_bound(self.terms)

    @property
    def ratio(self) -> float:
        return self.bound / self.observed if self.observed > 0 else float("inf")

    @property
    def tight_terms(self) -> int:
        return max(1, self.terms // max(self.output_rows, 1))

    @property
    def tight_bound(self) -> float:
        return relative_bound(self.tight_terms)

    @property
    def tight_ratio(self) -> float:
        return self.tight_bound / self.observed if self.observed > 0 else float("inf")


# Pre-registered before any of this was built: observed drift on the benign step
# must sit at least this far inside the computed bound, or the bound is binding
# and the derivation gets revisited rather than the threshold moved.
HEADROOM_REQUIRED = 1000.0


@dataclass(frozen=True)
class StepVerdict:
    step: StepMeasurement
    fired: int
    tolerated: int
    bound: DriftBound | None = None
    blocked: str | None = None
    unverified: tuple[str, ...] = field(default_factory=tuple)


def judge(step: StepMeasurement, policy: Policy) -> StepVerdict:
    bound = _bound_for(step)
    if not policy.manual and policy.name != REDUCTION_ORDER:
        return StepVerdict(step=step, fired=step.fired, tolerated=0, bound=bound)

    # Condition 1 of the conjunction, and it is step-wide rather than per
    # comparison. A step that lost a row in any comparison is not a step that
    # merely reassociated in the others.
    pure = step.classes == frozenset({Divergence.VALUE_DRIFT})
    fired = tolerated = 0
    for round_ in step.diverged:
        if not round_:
            continue
        if all(_tolerable(f, policy, bound, pure) for f in round_):
            tolerated += 1
        else:
            fired += 1

    unverified = (MISSING_CONDITION,) if tolerated and policy.name == REDUCTION_ORDER else ()
    return StepVerdict(
        step=step,
        fired=fired,
        tolerated=tolerated,
        bound=bound,
        blocked=_blocked(step, policy, bound, pure) if fired and not tolerated else None,
        unverified=unverified,
    )


def _bound_for(step: StepMeasurement) -> DriftBound | None:
    observed = step.max_relative
    if observed is None:
        return None
    drifting = max(
        (f for round_ in step.rounds for f in round_ if f.drift_rows),
        key=lambda f: f.drift_rows,
        default=None,
    )
    return DriftBound(
        terms=step.terms,
        output_rows=drifting.reference_rows if drifting else 0,
        observed=observed,
    )


def _tolerable(
    findings: ArtifactFindings, policy: Policy, bound: DriftBound | None, pure: bool
) -> bool:
    """Whether one artifact's findings can be downgraded.

    Anything other than VALUE_DRIFT blocks it outright, and so does drift on an
    exact column: no reordering of a fixed-point addition or a string
    concatenation changes the answer, so a DECIMAL that moved is a wrong answer
    whatever its size.
    """
    if findings.classes != frozenset({Divergence.VALUE_DRIFT}):
        return False
    if any(not moved.approximate for moved in findings.drift):
        return False

    worst_relative = max((m.max_relative or 0.0) for m in findings.drift)
    worst_ulps = max((m.max_ulps or 0) for m in findings.drift)

    if policy.manual and _inside_manual(policy, worst_relative, worst_ulps):
        return True
    return (
        policy.name == REDUCTION_ORDER
        and pure
        and bound is not None
        and worst_relative <= bound.bound
    )


def _inside_manual(policy: Policy, relative: float, ulps: int) -> bool:
    """Every threshold that was set has to be cleared, not just one of them."""
    return not (
        (policy.tolerance_relative is not None and relative > policy.tolerance_relative)
        or (policy.tolerance_ulps is not None and ulps > policy.tolerance_ulps)
    )


def _blocked(
    step: StepMeasurement, policy: Policy, bound: DriftBound | None, pure: bool
) -> str | None:
    """Why a step's drift was not downgraded. Silent on a step with no drift to downgrade."""
    if policy.name != REDUCTION_ORDER or Divergence.VALUE_DRIFT not in step.classes:
        return None
    if not pure:
        others = step.classes - {Divergence.VALUE_DRIFT}
        return (
            f"drift not downgraded: the step also shows {name_classes(others)}, "
            f"which reassociation cannot produce"
        )
    if bound is not None and bound.observed > bound.bound:
        return (
            f"not downgraded: max relative drift {bound.observed:.4e} is outside "
            f"the reassociation bound {bound.bound:.4e}"
        )
    return None
