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
rather than a wrong answer, and it has to clear three conditions at once: the
step's only class is VALUE_DRIFT, the step does not diverge at threads=1, and
the magnitude is inside the derived bound. None of the three is measured here.
The first two come off the step's own measurements and the third off the term
count, so this file reads and decides and never writes.

The middle condition is what makes the verdict a claim about a mechanism rather
than about a size. Until the bisect existed, a TOLERATED rested on the drift
being small and float-only, which is "small enough to be reassociation". With
the threads=1 rate in hand it rests on the drift disappearing when the
parallelism does, which is a different and much stronger sentence. It is not
proof: 0 of 4 is 4 comparisons, and `cause.py` says what they are worth.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from twicerun.measurement import StepMeasurement, name_classes
from twicerun.oracle import ArtifactFindings, Divergence

STRICT = "strict"
REDUCTION_ORDER = "reduction-order"

# Which of the two routes downgraded a finding. They carry different claims and
# clear different conditions, so the report cannot describe one in the other's
# terms: the derived route asserts a mechanism and has to show the drift going
# away at threads=1, while a manual threshold is a user saying a difference of
# that size does not matter to them and needs no mechanism at all.
BY_BOUND = "the derived reassociation bound"
BY_THRESHOLD = "a --tolerance threshold"
# Tried in this order and reported in it, so the stronger claim comes first and
# a sort by spelling cannot decide which route a report leads with.
ROUTES = (BY_BOUND, BY_THRESHOLD)

# The unit roundoff for float64. Half an ulp at 1.0.
UNIT_ROUNDOFF = 2.0**-53

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
    # Why the derived route did not cover this step, where it did not. Named
    # for the bound rather than for the outcome because it prints whether or
    # not anything was blocked: a --tolerance threshold can downgrade every
    # comparison, and the reason reassociation does not explain the step is
    # still the thing a reader needs.
    bound_refusal: str | None = None
    # Which routes actually downgraded something here, so the report can say
    # what a TOLERATED rests on instead of leaving two different claims looking
    # identical on the step line.
    routes: tuple[str, ...] = field(default_factory=tuple)


def judge(step: StepMeasurement, policy: Policy) -> StepVerdict:
    bound = _bound_for(step)
    if not policy.manual and policy.name != REDUCTION_ORDER:
        return StepVerdict(step=step, fired=step.fired, tolerated=0, bound=bound)

    # Conditions 1 and 2, both step-wide rather than per comparison. A step that
    # lost a row in any comparison is not a step that merely reassociated in the
    # others, and a step that still diverges on one thread has not shown that
    # the order of a parallel reduction is what moved it.
    pure = step.classes == frozenset({Divergence.VALUE_DRIFT})
    quiet_at_one_thread = step.bisect is not None and step.bisect.fired == 0
    fired = tolerated = 0
    routes: set[str] = set()
    for round_ in step.diverged:
        if not round_:
            continue
        took = [_tolerable(f, policy, bound, pure and quiet_at_one_thread) for f in round_]
        if all(took):
            tolerated += 1
            routes.update(took)
        else:
            fired += 1

    return StepVerdict(
        step=step,
        fired=fired,
        tolerated=tolerated,
        bound=bound,
        bound_refusal=_bound_refusal(step, policy, bound, pure, quiet_at_one_thread),
        routes=tuple(route for route in ROUTES if route in routes),
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
    findings: ArtifactFindings, policy: Policy, bound: DriftBound | None, mechanism: bool
) -> str | None:
    """Which route downgrades one artifact's findings, or None if neither does.

    `mechanism` is conditions 1 and 2 together, decided once for the step. It
    gates the derived route and not the manual one, because a threshold is a
    user's claim about their own data rather than a claim about reduction order,
    and it is complete without any evidence from the bisect.

    Returning the route rather than a bool is what lets the report say what a
    downgrade rests on. The two are not interchangeable.

    Anything other than VALUE_DRIFT blocks both routes outright, and so does
    drift on an exact column: no reordering of a fixed-point addition or a
    string concatenation changes the answer, so a DECIMAL that moved is a wrong
    answer whatever its size.
    """
    if findings.classes != frozenset({Divergence.VALUE_DRIFT}):
        return None
    if any(not moved.approximate for moved in findings.drift):
        return None

    worst_relative = _worst(m.max_relative for m in findings.drift)
    worst_ulps = _worst(m.max_ulps for m in findings.drift)
    if worst_relative is None:
        return None

    # The derived bound is tried first so that whenever it can explain a
    # difference it is the reason on record, and a manual threshold only ever
    # covers what the bound could not. Checking manual first hid the bound's
    # work behind a user's threshold and made the note on the missing condition
    # depend on flag order.
    if (
        policy.name == REDUCTION_ORDER
        and mechanism
        and bound is not None
        and worst_relative <= bound.bound
    ):  # worst_relative is never None here, and never NaN: sql.py sends both to infinity

        return BY_BOUND
    if policy.manual and _inside_manual(policy, worst_relative, worst_ulps):
        return BY_THRESHOLD
    return None


def _worst(measured: Iterable[float | int | None]) -> float | int | None:
    """The largest magnitude, or None if any column could not be measured at all.

    None here means unmeasurable rather than small, and the difference decides
    an exit code. ULP distance comes back NULL when every differing pair had a
    NULL on one side, because abs(NULL - x) is NULL and max() over only NULLs is
    NULL. Folding that to zero with `or 0` told --tolerance-ulps that an
    artifact whose every float had been replaced by NULL was nought last-bit
    steps away from the original, and the tool exited 0 on it.
    """
    seen = list(measured)
    return None if any(m is None for m in seen) else max(seen)


def _inside_manual(policy: Policy, relative: float, ulps: int | None) -> bool:
    """Every threshold that was set has to be cleared, not just one of them.

    A threshold cannot clear a figure that does not exist, so an unmeasurable
    ULP distance refuses the ULP threshold rather than passing it by default.
    """
    if policy.tolerance_ulps is not None and (ulps is None or ulps > policy.tolerance_ulps):
        return False
    return policy.tolerance_relative is None or relative <= policy.tolerance_relative


def _bound_refusal(
    step: StepMeasurement,
    policy: Policy,
    bound: DriftBound | None,
    pure: bool,
    quiet_at_one_thread: bool,
) -> str | None:
    """Why the derived route does not cover this step's drift, if it does not.

    This has been widened twice for the same reason and the second time is why
    it is now about the bound rather than about the outcome. It first required
    that nothing at all had been downgraded, so a step tolerated on one
    comparison and firing on another explained the firing to nobody. It then
    required that something still fired, so a --tolerance threshold wide enough
    to downgrade every comparison deleted the sentence saying the mechanism
    story does not hold. A step diverging 4 of 4 at threads=1, moving five
    orders of magnitude outside the bound, exited 0 saying nothing about
    either.

    So it is computed for every judged step and returns None when there is
    nothing to say, which is what a step the bound genuinely covers gets.

    The conditions are tested in the order a reader would ask about them: what
    else the step did, then whether one thread makes it stop, then how far the
    drift went. The figures are step-wide, which the wording says, because a
    per-comparison reason needs a per-comparison line and the report prints one
    summary per step.
    """
    if policy.name != REDUCTION_ORDER or Divergence.VALUE_DRIFT not in step.classes:
        return None
    if not pure:
        others = step.classes - {Divergence.VALUE_DRIFT}
        return (
            f"the bound does not explain this step: it also shows {name_classes(others)}, "
            f"which reassociation cannot produce"
        )
    if not quiet_at_one_thread:
        return f"the bound does not explain this step: {_no_mechanism(step)}"
    if bound is not None and bound.observed > bound.bound:
        return (
            f"the bound does not explain this step: its furthest move, "
            f"{bound.observed:.4e}, is outside the reassociation bound {bound.bound:.4e}"
        )
    return None


def _no_mechanism(step: StepMeasurement) -> str:
    """Condition 2 refusing, which is the condition that only started existing in slice 3.

    Two ways to fail it and they are different findings. A step that still
    moves at threads=1 has drift the reassociation story does not explain, so
    the tool reports it and says why. A step with no bisect at all has no
    evidence either way, and no evidence is not the same as evidence of
    reassociation.
    """
    if step.bisect is None:
        return (
            "it was not re-executed at threads=1, so nothing here shows the drift is "
            "reduction order"
        )
    return (
        f"it still diverges at threads=1, {step.bisect.fired} of "
        f"{step.bisect.comparisons}, so the order of a parallel reduction is not what moved it"
    )
