"""Trials built by hand, so a whole eval run costs milliseconds instead of nine minutes.

The conditions read counts the oracle already computed and nothing that points
at a Parquet file, so a trial can be constructed rather than measured. That is
what lets the tests give one the shape a healthy laptop never produces: ten
trials where every step wrote nothing, a twin that fired, a broken step that
went quiet for one trial out of ten.

It lives apart from the tests because two files need it now. The artifact
`twicerun report` reads is assembled from the same `Figures` the conditions come
out of, so the tests for the two halves want the same fixtures.
"""

from __future__ import annotations

from eval import BENIGN, INTERMITTENT, PAIRS, NaiveScore, Trial
from twicerun.amplify import Amplification
from twicerun.measurement import StepMeasurement
from twicerun.oracle import ArtifactFindings

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
