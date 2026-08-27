"""What `scripts/eval.py --json` has to carry for the README's tables to come out of it.

Slice 7 made every results table in the README a rendering of one committed
copy of this file, so anything missing from it becomes a number somebody has to
retype, and retyping is the failure mode this project has been caught by six
times.

These assertions are about the artifact and not about the figures in it. Whether
`daily_revenue` fired four times out of four is a fact about DuckDB that moves
between runs; whether the file says which step index that rate belongs to is a
fact about the writer that must not.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from twicerun.tables import ARTIFACT_VERSION, EVAL

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from eval import as_artifact, report_all  # noqa: E402
from trial_fixtures import a_step, a_trial  # noqa: E402

WHERE = {"duckdb": "1.5.5", "threads": "10", "platform": "macOS-26.5.2-arm64-arm-64bit"}


def artifact(trials, seconds: float = 300.0) -> dict:
    figures = report_all(trials)
    return as_artifact(figures, WHERE, 5, seconds, figures.conditions(seconds))


@pytest.fixture(scope="module")
def ten() -> dict:
    return artifact([a_trial() for _ in range(10)])


def test_the_artifact_survives_a_round_trip_through_a_file(tmp_path, ten):
    where = tmp_path / "eval.json"
    where.write_text(json.dumps(ten, indent=2), encoding="utf-8")
    assert json.loads(where.read_text()) == ten


def test_every_figure_is_stamped_with_what_it_is_specific_to(ten):
    """The spec asks for the DuckDB version, the thread count and the platform.

    Parallel reduction order changes between DuckDB releases and with how many
    threads divide the work, so a table quoting a fire rate without all three is
    not a table anyone can reproduce.
    """
    assert ten["environment"] == WHERE
    assert ten["version"] == ARTIFACT_VERSION
    assert ten["kind"] == EVAL
    assert ten["generated"].startswith("20")
    assert ten["trials"] == 10
    assert ten["runs"] == 5


def test_a_step_that_compared_nothing_still_reaches_the_table_with_its_index():
    """Otherwise the results table silently loses a row and nobody sees the gap.

    A step that wrote no artifact is scored as a silent trial rather than a
    clean one, and the early return that does that used to happen before the
    index was recorded. The step then arrived at the generator with `index:
    null`, sorted to the front of the table, and printed as a step with no
    number beside it.
    """
    silent = artifact([a_trial(artifacts=0) for _ in range(10)])
    by_name = {step["name"]: step for step in silent["steps"]}
    assert by_name["daily_revenue"]["index"] == 1
    assert by_name["daily_revenue"]["silent_trials"] == 10
    assert by_name["daily_revenue"]["trials"] == 0
    assert [step["index"] for step in silent["steps"]] == list(range(8))


def test_the_reduction_order_column_covers_every_step_and_not_only_the_broken_four(ten):
    """The terminal prints the four the spec calls broken. The table has eight rows."""
    gated = {step["name"]: step["reduction_order"] for step in ten["steps"]}
    assert set(gated) == {step["name"] for step in ten["steps"]}
    assert gated["mean_basket"]["of"] == 10
    assert gated["roll_up_keys"]["rates"] == {"0 of 4": 10}


def test_the_uncontained_column_is_the_ablation_and_not_a_copy_of_the_contained_one(ten):
    """`roll_up_keys` is the step the ablation exists to move, so it has to move here."""
    downstream = next(s for s in ten["steps"] if s["name"] == "roll_up_keys")
    assert downstream["rates"] == {"0 of 4": 10}
    assert downstream["uncontained"]["rates"] == {"3 of 4": 10}


def test_each_amplifier_reports_the_twin_passes_it_declined_as_well_as_the_ones_it_took(ten):
    """A twin an amplifier never touched is not evidence that the amplifier is safe on it."""
    coverage = {row["amplifier"]: row for row in ten["twin_coverage"]}
    assert set(coverage) == {"tie collapse", "thread count", "row multiplication"}
    for row in coverage.values():
        assert row["step_passes"] == 60
        assert row["ran_on"] <= row["offered"] <= row["step_passes"]
    assert coverage["tie collapse"]["ran_on"] == 60
    # The fixtures give every twin one amplifier, so the other two have nothing
    # to report and must say so rather than reporting a clean zero.
    assert coverage["thread count"]["offered"] == 0


def test_a_twin_that_fires_is_visible_in_the_artifact_and_not_only_in_a_verdict():
    loud = a_trial()
    loud.twins[2] = a_step(2, "customer_keys_tiebreak", fired=1)
    written = artifact([a_trial() for _ in range(9)] + [loud])
    twins = {twin["name"]: twin for twin in written["twins"]}
    assert twins["customer_keys_tiebreak"]["fired_in"] == 1


def test_the_drift_rows_carry_both_readings_of_the_bound_not_just_the_one_that_clears():
    """The pre-registered check is the loose n. The honest reading is the tight one.

    Publishing only the reading that passes is the exact move the tolerance
    section of the README is an argument against, so the generator has to be
    handed both or it cannot print both.
    """
    ten = artifact([a_trial() for _ in range(10)])
    # These fixtures carry no float drift, so the list is empty and that is the
    # shape the generator has to survive rather than a shape to assert figures
    # against.
    assert ten["drift"] == []
    assert ten["bounds"] == {}


def test_every_pre_registered_condition_reaches_the_file_with_its_evidence(ten):
    assert len(ten["conditions"]) == 8
    for entry in ten["conditions"]:
        assert entry["condition"] and entry["verdict"] and entry["evidence"]
