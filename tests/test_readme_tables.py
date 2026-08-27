"""The README's tables against the artifacts they are supposed to be rendered from.

This is the whole point of slice 7 expressed as a build failure. Six figures in
that file have been published and then beaten by a longer run, and every one of
the six was transcribed by hand out of a terminal. Transcription is removed as a
way for a table to go false: edit a cell and this fails, change what the eval
measures and this fails, and the only way to move a number is to run the
measurement again and commit what came out.

It runs in CI, offline, against committed JSON. It does not run the eval.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from twicerun.tables import EVAL, GAP, TABLES, blocks_in, drifted, load, render, rewrite

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
RESULTS = ROOT / "results"

REGENERATE = (
    "Run `uv run twicerun report results/*.json --update README.md` and commit what it writes. "
    "If the figures themselves should change, run the two measurement scripts first."
)


@pytest.fixture(scope="module")
def artifacts() -> dict[str, dict]:
    return load(sorted(RESULTS.glob("*.json")))


@pytest.fixture(scope="module")
def markdown() -> str:
    return README.read_text(encoding="utf-8")


def test_results_holds_one_artifact_of_each_kind_and_nothing_else(artifacts):
    """Two eval runs in the directory and the README would render from whichever sorted first.

    Superseded runs live in git history rather than beside the current one. The
    loader refuses a second of the same kind, so this asserts the other half:
    that both kinds are actually there.
    """
    assert set(artifacts) == {EVAL, GAP}


def test_every_table_the_generator_offers_has_a_home_in_the_readme(markdown, artifacts):
    """Both directions, because a marker pair nobody rendered is the failure that hides.

    A block dropped from the README stops being regenerated and keeps whatever
    it last said, which reads exactly like a maintained table.
    """
    rewrite(markdown, render(artifacts))
    assert set(blocks_in(markdown)) == {name for kind in TABLES for name in TABLES[kind]}


def test_no_table_in_the_readme_has_drifted_from_the_artifact_that_produced_it(
    markdown, artifacts
):
    assert drifted(markdown, render(artifacts)) == [], REGENERATE


def test_the_stamp_in_the_readme_names_the_artifact_it_came_from(markdown, artifacts):
    """Otherwise a reader cannot tell which file to re-render to check a figure."""
    for artifact in artifacts.values():
        assert artifact["source"] in markdown


def test_the_artifacts_were_written_by_the_current_pipelines(artifacts):
    """A stale artifact renders a table about steps the reference pipeline no longer has.

    The pipeline is the anchor, which this docstring said and the assertion did
    not. It compared the artifact against `eval.WHAT_EACH_STEP_IS`, a dict typed
    into the eval, so the two could agree with each other while both had drifted
    away from the file that produces the artifacts. Three lists of step names
    have to line up and only two of the three edges were checked: renaming
    `roll_up_keys` in the pipeline and in `test_runner.py` left all 388 tests
    green, and `report_sensitivity` skips a name it cannot find, so the step
    dropped out of the eval's own table without a word.

    Order too, not just membership. The dict is the order the terminal prints in
    and the list is execution order, and the artifact carries a step index off
    the second one.
    """
    from eval import PAIRS, REFERENCE, WHAT_EACH_STEP_IS  # noqa: PLC0415
    from twicerun.runner import load_steps  # noqa: PLC0415

    executed = [step.__name__ for step in load_steps(REFERENCE)]
    assert list(WHAT_EACH_STEP_IS) == executed, "the eval describes steps the pipeline runs"
    measured = {step["name"] for step in artifacts[EVAL]["steps"]}
    assert measured == set(executed)
    assert {twin["name"] for twin in artifacts[EVAL]["twins"]} == {twin for _, twin in PAIRS}
