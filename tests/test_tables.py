"""The markdown generator, and the two directions in which it refuses to line up.

`twicerun report` exists so that no figure in the README is a figure somebody
typed. The rendering is the easy half. The half worth testing is what happens
when the markdown and the artifact stop agreeing about which tables exist,
because the failure that matters is not a wrong number: it is a table quietly
dropping out of the file and keeping whatever it said last.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from twicerun.cli import main
from twicerun.tables import (
    CLOSE,
    EVAL,
    GAP,
    OPEN,
    MarkerError,
    UnreadableArtifact,
    distribution,
    drifted,
    every_table,
    load,
    render,
    rewrite,
    tally,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from eval import as_artifact, report_all  # noqa: E402
from trial_fixtures import a_trial  # noqa: E402

WHERE = {"duckdb": "1.5.5", "threads": "10", "platform": "macOS-26.5.2-arm64-arm-64bit"}


@pytest.fixture(scope="module")
def artifact() -> dict:
    figures = report_all([a_trial() for _ in range(10)])
    written = as_artifact(figures, WHERE, 5, 300.0, figures.conditions(300.0))
    written["source"] = "results/eval-fixture.json"
    return written


@pytest.fixture(scope="module")
def gap_artifact() -> dict:
    return {
        "version": 1,
        "kind": GAP,
        "generated": "2026-08-27T09:00:00+00:00",
        "source": "results/gap-fixture.json",
        "environment": WHERE,
        "step": "sparse_customer_keys",
        "passes": 40,
        "seconds": 372.0,
        "rows": [
            {"source": "the five-run loop", "fired": 87, "comparisons": 160,
             "clean": 7, "scored": 40, "unusable": 0},
            {"source": "tie collapse", "fired": 153, "comparisons": 160,
             "clean": 0, "scored": 40, "unusable": 0},
            {"source": "thread count", "fired": 91, "comparisons": 144,
             "clean": 8, "scored": 40, "unusable": 0},
            {"source": "row multiplication", "fired": 145, "comparisons": 160,
             "clean": 0, "scored": 40, "unusable": 0},
        ],
        "loop_looked": 40,
        "loop_silent": 7,
        "amplifier_caught": 7,
    }


@pytest.fixture(scope="module")
def rendered(artifact, gap_artifact) -> dict[str, str]:
    return render({EVAL: artifact, GAP: gap_artifact})


def a_page(rendered: dict[str, str], *, skip: str = "") -> str:
    body = [
        f"{OPEN.format(name=name)}\n{block}\n{CLOSE.format(name=name)}"
        for name, block in rendered.items()
        if name != skip
    ]
    return "# a page\n\nprose above\n\n" + "\n\nprose between\n\n".join(body) + "\n\nprose below\n"


def test_every_table_renders_and_none_of_them_is_empty(rendered):
    assert set(rendered) == set(every_table())
    for name, block in rendered.items():
        assert block.strip(), name


def test_the_stamp_names_the_four_things_the_figures_are_specific_to(rendered):
    """DuckDB, the thread count, the platform and the trial count. The spec asks for all four."""
    stamp = rendered["provenance"]
    assert "1.5.5" in stamp
    assert "threads=10" in stamp
    assert "macOS-26.5.2-arm64-arm-64bit" in stamp
    assert "10 trials" in stamp
    assert "results/eval-fixture.json" in stamp


def test_a_step_that_never_fired_says_it_was_not_bisected_rather_than_showing_a_zero(rendered):
    """`0 of 4 at threads=1` on a step that never fired would be four comparisons of nothing."""
    row = next(line for line in rendered["results"].splitlines() if "generate_inputs" in line)
    assert "not bisected" in row


def test_rewriting_replaces_the_bodies_and_leaves_everything_else_alone(rendered):
    page = a_page(rendered)
    stale = page.replace("| 4 `append_audit_log` |", "| 4 `something_else` |")
    # Two tables carry that step, and both have to come back as drifted or the
    # check is reporting on the first block it happens to look at.
    assert drifted(stale, rendered) == ["containment", "results"]
    restored = rewrite(stale, rendered)
    assert drifted(restored, rendered) == []
    assert restored.count("prose between") == len(every_table()) - 1
    assert restored.startswith("# a page")
    assert restored.endswith("prose below\n")


def test_a_table_the_markdown_stopped_carrying_is_refused_rather_than_skipped(rendered):
    """A dropped marker pair is a table that stops being regenerated and keeps its last text."""
    with pytest.raises(MarkerError, match="Missing from the markdown: results"):
        rewrite(a_page(rendered, skip="results"), rendered)


def test_a_marker_with_no_generator_behind_it_is_refused_too(rendered):
    page = a_page(rendered)
    with pytest.raises(MarkerError, match="No generator for: results"):
        rewrite(page, {k: v for k, v in rendered.items() if k != "results"})


def test_a_marker_that_never_closes_is_an_error_and_not_a_block_to_the_end_of_the_file(rendered):
    last = list(rendered)[-1]
    page = a_page(rendered).replace(CLOSE.format(name=last), "")
    with pytest.raises(MarkerError, match="never closes"):
        rewrite(page, rendered)


def test_a_marker_left_open_inside_another_is_named_with_both_line_numbers(rendered):
    page = a_page(rendered).replace(CLOSE.format(name="provenance"), "")
    with pytest.raises(MarkerError, match="is still open at line"):
        rewrite(page, rendered)


def test_a_second_opening_of_the_same_table_is_an_error(rendered):
    page = a_page(rendered) + f"\n{OPEN.format(name='results')}\n{CLOSE.format(name='results')}\n"
    with pytest.raises(MarkerError, match="opened twice"):
        rewrite(page, rendered)


def test_a_close_without_an_open_is_an_error(rendered):
    page = a_page(rendered) + f"\n{CLOSE.format(name='results')}\n"
    with pytest.raises(MarkerError, match="which is not open"):
        rewrite(page, rendered)


def test_an_artifact_from_a_shape_this_build_does_not_know_is_refused(tmp_path, artifact):
    where = tmp_path / "old.json"
    where.write_text(json.dumps({**artifact, "version": 99}), encoding="utf-8")
    with pytest.raises(UnreadableArtifact, match="version 99"):
        load([where])


def test_an_artifact_that_does_not_say_what_it_measured_is_refused(tmp_path, artifact):
    where = tmp_path / "nameless.json"
    where.write_text(json.dumps({**artifact, "kind": "something"}), encoding="utf-8")
    with pytest.raises(UnreadableArtifact, match="kind 'something'"):
        load([where])


def test_two_artifacts_of_one_kind_are_refused_rather_than_resolved_by_order(tmp_path, artifact):
    """The second silently winning is how a table gets rendered from a file nobody meant."""
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    for where in (first, second):
        where.write_text(json.dumps(artifact), encoding="utf-8")
    with pytest.raises(UnreadableArtifact, match="two eval artifacts given"):
        load([first, second])


def test_the_distribution_prints_the_buckets_that_did_not_come_up(rendered):
    assert distribution({"4 of 4": 3, "2 of 4": 1}, 4) == (
        "4 of 4 x3, 3 of 4 x0, 2 of 4 x1, 1 of 4 x0, 0 of 4 x0"
    )
    assert distribution({"0 of 4": 10}, 4) == "0 of 4 on all 10"
    assert distribution({}, 4) == "nothing compared"


def test_a_rate_out_of_a_smaller_denominator_keeps_its_own_denominator():
    """A step that wrote nothing on one round has a real rate that is not out of four."""
    spelled = distribution({"4 of 4": 9, "1 of 2": 1}, 4)
    assert spelled.endswith("1 of 2 x1")
    assert "4 of 4 x9" in spelled


def test_the_tally_drops_the_count_where_it_is_one():
    assert tally({"PARALLEL_ORDER": 9, "PERSISTS_SINGLE_THREADED": 1}) == (
        "PARALLEL_ORDER x9, PERSISTS_SINGLE_THREADED"
    )
    assert tally({}) == "nothing seen"


def written(tmp_path, artifact, gap_artifact) -> list[str]:
    both = []
    for name, one in (("eval.json", artifact), ("gap.json", gap_artifact)):
        where = tmp_path / name
        where.write_text(json.dumps(one), encoding="utf-8")
        both.append(str(where))
    return both


def test_the_command_writes_the_file_and_says_which_blocks_moved(
    tmp_path, artifact, gap_artifact, rendered, capsys
):
    both = written(tmp_path, artifact, gap_artifact)
    page = tmp_path / "README.md"
    page.write_text(a_page(rendered).replace("| 0 `generate_inputs` |", "| 0 `wrong` |"), "utf-8")

    assert main(["report", *both, "--format", "md", "--update", str(page)]) == 0
    assert "results" in capsys.readouterr().out
    assert main(["report", *both, "--format", "md", "--update", str(page)]) == 0
    assert "already matches" in capsys.readouterr().out


def test_the_command_refuses_a_markdown_file_missing_a_table_rather_than_writing_it(
    tmp_path, artifact, gap_artifact, rendered, capsys
):
    both = written(tmp_path, artifact, gap_artifact)
    page = tmp_path / "README.md"
    short = a_page(rendered, skip="conditions")
    page.write_text(short, encoding="utf-8")

    assert main(["report", *both, "--update", str(page)]) == 2
    assert "conditions" in capsys.readouterr().err
    assert page.read_text(encoding="utf-8") == short, "it wrote to a file it had refused"


def test_an_artifact_left_off_the_command_line_refuses_rather_than_leaving_its_table_stale(
    tmp_path, artifact, gap_artifact, rendered, capsys
):
    """The gap table is the headline evidence for amplification. It cannot go quietly stale."""
    both = written(tmp_path, artifact, gap_artifact)
    page = tmp_path / "README.md"
    page.write_text(a_page(rendered), encoding="utf-8")

    assert main(["report", both[0], "--update", str(page)]) == 2
    assert "amplification-gap" in capsys.readouterr().err
