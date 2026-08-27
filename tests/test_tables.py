"""The markdown generator, and the two directions in which it refuses to line up.

`twicerun report` exists so that no figure in the README is a figure somebody
typed. The rendering is the easy half. The half worth testing is what happens
when the markdown and the artifact stop agreeing about which tables exist,
because the failure that matters is not a wrong number: it is a table quietly
dropping out of the file and keeping whatever it said last.
"""

from __future__ import annotations

import json
from copy import deepcopy

import pytest

from eval import as_artifact, report_all
from trial_fixtures import a_trial
from twicerun import cli
from twicerun.cli import main
from twicerun.tables import (
    CLOSE,
    EVAL,
    GAP,
    OPEN,
    MarkerError,
    UnreadableArtifact,
    baselines,
    blocks_in,
    distribution,
    drifted,
    every_table,
    load,
    render,
    results,
    rewrite,
    specificity,
    tally,
)

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
def mixed_artifact() -> dict:
    """Seven trials that compared something and three that did not, which is the partial case.

    Ten silent trials leave every cell reading `nothing compared`, which is
    visible. Three out of ten leave the cells reading a rate out of seven under
    a header that says ten, which is not.
    """
    trials = [a_trial() for _ in range(7)] + [a_trial(artifacts=0) for _ in range(3)]
    figures = report_all(trials)
    written = as_artifact(figures, WHERE, 5, 300.0, figures.conditions(300.0))
    written["source"] = "results/eval-mixed.json"
    return written


@pytest.fixture(scope="module")
def silent_artifact() -> dict:
    """Ten trials in which no step wrote anything, which is a shape a real run has produced.

    Every rate in it is an absence rather than a zero, so it is the artifact
    that finds the cells reading a figure without first asking whether there was
    one to read.
    """
    figures = report_all([a_trial(artifacts=0) for _ in range(10)])
    written = as_artifact(figures, WHERE, 5, 300.0, figures.conditions(300.0))
    written["source"] = "results/eval-silent.json"
    return written


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


def test_the_gap_table_carries_the_same_stamp_as_the_eval_one(rendered):
    """Both scripts measure on one laptop at one thread count, and both have to say so.

    They are separate runs at separate sample sizes, so a reader comparing a
    rate in one against a rate in the other needs each block to name its own
    conditions rather than inheriting the other's.
    """
    stamp = rendered["gap-provenance"]
    assert "1.5.5" in stamp
    assert "threads=10" in stamp
    assert "macOS-26.5.2-arm64-arm-64bit" in stamp
    assert "40 passes" in stamp
    assert "results/gap-fixture.json" in stamp


def a_row(block: str, step: str) -> str:
    # Found by the step's own backticked cell, not by row number, so reordering
    # the table does not silently point these assertions at a different step.
    return next(line for line in block.splitlines() if f"`{step}`" in line)


def test_a_step_that_never_fired_says_it_was_not_bisected_rather_than_showing_a_zero(rendered):
    """`0 of 4 at threads=1` on a step that never fired would be four comparisons of nothing."""
    assert "not bisected" in a_row(rendered["results"], "generate_inputs")


def test_a_step_that_fired_and_got_no_threads_one_rate_is_not_called_unbisected(artifact):
    """One message covered three states and only one of them was what it said.

    `single_threaded` comes back empty when the step never fired, when the
    bisect raised, and when the bisect ran and the re-executed step wrote
    nothing to compare. The cell read "not bisected, since it never fired" for
    all three. That is not cosmetic: `PARALLEL_ORDER` in the next column over
    rests on a measured zero at threads=1, so a step whose bisect produced no
    rate has no second axis rather than nothing to explain.

    This test only covered `generate_inputs`, which genuinely never fires, and
    the fixture it read gives no step a bisect at all, so every other row of it
    was making the false claim already.
    """
    block = "\n".join(results(artifact))
    assert "not bisected, since it never fired" in a_row(block, "generate_inputs")
    assert "no rate, on 10 trials that fired" in a_row(block, "daily_revenue")

    bisected = deepcopy(artifact)
    next(s for s in bisected["steps"] if s["name"] == "daily_revenue")["single_threaded"] = {
        "0 of 4": 10
    }
    assert "0 of 4 on all 10" in a_row("\n".join(results(bisected)), "daily_revenue")


def test_the_threads_one_buckets_come_out_of_the_bisects_own_denominator(artifact):
    """A cell headed threads=1 cannot enumerate the main loop's range.

    The two are runs minus one either way and part company when the main loop
    had a round that compared nothing, which lowers its measured denominator and
    not the bisect's. This cell then listed buckets out of 3 and appended the
    bisect's real `0 of 4` as an oddity beside them.
    """
    moved = deepcopy(artifact)
    step = next(s for s in moved["steps"] if s["name"] == "daily_revenue")
    step["single_threaded"] = {"0 of 4": 8, "4 of 4": 2}
    step["comparisons"] = 3

    cell = a_row("\n".join(results(moved)), "daily_revenue")
    assert "0 of 4 x8" in cell and "4 of 4 x2" in cell
    assert "of 3" not in cell, "the main loop's denominator has no business in this column"


@pytest.mark.parametrize(
    ("table", "counted"),
    # Per step for the results table, so 3 of the 10 trials. Per twin step-pass
    # for specificity, so 6 twins across those same 3 trials.
    [(results, "3 trials on `generate_inputs`"), (specificity, "18 twin step-passes")],
)
def test_every_fire_rate_table_says_which_trials_it_is_not_out_of(table, counted, mixed_artifact):
    """Four surfaces publish a fire rate and the README's first table was the one without this.

    Its header names ten trials and every cell in it read `4 of 4 on all 7`. The
    terminal has printed the missing three under each step since slice 5 and the
    specificity table below has printed a total, so the omission was in one of
    four places rather than a decision.

    Both tables are checked in one test because the specificity note was
    unenforced too: deleting it left the whole suite green.
    """
    block = "\n".join(table(mixed_artifact))
    assert "compared no artifact at all" in block
    assert counted in block, "the count, not just the fact of it"


def test_a_table_with_nothing_to_disclose_does_not_disclose_it(artifact):
    """The control. A note that is always there is a note nobody reads."""
    assert "compared no artifact at all" not in "\n".join(results(artifact))
    assert "compared no artifact at all" not in "\n".join(specificity(artifact))


def test_the_baselines_table_renders_from_a_run_where_nothing_compared(silent_artifact):
    """The cell that used to raise KeyError, and the row that used to disappear with it.

    Baseline 1's figures came out of an early return holding three of ten keys
    when the benign step wrote nothing, and this table reads five of the seven
    it did not write. It is the only table in the file that reads `baseline_1`,
    so the whole block went with it.
    """
    block = "\n".join(baselines(silent_artifact))
    assert "an absence of measurement rather than a baseline" in block
    assert "run-matched" in block, "the run-matched control is one of six earned corrections"
    assert "0 of 0 step-trials" in block


def test_a_report_that_cannot_render_exits_three_rather_than_one(tmp_path, silent_artifact, capsys):
    """1 is divergence in this program's epilog, so nothing else may hand the shell a 1.

    `judge` got this guard when it was found dispatched outside the try block.
    `report` was the third subcommand and did not, and its `_report` catches
    only OSError and the two artifact errors, so a KeyError out of the
    generator reached the shell as a pipeline that gave two answers.
    """
    where = tmp_path / "eval.json"
    where.write_text(json.dumps(silent_artifact), encoding="utf-8")
    assert main(["report", str(where)]) == 0, "the artifact that used to crash it"
    capsys.readouterr()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(cli, "render", _explode)
        assert main(["report", str(where)]) == 3
    assert "Exit 3 is a crash, not a divergence" in capsys.readouterr().err


def _explode(*args, **kwargs):
    raise KeyError("benign_rows_compared")


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


def test_a_marker_pair_whose_generator_was_deleted_is_seen_at_all(rendered):
    """The version of the check above that could not fail, and the one that matters.

    `blocks_in` used to scan for the names `TABLES` offers, so a pair naming
    anything else was invisible: both sides of the test comparing the markdown's
    blocks against the generator's tables came out of the same dict. Deleting a
    generator and leaving its markers in place therefore left a hand-typed block
    in the file with `--update` reporting that the file already matched.

    Both directions are asserted here, because the failure is symmetric: an
    invented name nobody generates, and a real table whose generator went away.
    """
    orphan = (
        a_page(rendered)
        + f"\n{OPEN.format(name='invented-table')}\n42 of 42\n"
        + f"{CLOSE.format(name='invented-table')}\n"
    )
    assert "invented-table" in blocks_in(orphan)
    with pytest.raises(MarkerError, match="No generator for: invented-table"):
        rewrite(orphan, rendered)
    with pytest.raises(MarkerError, match="No generator for: invented-table"):
        drifted(orphan, rendered)

    without = {k: v for k, v in rendered.items() if k != "attribution"}
    with pytest.raises(MarkerError, match="No generator for: attribution"):
        drifted(a_page(rendered), without)


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


def test_a_bare_label_means_every_trial_in_the_terminal_as_well_as_in_a_cell():
    """The two conventions were opposites, on output a reader sees side by side.

    `tally` dropped the count where it was one and `_codes` drops it where the
    label held on every trial, so a bare `VALUE_DRIFT` out of the eval meant one
    trial and a bare `VALUE_DRIFT` out of the README's table meant ten. Without a
    total there is nothing for a bare label to mean, so every count prints.
    """
    counted = {"PARALLEL_ORDER": 9, "PERSISTS_SINGLE_THREADED": 1}
    assert tally(counted, 10) == "PARALLEL_ORDER x9, PERSISTS_SINGLE_THREADED x1"
    assert tally({"PARALLEL_ORDER": 10}, 10) == "PARALLEL_ORDER"
    assert tally(counted) == "PARALLEL_ORDER x9, PERSISTS_SINGLE_THREADED x1"
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


def test_the_path_written_into_the_file_is_relative_to_the_file(
    tmp_path, artifact, gap_artifact, rendered, monkeypatch
):
    """Run from anywhere, and the provenance block still names a path a reader can follow.

    The stamp is rendered into a committed file, and the artifact's path used to
    be resolved against the working directory. Running the documented command
    from anywhere but the repository root therefore wrote an absolute path, and
    whoever ran it, into the README.
    """
    both = written(tmp_path, artifact, gap_artifact)
    page = tmp_path / "README.md"
    page.write_text(a_page(rendered), encoding="utf-8")
    monkeypatch.chdir(tmp_path.parent)

    assert main(["report", *both, "--update", str(page)]) == 0
    updated = page.read_text(encoding="utf-8")
    assert "from `eval.json`" in updated
    assert str(tmp_path) not in updated


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
