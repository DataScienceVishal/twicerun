"""Tests for the N-run loop.

Nothing here waits on a real race. The pipelines below decide when to diverge
from a counter they own, so the fire-rate arithmetic is asserted exactly rather
than probabilistically. The real races are measured by scripts/measure_duckdb.py
and exercised by running the reference pipeline, neither of which belongs in a
suite that has to pass every time.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pytest

from twicerun.runner import PipelineError, load_steps, prune_run_dirs, run_pipeline

CONTROLLED = '''
CALLS = {"n": 0}
DIVERGES_ON = {2, 4}


def steady(ctx):
    ctx.write("steady", "SELECT i FROM range(10) AS s(i)")


def flaky(ctx):
    CALLS["n"] += 1
    shift = 1 if CALLS["n"] in DIVERGES_ON else 0
    ctx.write("flaky", f"SELECT i + {shift} AS i FROM range(10) AS s(i)")


STEPS = [steady, flaky]
'''

ALWAYS_CLEAN = '''
def only_step(ctx):
    ctx.write("rows", "SELECT i FROM range(4) AS s(i)")


STEPS = [only_step]
'''

DROPS_AN_ARTIFACT = '''
CALLS = {"n": 0}


def sometimes(ctx):
    CALLS["n"] += 1
    ctx.write("always", "SELECT 1 AS i")
    if CALLS["n"] == 1:
        ctx.write("only_on_the_first_run", "SELECT 2 AS i")


STEPS = [sometimes]
'''

ADDS_AN_ARTIFACT = DROPS_AN_ARTIFACT.replace('CALLS["n"] == 1', 'CALLS["n"] != 1').replace(
    "only_on_the_first_run", "only_after_the_first_run"
)


SCHEMA_CHANGE_BESIDE_A_TINY_DRIFT = '''
CALLS = {"n": 0}


def two_outputs(ctx):
    CALLS["n"] += 1
    widened = "BIGINT" if CALLS["n"] > 1 else "INTEGER"
    ctx.write("noisy", f"SELECT i + {CALLS['n']} AS i FROM range(2) AS s(i)")
    ctx.write("schema", f"SELECT 1::{widened} AS v")


STEPS = [two_outputs]
'''


WRITES_NOTHING = '''
def real_work(ctx):
    ctx.write("rows", "SELECT i FROM range(3) AS s(i)")


def forgot_to_write(ctx):
    ctx.sql("CREATE TABLE scratch AS SELECT i FROM range(3) AS s(i)")


STEPS = [real_work, forgot_to_write]
'''


def write_pipeline(tmp_path: Path, body: str, name: str = "fake") -> Path:
    where = tmp_path / f"{name}.py"
    where.write_text(body, encoding="utf-8")
    return where


def test_fire_rate_counts_the_comparisons_that_diverged(tmp_path):
    report, _ = run_pipeline(
        write_pipeline(tmp_path, CONTROLLED), runs=5, parent=tmp_path / "artifacts"
    )
    steady, flaky = report.steps
    assert (steady.fired, steady.comparisons) == (0, 4)
    assert (flaky.fired, flaky.comparisons) == (2, 4)
    assert (flaky.worst.row_missing, flaky.worst.row_extra) == (1, 1)


def test_run_one_is_the_reference_so_its_own_divergence_is_not_counted(tmp_path):
    """Diverging on run 1 makes every later run disagree with it, not two of them."""
    body = CONTROLLED.replace("DIVERGES_ON = {2, 4}", "DIVERGES_ON = {1}")
    report, _ = run_pipeline(write_pipeline(tmp_path, body), runs=5, parent=tmp_path / "artifacts")
    assert report.steps[1].fired == 4


def test_a_step_that_stops_writing_an_artifact_is_a_divergence(tmp_path):
    report, _ = run_pipeline(
        write_pipeline(tmp_path, DROPS_AN_ARTIFACT), runs=3, parent=tmp_path / "artifacts"
    )
    step = report.steps[0]
    assert step.fired == 2
    assert step.hints == ["the reference run wrote only_on_the_first_run and this run did not"]


def test_a_step_that_starts_writing_an_extra_artifact_is_a_divergence(tmp_path):
    """The mirror of the case above, and the one that used to exit 0.

    Walking only the reference run's artifact names never visits a name that
    appears for the first time on a later run, so a pipeline whose second run
    produced an output its first did not was reported clean.
    """
    report, _ = run_pipeline(
        write_pipeline(tmp_path, ADDS_AN_ARTIFACT), runs=3, parent=tmp_path / "artifacts"
    )
    step = report.steps[0]
    assert step.fired == 2
    assert step.hints == ["this run wrote only_after_the_first_run and the reference run did not"]


def test_a_schema_change_outranks_a_bigger_row_drift_in_the_report(tmp_path):
    """Only one diff per step gets printed, so the ranking decides what is seen.

    The schema diff carries no row counts, so ranking purely on volume hid a
    column type change behind a two-row drift on a sibling artifact.
    """
    report, _ = run_pipeline(
        write_pipeline(tmp_path, SCHEMA_CHANGE_BESIDE_A_TINY_DRIFT),
        runs=3,
        parent=tmp_path / "artifacts",
    )
    worst = report.steps[0].worst
    assert worst.name == "schema"
    assert "INTEGER to BIGINT" in worst.schema_note


def test_the_manifest_records_every_run_and_the_environment(tmp_path):
    _, manifest = run_pipeline(
        write_pipeline(tmp_path, CONTROLLED), runs=3, parent=tmp_path / "artifacts"
    )
    saved = json.loads((manifest.root / "manifest.json").read_text())

    assert len(saved["runs"]) == 3
    assert saved["environment"]["duckdb_version"]
    assert saved["environment"]["threads"] >= 1
    assert [s["name"] for s in saved["runs"][0]["steps"]] == ["steady", "flaky"]
    assert saved["runs"][0]["steps"][0]["artifacts"][0]["rows"] == 10


def test_artifacts_from_different_runs_do_not_share_a_path(tmp_path):
    _, manifest = run_pipeline(
        write_pipeline(tmp_path, ALWAYS_CLEAN), runs=4, parent=tmp_path / "artifacts"
    )
    paths = {run.steps[0].artifacts[0].path for run in manifest.runs}
    assert len(paths) == 4


def test_one_run_is_refused_because_there_is_nothing_to_compare(tmp_path):
    with pytest.raises(PipelineError, match="at least 2"):
        run_pipeline(write_pipeline(tmp_path, ALWAYS_CLEAN), runs=1, parent=tmp_path / "artifacts")


def test_a_module_without_steps_says_so(tmp_path):
    with pytest.raises(PipelineError, match="defines no STEPS"):
        load_steps(write_pipeline(tmp_path, "ANSWER = 42\n"))


def test_a_step_that_raises_is_not_swallowed(tmp_path):
    body = "def broken(ctx):\n    ctx.write('x', 'SELECT * FROM nope')\n\n\nSTEPS = [broken]\n"
    with pytest.raises(duckdb.CatalogException, match="nope"):
        run_pipeline(write_pipeline(tmp_path, body), runs=2, parent=tmp_path / "artifacts")


def test_the_reference_pipeline_still_loads():
    """Cheap guard, and deliberately not a run of it.

    Executing the reference pipeline takes six seconds and its whole purpose is
    to give a different answer each time, so it does not belong in a suite that
    has to pass on a 4-vCPU runner. Importing it catches the thing CI can catch:
    a broken import or a STEPS list that no longer matches the file.
    """
    steps = load_steps(Path(__file__).resolve().parents[1] / "pipelines" / "reference.py")
    assert [s.__name__ for s in steps] == [
        "generate_inputs",
        "daily_revenue",
        "customer_keys",
        "apply_price_updates",
        "append_audit_log",
        "mean_basket",
        "sparse_customer_keys",
    ]


def test_a_step_that_wrote_nothing_is_not_reported_as_four_clean_comparisons(tmp_path):
    """0 of 4 from a step with no artifacts is not evidence about that step.

    A typo in a ctx.write name, or a step that only ever calls ctx.sql, used to
    render identically to a step that compared four artifacts bit-exactly.
    """
    report, _ = run_pipeline(
        write_pipeline(tmp_path, WRITES_NOTHING), runs=5, parent=tmp_path / "artifacts"
    )
    did, did_not = report.steps
    assert did.artifacts_compared == 4
    assert did_not.artifacts_compared == 0

    printed = report.render()
    assert "wrote no artifacts, so nothing was compared" in printed
    assert "not checked at all: forgot_to_write" in printed


def test_only_the_last_few_run_directories_are_kept(tmp_path):
    where = write_pipeline(tmp_path, ALWAYS_CLEAN)
    parent = tmp_path / "artifacts"
    for _ in range(5):
        run_pipeline(where, runs=2, parent=parent, keep=2)
    assert len(list(parent.glob("run-*"))) == 2


def test_keep_zero_keeps_everything(tmp_path):
    where = write_pipeline(tmp_path, ALWAYS_CLEAN)
    parent = tmp_path / "artifacts"
    for _ in range(3):
        run_pipeline(where, runs=2, parent=parent, keep=0)
    assert len(list(parent.glob("run-*"))) == 3


def test_pruning_leaves_anything_it_did_not_name_alone(tmp_path):
    """--run-dir could be pointed at a directory holding other things."""
    parent = tmp_path / "artifacts"
    parent.mkdir()
    bystander = parent / "run-notes.txt"
    bystander.write_text("keep me", encoding="utf-8")
    (parent / "run-of-the-mill").mkdir()

    where = write_pipeline(tmp_path, ALWAYS_CLEAN)
    for _ in range(3):
        run_pipeline(where, runs=2, parent=parent, keep=1)

    assert bystander.read_text(encoding="utf-8") == "keep me"
    assert (parent / "run-of-the-mill").is_dir()


def test_pruning_never_deletes_the_run_that_is_starting(tmp_path):
    """The flake slice 2's suite turned up, made deterministic.

    Names carry a second-resolution timestamp, so a name this function frees
    can be taken back by the next invocation inside the same second. The newest
    directory on disk then sorts first and was the next to be deleted, which
    deleted the run that was starting. That run went on writing and recreated
    its own directory, so --keep 2 left three behind about one time in fifty.
    """
    parent = tmp_path / "artifacts"
    parent.mkdir()
    for name in ("run-20260826-100715-1", "run-20260826-100715-2"):
        (parent / name).mkdir()
    current = parent / "run-20260826-100715"
    current.mkdir()

    prune_run_dirs(parent, keep=2, current=current)
    assert current.is_dir()
    assert len(list(parent.glob("run-*"))) == 2


def test_recency_comes_from_the_clock_not_from_the_name(tmp_path):
    parent = tmp_path / "artifacts"
    parent.mkdir()
    older, newer = parent / "run-20260826-100715-9", parent / "run-20260826-100715-1"
    older.mkdir()
    newer.mkdir()
    os.utime(older, (1, 1))

    assert prune_run_dirs(parent, keep=1) == [older]
    assert newer.is_dir()
