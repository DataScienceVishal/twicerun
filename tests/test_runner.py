"""Tests for the N-run loop.

Nothing here waits on a real race. The pipelines below decide when to diverge
from a counter they own, so the fire-rate arithmetic is asserted exactly rather
than probabilistically. The real races are measured by scripts/measure_duckdb.py
and exercised by running the reference pipeline, neither of which belongs in a
suite that has to pass every time.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from twicerun.runner import PipelineError, load_steps, run_pipeline

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
    assert flaky.worst.only_in_reference == 1


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
    assert "absent from this one" in step.worst.schema_note


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
