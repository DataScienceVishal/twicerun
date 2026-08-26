"""Tests for the single-threaded bisect and the two rates it produces.

The pipelines here decide when to diverge from a counter they own, so the label
is asserted exactly. Where a fixture reads PARALLEL_ORDER, the real cause is a
counter rather than a race: what is under test is the arithmetic that turns two
fire rates into a label, not DuckDB's thread pool. The one thing a counter
cannot fake is that the bisect really ran at one thread, and the last test here
reads that back out of the artifacts.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from twicerun.cause import PARALLEL_ORDER, PERSISTS_SINGLE_THREADED, upper_bound
from twicerun.runner import run_pipeline

# Fires on runs 2 and 4 of the main loop and never again, so the five
# single-threaded executions that follow all agree with each other.
SETTLES_DOWN = '''
CALLS = {"n": 0}
DIVERGES_ON = {2, 4}


def wobble(ctx):
    CALLS["n"] += 1
    shift = 1 if CALLS["n"] in DIVERGES_ON else 0
    ctx.write("rows", f"SELECT i + {shift} AS i FROM range(4) AS s(i)")


STEPS = [wobble]
'''

# Every execution differs from the last, so no thread count helps.
NEVER_SETTLES = SETTLES_DOWN.replace(
    'shift = 1 if CALLS["n"] in DIVERGES_ON else 0', 'shift = CALLS["n"]'
)

APPENDS_TO_ITS_OWN_STATE = '''
def append(ctx):
    ctx.state("log", "SELECT 0 AS i WHERE false")
    ctx.write("log", "SELECT i FROM log UNION ALL SELECT 1 AS i")


STEPS = [append]
'''

# The shape the fixture above cannot reach: the name a divergent step carries
# state under was written by a step that does not diverge, so the bisect skips
# the writer. The seed is typed INTEGER against the artifact's BIGINT, which
# makes a bisect that fell back to the seed visible as a schema difference
# rather than as an absence.
STATE_WRITTEN_BY_ANOTHER_STEP = '''
def seed(ctx):
    ctx.write("ledger", "SELECT 1::BIGINT AS id")


def consume(ctx):
    ctx.state("ledger", "SELECT 0::INTEGER AS id WHERE false")
    ctx.write("out", "SELECT id FROM ledger")


STEPS = [seed, consume]
'''

STAMPS_THE_THREAD_COUNT = '''
CALLS = {"n": 0}


def stamp(ctx):
    CALLS["n"] += 1
    ctx.write(
        "rows",
        f"SELECT {CALLS['n']} AS attempt, current_setting('threads')::BIGINT AS threads",
    )


STEPS = [stamp]
'''


def write_pipeline(tmp_path: Path, body: str) -> Path:
    where = tmp_path / "p.py"
    where.write_text(body, encoding="utf-8")
    return where


def only_step(tmp_path: Path, body: str, runs: int = 5):
    report, manifest = run_pipeline(
        write_pipeline(tmp_path, body), runs=runs, parent=tmp_path / "artifacts"
    )
    return report.steps[0], manifest


def test_a_step_that_stops_diverging_at_one_thread_is_labelled_parallel_order(tmp_path):
    step, _ = only_step(tmp_path, SETTLES_DOWN)
    assert (step.fired, step.comparisons) == (2, 4)
    assert (step.bisect.fired, step.bisect.comparisons) == (0, 4)
    assert step.cause == PARALLEL_ORDER


def test_a_step_that_keeps_diverging_at_one_thread_says_the_cause_is_elsewhere(tmp_path):
    step, _ = only_step(tmp_path, NEVER_SETTLES)
    assert (step.fired, step.bisect.fired) == (4, 4)
    assert step.cause == PERSISTS_SINGLE_THREADED


def test_the_bisect_starts_from_no_carried_state_the_way_run_one_did(tmp_path):
    """The fix a reference-pipeline run turned up, and it is worth a test of its own.

    Seeding the single-threaded runs from the main loop's run 1 had all five of
    them append to the same log, agree with each other, and label a duplicating
    append PARALLEL_ORDER. Duplicating a log on rerun has nothing to do with
    threads. The bisect has to be the main loop again at one thread.
    """
    step, _ = only_step(tmp_path, APPENDS_TO_ITS_OWN_STATE)
    assert step.cause == PERSISTS_SINGLE_THREADED
    assert (step.fired, step.bisect.fired) == (4, 4)


def test_state_a_skipped_step_wrote_still_reaches_the_bisect(tmp_path):
    """The state fix arriving through the door the first fix left open.

    `consume` carries state under a name `seed` wrote, and `seed` never
    diverges, so the bisect does not re-execute it. Seeding the bisect from
    only what it re-produced left `ledger` missing on every single-threaded
    run, all five fell back to the INTEGER seed, all five agreed, and a step
    that diverges because it reads a previous run's output was labelled
    PARALLEL_ORDER.

    The single-step fixture above cannot catch this. Its state name and its
    write name belong to the same step, so the bisect always re-produces it.
    """
    report, _ = run_pipeline(
        write_pipeline(tmp_path, STATE_WRITTEN_BY_ANOTHER_STEP),
        runs=5,
        parent=tmp_path / "artifacts",
    )
    consume = report.steps[1]
    assert (consume.fired, consume.bisect.fired) == (4, 4)
    assert consume.cause == PERSISTS_SINGLE_THREADED


def test_both_rates_come_out_of_the_same_denominator(tmp_path):
    """Three runs, not five. A fixed bisect size would print two unrelated fractions."""
    step, _ = only_step(tmp_path, NEVER_SETTLES, runs=3)
    assert step.comparisons == step.bisect.comparisons == 2


def test_only_the_steps_that_fired_are_re_executed(tmp_path):
    body = '''
CALLS = {"n": 0}


def steady(ctx):
    ctx.write("steady", "SELECT 1 AS i")


def wobble(ctx):
    CALLS["n"] += 1
    ctx.write("wobble", f"SELECT {CALLS['n']} AS i")


STEPS = [steady, wobble]
'''
    report, manifest = run_pipeline(
        write_pipeline(tmp_path, body), runs=4, parent=tmp_path / "artifacts"
    )
    steady, wobbled = report.steps
    assert steady.bisect is None and steady.cause is None
    assert wobbled.cause == PERSISTS_SINGLE_THREADED
    assert [[s.name for s in run.steps] for run in manifest.bisect] == [["wobble"]] * 4


def test_a_step_that_never_fired_is_not_bisected_at_all(tmp_path):
    body = "def calm(ctx):\n    ctx.write('rows', 'SELECT 1 AS i')\n\n\nSTEPS = [calm]\n"
    report, manifest = run_pipeline(
        write_pipeline(tmp_path, body), runs=3, parent=tmp_path / "artifacts"
    )
    assert manifest.bisect == []
    assert report.steps[0].cause is None
    assert "cause, from re-executing" not in report.render()


def test_the_bisect_really_runs_at_one_thread(tmp_path):
    """The one claim a counter-driven fixture cannot make on its own.

    Every other test here would pass if the bisect quietly used the default
    thread count, so this reads the setting back out of what the step wrote.
    """
    _, manifest = only_step(tmp_path, STAMPS_THE_THREAD_COUNT)
    con = duckdb.connect()
    try:
        stamped = {
            con.execute(f"SELECT threads FROM read_parquet('{a.path}')").fetchone()[0]
            for run in manifest.bisect
            for step in run.steps
            for a in step.artifacts
        }
    finally:
        con.close()
    assert stamped == {1}


# `build` never diverges, so the bisect skips it, and the table it left in the
# connection is not an artifact anything can resolve.
SCRATCH_TABLE_FROM_A_SKIPPED_STEP = '''
CALLS = {"n": 0}


def build(ctx):
    ctx.sql("CREATE TABLE scratch AS SELECT 1 AS i")
    ctx.write("built", "SELECT * FROM scratch")


def uses(ctx):
    CALLS["n"] += 1
    ctx.write("out", f"SELECT i + {CALLS['n']} AS i FROM scratch")


STEPS = [build, uses]
'''


def test_a_bisect_that_cannot_run_keeps_the_measurement_it_was_diagnosing(tmp_path):
    """Five completed runs must survive a diagnostic that failed after them.

    This used to raise out of run_pipeline before manifest.save(), so the
    command exited 3 with no report and no manifest, and judge could not
    recover it either. The measurement was finished and correct at that point.
    """
    report, manifest = run_pipeline(
        write_pipeline(tmp_path, SCRATCH_TABLE_FROM_A_SKIPPED_STEP),
        runs=5,
        parent=tmp_path / "artifacts",
    )
    assert report.steps[1].fired == 4
    assert report.steps[1].cause is None
    assert manifest.bisect == []
    assert "does not exist" in manifest.bisect_error
    assert (manifest.root / "manifest.json").is_file()

    printed = report.render()
    assert "the bisect did not run, so no step has one" in printed
    assert "their comparison is unaffected" in printed


# The careful author's shape: a step checking for a table an earlier step left
# in the connection, and doing nothing where it is absent. Under `only` the
# earlier step is skipped, so the re-execution writes nothing at all.
GUARDS_AGAINST_A_MISSING_TABLE = '''
CALLS = {"n": 0}


def build(ctx):
    ctx.sql("CREATE TABLE scratch AS SELECT 1 AS i")
    ctx.write("built", "SELECT * FROM scratch")


def guarded(ctx):
    CALLS["n"] += 1
    present = ctx.sql(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'scratch'"
    ).fetchone()[0]
    if not present:
        return
    ctx.write("out", f"SELECT {CALLS['n']} AS i FROM scratch")


STEPS = [build, guarded]
'''


def test_a_bisect_that_compared_no_artifacts_gets_no_label(tmp_path):
    """The same guard the main loop has, which the bisect was missing.

    Four comparisons of nothing sum to `0 of 4` because `any([])` is False, and
    the step printed PARALLEL_ORDER over nothing compared against nothing. The
    main loop says "wrote no artifacts, so nothing was compared" in that
    situation and has since slice 1.
    """
    report, _ = run_pipeline(
        write_pipeline(tmp_path, GUARDS_AGAINST_A_MISSING_TABLE),
        runs=5,
        parent=tmp_path / "artifacts",
    )
    guarded = report.steps[1]
    assert guarded.fired == 4
    assert guarded.bisect.fired == 0
    assert guarded.bisect.artifacts_compared == 0
    assert guarded.cause is None

    printed = report.render()
    assert "wrote no artifacts, so there was nothing to compare" in printed
    assert "PARALLEL_ORDER" not in printed


def test_the_report_prints_both_rates_and_what_the_zero_is_worth(tmp_path):
    report, _ = run_pipeline(
        write_pipeline(tmp_path, SETTLES_DOWN), runs=5, parent=tmp_path / "artifacts"
    )
    printed = report.render()
    assert "2 of 4 at threads=" in printed
    assert "0 of 4 at threads=1" in printed
    assert "rules out a per-comparison rate" in printed
    assert "above 53 percent" in printed


# comparisons, and the percent the spec's run-count table published for it.
SPEC_TABLE = [(1, 95), (2, 78), (4, 53), (9, 28), (19, 15)]


@pytest.mark.parametrize(("comparisons", "percent"), SPEC_TABLE)
def test_the_one_sided_bound_matches_the_table_the_run_count_was_argued_from(
    comparisons, percent
):
    """The last column of the table that settled on five runs, recomputed rather than quoted.

    The table is whole percents and this is where the figure it rounded comes
    from, so the check is on the rounding rather than on decimals typed out by
    hand. Getting those wrong is how this test failed first time: 19
    comparisons is 14.59 percent, which the table published as 15.
    """
    assert round(upper_bound(comparisons) * 100) == percent


def test_no_comparisons_leaves_the_bound_at_everything():
    """A rate out of nothing rules out nothing, and 0 comparisons would divide by zero."""
    assert upper_bound(0) == 1.0
