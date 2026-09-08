"""The four statuses, on pipelines that decide when to diverge rather than racing.

Every fixture here is a step whose output depends on a property of the input it
was handed, so the amplifier that changes that property makes it fire on cue and
the ones that do not leave it alone. The real race is what the reference pipeline
measures; this is the part that has to hold on every machine and in CI.

The pipelines are small on purpose. Tie collapse aims at 500 rows per distinct
value, so a six-row artifact collapses to one bucket, which is the loudest
version of the same transformation and takes milliseconds.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from twicerun import runner
from twicerun.amplify import (
    AMPLIFICATION_FAILED,
    NO_DIVERGENCE_OBSERVED,
    STABLE_ON_THIS_INPUT,
)
from twicerun.policy import Policy
from twicerun.runner import (
    amplify_runs,
    attach_amplification,
    load_steps,
    rejudge,
    run_pipeline,
)

# Diverges only when its input has been collapsed to a single distinct key, which
# is what tie collapse does and what neither of the other two amplifiers does.
# Row multiplication doubles the rows and leaves six distinct keys; the thread
# count amplifier does not touch the data at all.
ONLY_ON_TIED_INPUT = '''
CALLS = {"n": 0}


def generate(ctx):
    ctx.write("src", "SELECT i::INTEGER AS k, i AS id FROM range(6) AS s(i)")


def sensitive(ctx):
    ctx.read("src")
    CALLS["n"] += 1
    collapsed = ctx.sql("SELECT count(DISTINCT k) = 1 FROM src").fetchone()[0]
    shift = CALLS["n"] if collapsed else 0
    ctx.write("out", f"SELECT k, id + {shift} AS id FROM src")


STEPS = [generate, sensitive]
'''

# The uniqueness constraint the spec names as the reason AMPLIFICATION_FAILED has
# to exist. Collapsing k or duplicating rows both put two rows on one key.
BREAKS_ON_A_DUPLICATE_KEY = '''
def generate(ctx):
    ctx.write("src", "SELECT i::INTEGER AS k, i AS id FROM range(6) AS s(i)")


def insists_on_uniqueness(ctx):
    ctx.read("src")
    ctx.sql("CREATE TABLE unique_keys (k INTEGER PRIMARY KEY)")
    ctx.sql("INSERT INTO unique_keys SELECT k FROM src")
    ctx.write("keys", "SELECT k FROM unique_keys")


STEPS = [generate, insists_on_uniqueness]
'''

# Writes an artifact in the full pipeline and nothing when re-executed on its
# own, because the table it reads was created by a step the amplifier skips.
# Its amplified rate is then 0 of 2 out of two comparisons of nothing.
WRITES_NOTHING_ON_ITS_OWN = '''
def stage(ctx):
    ctx.sql("CREATE TABLE staged AS SELECT i FROM range(4) AS s(i)")
    ctx.write("marker", "SELECT i::INTEGER AS ok, i AS id FROM range(6) AS s(i)")


def copies_the_staged_table(ctx):
    ctx.read("marker")
    staged = ctx.sql("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'staged'")
    if staged.fetchone()[0]:
        ctx.write("copied", "SELECT i FROM staged")


STEPS = [stage, copies_the_staged_table]
'''

WRITES_NOTHING_AT_ALL = '''
def looks_busy(ctx):
    ctx.sql("SELECT 1")


def writes_one(ctx):
    ctx.write("rows", "SELECT i FROM range(3) AS s(i)")


STEPS = [looks_busy, writes_one]
'''


def run(tmp_path: Path, body: str, **kwargs):
    where = tmp_path / "p.py"
    where.write_text(body, encoding="utf-8")
    report, manifest = run_pipeline(where, runs=5, parent=tmp_path / "rd", **kwargs)
    return report, manifest


def step_named(report, name: str):
    return next(s for s in report.steps if s.name == name)


def test_a_step_quiet_on_its_own_input_and_loud_under_one_amplifier(tmp_path):
    """The case a plain five-run loop calls clean, which is the argument for amplifying at all.

    The step gives the same answer four times out of four on the input the
    pipeline handed it. Collapsing that input's key makes it disagree with
    itself, so the zero above was a fact about the data rather than about the
    code.
    """
    report, _ = run(tmp_path, ONLY_ON_TIED_INPUT)
    sensitive = step_named(report, "sensitive")

    assert sensitive.fired == 0
    assert sensitive.status == STABLE_ON_THIS_INPUT
    assert [a.amplifier for a in sensitive.amplified_by] == ["tie collapse"]


def test_the_amplifier_that_fired_is_escalated_to_the_main_loop_run_count(tmp_path):
    """Two rates that share a denominator can be read against each other.

    An amplifier probes at 3 runs. Leaving it there would print `0 of 4` beside
    `2 of 2` and invite a comparison the arithmetic does not support.
    """
    report, _ = run(tmp_path, ONLY_ON_TIED_INPUT)
    sensitive = step_named(report, "sensitive")
    fired, quiet = sensitive.amplified_by[0], sensitive.amplifications[1]

    assert (fired.fired, fired.comparisons) == (4, 4)
    assert (quiet.fired, quiet.comparisons) == (0, 2)


def test_a_step_the_amplifier_makes_raise_never_falls_back_to_a_clean_status(tmp_path):
    report, _ = run(tmp_path, BREAKS_ON_A_DUPLICATE_KEY)
    strict = step_named(report, "insists_on_uniqueness")

    assert strict.fired == 0
    assert strict.status == AMPLIFICATION_FAILED
    raised = [a for a in strict.amplifications if a.error]
    assert [a.amplifier for a in raised] == ["tie collapse", "row multiplication"]
    assert "PRIMARY KEY" in raised[0].error or "unique" in raised[0].error.lower()


def test_the_error_reaches_the_report_rather_than_being_counted_as_a_zero(tmp_path):
    """The step line carries the status and the section underneath carries the error.

    `generate` in the same pipeline comes out NO_DIVERGENCE_OBSERVED, which is
    what stops this passing on a report that simply never reaches a status.
    """
    printed = run(tmp_path, BREAKS_ON_A_DUPLICATE_KEY)[0].render()
    on_the_step = next(
        line for line in printed.splitlines() if line.startswith("  1 insists_on_uniqueness")
    )

    assert AMPLIFICATION_FAILED in on_the_step
    assert NO_DIVERGENCE_OBSERVED not in on_the_step
    assert "insists_on_uniqueness, tie collapse:" in printed
    assert "raised:" in printed


def test_an_amplifier_that_compared_nothing_is_not_an_amplifier_that_came_back_quiet(
    tmp_path,
):
    """The guard the main loop and the bisect already carry, in the third loop.

    `any([])` is False, so a step that wrote no artifact when re-executed scores
    0 of 2 out of two comparisons of nothing and reads exactly like two clean
    ones. This fixture is that step: `staged` is created by a step the amplifier
    skips, so the re-execution writes nothing.
    """
    report, _ = run(tmp_path, WRITES_NOTHING_ON_ITS_OWN)
    copier = step_named(report, "copies_the_staged_table")

    assert [a.comparisons for a in copier.amplifications] == [2, 2, 2]
    assert [a.artifacts_compared for a in copier.amplifications] == [0, 0, 0]
    assert not any(a.measured for a in copier.amplifications)

    # The four assertions above all held while the step was still being called
    # NO_DIVERGENCE_OBSERVED, which is the whole finding. A guard's test has to
    # assert the thing the guard decides.
    assert copier.status is None
    printed = report.render()
    assert "the step wrote nothing when re-executed on its own" in printed
    assert "No status on 1 copies_the_staged_table" in printed
    assert NO_DIVERGENCE_OBSERVED not in printed.split("No status on")[0].splitlines()[1]


def test_a_step_that_wrote_no_artifact_at_all_gets_no_status(tmp_path):
    """0 of 4 over nothing is not one of the four, and must not be printed as one.

    The other steps in the same run do get statuses, which is what stops this
    test passing on a report that simply never prints any.
    """
    report, _ = run(tmp_path, WRITES_NOTHING_AT_ALL)

    assert step_named(report, "looks_busy").status is None
    assert step_named(report, "writes_one").status == NO_DIVERGENCE_OBSERVED
    lines = [ln for ln in report.render().splitlines() if "looks_busy" in ln]
    assert lines[0].split() == ["0", "looks_busy", "0", "of", "4"]


def test_no_step_is_amplified_twice_or_bisected_and_amplified(tmp_path):
    """The bisect takes the steps that fired and amplification takes the rest.

    Both loops re-execute steps, and a step in both would be paying twice for
    two answers to the same question.
    """
    report, manifest = run(tmp_path, ONLY_ON_TIED_INPUT)
    amplified = {entry.step_index for entry in manifest.amplified}
    bisected = {s.index for s in report.steps if s.bisect is not None}

    assert amplified == {0, 1}
    assert not amplified & bisected


@pytest.mark.parametrize("policy_name", ["strict", "reduction-order"])
def test_the_status_is_measured_rather_than_judged(tmp_path, policy_name):
    """A policy decides whether a difference matters, never whether one happened.

    Which steps get amplified comes off the measured fire rate for the same
    reason, so two policies over one pipeline re-execute the same steps and
    reach the same four statuses.
    """
    report, _ = run(tmp_path, ONLY_ON_TIED_INPUT, policy=Policy(policy_name))
    assert [s.status for s in report.steps] == [NO_DIVERGENCE_OBSERVED, STABLE_ON_THIS_INPUT]


# Fires under tie collapse the same way ONLY_ON_TIED_INPUT does, and raises on a
# chosen execution so that a crash can be aimed at the escalation. Main loop is
# executions 1 to 5, the tie collapse probe 6 to 8, its escalation 9 and 10.
RAISES_ON_A_CHOSEN_EXECUTION = '''
CALLS = {"n": 0}
RAISE_ON = %d


def generate(ctx):
    ctx.write("src", "SELECT i::INTEGER AS k, i AS id FROM range(6) AS s(i)")


def sensitive(ctx):
    ctx.read("src")
    CALLS["n"] += 1
    if CALLS["n"] == RAISE_ON:
        raise RuntimeError(f"execution {CALLS['n']} was told to fail")
    collapsed = ctx.sql("SELECT count(DISTINCT k) = 1 FROM src").fetchone()[0]
    shift = CALLS["n"] if collapsed else 0
    ctx.write("out", f"SELECT k, id + {shift} AS id FROM src")


STEPS = [generate, sensitive]
'''


def test_a_crash_while_escalating_keeps_what_the_probe_already_saw(tmp_path):
    """The failure this feature exists to prevent, arriving through the amplifier itself.

    The probe fired 2 of 2, so the tool has watched the step give two different
    answers. Discarding those records on the way out of the escalation turned
    that into AMPLIFICATION_FAILED, which is the one status that does not gate a
    release, on a flag-free path. An error that arrives after evidence does not
    delete the evidence.
    """
    report, _ = run(tmp_path, RAISES_ON_A_CHOSEN_EXECUTION % 9)
    sensitive = step_named(report, "sensitive")
    collapse = sensitive.amplifications[0]

    assert (collapse.fired, collapse.comparisons) == (2, 2)
    assert collapse.error is not None
    assert sensitive.status == STABLE_ON_THIS_INPUT


def test_the_report_prints_the_rate_and_the_error_from_one_amplifier(tmp_path):
    printed = run(tmp_path, RAISES_ON_A_CHOSEN_EXECUTION % 10)[0].render()
    line = next(ln for ln in printed.splitlines() if "tie collapse" in ln and "of" in ln)

    assert "2 of 2" in line
    assert "then raised: RuntimeError" in line


def test_escalation_continues_the_sequence_instead_of_restarting_it(tmp_path):
    """Runs 4 and 5 against the run 1 already on disk, not runs 1 to 5 again.

    Counting executions rather than records, because the records looked right
    either way: a restart produced run numbers 1 to 5 too, and threw away the
    three it had just redone. What it could not hide is that the step ran three
    more times than it should have.

    So the step is told to raise on its seventeenth execution. Continuing the
    sequence needs sixteen, five in the main loop and eleven amplified, and
    nothing raises. Restarting needs nineteen, and the seventeenth lands in the
    third amplifier.
    """
    report, manifest = run(tmp_path, RAISES_ON_A_CHOSEN_EXECUTION % 17)
    sensitive = step_named(report, "sensitive")

    assert [a.error for a in sensitive.amplifications] == [None, None, None]
    collapse = next(
        e for e in manifest.amplified if e.step_index == 1 and e.amplifier == "tie collapse"
    )
    assert [r.run for r in collapse.runs] == [1, 2, 3, 4, 5]


def test_judge_re_scores_the_amplified_runs_to_the_rate_the_run_reported(tmp_path):
    """The reason the records are kept at all, applied to the third loop.

    A restarted escalation left the manifest describing one set of executions
    while the Parquet held another, and this is the property that was supposed
    to make that impossible.
    """
    report, manifest = run(tmp_path, ONLY_ON_TIED_INPUT)
    from_the_run = [
        (a.amplifier, a.fired, a.comparisons)
        for s in report.steps
        for a in s.amplifications
    ]
    judged = rejudge(Path(manifest.root) / "manifest.json", Policy())
    from_the_judge = [
        (a.amplifier, a.fired, a.comparisons)
        for s in judged.steps
        for a in s.amplifications
    ]

    assert from_the_run == from_the_judge
    assert any(fired for _, fired, _ in from_the_run), "nothing fired, so this compared nothing"


def test_a_probe_is_never_longer_than_the_run_count_it_is_compared_against(tmp_path):
    """Below four runs the probe has to shrink, or the denominators stop matching.

    `0 of 1` beside `2 of 2` invites a comparison the arithmetic does not
    support, which is the reason the bisect runs at the main loop's N.
    """
    where = tmp_path / "p.py"
    where.write_text(ONLY_ON_TIED_INPUT, encoding="utf-8")
    report, _ = run_pipeline(where, runs=2, parent=tmp_path / "rd")

    for step in report.steps:
        for attempt in step.amplifications:
            assert attempt.comparisons in (0, step.comparisons)


READS_NOTHING = '''
def alone(ctx):
    ctx.write("rows", "SELECT i FROM range(4) AS s(i)")


STEPS = [alone]
'''


def test_a_step_no_amplifier_could_touch_gets_the_same_answer_as_no_amplify(tmp_path):
    """Three amplifiers that all decline leave a step knowing what --no-amplify leaves it.

    A step reading no artifact gives tie collapse and row multiplication nothing
    to substitute, and a machine already running 64 threads gives the third
    nothing to raise. Calling that NO_DIVERGENCE_OBSERVED made the difference
    between two identical bodies of evidence a property of the box it ran on.
    """
    where = tmp_path / "p.py"
    where.write_text(READS_NOTHING, encoding="utf-8")
    report, manifest = run_pipeline(where, runs=3, parent=tmp_path / "rd", amplify=False)

    manifest.amplified = amplify_runs(
        load_steps(where), manifest.runs[0], [0], Path(manifest.root), 3, 64, {}
    )
    attach_amplification(manifest, report.steps, {})

    assert [a.ran for a in report.steps[0].amplifications] == [False, False, False]
    assert report.steps[0].status is None
    assert "No status on 0 alone" in report.render()


def test_an_amplifier_that_fails_to_build_its_input_does_not_destroy_the_run(tmp_path):
    """The guard the bisect got after being bitten, which this loop did not inherit.

    Substituting the input was evaluated as an argument, outside the catch, and
    nothing above it was guarded either. A COPY that runs out of disk while
    materialising four million rows would have taken five finished runs, the
    bisect and the manifest with it.
    """

    def out_of_disk(con, into, inputs, threads):
        raise OSError(28, "No space left on device")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runner, "AMPLIFIERS", (("tie collapse", out_of_disk),))
        report, manifest = run(tmp_path, ONLY_ON_TIED_INPUT)

    assert (Path(manifest.root) / "manifest.json").is_file()
    assert len(manifest.runs) == 5
    sensitive = step_named(report, "sensitive")
    assert sensitive.status == AMPLIFICATION_FAILED
    assert "No space left on device" in sensitive.amplifications[0].error
