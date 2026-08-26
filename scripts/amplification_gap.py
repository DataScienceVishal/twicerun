#!/usr/bin/env python3
"""How often the five-run loop misses a step, and what each amplifier does to the same step.

The README's amplification table cannot be produced by `twicerun run`, and that
is not an oversight in the runner. Amplification deliberately only touches steps
the main loop found nothing in, so on most passes the intermittent step fires,
never reaches an amplifier, and contributes nothing to the second half of the
comparison. Waiting for the passes where it does not fire means throwing away
five passes in six.

So this points the amplifiers at one step every pass, whether or not the loop
already caught it. That is a measurement script and not a mode of the tool: a
flag doing this would make every clean pipeline pay for evidence it does not
need, and the cost argument for amplification is that it only runs where the
loop came back quiet.

    uv run python scripts/amplification_gap.py 40

Forty passes take about six minutes on a ten-core laptop. Your counts will not
match the README's, for the reason the whole project is about. What should hold
is the shape: the loop misses the step sometimes, tie collapse and row
multiplication almost never do, and the twins stay quiet.
"""

from __future__ import annotations

import shutil
import statistics
import sys
from pathlib import Path

from twicerun.amplify import AMPLIFIERS, Amplification
from twicerun.runner import amplify_runs, load_steps, run_pipeline, scored_runs

LOOP = "the five-run loop"
# In .gitignore and checked by a test, for the same reason the eval's is: the
# rmtree below is on the success path only.
WORKSPACE = Path(".twicerun-gap")
REFERENCE = Path(__file__).resolve().parent.parent / "pipelines" / "reference.py"
INTERMITTENT = 6


def one_pass(into: Path) -> dict[str, Amplification]:
    """The main loop's rate on the intermittent step, and each amplifier's on the same step.

    Every row of the table comes back in the shipped `Amplification`, including
    the loop's, which is the row where nothing was substituted. That is not
    tidiness: `measured` is the guard both halves need and neither had. The loop
    row took its denominator from `comparisons`, which is runs minus one whether
    or not the step wrote anything, and the amplifier rows took
    `scored_runs(...)[0]` and threw away the second element, which is the only
    thing that tells a re-execution that wrote nothing apart from one that
    agreed with itself.
    """
    report, manifest = run_pipeline(REFERENCE, runs=5, parent=into, amplify=False, keep=1)
    loop = next(s for s in report.steps if s.index == INTERMITTENT)
    rates = {
        LOOP: Amplification(
            amplifier=LOOP,
            note="no substitution, the pipeline's own input",
            comparisons=loop.measured_comparisons,
            fired=loop.fired,
            artifacts_compared=loop.artifacts_compared,
        )
    }
    for entry in amplify_runs(
        load_steps(REFERENCE),
        manifest.runs[0],
        [INTERMITTENT],
        Path(manifest.root),
        5,
        manifest.environment.threads,
        {},
    ):
        fired, compared = scored_runs(entry.runs, {})
        rates[entry.amplifier] = Amplification(
            amplifier=entry.amplifier,
            note=entry.note,
            comparisons=max(len(entry.runs) - 1, 0),
            fired=fired,
            artifacts_compared=compared,
            error=entry.error,
        )
    shutil.rmtree(into)
    return rates


def main(passes: int) -> int:
    collected: list[dict[str, Amplification]] = []
    for n in range(passes):
        collected.append(one_pass(WORKSPACE))
        print(f"pass {n + 1} of {passes}", file=sys.stderr, flush=True)

    names = [LOOP, *(name for name, _ in AMPLIFIERS)]
    step = load_steps(REFERENCE)[INTERMITTENT].__name__
    print(f"\n{step}, {passes} passes, DuckDB threads from the environment\n")
    print(f"{'':<20} {'fired':>12}  {'rate':>5}  {'passes clean':>12}  compared nothing")
    for name in names:
        seen = [p[name] for p in collected if name in p]
        scored = [a for a in seen if a.measured]
        fired, of = sum(a.fired for a in scored), sum(a.comparisons for a in scored)
        clean = f"{sum(1 for a in scored if not a.fired)} of {len(scored)}"
        rate = f"{fired} of {of}" if of else "nothing"
        print(
            f"{name:<20} {rate:>12}  {fired / max(of, 1):>5.2f}  {clean:>12}  "
            f"{len(seen) - len(scored)}"
        )

    def amplified_firings(one: dict[str, Amplification]) -> int:
        return sum(a.fired for name, a in one.items() if name != LOOP and a.measured)

    looked = [p for p in collected if p[LOOP].measured]
    missed = [p for p in looked if not p[LOOP].fired]
    caught = [p for p in missed if amplified_firings(p)]
    print(
        f"\nThe loop reported nothing on {len(missed)} of the {len(looked)} passes where it "
        f"compared anything. An amplifier fired on {len(caught)} of those {len(missed)}."
    )
    if missed:
        median = statistics.median(amplified_firings(p) for p in missed)
        print(f"Median amplified firings on a pass the loop missed: {median:.0f}.")
    # Nothing here asserts, because the quantity being measured is intermittent
    # by construction and a threshold on it would be the mistake this project
    # exists to point at. Read the shape.
    return 0


if __name__ == "__main__":
    raise SystemExit(main(int(sys.argv[1]) if len(sys.argv) > 1 else 40))
