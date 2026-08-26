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

from twicerun.amplify import AMPLIFIERS
from twicerun.runner import _scored, amplify_runs, load_steps, run_pipeline

LOOP = "the five-run loop"
REFERENCE = Path(__file__).resolve().parent.parent / "pipelines" / "reference.py"
INTERMITTENT = 6


def one_pass(into: Path) -> dict[str, tuple[int, int]]:
    """The main loop's rate on the intermittent step, and each amplifier's on the same step."""
    report, manifest = run_pipeline(REFERENCE, runs=5, parent=into, amplify=False, keep=1)
    loop = next(s for s in report.steps if s.index == INTERMITTENT)
    rates = {LOOP: (loop.fired, loop.comparisons)}
    for entry in amplify_runs(
        load_steps(REFERENCE),
        manifest.runs[0],
        [INTERMITTENT],
        Path(manifest.root),
        5,
        manifest.environment.threads,
        {},
    ):
        rates[entry.amplifier] = (_scored(entry.runs, {})[0], max(len(entry.runs) - 1, 0))
    shutil.rmtree(into, ignore_errors=True)
    return rates


def main(passes: int) -> int:
    collected: list[dict[str, tuple[int, int]]] = []
    for n in range(passes):
        collected.append(one_pass(Path(".twicerun-gap")))
        print(f"pass {n + 1} of {passes}", file=sys.stderr, flush=True)

    names = [LOOP, *(name for name, _ in AMPLIFIERS)]
    step = load_steps(REFERENCE)[INTERMITTENT].__name__
    print(f"\n{step}, {passes} passes, DuckDB threads from the environment\n")
    print(f"{'':<20} {'fired':>12}  {'rate':>5}  passes with nothing")
    for name in names:
        seen = [p[name] for p in collected if name in p]
        fired, of = sum(f for f, _ in seen), sum(o for _, o in seen)
        blank = sum(1 for f, _ in seen if not f)
        rate = f"{fired} of {of}"
        print(
            f"{name:<20} {rate:>12}  {fired / max(of, 1):>5.2f}  {blank} of {len(seen)}"
        )

    def amplified_firings(one: dict[str, tuple[int, int]]) -> int:
        return sum(fired for name, (fired, _) in one.items() if name != LOOP)

    missed = [p for p in collected if not p[LOOP][0]]
    caught = [p for p in missed if amplified_firings(p)]
    print(
        f"\nThe loop reported nothing on {len(missed)} of {passes} passes. "
        f"An amplifier fired on {len(caught)} of those {len(missed)}."
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
