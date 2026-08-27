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
    uv run python scripts/amplification_gap.py 40 --json results/gap.json

Forty passes take about six minutes on a ten-core laptop. Your counts will not
match the README's, for the reason the whole project is about. What should hold
is the shape: the loop misses the step sometimes, tie collapse and row
multiplication almost never do, and the twins stay quiet.

With --json it writes the table out as well as printing it, and that file is
what `twicerun report` renders into the README. The alternative was retyping
four rates out of a terminal, which is how six figures in that file came to be
beaten by a longer run.
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from twicerun.amplify import AMPLIFIERS, Amplification
from twicerun.runner import amplify_runs, load_steps, run_pipeline, scored_amplification
from twicerun.tables import ARTIFACT_VERSION, GAP

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
    row is built here because it is the one row with no amplifier behind it, and
    it took its denominator from `comparisons`, which is runs minus one whether
    or not the step wrote anything. The amplifier rows go through
    `scored_amplification`, which is where the other half of that correction
    lives.
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
        rates[entry.amplifier] = scored_amplification(entry, {})
    shutil.rmtree(into)
    return rates


def summarise(collected: list[dict[str, Amplification]]) -> list[dict]:
    """One row per source, with the three ways a pass can produce no evidence kept apart.

    An amplifier that declined to build an input, one that built an input the
    step wrote nothing from, and one that compared and found the step clean are
    three different things. Only the last is evidence about the step, and they
    were one column until the eval next door separated them.
    """
    rows = []
    for name in [LOOP, *(amplifier for amplifier, _ in AMPLIFIERS)]:
        seen = [one[name] for one in collected if name in one]
        scored = [a for a in seen if a.measured]
        rows.append(
            {
                "source": name,
                "fired": sum(a.fired for a in scored),
                "comparisons": sum(a.comparisons for a in scored),
                "clean": sum(1 for a in scored if not a.fired),
                "scored": len(scored),
                "unusable": len(seen) - len(scored),
            }
        )
    return rows


def amplified_firings(one: dict[str, Amplification]) -> int:
    return sum(a.fired for name, a in one.items() if name != LOOP and a.measured)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="amplification_gap.py",
        description="The five-run loop's rate on the intermittent step against each "
        "amplifier's, over the same passes.",
    )
    parser.add_argument("passes", type=int, nargs="?", default=40)
    parser.add_argument("--json", type=Path, help="write the table out as well as printing it")
    args = parser.parse_args(argv)

    began = time.perf_counter()
    probe = duckdb.connect()
    where = {
        "duckdb": duckdb.__version__,
        "threads": str(probe.execute("SELECT current_setting('threads')").fetchone()[0]),
        "platform": platform.platform(),
    }
    probe.close()

    collected: list[dict[str, Amplification]] = []
    for n in range(args.passes):
        collected.append(one_pass(WORKSPACE))
        print(f"pass {n + 1} of {args.passes}", file=sys.stderr, flush=True)

    step = load_steps(REFERENCE)[INTERMITTENT].__name__
    rows = summarise(collected)
    passes = "1 pass" if args.passes == 1 else f"{args.passes} passes"
    print(f"\n{step}, {passes}, DuckDB {where['duckdb']} "
          f"at threads={where['threads']} on {where['platform']}\n")
    print(f"{'':<20} {'fired':>12}  {'rate':>5}  {'passes clean':>12}  compared nothing")
    for row in rows:
        rate = f"{row['fired']} of {row['comparisons']}" if row["comparisons"] else "nothing"
        print(
            f"{row['source']:<20} {rate:>12}  "
            f"{row['fired'] / max(row['comparisons'], 1):>5.2f}  "
            f"{f'{row['clean']} of {row['scored']}':>12}  {row['unusable']}"
        )

    looked = [one for one in collected if one[LOOP].measured]
    missed = [one for one in looked if not one[LOOP].fired]
    caught = [one for one in missed if amplified_firings(one)]
    print(
        f"\nThe loop reported nothing on {len(missed)} of the {len(looked)} passes where it "
        f"compared anything. An amplifier fired on {len(caught)} of those {len(missed)}."
    )
    if missed:
        median = statistics.median(amplified_firings(one) for one in missed)
        print(f"Median amplified firings on a pass the loop missed: {median:.0f}.")

    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "version": ARTIFACT_VERSION,
                    "kind": GAP,
                    "generated": datetime.now(UTC).isoformat(timespec="seconds"),
                    "environment": where,
                    "step": step,
                    "passes": args.passes,
                    "seconds": time.perf_counter() - began,
                    "rows": rows,
                    "loop_looked": len(looked),
                    "loop_silent": len(missed),
                    "amplifier_caught": len(caught),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"figures written to {args.json}")
    # Nothing here asserts, because the quantity being measured is intermittent
    # by construction and a threshold on it would be the mistake this project
    # exists to point at. Read the shape.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
