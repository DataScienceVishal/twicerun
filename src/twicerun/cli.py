from __future__ import annotations

import argparse
import sys
from pathlib import Path

from twicerun.runner import PipelineError, run_pipeline
from twicerun.storage import MissingArtifact


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="twicerun",
        description="Run a pipeline several times on the same input and report, "
        "per step, how often it failed to give the same answer.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="execute a pipeline N times and compare the runs")
    run.add_argument("pipeline", type=Path, help="a Python file exporting a STEPS list")
    run.add_argument(
        "--runs",
        type=int,
        default=5,
        help="how many times to execute the pipeline. Five by default: with four comparisons "
        "a step that diverges half the time is missed 6.3%% of the time, against 50%% at two "
        "runs, for 2.5 times the runtime",
    )
    run.add_argument(
        "--run-dir",
        type=Path,
        default=Path(".twicerun"),
        help="where to keep artifacts and the manifest (default .twicerun)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report, _ = run_pipeline(args.pipeline, runs=args.runs, parent=args.run_dir)
    except (PipelineError, MissingArtifact) as exc:
        print(f"twicerun: {exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"twicerun: {exc}", file=sys.stderr)
        return 2

    print(report.render())
    # Non-zero when anything diverged, so this is usable as a gate. On the
    # reference pipeline that includes the benign float drift, which is the
    # point of slice 1 rather than a bug in it.
    return 1 if any(step.fired for step in report.steps) else 0


if __name__ == "__main__":
    sys.exit(main())
