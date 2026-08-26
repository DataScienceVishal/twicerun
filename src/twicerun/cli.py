from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

from twicerun.columns import UnknownKeyColumn, UnsupportedColumn
from twicerun.runner import PipelineError, run_pipeline
from twicerun.storage import MissingArtifact


class KeySyntaxError(ValueError):
    """--key was not spelled artifact=column."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="twicerun",
        description="Run a pipeline several times on the same input and report, "
        "per step, how often it failed to give the same answer.",
        epilog="Exit codes: 0 nothing diverged, 1 something diverged, "
        "2 bad input, 3 the run crashed. 1 means only divergence, so a release "
        "gate keyed on it does not also trip on a broken pipeline.",
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
    run.add_argument(
        "--keep",
        type=int,
        default=1,
        help="how many run directories to keep. One pass over the reference pipeline writes "
        "about 200 MB and nothing yet reads a previous one, so the default keeps only the "
        "current run. Use 0 to keep everything",
    )
    run.add_argument(
        "--key",
        action="append",
        default=[],
        metavar="ARTIFACT=COL[,COL]",
        help="match rows of one artifact on these columns instead of on every exact column. "
        "Repeatable, once per artifact. The columns it leaves out stop being part of what "
        "makes a row a row and start being compared as values",
    )
    return parser


def parse_keys(declared: list[str]) -> dict[str, tuple[str, ...]]:
    keys = {}
    for entry in declared:
        artifact, sep, columns = entry.partition("=")
        if not sep or not artifact.strip() or not columns.strip():
            raise KeySyntaxError(
                f"--key {entry!r} is not artifact=column[,column]. "
                f"For example --key daily_revenue=day"
            )
        keys[artifact.strip()] = tuple(c.strip() for c in columns.split(",") if c.strip())
    return keys


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.pipeline.is_file():
        print(f"twicerun: no pipeline file at {args.pipeline}", file=sys.stderr)
        return 2

    try:
        report, _ = run_pipeline(
            args.pipeline,
            runs=args.runs,
            parent=args.run_dir,
            keep=args.keep,
            keys=parse_keys(args.key),
        )
    except (PipelineError, MissingArtifact, KeySyntaxError) as exc:
        print(f"twicerun: {exc}", file=sys.stderr)
        return 2
    except (UnsupportedColumn, UnknownKeyColumn) as exc:
        # A column the oracle refuses is a fact about the pipeline's output,
        # not a crash, so it shares exit 2 with the other bad-input cases.
        print(f"twicerun: {exc}", file=sys.stderr)
        return 2
    except Exception:  # noqa: BLE001
        # A blanket catch is correct at exactly one place, and this is it. A
        # user's step can raise anything, and the point of catching is to give
        # a crash its own exit code rather than let it share 1 with divergence,
        # which would make a CI gate record the two as the same event. Nothing
        # is swallowed: the traceback goes to stderr unchanged.
        traceback.print_exc()
        print(
            "twicerun: the run crashed. Exit 3 is a crash, not a divergence.",
            file=sys.stderr,
        )
        return 3

    print(report.render())
    # 1 is reserved for divergence so this can gate a release. On the reference
    # pipeline that includes the benign float drift, which is the answer strict
    # is supposed to give rather than a bug in it.
    return 1 if any(step.fired for step in report.steps) else 0


if __name__ == "__main__":
    sys.exit(main())
