from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

from twicerun.columns import FloatInKey, UnknownKeyColumn, UnsupportedColumn
from twicerun.policy import REDUCTION_ORDER, STRICT, Policy
from twicerun.runner import PipelineError, UnknownArtifact, rejudge, run_pipeline
from twicerun.storage import MissingArtifact


class KeySyntaxError(ValueError):
    """--key was not spelled artifact=column."""


def _judge(args: argparse.Namespace) -> int:
    """Re-score a run that already happened.

    The point of this command is that it re-runs nothing. Two invocations of
    `twicerun run` under different policies execute the pipeline twice, so the
    figures move between them for exactly the reason this project exists, and
    the claim that a policy cannot move a measured number was left resting on a
    unit test. Judging one saved run twice puts it in front of a reader.
    """
    given = _newest(args.manifest)
    where = given / "manifest.json" if given.is_dir() else given
    if not where.is_file():
        print(f"twicerun: no manifest at {where}", file=sys.stderr)
        return 2
    if len(args.manifest) > 1:
        print(
            f"twicerun: {len(args.manifest)} run directories given, judging the newest, {given}",
            file=sys.stderr,
        )
    try:
        report = rejudge(where, _policy_from(args), parse_keys(args.key))
    except (PipelineError, MissingArtifact, KeySyntaxError) as exc:
        print(f"twicerun: {exc}", file=sys.stderr)
        return 2
    except (UnsupportedColumn, UnknownKeyColumn, FloatInKey, UnknownArtifact) as exc:
        print(f"twicerun: {exc}", file=sys.stderr)
        return 2

    print(report.render())
    return 1 if any(verdict.fired for verdict in report.verdicts) else 0


def _newest(given: list[Path]) -> Path:
    """The most recently written of what the shell handed over.

    The README documents `judge .twicerun/run-*`, and that glob matches one
    directory only while retention is 1. Two paragraphs later the README
    suggests --keep 0. The second directory then turned the documented command
    into a bare argparse usage message that never mentioned run directories at
    all.
    """
    return max(given, key=lambda p: p.stat().st_mtime if p.exists() else 0)


def _policy_from(args: argparse.Namespace) -> Policy:
    return Policy(
        name=args.policy,
        tolerance_relative=args.tolerance_rel,
        tolerance_ulps=args.tolerance_ulps,
    )


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
        "--no-containment",
        action="store_true",
        help="let runs 2 to N read their own artifacts instead of run 1's. This is the "
        "ablation: with it, one divergence early in a pipeline is counted again by every "
        "step that reads its output, and the per-step report becomes one finding followed "
        "by echoes of it",
    )
    run.add_argument(
        "--no-amplify",
        action="store_true",
        help="skip the amplifiers. Each of them re-executes a step that never fired against an "
        "input built to make it fire, at 3 runs, escalating to --runs on a hit. Without them a "
        "step's zero is only ever a zero on the one input you gave it, so none of the four "
        "statuses is printed and the report says so instead",
    )
    run.add_argument(
        "--keep",
        type=int,
        default=1,
        help="how many run directories to keep. One pass over the reference pipeline writes "
        "about 260 MB and nothing yet reads a previous one, so the default keeps only the "
        "current run. Use 0 to keep everything",
    )
    _policy_flags(run)
    judge = commands.add_parser(
        "judge",
        help="score a saved run again under a different policy, without re-running it",
    )
    judge.add_argument(
        "manifest",
        type=Path,
        nargs="+",
        help="a manifest.json written by a previous run, or the run directory holding one. "
        "Several are accepted so that a shell glob works under --keep 0, and the newest of "
        "them is judged",
    )
    _policy_flags(judge)
    return parser


def _policy_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--policy",
        choices=[STRICT, REDUCTION_ORDER],
        default=STRICT,
        help="strict counts any difference as a divergence and is the default. "
        "reduction-order downgrades float drift to TOLERATED when it is the step's only "
        "class and its size is inside the derived reassociation bound",
    )
    parser.add_argument(
        "--tolerance-rel",
        type=float,
        metavar="X",
        help="escape hatch for someone who knows their domain: treat a float difference "
        "at or below this relative size as TOLERATED. Never a default, and it does not "
        "excuse a missing or duplicated row",
    )
    parser.add_argument(
        "--tolerance-ulps",
        type=int,
        metavar="N",
        help="the same escape hatch measured in last-bit steps rather than relative size. "
        "Set both and a difference has to clear both",
    )
    parser.add_argument(
        "--key",
        action="append",
        default=[],
        metavar="ARTIFACT=COL[,COL]",
        help="match rows of one artifact on these columns instead of on every exact column. "
        "Repeatable, once per artifact. The columns it leaves out stop being part of what "
        "makes a row a row and start being compared as values. Float columns are refused: "
        "matching on one means joining on bit equality, and it would take the step out of "
        "the reassociation bound report entirely",
    )


def parse_keys(declared: list[str]) -> dict[str, tuple[str, ...]]:
    """Validate what came out of the split, not what went into it.

    `--key rows=,` passed the raw check, because "," survives strip(), and then
    the comprehension filtered every name out and left an empty tuple. An empty
    key is not a refusal: it puts the artifact in one group and pairs rows by
    sort order, which is the weakest comparison this tool has, silently.
    """
    keys = {}
    for entry in declared:
        artifact, sep, spelled = entry.partition("=")
        columns = tuple(c.strip() for c in spelled.split(",") if c.strip())
        if not sep or not artifact.strip() or not columns:
            raise KeySyntaxError(
                f"--key {entry!r} is not artifact=column[,column]. "
                f"For example --key daily_revenue=day"
            )
        keys[artifact.strip()] = columns
    return keys


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "judge":
        return _judge(args)
    if not args.pipeline.is_file():
        print(f"twicerun: no pipeline file at {args.pipeline}", file=sys.stderr)
        return 2
    if args.keep < 0:
        # 0 already means keep everything, so a negative is a typo rather than a
        # stronger form of it. It used to print "keeping -5 run directories".
        print(
            f"twicerun: --keep {args.keep} is not a number of directories. "
            f"Use 0 to keep everything",
            file=sys.stderr,
        )
        return 2

    try:
        report, _ = run_pipeline(
            args.pipeline,
            runs=args.runs,
            parent=args.run_dir,
            keep=args.keep,
            keys=parse_keys(args.key),
            policy=_policy_from(args),
            contained=not args.no_containment,
            amplify=not args.no_amplify,
        )
    except (PipelineError, MissingArtifact, KeySyntaxError) as exc:
        print(f"twicerun: {exc}", file=sys.stderr)
        return 2
    except (UnsupportedColumn, UnknownKeyColumn, FloatInKey, UnknownArtifact) as exc:
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
    # 1 is reserved for divergence so this can gate a release. Under the default
    # policy that includes the benign float drift, which is the answer strict is
    # supposed to give rather than a bug in it. A comparison downgraded to
    # TOLERATED is still printed with its magnitudes but does not set this.
    return 1 if any(verdict.fired for verdict in report.verdicts) else 0


if __name__ == "__main__":
    sys.exit(main())
