"""What the run command prints.

Two rules the format follows. Every header carries the DuckDB version and the
thread count, because every number underneath them is specific to both and a
table missing them is not reproducible. And a step that never diverged prints
`0 of 4`, never a word like stable or deterministic: four clean comparisons rule
out very little, and the tool saying otherwise would be the exact mistake it
exists to catch.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from twicerun.compare import ArtifactDiff
from twicerun.manifest import Environment


@dataclass
class StepVerdict:
    index: int
    name: str
    comparisons: int
    fired: int = 0
    artifacts_compared: int = 0
    worst: ArtifactDiff | None = field(default=None)

    def observe(self, diffs: list[ArtifactDiff]) -> None:
        self.artifacts_compared += len(diffs)
        diverged = [d for d in diffs if d.diverged]
        if not diverged:
            return
        self.fired += 1
        loudest = max(diverged, key=_rank)
        if self.worst is None or _rank(loudest) > _rank(self.worst):
            self.worst = loudest


def _rank(diff: ArtifactDiff) -> tuple[bool, int]:
    """How interesting a diff is, given only one of them gets printed.

    A schema change comes first regardless of size. It carries no row counts at
    all, because the row comparison is skipped when the columns do not line up,
    so ranking on volume alone sorted the one class that is never noise below a
    two-row drift on a sibling artifact and dropped it out of the report.
    """
    return (diff.note is not None, diff.only_in_reference + diff.only_in_candidate)


@dataclass
class Report:
    pipeline: str
    run_dir: str
    runs: int
    environment: Environment
    steps: list[StepVerdict]
    seconds: float
    pruned: int = 0
    keep: int = 1

    def render(self) -> str:
        env = self.environment
        header = [
            f"pipeline   {self.pipeline}",
            f"runs       {self.runs}, run 1 is the reference, "
            f"so {self.runs - 1} comparisons per step",
            f"duckdb     {env.duckdb_version}, threads={env.threads}",
            f"platform   {env.platform}",
            f"artifacts  {self.run_dir}",
            f"retention  keeping {self.keep} run director(ies)"
            + (f", dropped {self.pruned}" if self.pruned else ""),
            "",
        ]

        label_width = max((len(f"{s.index} {s.name}") for s in self.steps), default=10)
        lines = []
        for step in self.steps:
            label = f"{step.index} {step.name}".ljust(label_width)
            rate = f"{step.fired} of {step.comparisons}"
            if step.artifacts_compared == 0:
                detail = "wrote no artifacts, so nothing was compared"
            else:
                detail = step.worst.describe() if step.worst else ""
            lines.append(f"  {label}  {rate:>8}  {detail}".rstrip())

        fired = sum(1 for s in self.steps if s.fired)
        silent = [s.name for s in self.steps if s.artifacts_compared == 0]
        footer = [
            "",
            f"{fired} of {len(self.steps)} steps diverged in {self.seconds:.1f}s.",
            "Comparison is bit-exact over a canonical row hash, so a float aggregate that",
            "reassociates under parallelism counts here exactly as a wrong answer does.",
        ]
        if silent:
            footer += [
                "",
                f"{len(silent)} step(s) wrote nothing and were therefore not checked at all: "
                f"{', '.join(silent)}.",
                "A 0 of 4 from a step with no artifacts is not evidence about that step.",
            ]
        return "\n".join([*header, *lines, *footer])
