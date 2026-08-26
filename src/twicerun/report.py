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
    worst: ArtifactDiff | None = field(default=None)

    def observe(self, diffs: list[ArtifactDiff]) -> None:
        diverged = [d for d in diffs if d.diverged]
        if not diverged:
            return
        self.fired += 1
        loudest = max(diverged, key=lambda d: d.only_in_reference + d.only_in_candidate)
        if self.worst is None or _volume(loudest) > _volume(self.worst):
            self.worst = loudest


def _volume(diff: ArtifactDiff) -> int:
    return diff.only_in_reference + diff.only_in_candidate


@dataclass
class Report:
    pipeline: str
    run_dir: str
    runs: int
    environment: Environment
    steps: list[StepVerdict]
    seconds: float

    def render(self) -> str:
        env = self.environment
        header = [
            f"pipeline   {self.pipeline}",
            f"runs       {self.runs}, run 1 is the reference, "
            f"so {self.runs - 1} comparisons per step",
            f"duckdb     {env.duckdb_version}, threads={env.threads}",
            f"platform   {env.platform}",
            f"artifacts  {self.run_dir}",
            "",
        ]

        label_width = max((len(f"{s.index} {s.name}") for s in self.steps), default=10)
        lines = []
        for step in self.steps:
            label = f"{step.index} {step.name}".ljust(label_width)
            rate = f"{step.fired} of {step.comparisons}"
            detail = step.worst.describe() if step.worst else ""
            lines.append(f"  {label}  {rate:>8}  {detail}".rstrip())

        fired = sum(1 for s in self.steps if s.fired)
        footer = [
            "",
            f"{fired} of {len(self.steps)} steps diverged in {self.seconds:.1f}s.",
            "Comparison is bit-exact over a canonical row hash, so a float aggregate that",
            "reassociates under parallelism counts here exactly as a wrong answer does.",
        ]
        return "\n".join([*header, *lines, *footer])
