#!/usr/bin/env python3
"""Fail a commit that uses a word, phrase or character the style list rules out.

The list is `BANNED.md` beside this file, which also carries the punctuation
rules and the exceptions.

Two modes. `--staged` checks what git is about to commit, which is what the
pre-commit hook uses. Passing paths (or nothing, meaning the whole tree) checks
files on disk, which is what CI uses.

Prose files get the full treatment on every line. Code files get punctuation and
emoji checks on every line, but word checks only against comment and string
literal text. A column name that happens to collide with the word list therefore
survives, while a comment that actually reads like marketing copy does not.
"""

from __future__ import annotations

import argparse
import io
import re
import subprocess
import sys
import tokenize
from dataclasses import dataclass
from pathlib import Path

PROSE_SUFFIXES = {".md", ".markdown", ".txt", ".rst"}
CODE_SUFFIXES = {
    ".py", ".pyi", ".ts", ".tsx", ".js", ".jsx", ".mjs",
    ".yaml", ".yml", ".toml", ".sh", ".sql", ".css",
}
SKIP_DIRS = {
    ".git", ".venv", "venv", "node_modules", "__pycache__",
    ".pytest_cache", ".ruff_cache", "dist", "build", ".mypy_cache",
}
# These two quote the list in order to define it.
SKIP_NAMES = {"BANNED.md", "CLAUDE.md"}

# Built by concatenation on purpose. Spelling either marker out as one literal
# would make this file match its own escape hatch and skip itself.
_MARKER = "fingerprint-check:"
DISABLE_FILE_MARKER = f"{_MARKER} disable-file"
IGNORE_LINE_MARKER = f"{_MARKER} ignore"

# Written as escapes, not literal characters, so this file stays ASCII and does
# not trip the very checks it defines.
DASHES = {"\u2014": "em dash", "\u2013": "en dash", "\u2015": "horizontal bar"}
EMOJI = re.compile(
    "["
    "\U0001f000-\U0001faff"  # pictographs, emoticons, transport, extended-A
    "\U0001f1e6-\U0001f1ff"  # regional indicators, which build flags
    "\u2600-\u27bf"          # misc symbols and dingbats, includes the tick and cross
    "\u2b00-\u2bff"          # misc symbols and arrows, includes the star
    "\ufe0f\u200d"           # variation selector and the joiner
    "]"
)
GENERIC_COMMENT = re.compile(r"(//|#|/\*|<!--)|^\s*\*\s")


@dataclass(frozen=True)
class Rules:
    words: re.Pattern[str] | None
    phrases: re.Pattern[str] | None
    patterns: list[re.Pattern[str]]
    exceptions: tuple[str, ...]


@dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    kind: str
    quote: str


def parse_banned(md: Path) -> Rules:
    """Pull the fenced blocks out of the markdown so the list has exactly one home."""
    blocks: dict[str, list[str]] = {}
    current: str | None = None
    for raw in md.read_text(encoding="utf-8").splitlines():
        fence = re.match(r"^```([a-z-]+)\s*$", raw)
        if fence:
            current = fence.group(1)
            blocks.setdefault(current, [])
            continue
        if raw.startswith("```"):
            current = None
            continue
        if current and raw.strip():
            blocks[current].append(raw.strip())

    def alternation(key: str) -> re.Pattern[str] | None:
        terms = blocks.get(key, [])
        if not terms:
            return None
        joined = "|".join(re.escape(t) for t in sorted(terms, key=len, reverse=True))
        return re.compile(rf"\b({joined})\b", re.IGNORECASE)

    return Rules(
        words=alternation("banned-words"),
        phrases=alternation("banned-phrases"),
        patterns=[re.compile(p, re.IGNORECASE) for p in blocks.get("banned-regex", [])],
        exceptions=tuple(c.lower() for c in blocks.get("allowed-contexts", [])),
    )


def find_word_list(start: Path) -> Path:
    """The nearest word list, walking up from `start`.

    The walk checks every level rather than only `start`. An earlier version
    looked at `start` once and then climbed, so running it from `tests/` skipped
    this repo's own copy and found one belonging to a different project higher up
    the disk. The suite passed here and failed with 18 errors in every clone,
    which is the kind of bug that only exists on the machine it was written on.
    """
    for parent in [start, *start.parents]:
        for candidate in (parent / "scripts" / "BANNED.md", parent / "BANNED.md"):
            if candidate.is_file():
                return candidate
    raise SystemExit("check_fingerprint: no BANNED.md in this repo's scripts/ or root")


def excused(text: str, rules: Rules) -> bool:
    lowered = text.lower()
    if IGNORE_LINE_MARKER in lowered:
        return True
    return any(context in lowered for context in rules.exceptions)


def every_line(source: str) -> list[tuple[int, str]]:
    return list(enumerate(source.splitlines(), 1))


def python_segments(source: str) -> list[tuple[int, str]]:
    """Comment and string-literal text only, located by the real tokenizer."""
    segments: list[tuple[int, str]] = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(source).readline):
            if tok.type not in (tokenize.COMMENT, tokenize.STRING):
                continue
            for offset, piece in enumerate(tok.string.splitlines()):
                segments.append((tok.start[0] + offset, piece))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        # A file caught mid-edit still deserves checking, so fall back to
        # whole lines rather than silently passing it.
        return every_line(source)
    return segments


def generic_segments(source: str) -> list[tuple[int, str]]:
    segments = []
    for n, line in enumerate(source.splitlines(), 1):
        marker = GENERIC_COMMENT.search(line)
        if marker:
            segments.append((n, line[marker.start():]))
    return segments


def word_targets(path: Path, source: str) -> list[tuple[int, str]]:
    if path.suffix.lower() in PROSE_SUFFIXES:
        return every_line(source)
    if path.suffix.lower() in (".py", ".pyi"):
        return python_segments(source)
    return generic_segments(source)


def check_file(path: Path, rules: Rules) -> list[Finding]:
    try:
        source = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    if DISABLE_FILE_MARKER in source:
        return []

    findings: list[Finding] = []

    for n, line in every_line(source):
        if excused(line, rules):
            continue
        for char, name in DASHES.items():
            if char in line:
                findings.append(Finding(path, n, name, line.strip()[:120]))
        emoji = EMOJI.search(line)
        if emoji:
            findings.append(Finding(path, n, f"emoji {emoji.group()!r}", line.strip()[:120]))

    for n, text in word_targets(path, source):
        if excused(text, rules):
            continue
        quote = text.strip()[:120]
        for label, pattern in (("banned word", rules.words), ("banned phrase", rules.phrases)):
            if pattern:
                for hit in pattern.finditer(text):
                    findings.append(Finding(path, n, f"{label} {hit.group()!r}", quote))
        for pattern in rules.patterns:
            hit = pattern.search(text)
            if hit:
                findings.append(Finding(path, n, f"banned pattern {hit.group()!r}", quote))

    return sorted(findings, key=lambda f: (f.line, f.kind))


def staged_files(root: Path) -> list[Path]:
    listing = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
        cwd=root, capture_output=True, text=True, check=True,
    )
    return [root / name for name in listing.stdout.split("\n") if name]


def walk(root: Path) -> list[Path]:
    return [
        path for path in root.rglob("*")
        if path.is_file() and not any(part in SKIP_DIRS for part in path.parts)
    ]


def interesting(path: Path) -> bool:
    if path.name in SKIP_NAMES:
        return False
    suffix = path.suffix.lower()
    return suffix in PROSE_SUFFIXES or suffix in CODE_SUFFIXES


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--staged", action="store_true", help="check the git index, not the tree")
    parser.add_argument(
        "--banned", type=Path, help="path to the word list, otherwise discovered"
    )
    args = parser.parse_args()

    root = Path.cwd()
    rules = parse_banned(args.banned or find_word_list(root))

    if args.staged:
        candidates = staged_files(root)
    elif args.paths:
        candidates = [p for arg in args.paths for p in (walk(arg) if arg.is_dir() else [arg])]
    else:
        candidates = walk(root)

    checked = [p for p in candidates if p.is_file() and interesting(p)]
    findings = [f for path in checked for f in check_file(path, rules)]

    if not findings:
        print(f"check_fingerprint: clean across {len(checked)} file(s)")
        return 0

    for f in findings:
        try:
            shown = f.path.relative_to(root)
        except ValueError:
            shown = f.path
        print(f"{shown}:{f.line}: {f.kind}\n    {f.quote}", file=sys.stderr)

    print(
        f"\ncheck_fingerprint: {len(findings)} hit(s) in {len(checked)} file(s). Fix them, "
        f"or put '{IGNORE_LINE_MARKER}' on a line that is genuinely a false positive.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
