"""How many `except Exception` handlers this codebase has, counted rather than remembered.

There are four, each one turning arbitrary user code raising into a reported
outcome with an exit code of its own. Two comments describe the set by size and
both said three: `cli.py` calling its own catch "the outermost of the three" and
`runner.py` explaining itself "for the same reason as the other two". The
`pyproject.toml` comment that turned BLE on said a fourth "could have arrived
without anyone noticing", and by then it had. Ruff can insist that each one
carries a noqa. It cannot count them.

So the count and the sentences that state it are checked here together. A fifth
catch fails the first test, and a sentence left behind by it fails the second.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SEARCHED = sorted((REPO / "src").rglob("*.py")) + sorted((REPO / "scripts").glob("*.py"))

# Every sentence in the tree that states how many there are, and what it should
# say relative to the total. The last one describes the set from inside it.
CLAIMS = (
    (r"the (\w+) deliberate blanket catches", 0),
    (r"the outermost of the (\w+)", 0),
    (r"for the same reason as the other (\w+)", -1),
)
SPELLED = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6}


def blanket_catches() -> list[str]:
    found = []
    for path in SEARCHED:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.ExceptHandler):
                continue
            caught = node.type
            bare = caught is None
            broad = isinstance(caught, ast.Name) and caught.id in {"Exception", "BaseException"}
            if bare or broad:
                found.append(f"{path.relative_to(REPO)}:{node.lineno}")
    return found


def test_there_are_four_of_them():
    found = blanket_catches()
    assert len(found) == 4, found


def test_every_sentence_that_counts_them_agrees_with_the_code():
    total = len(blanket_catches())
    prose = "\n".join(
        path.read_text(encoding="utf-8") for path in (*SEARCHED, REPO / "pyproject.toml")
    )
    for pattern, offset in CLAIMS:
        written = re.findall(pattern, prose)
        assert written, f"nothing matches {pattern!r} any more, so the count has drifted loose"
        for word in written:
            assert SPELLED[word] == total + offset, f"{pattern!r} says {word}, there are {total}"
