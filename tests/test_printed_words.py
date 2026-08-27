"""The four words nothing in this repository may print, applied to everything that prints.

No practical number of runs proves that a step always gives the same answer, so
the tool does not say so. That argument does not stop at the tool. The README
publishes the output of four scripts beside the tool's own report, quotes
`count()` stable from one of them as an invariant, and that invariant is three
observations. Three is a finite sample and the whole point of the list is that a
finite sample cannot license the word.

`scripts/measure_duckdb.py` printed `(1 means stable)` and `count() stable` for
that reason and nothing in the suite could see it: `test_eval.py` held one script
to the list and `test_cli.py` held the tool's report, and the other four scripts
were held to nothing. Inserting `deterministic` into `amplification_gap.py` left
all 363 tests green.

Read out of the source rather than by running anything. Two of these scripts take
minutes and one of them opens a socket, and the words are in string literals
either way.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from twicerun.amplify import STABLE_ON_THIS_INPUT

REPO = Path(__file__).resolve().parent.parent
# check_fingerprint.py is the style checker and its word lists are data, so
# holding it to its own vocabulary would be circular. Nothing publishes its
# output as a measurement either.
PUBLISHED = [
    path
    for path in sorted((REPO / "scripts").glob("*.py"))
    if path.name != "check_fingerprint.py"
] + sorted((REPO / "src" / "twicerun").glob("*.py"))

FORBIDDEN = ("deterministic", "stable", "reproducible", "passed")


def printed_prose(path: Path) -> str:
    """Every string literal in one file that is prose rather than an identifier.

    Inclusive rather than call by call, which is the correction `test_eval.py`
    arrived at first. Collecting the constants inside `say(...)` and nothing else
    reached none of the text a helper composes and returns, and three mutations
    putting a forbidden word where the eval really prints it left the suite
    green.

    Dict keys and subscript keys come back out, because they are identifiers
    rather than words: one of them is `stable_trials`, which goes to a JSON file
    and is never printed, and a test that fails on a dict key is a test that gets
    deleted. Docstrings come out because nothing prints them, and two of them
    here are the argument for this check.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    not_prose = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            not_prose.update(k for k in node.keys if isinstance(k, ast.Constant))
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            not_prose.add(node.slice)
        elif isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef):
            first = node.body[0] if node.body else None
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                not_prose.add(first.value)
    return "\n".join(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node not in not_prose
    )


@pytest.mark.parametrize("path", PUBLISHED, ids=lambda p: p.name)
def test_nothing_this_repository_prints_claims_a_result_is_settled(path):
    """The status token comes out first, the same way the CLI's version of this does it.

    `STABLE_ON_THIS_INPUT` is allowed and the bare word is not. The status says
    a step gave two answers on an input twicerun fabricated, which is a finding
    rather than a claim about what the step always does.
    """
    prose = printed_prose(path).replace(STABLE_ON_THIS_INPUT, "").lower()
    printed = [word for word in FORBIDDEN if word in prose]
    assert not printed, f"{path.relative_to(REPO)} prints {printed}"


def test_the_list_covers_every_script_the_readme_publishes_the_output_of():
    """A script added to `scripts/` and left out of this is a surface with no check on it.

    The exclusion is one file and it is named in the source. Enumerating the
    directory rather than listing the scripts is the difference between a guard
    and a list somebody has to remember to extend.
    """
    every = {path.name for path in (REPO / "scripts").glob("*.py")}
    assert every - {path.name for path in PUBLISHED} == {"check_fingerprint.py"}
    assert "eval.py" in every and "measure_duckdb.py" in every
