"""How many places refuse to read a comparison of nothing as a clean result, counted.

`any([])` is False, so a step that wrote no artifact scores 0 of 4 out of four
comparisons of nothing and reads exactly like four clean ones. Six times a guard
written for one loop failed to reach its twins, and the prose describing the set
went stale each time: `test_eval.py` said four loops when five carry it,
`gate_under` said "four other loops in this file" when eight functions in that
file do, and the README's first table had lost the disclosure entirely.

This is `test_blanket_catches.py`'s argument at one remove. Ruff can require a
noqa on each blanket catch and cannot count them, so a sentence saying three
survived a fourth arriving. Nothing at all was counting these.

What it cannot do is find a loop that carries no guard, because a guard's absence
has no syntax. It catches the two failures that have actually happened here: a
guard deleted from a function that is supposed to have one, and a sentence whose
number stopped matching the code.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

# The vocabulary a refusal is written in. Every one of them asks the same
# question in the same words, which is what makes the set findable at all.
VOCABULARY = {"measured", "artifacts_compared", "artifacts", "silent_trials"}

# Every function in the eval that refuses on it, with the text of the refusal.
# `report_run_matched` is here and is the reason this list is written out rather
# than derived: it asks the question of a tuple element, so no attribute name
# appears in it and no AST walk finds it.
IN_THE_EVAL = {
    "observe": "if step.artifacts_compared == 0:",
    "report_twin_comparisons": "if a.measured",
    "twin_coverage": "usable = [a for a in offered if a.measured]",
    "report_baseline_one": "if n and n.artifacts",
    "report_run_matched": "if found is None or not found[1]:",
    "report_baseline_four": "usable = [a for a in seen if a.measured]",
    "_compared": "step.artifacts_compared > 0",
    "gate_under": "if step is None or step.artifacts_compared == 0:",
}

# The two functions that print the count the eight above refused on, which is
# the other half of the job: a dropped trial that nobody is told about is a
# denominator that quietly shrank.
DISCLOSURES = {"report_sensitivity", "report_specificity"}

# And the three that record the count without deciding anything with it. They
# touch the same words and refuse nothing, so they are named rather than counted.
PRODUCERS = {"naive_pass", "run_matched", "figures"}

# One entry per loop that compares runs, which is the count `test_eval.py`
# states. `tables.py` is a rendering surface rather than a loop and carries the
# refusal twice, once per fire-rate table, so it is checked and not counted.
LOOPS = {
    "the main loop": ("src/twicerun/measurement.py", "measured_comparisons"),
    "the single-threaded bisect": ("src/twicerun/cause.py", "measured"),
    "amplification": ("src/twicerun/amplify.py", "measured"),
    "the eval's scoring": ("scripts/eval.py", "observe"),
    "baseline 1 at both run counts": ("scripts/eval.py", "report_baseline_one"),
}
SURFACES = {
    "the results table": ("src/twicerun/tables.py", "_silent_note"),
    "the specificity table": ("src/twicerun/tables.py", "specificity"),
}

# Where a count of these is written down, and what it should say relative to the
# set it describes. `gate_under` describes the set from inside it.
CLAIMS = (
    ("scripts/eval.py", r"(\w+) other functions in this file carry the same", len(IN_THE_EVAL) - 1),
    ("tests/test_eval.py", r"There are (\w+) loops in this repository", len(LOOPS)),
    ("tests/test_eval.py", r"the ones the other (\w+) loops already carry", len(LOOPS) - 1),
)
SPELLED = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8}


def source_of(where: str, function: str) -> str:
    path = REPO / where
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == function
        ),
        None,
    )
    assert found is not None, f"{where} has no {function}"
    lines = path.read_text(encoding="utf-8").splitlines()
    return "\n".join(lines[found.lineno - 1 : found.end_lineno])


@pytest.mark.parametrize(("function", "refusal"), sorted(IN_THE_EVAL.items()))
def test_every_named_refusal_in_the_eval_is_still_written(function, refusal):
    assert refusal in source_of("scripts/eval.py", function)


@pytest.mark.parametrize("named", sorted({**LOOPS, **SURFACES}.items()))
def test_every_loop_and_every_table_asks_whether_anything_was_compared(named):
    _, (where, function) = named
    body = source_of(where, function)
    assert any(word in body for word in VOCABULARY), f"{where}:{function} asks nothing"


def test_no_function_in_the_eval_touches_the_vocabulary_without_being_accounted_for():
    """A ninth loop arriving is the failure this catches, and it has happened five times.

    It cannot catch a loop that carries no guard at all. It catches one written
    in the same words as the other eight and left out of the list, which is what
    a new loop looks like while it is being written.
    """
    tree = ast.parse((REPO / "scripts" / "eval.py").read_text(encoding="utf-8"))
    touching = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            inner.attr in VOCABULARY
            for inner in ast.walk(node)
            if isinstance(inner, ast.Attribute)
        )
    }
    assert touching - set(IN_THE_EVAL) == DISCLOSURES | PRODUCERS


def test_every_sentence_that_counts_them_agrees_with_the_code():
    for where, pattern, expected in CLAIMS:
        prose = (REPO / where).read_text(encoding="utf-8")
        written = re.findall(pattern, prose)
        assert written, f"nothing in {where} matches {pattern!r}, so the count has drifted loose"
        for word in written:
            # Lowered, because one of these sentences starts with the number.
            assert SPELLED[word.lower()] == expected, (
                f"{where} says {word}, the answer is {expected}"
            )
