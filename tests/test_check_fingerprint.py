"""Tests for the fingerprint checker.

fingerprint-check: disable-file

That marker is not a dodge. This file has to contain the exact tells it is
testing for, so scanning it would report every fixture as a violation.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


def _script_dir() -> Path:
    """Sibling in the factory, ../scripts once init_project.sh has split them."""
    here = Path(__file__).resolve().parent
    for candidate in (here, here.parent / "scripts"):
        if (candidate / "check_fingerprint.py").is_file():
            return candidate
    raise RuntimeError("check_fingerprint.py is neither beside this file nor in ../scripts")


SCRIPT_DIR = _script_dir()
sys.path.insert(0, str(SCRIPT_DIR))

import check_fingerprint as fp  # noqa: E402


@pytest.fixture(scope="module")
def rules() -> fp.Rules:
    return fp.parse_banned(fp.find_word_list(Path(__file__).resolve().parent))


def scan(tmp_path: Path, name: str, body: str, rules: fp.Rules) -> list[fp.Finding]:
    target = tmp_path / name
    target.write_text(body, encoding="utf-8")
    return fp.check_file(target, rules)


def kinds(findings: list[fp.Finding]) -> str:
    return " | ".join(f.kind for f in findings)


def test_banned_list_actually_loaded(rules: fp.Rules):
    assert rules.words is not None
    assert rules.phrases is not None
    assert rules.patterns, "the regex block failed to parse"
    assert "eval harness" in rules.exceptions


def test_em_dash_in_prose(tmp_path, rules):
    found = scan(tmp_path, "a.md", "One thing — then another.\n", rules)
    assert "em dash" in kinds(found)


def test_en_dash_counts_as_the_same_offence(tmp_path, rules):
    found = scan(tmp_path, "a.md", "Ran 2020–2024.\n", rules)
    assert "en dash" in kinds(found)


def test_emoji_including_the_tick_marks(tmp_path, rules):
    found = scan(tmp_path, "a.md", "Build passing ✅\n", rules)
    assert "emoji" in kinds(found)


def test_banned_word_and_its_inflections(tmp_path, rules):
    found = scan(tmp_path, "a.md", "It delves into things.\nIt is robust.\n", rules)
    assert "delves" in kinds(found)
    assert "robust" in kinds(found)


def test_banned_phrase(tmp_path, rules):
    found = scan(tmp_path, "a.md", "At its core, it parses YAML.\n", rules)
    assert "banned phrase" in kinds(found)


def test_banned_regex_pattern(tmp_path, rules):
    found = scan(tmp_path, "a.md", "It is not only fast but also cheap.\n", rules)
    assert "banned pattern" in kinds(found)


def test_clean_prose_passes(tmp_path, rules):
    body = "Parses 4.2M rows in 11s. Fails on the 300 rows with a null ISO code.\n"
    assert scan(tmp_path, "a.md", body, rules) == []


def test_code_identifier_colliding_with_the_list_is_left_alone(tmp_path, rules):
    body = "landscape = load()\nrealm = landscape.crop()\n"
    assert scan(tmp_path, "a.py", body, rules) == []


def test_comment_in_code_is_still_checked(tmp_path, rules):
    found = scan(tmp_path, "a.py", "# a comprehensive rewrite\nx = 1\n", rules)
    assert "comprehensive" in kinds(found)


def test_docstring_is_still_checked(tmp_path, rules):
    body = 'def f():\n    """Meticulous parsing."""\n    return 1\n'
    found = scan(tmp_path, "a.py", body, rules)
    assert "eticulous" in kinds(found)


def test_multiline_docstring_reports_the_offending_line(tmp_path, rules):
    body = 'def f():\n    """Line one.\n\n    A seamless design.\n    """\n    return 1\n'
    found = scan(tmp_path, "a.py", body, rules)
    assert [f.line for f in found if "seamless" in f.kind] == [4]


def test_unparseable_python_still_gets_checked(tmp_path, rules):
    found = scan(tmp_path, "a.py", "def f(:\n  # a robust idea\n", rules)
    assert "robust" in kinds(found)


def test_a_file_that_cannot_be_decoded_is_reported_rather_than_counted_as_clean(tmp_path, rules):
    """Regression: the read used to return [], which reads as "checked, clean".

    A checker that exits 0 over a file it never opened is the failure mode this
    whole repo is about, one level up from the pipelines it grades.
    """
    undecodable = tmp_path / "wrong-encoding.md"
    undecodable.write_bytes(b"\xff\xfe\x00\x01 robust and seamless\n")

    found = fp.check_file(undecodable, rules)
    assert "unreadable" in kinds(found)


def test_allowed_context_suppresses_the_hit(tmp_path, rules):
    assert scan(tmp_path, "a.md", "The eval harness reports F1.\n", rules) == []


def test_bare_harness_is_still_caught(tmp_path, rules):
    found = scan(tmp_path, "a.md", "We harness the power of embeddings.\n", rules)
    assert "harness" in kinds(found)


def test_ignore_marker_suppresses_one_line(tmp_path, rules):
    body = f"A robust thing.  <!-- {fp.IGNORE_LINE_MARKER} -->\nA seamless thing.\n"
    found = scan(tmp_path, "a.md", body, rules)
    assert [f.line for f in found] == [2]


def test_disable_marker_suppresses_the_file(tmp_path, rules):
    body = f"{fp.DISABLE_FILE_MARKER}\nA robust and seamless thing.\n"
    assert scan(tmp_path, "a.md", body, rules) == []


def test_checker_does_not_exempt_itself(rules):
    """Regression: the markers used to appear verbatim here and skip this file."""
    source = (SCRIPT_DIR / "check_fingerprint.py").read_text()
    assert fp.DISABLE_FILE_MARKER not in source
    assert fp.IGNORE_LINE_MARKER not in source


def test_finds_a_repo_local_banned_md_from_a_subdirectory(tmp_path):
    """Regression: this is what made the suite pass only on the author's machine.

    The walk used to check `start` once and then climb without re-checking each
    level. Called from `tests/`, it skipped this repo's own copy and kept going
    until it found one belonging to another project further up the disk. Green
    locally, 18 errors in every clone.

    Both places a repo can keep it, since this one moved from the root to
    `scripts/` and the walk has to find either.
    """
    for where in ("scripts", ""):
        repo = tmp_path / f"project-{where or 'root'}"
        (repo / "tests").mkdir(parents=True)
        holding = repo / where if where else repo
        holding.mkdir(exist_ok=True)
        listed = holding / "BANNED.md"
        listed.write_text("```banned-words\nrobust\n```\n", encoding="utf-8")

        assert fp.find_word_list(repo / "tests") == listed


def test_a_list_belonging_to_a_project_further_up_does_not_win(tmp_path):
    """The nearer list wins, so a stray copy on the disk cannot change a verdict."""
    (tmp_path / "BANNED.md").write_text("```banned-words\ndelve\n```\n", encoding="utf-8")
    repo = tmp_path / "someproject"
    (repo / "scripts").mkdir(parents=True)
    (repo / "tests").mkdir()
    mine = repo / "scripts" / "BANNED.md"
    mine.write_text("```banned-words\nrobust\n```\n", encoding="utf-8")

    assert fp.find_word_list(repo / "tests") == mine


def test_no_list_anywhere_is_an_error_rather_than_an_empty_rule_set(tmp_path):
    """An empty rule set would pass every file, which is worse than failing."""
    with pytest.raises(SystemExit):
        fp.find_word_list(tmp_path)

def test_banned_and_claude_md_are_skipped_by_name():
    assert not fp.interesting(Path("BANNED.md"))
    assert not fp.interesting(Path("CLAUDE.md"))
    assert fp.interesting(Path("README.md"))
