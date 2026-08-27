"""Every directory this repository writes into by default, run past .gitignore.

`.gitignore` covered `.twicerun` and neither `.twicerun-eval` nor
`.twicerun-gap`. Both of those get an `rmtree` on the way out, but only on the
success path, so a Ctrl-C part way through the eval left up to 1,292 MB of
TLC-derived Parquet sitting in `git status` as untracked, in a repository whose
whole licence position is that it commits no data bytes.

The defaults are imported from the code that writes them rather than retyped, so
renaming one and forgetting the ignore rule fails here. `git check-ignore` does
the matching, because a substring search of `.gitignore` would pass on a
commented-out line and would not know that `data/tlc` is covered by `data/`.

The question is asked about a file inside each directory rather than about the
directory. A pattern written with a trailing slash matches directories only, and
`check-ignore` cannot tell that a path which does not exist yet is one, so
`.twicerun-eval` comes back unignored while `.twicerun-eval/anything` does not.
What actually has to be unstageable is the Parquet inside.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

import amplification_gap
import eval as eval_script
import fetch_tlc
from twicerun.cli import build_parser

REPO = Path(__file__).resolve().parent.parent
GIT = shutil.which("git")


def inside_a_work_tree() -> bool:
    """A source tarball has no .git, and there is nothing to check without one."""
    if GIT is None:
        return False
    asked = subprocess.run(
        [GIT, "rev-parse", "--is-inside-work-tree"], cwd=REPO, capture_output=True
    )
    return asked.returncode == 0


WRITTEN_BY_DEFAULT = [
    build_parser().parse_args(["run", "pipelines/reference.py"]).run_dir,
    eval_script.WORKSPACE,
    amplification_gap.WORKSPACE,
    fetch_tlc.DEFAULT_DIR.relative_to(REPO),
]


@pytest.mark.skipif(not inside_a_work_tree(), reason="no git work tree to consult")
@pytest.mark.parametrize("where", WRITTEN_BY_DEFAULT, ids=str)
def test_the_default_output_directory_is_ignored(where):
    written = where / "run-01" / "rows.parquet"
    asked = subprocess.run(
        [GIT, "check-ignore", "-q", str(written)], cwd=REPO, capture_output=True
    )
    assert asked.returncode == 0, (
        f"{written} is not ignored, so an interrupted run leaves it stageable: "
        f"{asked.stderr.decode().strip()}"
    )
