"""The README's results tables, rendered from one committed copy of what the eval measured.

Six times a figure in that file was published and then beaten by a longer run.
Every correction was made in good faith and every one was retyped by hand from a
terminal into a table, which is the step where a figure and the code that
produced it come apart. So the tables are generated: `twicerun report` reads the
JSON `scripts/eval.py --json` writes, renders the blocks between the markers in
the README, and a test fails the build if the file on disk and the artifact
disagree.

What that buys is narrow and worth being exact about. It does not make a number
right, and it does not make a ten-trial sample bigger than ten trials. It makes
the README's tables a function of a file that was written by the measurement,
so the only way to change one is to run the eval again and commit what came out.

Two of the helpers here are also imported back by the eval. A fire-rate
distribution and a tally of labels have to read identically in the terminal and
in the table or the two disagree about the same run, which is the drift this
module exists to stop, one layer down.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

# What `twicerun report` will read. Bumped when a field changes meaning rather
# than when one is added, since a generator that ignores an unknown key is fine
# and one that misreads a known key is not.
ARTIFACT_VERSION = 1

OPEN = "<!-- twicerun: {name} -->"
CLOSE = "<!-- /twicerun: {name} -->"

# Two scripts write artifacts and they measure different things, so a file says
# which it is rather than being sniffed for the keys it happens to carry.
EVAL = "eval"
GAP = "amplification-gap"


class UnreadableArtifact(ValueError):
    """The JSON is not an artifact this version knows how to render."""


class MarkerError(ValueError):
    """The markdown's generated blocks do not line up with the tables on offer."""


def distribution(rates: dict[str, int], comparisons: int) -> str:
    """Every rate the step took with its count, highest first, the zeros included.

    Ordered by the rate rather than by the count, so the shape reads down a
    column and two steps compare line to line.

    A step that gave the same rate every trial collapses to `0 of 4 on all 10`,
    which omits nothing: naming the trial count accounts for every trial. Where
    the rate did move, every bucket prints, because an omitted bucket reads as
    the value being impossible when it only means unobserved. That distinction
    cost this project a published table: `4 of 4 x27, 3 x5, 2 x5, 1 x3` was
    called complete, and a later run gave the same step a flat zero.

    Rates out of a smaller denominator print after the buckets rather than being
    folded into them, since a step that wrote nothing on one round of one trial
    has a real rate out of a real denominator that is not this one.
    """
    if not rates:
        return "nothing compared"
    if len(rates) == 1:
        only, times = next(iter(rates.items()))
        return f"{only} on all {times}"
    spelled = [
        f"{k} of {comparisons} x{rates.get(f'{k} of {comparisons}', 0)}"
        for k in range(comparisons, -1, -1)
    ]
    odd = [f"{rate} x{n}" for rate, n in rates.items() if not rate.endswith(f" of {comparisons}")]
    return ", ".join(spelled + odd)


def tally(counted: dict[str, int]) -> str:
    """Labels with their counts, commonest first, and the count dropped where it is one.

    Ties break on the label rather than on insertion order. `Counter.most_common`
    breaks them on which trial happened to come first, which is fine on a screen
    and not fine in a committed table: two runs with identical counts would
    render two different files and the drift check would fail on the ordering.
    """
    if not counted:
        return "nothing seen"
    ordered = sorted(counted.items(), key=lambda pair: (-pair[1], pair[0]))
    return ", ".join(f"{label} x{n}" if n > 1 else label for label, n in ordered)


def load(paths: Iterable[Path]) -> dict[str, dict]:
    """Read the committed artifacts, keyed by what each one measures.

    Two of the same kind is refused rather than resolved by order. Handing this
    two eval runs and having the second silently win is how a table ends up
    rendered from a file nobody meant to publish.
    """
    loaded: dict[str, dict] = {}
    for where in paths:
        try:
            artifact = json.loads(where.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise UnreadableArtifact(f"{where} is not JSON: {exc}") from exc
        version = artifact.get("version")
        if version != ARTIFACT_VERSION:
            raise UnreadableArtifact(
                f"{where} says version {version!r} and this build renders version "
                f"{ARTIFACT_VERSION}. Re-run the script that wrote it to get a current one."
            )
        kind = artifact.get("kind")
        if kind not in TABLES:
            raise UnreadableArtifact(
                f"{where} says kind {kind!r} and this build renders {sorted(TABLES)}"
            )
        if kind in loaded:
            raise UnreadableArtifact(
                f"two {kind} artifacts given, {loaded[kind]['source']} and {where}"
            )
        # Relative to where the command was run, because this string is
        # rendered into a committed file. An absolute path would put whoever
        # ran it into the README and would differ between the person
        # regenerating the tables and the test checking them.
        artifact["source"] = str(_relative(where))
        loaded[kind] = artifact
    return loaded


def _relative(where: Path) -> Path:
    here = Path.cwd().resolve()
    resolved = where.resolve()
    return resolved.relative_to(here) if resolved.is_relative_to(here) else resolved


def _plural(n: int, singular: str, plural: str | None = None) -> str:
    return f"{n} {singular}" if n == 1 else f"{n} {plural or singular + 's'}"


def _code(token: str) -> str:
    return f"`{token}`"


def _codes(counted: dict[str, int], of: int) -> str:
    """Class or cause labels, with the count dropped where it held on every trial.

    `VALUE_DRIFT x10` in a table whose every row is ten trials repeats the
    header. `VALUE_DRIFT x8, SCHEMA x2` says the step changed its mind between
    trials, which is the only thing the counts are here for.
    """
    ordered = sorted(counted.items(), key=lambda pair: (-pair[1], pair[0]))
    spelled = []
    for label, n in ordered:
        marked = " ".join(_code(token) for token in label.split())
        spelled.append(marked if n == of else f"{marked} x{n}")
    return ", ".join(spelled)


def _step_cell(step: dict) -> str:
    return f"{step['index']} {_code(step['name'])}"


def _rate_cell(step: dict) -> str:
    return distribution(step["rates"], step["comparisons"])


def _thousands(value: float) -> str:
    return f"{value:,.0f}"


def magnitudes(step: dict) -> list[str]:
    """How far the step moved: the rows that found no partner, then the float drift.

    The third helper the eval imports back, for the reason it imports the other
    two. The two copies of this decided which side of an unmatched pair to print
    differently, and they disagreed on the mix that an append with no unique key
    produces once one row also goes missing. The terminal asked whether any
    trial had a reference-side count and then printed the median, which on six
    pure appends and four mixed trials is `median 0 of the 3,953 reference rows
    found no partner` with the extra-rows clause suppressed behind it. That is
    the shape a test in `test_eval.py` calls the one this project exists to stop
    publishing, and the table next door printed 3,953 extra rows off the same
    counts.

    So the question is asked about the number that will be printed. Which side
    is non-zero is a fact about one artifact and the median is a fact about ten,
    and only the second one goes in a cell. The two medians cannot both be zero:
    `StepScore.observe` records a trial only when one side of it moved, so more
    than half the trials would have to be zero on both.

    The row counts print against the hard maximum they came out of, because a
    step comparing 500,000 rows cannot lose more than 500,000 of them and a
    ceiling cannot be beaten. The ulp and relative figures are maxima over a
    thousand groups, so they print as a median with an n and no bracket at all.
    """
    parts = []
    if step["unmatched_median"]:
        parts.append(
            f"median {_thousands(step['unmatched_median'])} of the "
            f"{_thousands(step['of_rows_median'])} reference rows found no partner"
        )
    elif step["extra_median"]:
        parts.append(
            f"median {_thousands(step['extra_median'])} extra rows against "
            f"{_thousands(step['of_rows_median'])} reference rows"
        )
    if step["relative_median"] is not None:
        parts.append(
            f"median {step['ulps_median']:g} ulp and {step['relative_median']:.1e} relative, "
            f"n={step['magnitudes_n']}"
        )
    return parts


def _moved(step: dict) -> str:
    """Class, cause and how far the step moved, in the order a reader asks for them."""
    parts = []
    if step["classes"]:
        parts.append(_codes(step["classes"], step["trials"]))
    if step["causes"]:
        parts.append(f"cause {_codes(step['causes'], step['trials'])}")
    parts += magnitudes(step)
    return ", ".join(parts) or step["what"]


def _table(header: Iterable[str], rows: Iterable[Iterable[str]]) -> list[str]:
    columns = list(header)
    return [
        "| " + " | ".join(columns) + " |",
        "|" + "---|" * len(columns),
        *("| " + " | ".join(str(cell) for cell in row) + " |" for row in rows),
    ]


def _ordered(steps: list[dict]) -> list[dict]:
    last = 1 << 30
    return sorted(steps, key=lambda step: last if step["index"] is None else step["index"])


def provenance(artifact: dict) -> list[str]:
    """What every figure below is specific to, which is four things and not one.

    DuckDB pins the parallel behaviour, the thread count picks how the work gets
    divided, the platform decides both, and the trial count is what any of these
    rates is out of. A table missing one of them is not reproducible, so the
    stamp is generated with the tables rather than typed above them.
    """
    where = artifact["environment"]
    return [
        f"Generated by `twicerun report` from `{artifact['source']}`, "
        f"written {artifact['generated'][:10]} by "
        f"`uv run python scripts/eval.py --trials {artifact['trials']} --json`.",
        "",
        f"DuckDB {where['duckdb']} at `threads={where['threads']}` on {where['platform']}. "
        f"{_plural(artifact['trials'], 'trial')} of {artifact['runs']} runs, so "
        f"{_plural(artifact['runs'] - 1, 'comparison')} per step per trial and "
        f"{artifact['trials'] * (artifact['runs'] - 1)} in all. "
        f"{artifact['seconds']:,.0f}s, a median {artifact['seconds_per_trial']:,.0f}s a trial.",
    ]


def _silent_note(artifact: dict) -> list[str]:
    """Trials the rates above are not out of, which the header's trial count does not say.

    Three of the four places this eval publishes a fire rate disclose this. The
    terminal's sensitivity table prints it under each step, the terminal's
    specificity table prints a total, and the specificity table below does too.
    This one, which is the first table in the README and the one a reader meets
    first, printed `4 of 4 on all 7` under a header saying 10 trials and said
    nothing about the other three.

    Grouped by count rather than listed per step, because the usual shape is a
    trial that died before writing anything and took every step down with it,
    and eight identical clauses would bury the one step that differed.
    """
    quiet: dict[int, list[str]] = {}
    for step in _ordered(artifact["steps"]):
        if step["silent_trials"]:
            quiet.setdefault(step["silent_trials"], []).append(step["name"])
    if not quiet:
        return []
    spelled = "; ".join(
        f"{_plural(n, 'trial')} on {', '.join(_code(name) for name in names)}"
        for n, names in sorted(quiet.items(), reverse=True)
    )
    return [
        "",
        f"{spelled} compared no artifact at all. Those are not counted in the rates above, so "
        f"a cell there can be out of fewer trials than the header names.",
    ]


def results(artifact: dict) -> list[str]:
    return _table(
        [
            "step",
            f"fires under `strict`, {_plural(artifact['trials'], 'trial')}",
            "at `threads=1`",
            "under `reduction-order`",
            "what the oracle called it, and how far it moved",
        ],
        [
            [
                _step_cell(step),
                _rate_cell(step),
                distribution(step["single_threaded"], step["comparisons"])
                if step["single_threaded"]
                else "not bisected, since it never fired",
                distribution(step["reduction_order"]["rates"], step["comparisons"]),
                _moved(step),
            ]
            for step in _ordered(artifact["steps"])
        ],
    ) + _silent_note(artifact)


def specificity(artifact: dict) -> list[str]:
    counted = artifact["twin_comparisons"]
    quiet = sum(twin["silent_trials"] for twin in artifact["twins"])
    lines = _table(
        ["twin, one line of difference from its partner", "trials it fired in", "fire rate"],
        [
            [
                _code(twin["name"]),
                f"{twin['fired_in']} of {twin['trials']}",
                distribution(twin["rates"], twin["comparisons"]),
            ]
            for twin in artifact["twins"]
        ],
    )
    lines += [
        "",
        f"{counted['main_loop']} main-loop comparisons and {counted['amplified']} amplified "
        f"ones on correct code, with {counted['declined']} amplifier attempts declining rather "
        f"than passing.",
    ]
    if quiet:
        lines.append(
            f"{quiet} twin step-passes compared no artifact at all and are not counted as clean."
        )
    return lines


def twin_coverage(artifact: dict) -> list[str]:
    return _table(
        ["amplifier", "twin step-passes it ran on", "comparisons", "fired", "raised"],
        [
            [
                row["amplifier"],
                f"{row['ran_on']} of {row['step_passes']}",
                row["comparisons"],
                row["fired"],
                row["raised"],
            ]
            for row in artifact["twin_coverage"]
        ],
    )


def containment(artifact: dict) -> list[str]:
    ablation = artifact["baseline_3"]
    return _table(
        ["step", "contained", "uncontained"],
        [
            [
                _step_cell(step),
                _rate_cell(step),
                distribution(step["uncontained"]["rates"], step["uncontained"]["comparisons"]),
            ]
            for step in _ordered(artifact["steps"])
        ],
    ) + [
        "",
        f"The append with no unique key reports {ablation['overstated']:,} extra rows "
        f"uncontained against {ablation['contained_magnitude']:,} contained, which is four "
        f"reruns' worth of duplication charged to one step against what one rerun of it does.",
    ]


def drift_bound(artifact: dict) -> list[str]:
    """Observed drift against both readings of the derived bound, one column per float step.

    Both readings, always. The check the spec pre-registered is the loose one and
    the honest reading is the tight one, and on this pipeline they differ by
    exactly the multiplier the check asks for. Printing only the reading that
    clears would be the move the whole tolerance section argues against.
    """
    drifted = sorted(artifact["drift"], key=lambda row: row["index"])
    if not drifted:
        return ["No float step drifted in any trial, so the headroom check had nothing to run on."]
    return _table(
        ["", *(_code(row["step"]) for row in drifted)],
        [[label, *cells] for label, cells in _drift_rows(drifted)],
    )


def _drift_rows(drifted: list[dict]) -> list[tuple[str, list[str]]]:
    first = drifted[0]
    return [
        (
            f"median furthest relative drift, n={first['step_passes']}",
            [f"{row['observed_median']:.2g}" for row in drifted],
        ),
        (
            f"bound at `n` = {first['terms']:,.0f} rows read",
            [f"{row['bound_loose']:.3g}" for row in drifted],
        ),
        (
            f"bound at `n` = {first['tight_terms']:,.0f} terms per output row",
            [f"{row['bound_tight']:.3g}" for row in drifted],
        ),
        (
            "headroom at the loose `n`, and how often it cleared 1,000x",
            [
                f"{row['loose_ratio_median']:,.0f}x, {row['loose_cleared']} of {row['step_passes']}"
                for row in drifted
            ],
        ),
        (
            "headroom at the tight `n`, and how often it cleared 1,000x",
            [
                f"{row['tight_ratio_median']:,.0f}x, {row['tight_cleared']} of {row['step_passes']}"
                for row in drifted
            ],
        ),
    ]


def attribution(artifact: dict) -> list[str]:
    """Leave-one-out's hit rate, and how much of it the counts actually decided.

    The second number is why this is not published as an accuracy figure on its
    own. On both `row_number` steps dropping either of two columns takes the
    unmatched count to zero, so nothing in the arithmetic chooses and the
    tie-break does. Preferring the column the step invented is right and is
    asserted as a unit test; scoring that preference against itself ten times a
    run and calling the result accuracy is not.
    """
    scored = artifact["attribution"]
    seen = scored["attribution_seen"]
    if not seen:
        return ["Nothing was attributed in any trial, so there is no rate here rather than a zero."]
    return [
        f"Leave-one-out named the column the step invented, rather than one it copied in, on "
        f"{scored['attribution_right']} of {seen} attributions. "
        f"{scored['attribution_tied']} of those {seen} had two columns leaving the same count "
        f"behind, so the counts chose nothing and the tie-break chose."
    ]


def conditions(artifact: dict) -> list[str]:
    return _table(
        ["condition, as written in the spec", "verdict", "what it came out as"],
        [
            [entry["condition"], f"**{entry['verdict']}**", entry["evidence"]]
            for entry in artifact["conditions"]
        ],
    )


def _benign_cell(naive: dict) -> str:
    """Baseline 1's headline figure, or the sentence that says it has none.

    `benign_trials` is the count of trials where the step wrote something for
    the cheap comparison to look at, and every median beside it is None when
    that count is zero. A cell reading `a median 0 of the 0 rows compared
    failing to pair` is the shape three of the eight pre-registered conditions
    are written to refuse, and this table is the surface a reader meets first.
    """
    if not naive["benign_trials"]:
        return (
            "nothing. `mean_basket` wrote no artifact for it to compare in any trial, so this "
            "row is an absence of measurement rather than a baseline with no false positives"
        )
    return (
        f"fired on `mean_basket` in {naive['benign_fired']} of {naive['benign_trials']} "
        f"trials, at a median {naive['benign_median']:,.0f} of the "
        f"{naive['benign_rows_compared']:,.0f} rows compared failing to pair, on a step "
        f"where nothing is wrong. {naive['benign_reference_side']:,.0f} of those are on "
        f"the reference side and {naive['benign_later_side']:,.0f} on the later run's"
    )


def baselines(artifact: dict) -> list[str]:
    naive = artifact["baseline_1"]
    static = artifact["baseline_2"]
    ablation = artifact["baseline_3"]
    amplified = artifact["baseline_4"]
    benign = next(step for step in artifact["steps"] if step["name"] == "mean_basket")
    intermittent = next(
        step for step in artifact["steps"] if step["name"] == "sparse_customer_keys"
    )
    downstream = next(step for step in artifact["steps"] if step["name"] == "roll_up_keys")
    return _table(
        ["baseline", "what it is", "what it gave"],
        [
            [
                "1",
                "two runs, bit-exact multiset equality, no tolerance and no classes. Rescored "
                "from each trial's own runs 1 and 2, so it sees the same bytes under the same "
                "containment",
                _benign_cell(naive),
            ],
            [
                "1, run-matched",
                "the same comparison over all of the oracle's comparisons rather than one",
                f"disagreed with the oracle's fire count on "
                f"{naive['matched_disagreements']} of {naive['matched_cells']} step-trials, and "
                f"fired on {naive['matched_twin_fires']} of {naive['matched_twin_passes']} twin "
                f"step-passes",
            ],
            [
                "2",
                "four static patterns over the pipeline source, parsed per step",
                f"separated {static['pairs_separated']} of the 5 matched pairs and flagged "
                f"{static['false_positives']} step with no bug. It cannot separate the `MERGE` "
                f"from its own fix",
            ],
            [
                "3",
                "the same oracle with `--no-containment`",
                f"`roll_up_keys` fires on {ablation['falsely_divergent_trials']} of "
                f"{ablation['downstream_trials']} trials uncontained, "
                f"{downstream['fired_in']} of {downstream['trials']} contained. The append "
                f"reports {ablation['overstated']:,} extra rows uncontained against "
                f"{ablation['contained_magnitude']:,}",
            ],
            [
                "4",
                "the same measurements with the amplifiers dropped, scored through the tool's "
                "own `exit_code`",
                f"{amplified['false_negative_trials']} of {artifact['trials']} trials would "
                f"exit 0 without them. Per comparison on `sparse_customer_keys` the loop ran "
                f"{amplified['loop_rate']:.2f} against the best amplifier's "
                f"{amplified['best_amplifier_rate']:.2f}",
            ],
        ],
    ) + [
        "",
        f"The intermittent step is where the run count and the comparison method pull apart. "
        f"The five-run loop caught `sparse_customer_keys` in {intermittent['fired_in']} of "
        f"{intermittent['trials']} trials; the benign float step fired in "
        f"{benign['fired_in']} of {benign['trials']} and is correct code.",
    ]


def gap_provenance(artifact: dict) -> list[str]:
    where = artifact["environment"]
    return [
        f"Generated by `twicerun report` from `{artifact['source']}`, written "
        f"{artifact['generated'][:10]} by "
        f"`uv run python scripts/amplification_gap.py {artifact['passes']} --json`. "
        f"DuckDB {where['duckdb']} at `threads={where['threads']}` on {where['platform']}, "
        f"{_plural(artifact['passes'], 'pass', 'passes')} over `{artifact['step']}`, "
        f"{artifact['seconds']:,.0f}s.",
    ]


def gap(artifact: dict) -> list[str]:
    """The loop's rate on the intermittent step against each amplifier's, over the same passes.

    `twicerun run` cannot produce this and is not meant to. Amplification only
    touches steps the main loop found nothing in, so on most passes the step
    fires, never reaches an amplifier, and contributes nothing to the second
    half of the comparison. The script points them at it every pass instead.
    """
    return _table(
        [
            "",
            "comparisons that fired",
            "per-comparison rate",
            "passes it found nothing on",
            "passes it could not ask",
        ],
        [
            [
                row["source"],
                f"{row['fired']} of {row['comparisons']}" if row["comparisons"] else "nothing",
                f"{row['fired'] / row['comparisons']:.2f}" if row["comparisons"] else "none",
                f"{row['clean']} of {row['scored']}",
                row["unusable"],
            ]
            for row in artifact["rows"]
        ],
    ) + [
        "",
        f"The loop reported nothing on {artifact['loop_silent']} of the "
        f"{artifact['loop_looked']} passes where it compared anything, and an amplifier fired "
        f"on {artifact['amplifier_caught']} of those {artifact['loop_silent']}.",
    ]


TABLES = {
    EVAL: {
        "provenance": provenance,
        "results": results,
        "specificity": specificity,
        "baselines": baselines,
        "conditions": conditions,
        "attribution": attribution,
        "containment": containment,
        "twin-coverage": twin_coverage,
        "drift-bound": drift_bound,
    },
    GAP: {
        "gap-provenance": gap_provenance,
        "amplification-gap": gap,
    },
}


def render(loaded: dict[str, dict]) -> dict[str, str]:
    """Every block the given artifacts can produce, keyed by marker name.

    Only the kinds handed over. Rendering a table out of an artifact nobody
    passed would mean inventing one, and rewriting a file with fewer tables than
    it carries is refused a layer up rather than silently leaving one stale.
    """
    return {
        name: "\n".join(build(artifact))
        for kind, artifact in loaded.items()
        for name, build in TABLES[kind].items()
    }


def every_table() -> list[str]:
    return [name for offered in TABLES.values() for name in offered]


def blocks_in(markdown: str) -> dict[str, tuple[int, int]]:
    """Where each generated block starts and ends, by line number, half-open.

    Raises rather than skipping on a marker that opens and never closes. A
    silently ignored block is a table that stops being regenerated and starts
    being whatever it was the last time somebody edited it, which is the state
    this whole command exists to leave behind.
    """
    lines = markdown.splitlines()
    found: dict[str, tuple[int, int]] = {}
    opened: tuple[str, int] | None = None
    for n, line in enumerate(lines):
        stripped = line.strip()
        for name in every_table():
            if stripped == OPEN.format(name=name):
                if opened is not None:
                    raise MarkerError(
                        f"line {n + 1} opens {name} while {opened[0]} is still open at "
                        f"line {opened[1] + 1}"
                    )
                if name in found:
                    raise MarkerError(
                        f"{name} is opened twice, at lines {found[name][0]} and {n + 1}"
                    )
                opened = (name, n)
            elif stripped == CLOSE.format(name=name):
                if opened is None or opened[0] != name:
                    raise MarkerError(f"line {n + 1} closes {name}, which is not open")
                found[name] = (opened[1] + 1, n)
                opened = None
    if opened is not None:
        raise MarkerError(f"{opened[0]} opens at line {opened[1] + 1} and never closes")
    return found


def rewrite(markdown: str, rendered: dict[str, str]) -> str:
    """Replace the body of every generated block, refusing anything that does not line up.

    Both directions are checked, and the second one is the point. A table the
    markdown does not carry is a table that quietly stopped being published, and
    a marker with no generator behind it is a block that will never be updated
    again. `--key` and `--tolerance` have each already deleted one of this
    project's own falsifiable checks by being permissive about something
    adjacent, so this is strict in both directions and says which is missing.
    """
    found = blocks_in(markdown)
    missing = sorted(set(rendered) - set(found))
    unknown = sorted(set(found) - set(rendered))
    if missing or unknown:
        raise MarkerError(
            "the markdown and the artifact do not carry the same tables. "
            + (f"Missing from the markdown: {', '.join(missing)}. " if missing else "")
            + (f"No generator for: {', '.join(unknown)}. " if unknown else "")
            + "Add the marker pair, or delete the generator."
        )
    lines = markdown.splitlines()
    for name, (start, end) in sorted(found.items(), key=lambda pair: -pair[1][0]):
        lines[start:end] = rendered[name].splitlines()
    return "\n".join(lines) + ("\n" if markdown.endswith("\n") else "")


def drifted(markdown: str, rendered: dict[str, str]) -> list[str]:
    """Which generated blocks in `markdown` are not what the artifact produces."""
    lines = markdown.splitlines()
    return [
        name
        for name, (start, end) in sorted(blocks_in(markdown).items())
        if "\n".join(lines[start:end]) != rendered[name]
    ]
