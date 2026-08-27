# The run loop

Five runs on the same input, run 1 as the reference, runs 2 to N each compared against it, and
every step scored `k of m`. Three choices in that sentence are arguable, so this file argues them:
the run count, resolving every later read against run 1, and re-executing at `threads=1` to say
where to look. The last section is what a pass costs, which turned out not to be the runs.

## Why five and not two

A two-run tool cannot tell "deterministic" from "non-deterministic and lucky this time". Step 6 of
the reference pipeline is the proof. It is the same `row_number()` bug as step 2 at a lower tie
density, and its cell in the README's results table spans four of the five values a rate out of
four can take. At 1 of 4 a two-run checker reports nothing three times in four on a step that is
definitely broken, and at a flat 0 of 4 even five runs miss it outright.

How often that flat zero comes up is the quantity nobody can pin down, which is the point rather
than a gap in the measurement. Three of the first 10 passes ever taken of this step were flat zeros
and none of the next 20 were, measured on 2026-08-26, with nothing changed between them but the
sample. The gap table in the README is 40 more passes on 2026-08-27 and puts it at 5.

With `m` comparisons and a per-comparison divergence probability `p`, a step is missed with
probability `(1-p)^m`. At `p = 0.5`, going from two runs to five takes the miss rate from 50 percent
to 6.3 percent for 2.5 times the runtime. Going from five to ten takes it to 0.2 percent for twice
as much again. The first trade is obviously worth making, the second is a judgement call, so five is
the default and `--runs` moves it.

Those zero results are the argument for [amplification](amplification.md), and it has a number in
the README's gap table. More runs lower the miss rate for a given `p` and do not change `p`.
Stressing the input changes `p`, and on that step it roughly doubles it.

No practical number of runs proves determinism, which is why nothing here prints the word.
"deterministic", "stable", "reproducible" and "passed" are refused in the tool's report, in the
eval's output, and in every other script whose output the README publishes.
`tests/test_printed_words.py` reads the string literals out of all of them and `tests/test_cli.py`
runs the report and greps it.

That covered the tool and the eval and not the other four scripts, which is how
`scripts/measure_duckdb.py` came to print `count() stable` off three observations, with the README
repeating it as an invariant. Holding the tool to a rule the scripts beside it are exempt from is
not a rule, so the guard was widened rather than the claim narrowed.

## Containment

Runs 2 to N resolve every read against run 1's artifacts. A step that reads a diverging step's
output therefore reads the same bytes every time, so it reports what it did rather than what the
step above it did.

The idea is [Spot's](https://academic.oup.com/gigascience/article/9/12/giaa106/5998300) (Salari,
Kiar, Lewis, Evans and Glatard, GigaScience 9(12), [arXiv:2006.04684](https://arxiv.org/abs/2006.04684)),
which compares two conditions of a neuroimaging pipeline "in a step-by-step execution that prevents
the propagation of differences in the pipeline", and does it by copying the first condition's output
files into the second. The borrowing is the idea and not the implementation: Spot's tool used
ReproZip syscall interception, has not been touched since 2020, and works on a domain unrelated to
this one. Here artifacts are already addressed by `(run, step index, name)`, so containment is a
dictionary lookup rather than a file copy, and it costs nothing measurable.

`--no-containment` runs the ablation, so the numbers below are something you can reproduce rather
than a claim to take on trust.

On the reference pipeline the step it changes most is `append_audit_log`, the append with no unique
key. Two steps read their own last output through `ctx.state`, and this is the one where the
difference is a clean number: `apply_price_updates` merges over its previous catalogue and the
divergence is mostly overwritten each run, while an append keeps every copy. Extra rows reported per
comparison, against a 3,953-row reference:

| | comparison 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| `--no-containment` | 3,953 | 7,906 | 11,859 | 15,812 |
| contained | 3,953 | 3,953 | 3,953 | 3,953 |

Measured on 2026-08-26 over 10 uncontained passes and 20 contained ones, and both rows held on every
one of them. That table is not regenerated, because the eval keeps the loudest comparison per step
rather than all four, so the progression is not in the artifact. The uncontained row is four reruns'
worth of duplication charged to one step: run 4 appends to run 3's log, which already had run 2's in
it. The contained row is what one rerun of that step actually does, and the eval does carry that
pair.

Running the ablation shows you the loudest of those four figures, not all four: the report prints
one comparison per step and 15,812 is the one it picks. The other three are in the manifest, and
this prints the row counts they come from:

```bash
uv run twicerun run pipelines/reference.py --no-containment
uv run python -c "
import json, sys
runs = json.load(open(sys.argv[1]))['runs']
rows = [s['artifacts'][0]['rows'] for r in runs for s in r['steps'] if s['name'] == 'append_audit_log']
assert rows, 'that manifest has no append_audit_log: it is a run of some other pipeline'
print(rows)" .twicerun/run-*/manifest.json
```

The assert is there because the obvious way to get this wrong is to run it after the twins.  Without
it the comprehension found nothing, printed `[]`, and exited 0.

Uncontained that gives `[3953, 7906, 11859, 15812, 19765]`, one run per entry, and contained it
gives `[3953, 7906, 7906, 7906, 7906]`. The bug is the same bug either way and the fire rate is 4 of
4 either way, so what containment bought here is the magnitude being a fact about the step rather
than about how many times the tool ran.

The second number is the count of steps reported divergent, and getting it took a change to the
reference pipeline that is worth being explicit about. Every step in that file read the control
step's artifacts, which never differ, so nothing in it ever read a diverging artifact and the
cascade could not happen. The ablation scored zero on the quantity baseline 3 was pre-registered to
use, which reads as evidence against a feature that was simply never exercised.

So `roll_up_keys` exists. It reads `customer_keys`, whose surrogate ids shuffle between runs, and
computes an integer minimum that cannot reassociate, so a fire rate above zero there means it was
fed something different and never that it computed something different. It is constructed to
exercise containment and is not a failure anyone here has been bitten by, unlike three of the four
bugs beside it. The README's containment table is every step of the committed run with the ablation
beside it, and only that bottom row is what containment is for. Read the others as noise rather than
as evidence either way: two steps in this pipeline are intermittent, so a contained pass and an
uncontained pass differ mostly by which of them happened to fire. Counting
steps-divergent-per-pass and subtracting was the first way this was measured and it was exactly that
noise, which is why the quantity has to be per step. In the committed run that step is `0 of 4` on
all ten contained trials and fires on all ten uncontained ones, seven of them at four out of four.
Every earlier ten-trial run agreed with that, and none of their JSON was written, so the generated
table is the only version of it anyone can check.

That one step is the whole difference. It is a small number and it is the honest one: on a pipeline
where nothing reads a diverging artifact, containment removes no false step at all, and this
pipeline had to be given a step that does.

There is a test that measures the same thing without the reference pipeline. On a three-step fixture
where two steps do nothing but copy a wobbling step's output, the report goes from one finding to
three with `--no-containment`.

## The cause axis

Class says what went wrong. Cause says where to look. Every step that fired is re-executed at
`threads=1`, five times, and the report prints both rates:

```
  1 daily_revenue         PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1
  4 append_audit_log      PERSISTS_SINGLE_THREADED  4 of 4 at threads=10, 4 of 4 at threads=1
```

Same denominator on both sides, which is the only reason the two halves of that sentence can be read
against each other. The bisect runs at the same N as the main loop for exactly that reason.

`PERSISTS_SINGLE_THREADED` is where the tool stops. It says the thread count is not the explanation
and does not guess between a clock read, a data-dependent branch, appended state and something
outside the pipeline.

A zero at `threads=1` is not proof of anything, and the report says so on the line above the rates.
Four clean comparisons put a 95 percent one-sided upper bound of 53 percent on the per-comparison
rate, which is the same arithmetic that argues for five runs rather than two, applied to the
bisect's own evidence. It also assumes the comparisons are independent, and they are not: they share
a process, a page cache and a machine. So `PARALLEL_ORDER` is a reading of two measured rates, not a
finding that single-threaded execution cannot diverge, and a test asserts that no shape of report
contains the words deterministic, stable, reproducible or passed. `STABLE_ON_THIS_INPUT` has one of
those words inside it, so the test strips the token out of the text first and what is left still has
to be clean, and a second test requires the token to be printed with the paragraph saying what it
does not mean.

The bisect starts each single-threaded sequence from no carried state, exactly as run 1 of the main
loop did, and its later runs carry run 1's artifacts with anything the bisect itself re-produced
laid over the top. Both halves of that were wrong once and each cost the same mislabelling.

Seeding the sequence from run 1's state was the first implementation: all five single-threaded
executions of `append_audit_log` then read the same log, agreed with each other, and the step came
out `PARALLEL_ORDER`. Duplicating a log on rerun has nothing to do with threads. Carrying only what
the bisect re-produced was the second: a step carrying state under a name a *non-divergent* step
wrote found nothing there, because the bisect skips steps that did not fire, so all five runs fell
back to the seed and agreed for the same empty reason. The regression test for the first shape could
not catch the second, because its state name and its write name belong to one step.

The bisect resolves its reads against run 1 even under `--no-containment`. A single step cannot be
re-executed on its own without something to read, so the ablation ablates the main loop and not the
bisect. A step that only inherited a divergence can therefore come out `PARALLEL_ORDER` in an
uncontained report, and the report says so where it happens.

## What a pass costs

Measured on 2026-08-26, because the spec estimated it by counting step executions and the estimate
was three times out. None of the figures in this section is regenerated: the timings come from an
instrumented script that is not committed, and wall clock on a laptop is not a quantity a committed
artifact would make honest anyway.

A pass over the reference pipeline is five executions of eight steps, plus five single-threaded
executions of each step that fired, plus three to five executions of each step that did not, once
per amplifier that can touch it. Two real passes: 40 + 30 + 12 = 82 in the pass these timings come
from, and 40 + 25 + 27 = 92 in the transcript at the top of the README. Against 8 for running the
pipeline once that is 10x to 12x, and if nothing fires at all the arithmetic reaches 150. The spec
estimated 5x to 15x by counting executions, so the count lands at the top of its range rather than
in the middle.

That figure said 65 to 95 here for four revisions, and 65 counts amplification as zero, which it
cannot be: a step that never fires is exactly the step an amplifier is for.

The wall clock is a different story. Over 20 passes, one pass took a **median 25 times as long as a single
execution of the same pipeline**, the extremes being 21x and 43x. The denominator is run 1's own
recorded time out of the manifest, because the tool cannot produce it: `--runs 1` exits 2, since one
run has nothing to compare against. Where that goes, at the median:

| | share of a pass | in units of one plain execution |
|---|---|---|
| the five runs | 19% | 5.0x |
| the single-threaded bisect | 15% | 4.0x |
| comparing the artifacts | 65% | 16.6x |

**Two thirds of the cost is the oracle, not the re-execution.** Joining two 500,000-row artifacts on
a composite key, four times per step, costs more than running the pipeline that produced them.
Anyone reasoning about this tool's cost from the number of runs it does will be wrong in the same
direction the spec was.

The spread is wide because that two thirds is IO-bound, and it moves with what else the machine is
doing rather than with anything in the code. Two sessions of the same measurement gave medians of
25x and 37x on this laptop, and someone else's 28 passes gave 32.5x with the same 19/15/65 split
inside it. Treat the multiplier as an order of magnitude, not a figure.

The bisect is the 15% row: 4.0x one execution. Containment added nothing measurable, since it
changes which file a read opens and not how much work is done.

### What amplification added

Measured the same way, 12 fresh passes each, alternating so that whatever else the machine was doing
lands on both:

| | multiple of one plain execution | slowest of the 12 | seconds | Parquet written |
|---|---|---|---|---|
| `--no-amplify` | median 28.0x | 36.6x | 6.7s | 272 MB |
| with the amplifiers | median 37.2x | 45.0x | 9.0s | 376 MB |

Medians over 12 passes each. The third column is the slowest pass that happened rather than a
ceiling, and a thirteenth pass can exceed it: this is wall clock on a laptop with other things
running, so it has no bound at all. **Amplification cost 1.33 times the pass**, which is a quarter
of the amplified pass and more than the single-threaded bisect's 15 percent.

That ordering is not what the execution counts predict, and the reason is worth having. The bisect
re-executed six steps five times each, 30 executions. Amplification had two steps to work with, one
of which declines two of the three amplifiers because it reads no artifact, so 3 executions there
and 9 on the other: 12 against the bisect's 30. It still cost more, because which steps land in which
loop is decided by the fire rate and not by what they cost, and on this pipeline the one expensive
step is `generate_inputs`, which writes 205 MB, never fires, and therefore always lands in
amplification.

The shape of the cost is the same surprise as the pass as a whole. Executing the amplified steps is
a **median 6 percent of the amplified pass**, so most of the quarter is materialising the
substituted inputs and then comparing the artifacts that come out of them. The oracle again.

The two loops do partition the work. Amplification only touches steps that found nothing and the
bisect only touches steps that did, so between them every step is re-executed once more and neither
covers a step twice.

So this is a thing you run deliberately, before a release or on a schedule. `--runs` is the lever
that moves it most, and it moves the miss rate with it. `--no-amplify` is the second lever and it is
the one that costs the most evidence for what it saves.

## What it leaves on disk

One pass writes a median 376 MB of Parquet under `.twicerun/`, which is gitignored, over 12 passes:
272 MB for the five runs and the single-threaded bisect, and 104 MB more for the amplified inputs
and the runs over them. The README said 260 MB before that sample existed, which was the
decomposition rather than a measurement. `--no-amplify` takes it back to 272 MB. Retention keeps one
directory **per concurrent invocation**, so run it serially and the footprint stays there however
many times you run it. `--keep 0` turns pruning off, and `rm -rf .twicerun` reclaims the lot.

Two things follow from how retention works, both deliberate and neither obvious. Pruning happens at
the end of a successful run, not the start, so a run that fails cannot delete the run you would have
judged instead, and peak disk during a pass is one directory more than `--keep` says. And a run
still writing is never a deletion candidate, because deleting it would pull the Parquet out from
under another process.

That second one is a peak, not an end state, and the README had it wrong until someone ran it. Three
parallel invocations do hold three directories at once, and each report says so in its header, which
is the honest thing for a per-invocation view to say. But the last one to finish finds no live
markers left, prunes the other two, and the count comes back to `--keep` by itself. So the header's
line is true when it prints and stops being true a few seconds later, and the disk does not stay at
three times a pass.

## Two checks that looked like they passed

If you pipe the suite into anything, check `PIPESTATUS` or redirect instead. `uv run pytest | tail
-5` reports the exit code of `tail`, which is 0 whatever pytest did, and twice during this build a
slice was committed against a suite whose failure had been swallowed exactly that way. `uv run
pytest >/dev/null 2>&1; echo $?` is what the pre-commit hook and CI effectively do.

The same shape caught someone checking the policy-separation claim by hand. `zsh` does not word-split
an unquoted parameter expansion, so `FLAGS="--policy reduction-order"; twicerun judge "$RD" $FLAGS`
passes one argument spelled `--policy reduction-order` in `bash` and something else in `zsh`. Their
three reports had all run under the default policy and were identical for that reason rather than for
the interesting one. Both failures look like a passing check, which is the only thing they have in
common and the reason they are written down together.
