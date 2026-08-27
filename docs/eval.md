# The eval

A trial is three passes: the broken pipeline at the defaults, its matched twin at the defaults, and
the broken pipeline again with containment off. Ten trials, four baselines with the first of them
scored at two run counts, and eight failure conditions that were written into the spec before any of
this existed.

```bash
uv run python scripts/eval.py               # ten trials, the best part of ten minutes, 1.3 GB of peak disk
uv run python scripts/eval.py --trials 2    # the same shape, quicker
uv run python scripts/eval.py --json out.json
```

The eval produces every generated table in the README. `--json` writes what it measured,
`results/eval-2026-08-27.json` is the copy that file renders, and re-running it and committing the new
file is the only way any of those numbers changes.

**The eval always exits 0, including on a triggered condition.** A script that failed on one would be
a script with a reason to stop publishing it, and there is no CI job running this: the stamp above the
README's tables says how long the committed run took, and peak disk is 1.3 GB.

## Five runs that cannot be regenerated

Run F is the committed one. Five ten-trial runs came before it and they disagree with each other about
the headline number. All five are below, in the order they happened, and none of them can be
regenerated: their JSON was never written out, which is the defect slice 7 exists to fix and the
reason the table stops at E. A and B are the same code. C and D followed two changes to how a rate is
printed and nothing else, which is why they are here rather than replacing anything: a display change
is not a reason to drop a measurement, and the run that breached a pre-registered threshold is the
second column. E is the last run before the artifact existed.

Measured on 2026-08-26 on this laptop, DuckDB 1.5.5 at `threads=10`. Five observations, transcribed by
hand, and no way left to check them:

| step | run A | run B | run C | run D | run E |
|---|---|---|---|---|---|
| 0 `generate_inputs` | `0 of 4` on all 10 | on all 10 | on all 10 | on all 10 | on all 10 |
| 1 `daily_revenue` | `4 of 4` on all 10 | on all 10 | on all 10 | on all 10 | on all 10 |
| 2 `customer_keys` | 4x8 3x1 2x1 1x0 0x0 | 4x9 3x1 2x0 1x0 0x0 | 4x8 3x2 2x0 1x0 0x0 | 4x7 3x3 2x0 1x0 0x0 | 4x7 3x1 2x2 1x0 0x0 |
| 3 `apply_price_updates` | 4x5 3x1 2x2 1x2 0x0 | 4x5 3x2 2x0 1x2 **0x1** | 4x6 3x3 2x1 1x0 0x0 | 4x6 3x4 2x0 1x0 0x0 | 4x8 3x0 2x2 1x0 0x0 |
| 4 `append_audit_log` | `4 of 4` on all 10 | on all 10 | on all 10 | on all 10 | on all 10 |
| 5 `mean_basket` | `4 of 4` on all 10 | on all 10 | on all 10 | on all 10 | on all 10 |
| 6 `sparse_customer_keys` | 4x2 3x4 2x2 1x2 0x0 | 4x1 3x2 2x2 1x5 0x0 | 4x0 3x4 2x2 1x3 **0x1** | 4x3 3x2 2x2 1x2 **0x1** | 4x1 3x1 2x3 1x4 **0x1** |
| 7 `roll_up_keys` | `0 of 4` on all 10 | on all 10 | on all 10 | on all 10 | on all 10 |
| six twins, every step | nothing fired | nothing fired | nothing fired | nothing fired | nothing fired |
| wall clock | 536s | 363s | 406s | 356s | 532s |

Two of those columns need their conditions stated, and neither is in the table. Run A's wall clock is
the odd one because the laptop was running other things. Run E is 532s against B's 363 because it
gained baseline 1's run-matched row, which costs about 20 seconds a trial in comparisons and nothing
else.

**Run B breached the spec's sensitivity threshold**, which fixed 10 of 10 on the four broken steps.
`apply_price_updates` came out 9 of 10. The eval printed `TRIGGERED` next to it and exited 0, because a
triggered condition is a result and not a failed run. That is also the condition that cut slice 6.

There was also a ten-trial run in a fresh clone outside the working tree, on the slice-5 code. It is
not in the table because it is not the same code path being questioned, and calling it independent
would be overstating it: same laptop, same DuckDB pin, same pipelines, same author, independent of the
working tree and of nothing else. It gave `customer_keys` 4x6 3x4, `apply_price_updates` 4x6 3x3 2x1,
`sparse_customer_keys` 4x4 3x1 2x3 1x2, the other five steps unchanged, zero twin fires, 30 of 30 on
attribution, and every condition not triggered, in 370s.

## What the specificity denominator contains

Two things are worth naming. One of the six pairs is `generate_inputs`, which is the same function in
both files, so a sixth of it is the sensitivity table's control row counted a second time. And
`mean_basket` and `roll_up_keys` have no twin at all, so the correct-code step that fires in every
trial is not in the specificity set. The eval prints both of those under the table rather than leaving
the denominator to be read as that many independent chances to fail.

Until run E the main-loop figure was runs minus one times steps times trials rather than a count of
comparisons that happened, in the same function that filtered the amplified figure on whether anything
was compared. Both are counted now.

## The four baselines

Baseline 1's figures moved between the five runs before the artifact existed, and the direction of the
movement is the useful part: it fired on `mean_basket` in 10 of 10 trials in every one of them, at
medians of 1,265, 1,270, 1,269, 1,208 and 1,347 rows failing to pair, on a step where nothing is
wrong. The count is across both sides, out of the 2,000 the two runs put in front of it rather than the
1,000 groups. It caught the intermittent step in 7, 5, 4, 6 and 5 of 10, against the five-run loop's
10, 10, 9, 9 and 9.

**Baseline 1 was handed one comparison and the oracle four, and that is most of the gap between them.**
Until run E this section said the comparison method was the only difference, which was false in the
direction that flattered the oracle. The run-matched row is the control that was missing: same runs,
same bytes, same containment, and nothing but multiset equality over row hashes. It agrees with the
oracle on every cell of the sensitivity table and fires on no twin.

**So the sensitivity and specificity tables contain no evidence for the oracle over the cheap
comparison.** They are the wrong place to look for it. What the oracle produces that multiset equality
cannot is the divergence class, the ulp and relative magnitudes, the attributed column, the derived
reassociation bound and the single-threaded cause, and none of those five are scored in those two
tables. What baseline 1 as originally written does show is what one comparison costs against four,
which is a real result about run count and was published as a result about comparison method.

**Baseline 2 is the one worth reading twice, and it is stronger than the spec predicted.** The spec
expected a static check to catch the `INSERT`-shaped bug and miss the other three. It catches four of
the five, because the patterns were written here after the bugs were known, which is the largest thumb
anyone could put on a static checker's scale. What it still cannot do is the result:

- It flags `apply_price_updates` and `apply_price_updates_deduped` identically. Both contain `MERGE
  INTO ... ON t.sku = s.sku`; the fix is a `GROUP BY` in a different statement that makes the source
  unique on the join key. Telling them apart is data-flow analysis, not a pattern.
- It flags `mean_basket`, which is a correct average. That is the same false positive baseline 1 makes,
  and neither can be fixed by looking harder at the text, because whether a float aggregate is a bug
  depends on whether the answer was supposed to be exact.
- Its hits are a fact about the query text. Three of the five bugs are facts about the data.

The patterns run against parsed function bodies rather than the file. That is not tidiness: `grep` the
twins file for the append pattern and it hits, because `apply_price_updates_deduped` reads and writes
`prices`. A file-level check reports an append bug in the file whose entire purpose is that it has
none.

## The pre-registered conditions

Three of the eight declare a piece of this project unnecessary if they fire. All eight print on every
run whatever they say, and the README carries the table.

**Run F triggers the sensitivity condition, and it is the second run to do so.** The step is
`apply_price_updates`, the same one run B missed, and the shortfall is one trial. Nothing was changed
in response, because the threshold was pre-registered and the eval exits 0 on a triggered condition by
design. What the condition cannot distinguish is the detector missing a divergence from the fixture not
producing one, and on this step the fixture is intermittent: the `MERGE` bug needs a target between
roughly 100,000 and 125,000 rows and a source that happens to carry a repeated key, and the results
table shows it at four fires out of four in only 2 of the 10 trials. That is the reading, and it is a
reading rather than a measurement.

The other seven held in run F and in the five runs before it, with run B's sensitivity the only other
trigger.

Three of those eight can be read off an absence rather than off a measurement, and until this pass they
were. A step that writes no artifact compares nothing, fires on none of the nothing it compared, and
arrives at the conditions as a clean zero, so the naive baseline reports no false positives, the
amplification gap comes out as two zero rates and the uncontained pass removes no falsely divergent
step. All three then print TRIGGERED, which is the verdict that declares a piece of this project
unnecessary. Seven of the eight carry a third verdict now, `NOT MEASURED`, and the eighth is the wall
clock, which is measured whatever the pipeline did. The trigger in the committed run is a real 9 of 10
rather than a gap, and so was run B's.

**Two of the eight can trigger on something that is not the detector, and both were written that way in
the spec.** The amplification gap asks whether any amplifier beats the five-run loop, and a maximum
cannot beat a rate that is already at 1.00, so on a run where the loop saturates the condition fires
whatever amplification did. The eval says so in the evidence line when it happens rather than moving the
threshold. And the sensitivity condition fires when a broken step comes back 0 of 4 in one trial, which
on `apply_price_updates` has now happened in 2 of the 6 ten-trial runs taken. The distinction matters
because the two causes are indistinguishable from the condition's own output, and leaving the threshold
where the spec put it is the only way it stays worth anything.

The bound check is the one condition whose two readings disagree, and both are in the README's
drift-bound table. It clears at `n = rows read by the step` and fails at the tight `n`, the terms behind
one output value. That split has held on every sample taken: 0, 0, 1, 1 and 0 of 20 tight clearances
across the five runs before the artifact. The check as pre-registered passes and the honest reading of
it fails, so both print.

## Attribution, scored against itself

Three steps produce an attribution and ten trials give thirty chances, so a denominator below thirty is
a step that went quiet for a whole trial. The five runs before the artifact gave 30 of 30, then 29 of 29
four times.

**Two thirds of that is a sort key scored against itself.** On both `row_number` steps, dropping
`surrogate_id` and dropping `event_id` each take the unmatched count to zero, because the two columns
are a bijection whose pairing moved, so the counts choose nothing and the `(remaining, from_input,
column)` tie-break chooses. Preferring the column the step invented is right, and
`tests/test_oracle.py::test_a_tie_goes_to_the_column_the_step_invented` asserts it as a unit test;
re-scoring it ten times a run and calling the result an accuracy figure is not. The eval prints that
split per step as well as in total. `apply_price_updates` is the one where the counts do the work.

## Forcing the amplification gap into view

**The amplification gap has to be forced to be visible at all.** Amplification only touches steps the
main loop found nothing in, which is the whole cost argument for it, so on a trial where the loop
catches the intermittent step there is no amplified rate to compare against. Waiting for a quiet trial
throws most of them away, and the first row of the README's gap table is how many that was on the run
it published. So the eval points the shipped amplifiers at that one step every trial,
through the same `amplify_runs` the runner calls, and prints the four rates side by side. Forty trials
of that is an expensive way to get forty comparisons, so the table the README publishes comes from
`scripts/amplification_gap.py`, which does the same thing without the twin pass and the uncontained pass
underneath it.

## What a trial costs

**A trial is about 50 percent slower than it was, and the per-trial budget has roughly 12 percent of
headroom left.** Run E's 53 seconds a trial against B's 35 is the run-matched baseline, which costs
about 20 seconds a trial in comparisons and was worth it: it is the control that showed the sensitivity
table holds no evidence for the oracle over multiset equality. The pre-registered wall-clock condition
is a minute a trial, so what is left over is about seven seconds. A laptop doing something else while
this runs can trip that condition honestly, and run A came within 64 seconds of it, at 536s for work
that took run B 363. The condition is measuring the machine as much as the code, and it is left where
the spec put it rather than widened after the fact.

## The condition that would have made the oracle pointless

Pre-registered with the rest: **if the naive baseline's false-positive count on correct code had come
out at zero, the oracle would be more machinery than the problem needs**, and this section would say so.

It did not, and the README's baselines table has the current figure. Baseline 1 fired on `mean_basket`
in 10 of 10 trials in every one of the five runs before the artifact too, at medians of 1,265, 1,270,
1,269, 1,208 and 1,347 rows that failed to pair, on a step where nothing is wrong. The count is across
both sides, so it is out of 2,000 rather than 1,000: a group that differs loses a row each way.

The first four figures were published as `median 1,265 rows unmatched out of 1,000 on each side`, which
is a magnitude larger than the ceiling it names, on a line whose own rule is that a ceiling cannot be
beaten. The numerator summed the two sides and the denominator took one of them. The eval prints the two
sides separately now, against the total of both, and the four figures are unchanged because the quantity
was right and only its denominator was wrong.

`src/twicerun/compare.py` is that comparison, kept rather than deleted, and `scripts/eval.py` runs it
against each trial's own runs 1 and 2.

**A second and narrower version of this condition was missing until run E, and it is the one that
bites.** Baseline 1 gets one comparison where the oracle gets four, so the published gap between them
was mostly a run count. Given the same five runs, bit-exact multiset equality agrees with the oracle on
every cell of the sensitivity table and fires on no twin, which is the run-matched row of the baselines
table. The sensitivity and specificity tables are therefore not evidence for the oracle, and the eval
says so in the baseline 1 section on every run. The oracle earns its cost on what it produces beyond a
yes or no: the class, the magnitudes, the attributed column, the bound and the cause. If those turn out
not to be worth their runtime to anyone, the honest reading of that is the same as this section's.

The two-second version needs none of this repo: `scripts/measure_duckdb.py` re-derives the premise in
raw DuckDB, and it is the better check, because it is a fact about DuckDB rather than about my code.

Stating the condition matters more than the outcome. A tool whose author cannot say what would have made
it pointless has not tested the premise, and there are eight of these now, printed on every eval run
whatever they say, with a ninth in the baseline 1 section that is not a pre-registered threshold because
nobody thought to pre-register it.
