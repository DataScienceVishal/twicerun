# twicerun

[![ci](https://github.com/DataScienceVishal/twicerun/actions/workflows/ci.yml/badge.svg)](https://github.com/DataScienceVishal/twicerun/actions/workflows/ci.yml)

Run a batch pipeline several times on the same input and report, per step, how often it failed to give the same answer.

The premise is one measurement, and it takes two seconds to check. `SELECT g, sum(v) FROM t GROUP BY g` over 2,000,000 rows in 1,000 groups, run twice at `threads=8` on DuckDB 1.5.5, disagrees on several hundred of the 1,000 groups. Ten runs on this machine gave 492, 497, 510, 575, 599, 611, 618, 690, 700 and 738. Nothing is wrong with the query. Parallel reduction adds the terms in whatever order the threads finish in, and float addition is not associative. At `threads=1` it disagrees on none of them, and `count()` over the same table never moves, which is the control: integer arithmetic cannot reassociate into a different answer.

So the naive version of this tool, run it twice and diff, reports hundreds of findings on correct code. The comparison is the project. The runner is forty lines.

```
$ uv run twicerun run pipelines/reference.py
pipeline   pipelines/reference.py
runs       5, run 1 is the reference, so 4 comparisons per step
contained  on, so runs 2 to 5 read run 1's artifacts and a divergence at one step cannot reach the next
policy     strict, so any difference at all is a divergence
duckdb     1.5.5, threads=10
platform   macOS-26.5.2-arm64-arm-64bit
artifacts  .twicerun/run-20260826-170951
retention  keeping 1 run directory

  0 generate_inputs         0 of 4  NO_DIVERGENCE_OBSERVED
  1 daily_revenue           4 of 4  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER
      daily_revenue: 754 of 1,000 paired rows moved on revenue
      furthest move over 4 comparisons: revenue, 5 ulp and 5.84e-16 relative
      498492.76419699733 against 498492.76419699704
  2 customer_keys           4 of 4  DIVERGENT  ROW_MISSING ROW_EXTRA  cause PARALLEL_ORDER
      customer_keys: 398,160 of 500,000 reference rows and 398,160 later rows found no partner
      dropping surrogate_id from the key takes unmatched reference rows from 398,160 to 0
      event_id does the same, so surrogate_id is named first because it is the one no input to this step carries
  3 apply_price_updates     2 of 4  DIVERGENT  ROW_MISSING ROW_EXTRA  cause PARALLEL_ORDER
      prices: 25,856 of 125,000 reference rows and 25,856 later rows found no partner
      dropping price_cents from the key takes unmatched reference rows from 25,856 to 0
  4 append_audit_log        4 of 4  DIVERGENT  MULTIPLICITY  cause PERSISTS_SINGLE_THREADED
      audit_log: 3,953 later rows found no partner, against 3,953 reference rows
  5 mean_basket             4 of 4  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER
      mean_basket: 618 of 1,000 paired rows moved on mean_amount
      furthest move over 4 comparisons: mean_amount, 4 ulp and 4.63e-16 relative
      245.65407878963194 against 245.65407878963205
  6 sparse_customer_keys    0 of 4  STABLE_ON_THIS_INPUT
  7 roll_up_keys            0 of 4  NO_DIVERGENCE_OBSERVED

cause, from re-executing each divergent step 5 times at threads=1:
  1 daily_revenue        PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1
  2 customer_keys        PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1
  3 apply_price_updates  PARALLEL_ORDER            2 of 4 at threads=10, 0 of 4 at threads=1
  4 append_audit_log     PERSISTS_SINGLE_THREADED  4 of 4 at threads=10, 4 of 4 at threads=1
  5 mean_basket          PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1

  Both rates are out of 4, which is what lets them be read against each other.
  PARALLEL_ORDER means the step stopped diverging with one thread. That is 4 clean comparisons and no
  more than that: the 95 percent one-sided upper bound it leaves on the per-comparison rate is
  53 percent. The bound also assumes an independence these runs do not have, since they
  share a process, a page cache and a machine.
  PERSISTS_SINGLE_THREADED means the thread count is not the explanation. The tool stops there rather
  than guessing between a clock read, a data-dependent branch, appended state and something
  outside the pipeline.

amplification, 3 runs per amplifier on the 3 steps that never fired:
  0 generate_inputs
      tie collapse        not run: this step reads no artifact, so there is no input to substitute
      thread count        0 of 2, threads raised to 20 from the 10 in the loop above
      row multiplication  not run: this step reads no artifact, so there is no input to substitute
  6 sparse_customer_keys
      tie collapse        4 of 4, sparse_customers.cust into 1,000 buckets, from 2 to 500 rows per value
      thread count        3 of 4, threads raised to 20 from the 10 in the loop above
      row multiplication  4 of 4, sparse_customers 500,000 rows to 1,000,000
  7 roll_up_keys
      tie collapse        0 of 2, customer_keys.surrogate_id into 1,000 buckets, from 1 to 500 rows per value
      thread count        0 of 2, threads raised to 20 from the 10 in the loop above
      row multiplication  0 of 2, customer_keys 500,000 rows to 1,000,000

  Tie collapse leaves each artifact's most distinct column alone, so a correct tiebreak survives
  it, and every value it substitutes is another real value from the same column. Anything that
  fires here is re-run at 5 runs so its rate can be read against the one above it.

STABLE_ON_THIS_INPUT on 6 sparse_customer_keys. Each of those gave the same answer on 4
comparisons against the input this pipeline was handed, and stopped giving the same answer once
that input was stressed:
  6 sparse_customer_keys: 4 of 4 under tie collapse, 3 of 4 under thread count, 4 of 4 under row multiplication
  That is the case a plain 5-run loop reports as a clean zero, which is the case this tool exists
  for. It is not a milder DIVERGENT, and it is not this tool saying the step is fine. It says the
  step did not fire under these particular stresses, the ones named above, and nothing beyond
  that.

NO_DIVERGENCE_OBSERVED on 0 generate_inputs, 7 roll_up_keys. A fire rate and two lists, not a
verdict about the code. 4 clean comparisons rule out a per-comparison divergence probability above
53 percent, and 2 rule out one above 78 percent, 95 percent one-sided.
  0 generate_inputs  varied: run repetition 0 of 4, thread count 0 of 2. Not varied: tie collapse,
                     row multiplication, see the table above
  7 roll_up_keys     varied: run repetition 0 of 4, tie collapse 0 of 2, thread count 0 of 2, row
                     multiplication 0 of 2

reassociation bound, computed rather than picked:
  1,000x of headroom was fixed before any of this was written and has not moved since.
  Below is that one check at two choices of n. The first is the count the spec settled on and carries a
  factor of the output row count in slack. The second is the terms behind one output value and has none,
  so it is normally the one that fails. On this run it cleared on 0 of 2 float steps. Both print
  so the slack is visible rather than described.
  1 daily_revenue
      observed furthest relative drift 5.8384e-16
      n = 2,000,000 rows read by the step, bound 4.4409e-10, headroom 760,640x  CLEARS
      n = 2,000 terms per output row, bound 4.4409e-13, headroom 761x  FAILS
  5 mean_basket
      observed furthest relative drift 4.6279e-16
      n = 2,000,000 rows read by the step, bound 4.4409e-10, headroom 959,586x  CLEARS
      n = 2,000 terms per output row, bound 4.4409e-13, headroom 960x  FAILS

5 of 8 steps diverged in 13.4s.

Nothing above varied any of these, whatever status it carries: input distribution beyond what the
amplifiers above changed, memory limit and spill behaviour, DuckDB version, wall clock,
filesystem. Every bound above assumes an independence these runs do not have, since they share a
process, a page cache and a machine.
```

Step 5 is a correct float average. All 618 of those findings are the arithmetic behaving normally, and `--policy reduction-order` is the opt-in that says so.

Step 4 is the one to read twice. `cause PERSISTS_SINGLE_THREADED` next to `4 of 4 at threads=1` is the tool saying the thread count is not the problem, on the one bug in the file that a rerun causes rather than parallelism.

Step 6 is the one to read three times. It is broken, it gave the same answer four times out of four on the input it was handed, and a five-run loop that stopped there would have printed a clean zero. `STABLE_ON_THIS_INPUT` is what the tool prints instead, and the amplification block above it names the stress that made it disagree with itself. Step 3 does the same thing on other passes; on this one the loop caught it at 2 of 4.

**Your numbers will not match that transcript, and neither will mine on the next run.** This is a tool about non-determinism and its own output is non-deterministic, so quoting any single figure as fixed would be the wrong thing to do twice over.

So every results table below is generated. `scripts/eval.py --json` and `scripts/amplification_gap.py --json` each write a file of figures, both are committed under `results/`, and `twicerun report --format md --update README.md` renders the tables out of them. Nothing in those tables was typed by hand, and `tests/test_readme_tables.py` fails the build when this file stops matching the artifacts.

The reason is the section at the end of this file. Six figures published here have been beaten by a longer run, every correction was made in good faith, and every one of the six was retyped out of a terminal into a table. Retyping is the failure mode, so the structural answer is that a number nobody retypes cannot drift.

<!-- twicerun: provenance -->
Generated by `twicerun report` from `results/eval-2026-08-27.json`, written 2026-08-27 by `uv run python scripts/eval.py --trials 10 --json`.

DuckDB 1.5.5 at `threads=10` on macOS-26.5.2-arm64-arm-64bit. 10 trials of 5 runs, so 4 comparisons per step per trial and 40 in all. 502s, a median 49s a trial.
<!-- /twicerun: provenance -->

That is ten trials where the previous version of this table published forty passes, and the smaller sample is the price. What the artifact buys instead is that the table is a function of a measurement rather than of a transcription, and that running the eval again and committing what came out is the only way to change a cell. Forty passes of a figure nobody can regenerate turned out to be worth less than ten that anyone can.

**The ulp counts and the relative drift are maxima**, over 1,000 groups times four comparisons. They are extreme-value statistics: their observed range grows with how long you look, by construction, so any bracket on them can be beaten by looking longer. A bracket on a maximum is not a summary, it is a promise, and it is the one promise this file has already broken four times. So the maxima carry a median and a sample size and nothing else. The quantities with a real ceiling carry the ceiling, because a hard bound cannot be beaten: a step comparing 500,000 rows cannot lose more than 500,000 of them, and a fire rate out of four has five possible values, so the whole distribution fits in the cell.

<!-- twicerun: results -->
| step | fires under `strict`, 10 trials | at `threads=1` | under `reduction-order` | what the oracle called it, and how far it moved |
|---|---|---|---|---|
| 0 `generate_inputs` | 0 of 4 on all 10 | not bisected, since it never fired | 0 of 4 on all 10 | the control, which must never fire |
| 1 `daily_revenue` | 4 of 4 on all 10 | 0 of 4 on all 10 | 0 of 4 on all 10 | `VALUE_DRIFT`, cause `PARALLEL_ORDER`, median 4 ulp and 4.7e-16 relative, n=10 |
| 2 `customer_keys` | 4 of 4 x7, 3 of 4 x2, 2 of 4 x1, 1 of 4 x0, 0 of 4 x0 | 0 of 4 on all 10 | 4 of 4 x7, 3 of 4 x2, 2 of 4 x1, 1 of 4 x0, 0 of 4 x0 | `ROW_MISSING` `ROW_EXTRA`, cause `PARALLEL_ORDER`, median 444,840 of the 500,000 reference rows found no partner |
| 3 `apply_price_updates` | 4 of 4 x2, 3 of 4 x1, 2 of 4 x4, 1 of 4 x2, 0 of 4 x1 | 0 of 4 on all 9 | 4 of 4 x2, 3 of 4 x1, 2 of 4 x4, 1 of 4 x2, 0 of 4 x1 | `ROW_MISSING` `ROW_EXTRA` x9, cause `PARALLEL_ORDER` x9, median 27,616 of the 125,000 reference rows found no partner |
| 4 `append_audit_log` | 4 of 4 on all 10 | 4 of 4 on all 10 | 4 of 4 on all 10 | `MULTIPLICITY`, cause `PERSISTS_SINGLE_THREADED`, median 3,953 extra rows against 3,953 reference rows |
| 5 `mean_basket` | 4 of 4 on all 10 | 0 of 4 on all 10 | 0 of 4 on all 10 | `VALUE_DRIFT`, cause `PARALLEL_ORDER`, median 4.5 ulp and 5.2e-16 relative, n=10 |
| 6 `sparse_customer_keys` | 4 of 4 x4, 3 of 4 x2, 2 of 4 x2, 1 of 4 x0, 0 of 4 x2 | 0 of 4 on all 8 | 4 of 4 x4, 3 of 4 x2, 2 of 4 x2, 1 of 4 x0, 0 of 4 x2 | `ROW_MISSING` `ROW_EXTRA` x8, cause `PARALLEL_ORDER` x8, median 245,760 of the 500,000 reference rows found no partner |
| 7 `roll_up_keys` | 0 of 4 on all 10 | not bisected, since it never fired | 0 of 4 on all 10 | no bug of its own, and downstream of bug 2 |
<!-- /twicerun: results -->

A cell in that second column is a distribution, so `4 of 4 x7, 3 of 4 x1, 2 of 4 x2, 1 of 4 x0, 0 of 4 x0` would read as seven of the ten trials at four fires out of four, one at three, two at two, and none at one or at nothing. **The buckets that came out zero print rather than being left out**, which they were not when this table first appeared, and slice 5 is where that cost something: a later ten-trial run gave `apply_price_updates` the `0 of 4` its cell had never mentioned. An omitted bucket reads as the value being impossible when it only means unobserved. A step whose rate never varied collapses to `0 of 4 on all 10` instead, which accounts for every trial and leaves nothing unstated either.

The medians in the last column will move, and the ulp medians move by a whole ulp on a sample this size, which is why they carry an n.

The two float steps go to zero under `reduction-order` and nothing else moves. That is the whole claim for the oracle: the false positives disappear and the four real bugs are caught by the same code that dismissed them. `customer_keys` and the rest keep their rate because a missing row is not drift, and the policy blocks the downgrade outright on any class but `VALUE_DRIFT`.

The `threads=1` column is what slice 3 added, and one row of it is not like the others. Six steps stop diverging with one thread and one does not, which is the difference between a step whose answer depends on how the work was divided and a step whose answer depends on it having run before. No number of runs separates those two; a second thread count does it in one column. A step that never fired is not bisected at all, because there is nothing to explain, and the cell says so rather than showing a zero that would be four comparisons of nothing.

That table is the main loop, and the amplifiers do not change a rate in it. What they add is the status column, which is two sections down.

## Status

Slice 7 of 7, and the last one. What runs: the storage interface, the run layout, the artifact manifest, the five-run loop, the typed oracle, leave-one-out attribution, the policy layer, containment, the single-threaded bisect, the three input amplifiers, the four statuses, the NYC TLC backfill, the eval, and the report generator that keeps this file's tables tied to a committed measurement.

**Slice 6, crash injection, was cut**, by the contingency the spec wrote for it. It was pre-declared droppable on one condition: that slice 5's numbers miss their thresholds. Run B of the eval put `apply_price_updates` at 9 of 10 against a pre-registered 10 of 10, the condition fired, and the two days went to the detector and to this. The committed run puts the same step short of the threshold again, which is the one TRIGGERED in the conditions table below. Cutting a slice because a threshold written before the code said to is the only reason worth cutting one for, and the spec's own argument for crash injection was already that it was the weakest part of the project: 16 kill points across two write styles produced exactly one divergence, and that one was the `INSERT` bug that `grep` finds.

So there is no `twicerun crash`, the two negative results the spec wanted re-measured at N were not re-measured, and the step-boundary schedule stays what it was in the spec: an argument for the shape of the project rather than a code path.

Slice 2 shipped with a disclosure in the header of every report that downgraded anything, because `reduction-order` is a conjunction of three conditions and only two of them existed:

```
policy     reduction-order, so drift inside the reassociation bound is TOLERATED
           condition 2 of 3, that the step does not diverge at threads=1, is not implemented until slice 3. Every TOLERATED below rests on the other two
```

That line is gone, because the condition it stood in for is now measured and enforced. What a `TOLERATED` claims changed with it. It used to mean the drift was float-only and small enough that reassociation could account for it. It now means the drift also disappeared when the parallelism did, which is the difference between "small enough to be reassociation" and "demonstrably is reassociation". A step drifting inside the bound that keeps drifting at `threads=1` is now reported, with that rate as the reason.

## The eval

A trial is three passes: the broken pipeline at the defaults, its matched twin at the defaults, and the broken pipeline again with containment off. Ten trials, four baselines with the first of them scored at two run counts, and eight failure conditions that were written into the spec before any of this existed.

```bash
uv run python scripts/eval.py               # ten trials, the best part of ten minutes, 1.3 GB of peak disk
uv run python scripts/eval.py --trials 2    # the same shape, quicker
uv run python scripts/eval.py --json out.json
```

The eval is what produces every generated table above and below. `--json` writes what it measured, `results/eval-2026-08-27.json` is the copy this file renders, and re-running it and committing the new file is the only way any of those numbers changes.

**Run F is the committed one. Five ten-trial runs came before it and they disagree with each other about the headline number.** All five are below, in the order they happened, and none of them can be regenerated: their JSON was never written out, which is the defect slice 7 exists to fix and the reason the table stops at E. A and B are the same code. C and D followed two changes to how a rate is printed and nothing else, which is why they are here rather than replacing anything: a display change is not a reason to drop a measurement, and the run that breached a pre-registered threshold is the second column. E is the last run before the artifact existed.

Measured on 2026-08-26 on this laptop, DuckDB 1.5.5 at `threads=10`. Five observations, transcribed by hand, and no way left to check them:

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

Two of those columns need their conditions stated, and neither is in the table. Run A's wall clock is the odd one because the laptop was running other things. Run E is 532s against B's 363 because it gained baseline 1's run-matched row, which costs about 20 seconds a trial in comparisons and nothing else.

**Run B breached the spec's sensitivity threshold**, which fixed 10 of 10 on the four broken steps. `apply_price_updates` came out 9 of 10. The eval printed `TRIGGERED` next to it and exited 0, because a triggered condition is a result and not a failed run. That is also the condition that cut slice 6.

There was also a ten-trial run in a fresh clone outside the working tree, on the slice-5 code. It is not in the table because it is not the same code path being questioned, and calling it independent would be overstating it: same laptop, same DuckDB pin, same pipelines, same author, independent of the working tree and of nothing else. It gave `customer_keys` 4x6 3x4, `apply_price_updates` 4x6 3x3 2x1, `sparse_customer_keys` 4x4 3x1 2x3 1x2, the other five steps unchanged, zero twin fires, 30 of 30 on attribution, and every condition not triggered, in 370s.

The twins are the specificity half, and this is run F:

<!-- twicerun: specificity -->
| twin, one line of difference from its partner | trials it fired in | fire rate |
|---|---|---|
| `generate_inputs` | 0 of 10 | 0 of 4 on all 10 |
| `daily_revenue_decimal` | 0 of 10 | 0 of 4 on all 10 |
| `customer_keys_tiebreak` | 0 of 10 | 0 of 4 on all 10 |
| `apply_price_updates_deduped` | 0 of 10 | 0 of 4 on all 10 |
| `replace_audit_log` | 0 of 10 | 0 of 4 on all 10 |
| `sparse_customer_keys_tiebreak` | 0 of 10 | 0 of 4 on all 10 |

240 main-loop comparisons and 260 amplified ones on correct code, with 50 amplifier attempts declining rather than passing.
<!-- /twicerun: specificity -->

Two things that denominator contains are worth naming. One of the six pairs is `generate_inputs`, which is the same function in both files, so a sixth of it is the sensitivity table's control row counted a second time. And `mean_basket` and `roll_up_keys` have no twin at all, so the correct-code step that fires in every trial is not in the specificity set. The eval prints both of those under the table rather than leaving the denominator to be read as that many independent chances to fail. Until run E the main-loop figure was runs minus one times steps times trials rather than a count of comparisons that happened, in the same function that filtered the amplified figure on whether anything was compared. Both are counted now.

### The four baselines, implemented rather than named

<!-- twicerun: baselines -->
| baseline | what it is | what it gave |
|---|---|---|
| 1 | two runs, bit-exact multiset equality, no tolerance and no classes. Rescored from each trial's own runs 1 and 2, so it sees the same bytes under the same containment | fired on `mean_basket` in 10 of 10 trials, at a median 1,219 of the 2,000 rows compared failing to pair, on a step where nothing is wrong. 610 of those are on the reference side and 610 on the later run's |
| 1, run-matched | the same comparison over all of the oracle's comparisons rather than one | disagreed with the oracle's fire count on 0 of 80 step-trials, and fired on 0 of 60 twin step-passes |
| 2 | four static patterns over the pipeline source, parsed per step | separated 4 of the 5 matched pairs and flagged 1 step with no bug. It cannot separate the `MERGE` from its own fix |
| 3 | the same oracle with `--no-containment` | `roll_up_keys` fires on 10 of 10 trials uncontained, 0 of 10 contained. The append reports 15,812 extra rows uncontained against 3,953 |
| 4 | the same measurements with the amplifiers dropped, scored through the tool's own `exit_code` | 0 of 10 trials would exit 0 without them. Per comparison on `sparse_customer_keys` the loop ran 0.65 against the best amplifier's 0.95 |

The intermittent step is where the run count and the comparison method pull apart. The five-run loop caught `sparse_customer_keys` in 8 of 10 trials; the benign float step fired in 10 of 10 and is correct code.
<!-- /twicerun: baselines -->

Baseline 1's figures moved between the five runs before the artifact existed, and the direction of the movement is the useful part: it fired on `mean_basket` in 10 of 10 trials in every one of them, at medians of 1,265, 1,270, 1,269, 1,208 and 1,347 rows failing to pair, on a step where nothing is wrong. The count is across both sides, out of the 2,000 the two runs put in front of it rather than the 1,000 groups. It caught the intermittent step in 7, 5, 4, 6 and 5 of 10, against the five-run loop's 10, 10, 9, 9 and 9.

**Baseline 1 was handed one comparison and the oracle four, and that is most of the gap between them.** Until run E this section said the comparison method was the only difference, which was false in the direction that flattered the oracle. The run-matched row is the control that was missing: same runs, same bytes, same containment, and nothing but multiset equality over row hashes. It agrees with the oracle on every cell of the sensitivity table and fires on no twin.

**So the sensitivity and specificity tables contain no evidence for the oracle over the cheap comparison.** They are the wrong place to look for it. What the oracle produces that multiset equality cannot is the divergence class, the ulp and relative magnitudes, the attributed column, the derived reassociation bound and the single-threaded cause, and none of those five are scored in those two tables. What baseline 1 as originally written does show is what one comparison costs against four, which is a real result about run count and was published as a result about comparison method.

**Baseline 2 is the one worth reading twice, and it is stronger than the spec predicted.** The spec expected a static check to catch the `INSERT`-shaped bug and miss the other three. It catches four of the five, because the patterns were written here after the bugs were known, which is the largest thumb anyone could put on a static checker's scale. What it still cannot do is the result:

- It flags `apply_price_updates` and `apply_price_updates_deduped` identically. Both contain `MERGE INTO ... ON t.sku = s.sku`; the fix is a `GROUP BY` in a different statement that makes the source unique on the join key. Telling them apart is data-flow analysis, not a pattern.
- It flags `mean_basket`, which is a correct average. That is the same false positive baseline 1 makes, and neither can be fixed by looking harder at the text, because whether a float aggregate is a bug depends on whether the answer was supposed to be exact.
- Its hits are a fact about the query text. Three of the five bugs are facts about the data.

The patterns run against parsed function bodies rather than the file. That is not tidiness: `grep` the twins file for the append pattern and it hits, because `apply_price_updates_deduped` reads and writes `prices`. A file-level check reports an append bug in the file whose entire purpose is that it has none.

### What the pre-registered conditions returned

Three of these declare a piece of this project unnecessary if they fire. All eight print on every run whatever they say.

<!-- twicerun: conditions -->
| condition, as written in the spec | verdict | what it came out as |
|---|---|---|
| the naive baseline's false positives on correct code are zero, so the oracle is more machinery than the problem needs | **not triggered** | it fired on 10 of 10 trials, median 1,219 rows unmatched of the 2,000 compared, on a step where nothing is wrong |
| the amplification gap on the intermittent step is zero, so amplification is unmotivated on this evidence | **not triggered** | per comparison the loop is 0.65 over 40 and the best amplifier 0.95 over 114; at the trial level the loop said nothing on 2 of the 10 trials that compared the step and an amplifier fired on 2 of those |
| observed drift is not 1,000x inside the computed bound | **not triggered** | cleared on 20 of 20 at n = rows read, and 1 of 20 at the tight n |
| containment removes no falsely divergent step, so the Spot borrowing did not earn its place | **not triggered** | roll_up_keys fires uncontained on 10 of 10 trials that compared it, and contained on 0 of 10 |
| sensitivity below 10 of 10 on the four broken steps | **TRIGGERED** | daily_revenue 10 of 10, customer_keys 10 of 10, apply_price_updates 9 of 10, append_audit_log 10 of 10 |
| any twin fired at all | **not triggered** | 0 twin step-passes fired across 60 twin trials that compared anything, out of 60 attempted |
| the intermittent step reached neither DIVERGENT nor STABLE_ON_THIS_INPUT in every trial | **not triggered** | DIVERGENT x8, STABLE_ON_THIS_INPUT x2, over 10 of 10 trials that compared it |
| the whole eval took longer than 10 minutes, which is the spec's ten at the ten trials it fixed | **not triggered** | 502s over 10 trials, 50s each |
<!-- /twicerun: conditions -->

**Run F triggers the sensitivity condition, and it is the second run to do so.** The step is `apply_price_updates`, the same one run B missed, and the shortfall is one trial. Nothing was changed in response, because the threshold was pre-registered and the eval exits 0 on a triggered condition by design. What the condition cannot distinguish is the detector missing a divergence from the fixture not producing one, and on this step the fixture is intermittent: the `MERGE` bug needs a target between roughly 100,000 and 125,000 rows and a source that happens to carry a repeated key, and the results table above shows it at four fires out of four in only 2 of the 10 trials. That is the reading, and it is a reading rather than a measurement.

The other seven held in run F and in the five runs before it, with run B's sensitivity the only other trigger.

Three of those eight can be read off an absence rather than off a measurement, and until this pass they were. A step that writes no artifact compares nothing, fires on none of the nothing it compared, and arrives at the conditions as a clean zero, so the naive baseline reports no false positives, the amplification gap comes out as two zero rates and the uncontained pass removes no falsely divergent step. All three then print TRIGGERED, which is the verdict that declares a piece of this project unnecessary. Seven of the eight carry a third verdict now, `NOT MEASURED`, and the eighth is the wall clock, which is measured whatever the pipeline did. The trigger in the table above is a real 9 of 10 rather than a gap, and so was run B's.

**Two of the eight can trigger on something that is not the detector, and both were written that way in the spec.** The amplification gap asks whether any amplifier beats the five-run loop, and a maximum cannot beat a rate that is already at 1.00, so on a run where the loop saturates the condition fires whatever amplification did. The eval says so in the evidence line when it happens rather than moving the threshold. And the sensitivity condition fires when a broken step comes back 0 of 4 in one trial, which on `apply_price_updates` has now happened in 2 of the 6 ten-trial runs taken. The distinction matters because the two causes are indistinguishable from the condition's own output, and leaving the threshold where the spec put it is the only way it stays worth anything.

The bound check is the one condition whose two readings disagree, and both are in the table three sections down. It clears at `n = rows read by the step` and fails at the tight `n`, the terms behind one output value. That split has held on every sample taken: 0, 0, 1, 1 and 0 of 20 tight clearances across the five runs before the artifact. The check as pre-registered passes and the honest reading of it fails, so both print.

**On this fixture those are the same check, and the 1000x is cancelled exactly.** `relative_bound` is linear in the term count in this regime, so the loose ratio is the tight ratio times the number of output rows, and `reference.py` groups into `(i % 1000)` days. The multiplier the check asks for is 1,000 and the free parameter of my own test pipeline is 1,000, so **clearing the pre-registered check at the loose `n` is clearing the honest `n` with no margin at all.** An otherwise identical pipeline grouping into 100 days would fail the same check on the same drift. What the check establishes is that the derivation is not wildly wrong; what it does not establish is a thousandfold safety margin, and the number was picked before anyone multiplied the factor out.

<!-- twicerun: attribution -->
Leave-one-out named the column the step invented, rather than one it copied in, on 27 of 27 attributions. 18 of those 27 had two columns leaving the same count behind, so the counts chose nothing and the tie-break chose.
<!-- /twicerun: attribution -->

Three steps produce an attribution and ten trials give thirty chances, so a denominator below thirty is a step that went quiet for a whole trial. The five runs before the artifact gave 30 of 30, then 29 of 29 four times.

**Two thirds of that is a sort key scored against itself.** On both `row_number` steps, dropping `surrogate_id` and dropping `event_id` each take the unmatched count to zero, because the two columns are a bijection whose pairing moved, so the counts choose nothing and the `(remaining, from_input, column)` tie-break chooses. Preferring the column the step invented is right, and `tests/test_oracle.py::test_a_tie_goes_to_the_column_the_step_invented` asserts it as a unit test; re-scoring it ten times a run and calling the result an accuracy figure is not. The eval prints that split per step as well as in total. `apply_price_updates` is the one where the counts do the work.

**The amplification gap has to be forced to be visible at all.** Amplification only touches steps the main loop found nothing in, which is the whole cost argument for it, so on a trial where the loop catches the intermittent step there is no amplified rate to compare against. Waiting for a quiet trial throws away nine in ten. So the eval points the shipped amplifiers at that one step every trial, through the same `amplify_runs` the runner calls, and prints the four rates side by side. Forty trials of that is an expensive way to get forty comparisons, so the table the README publishes comes from `scripts/amplification_gap.py`, which does the same thing without the twin pass and the uncontained pass underneath it. It is under "the step this was built for" below.

**The eval always exits 0, including on a triggered condition.** A script that failed on one would be a script with a reason to stop publishing it, and there is no CI job running this: the stamp above says how long the committed run took, and peak disk is 1.3 GB.

**A trial is about 50 percent slower than it was, and the per-trial budget has roughly 12 percent of headroom left.** Run E's 53 seconds a trial against B's 35 is the run-matched baseline, which costs about 20 seconds a trial in comparisons and was worth it: it is the control that showed the sensitivity table holds no evidence for the oracle over multiset equality. The pre-registered wall-clock condition is a minute a trial, so what is left over is about seven seconds. A laptop doing something else while this runs can trip that condition honestly, and run A came within 64 seconds of it, at 536s for work that took run B 363. The condition is measuring the machine as much as the code, and it is left where the spec put it rather than widened after the fact.

## Containment, and what it is worth

Runs 2 to N resolve every read against run 1's artifacts. A step that reads a diverging step's output therefore reads the same bytes every time, so it reports what it did rather than what the step above it did.

The idea is [Spot's](https://academic.oup.com/gigascience/article/9/12/giaa106/5998300) (Salari, Kiar, Lewis, Evans and Glatard, GigaScience 9(12), [arXiv:2006.04684](https://arxiv.org/abs/2006.04684)), which compares two conditions of a neuroimaging pipeline "in a step-by-step execution that prevents the propagation of differences in the pipeline", and does it by copying the first condition's output files into the second. The borrowing is the idea and not the implementation: Spot's tool used ReproZip syscall interception, has not been touched since 2020, and works on a domain unrelated to this one. Here artifacts are already addressed by `(run, step index, name)`, so containment is a dictionary lookup rather than a file copy, and it costs nothing measurable.

`--no-containment` runs the ablation, so the number below is something you can reproduce rather than a claim to take on trust.

On the reference pipeline the step it changes most is `append_audit_log`, the append with no unique key. Two steps read their own last output through `ctx.state`, and this is the one where the difference is a clean number: `apply_price_updates` merges over its previous catalogue and the divergence is mostly overwritten each run, while an append keeps every copy. Extra rows reported per comparison, against a 3,953-row reference:

| | comparison 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| `--no-containment` | 3,953 | 7,906 | 11,859 | 15,812 |
| contained | 3,953 | 3,953 | 3,953 | 3,953 |

Measured on 2026-08-26 over 10 uncontained passes and 20 contained ones, and both rows held on every one of them. That is the one table in this section that is not regenerated, because the eval keeps the loudest comparison per step rather than all four, so the progression is not in the artifact. The uncontained row is four reruns' worth of duplication charged to one step: run 4 appends to run 3's log, which already had run 2's in it. The contained row is what one rerun of that step actually does, and the eval does carry that pair.

Running the ablation shows you the loudest of those four figures, not all four: the report prints one comparison per step and 15,812 is the one it picks. The other three are in the manifest, and this prints the row counts they come from:

```bash
uv run twicerun run pipelines/reference.py --no-containment
uv run python -c "
import json, sys
runs = json.load(open(sys.argv[1]))['runs']
rows = [s['artifacts'][0]['rows'] for r in runs for s in r['steps'] if s['name'] == 'append_audit_log']
assert rows, 'that manifest has no append_audit_log: it is a run of some other pipeline'
print(rows)" .twicerun/run-*/manifest.json
```

The assert is there because the obvious way to get this wrong is to run it after the twins, which is the command two sections down. Without it the comprehension found nothing, printed `[]`, and exited 0.

Uncontained that gives `[3953, 7906, 11859, 15812, 19765]`, one run per entry, and contained it gives `[3953, 7906, 7906, 7906, 7906]`. The bug is the same bug either way and the fire rate is 4 of 4 either way, so what containment bought here is the magnitude being a fact about the step rather than about how many times the tool ran.

The second number is the count of steps reported divergent, and getting it took a change to the reference pipeline that is worth being explicit about. Every step in that file read the control step's artifacts, which never differ, so nothing in it ever read a diverging artifact and the cascade could not happen. The ablation scored zero on the quantity baseline 3 was pre-registered to use, which reads as evidence against a feature that was simply never exercised.

So `roll_up_keys` exists. It reads `customer_keys`, whose surrogate ids shuffle between runs, and computes an integer minimum that cannot reassociate, so a fire rate above zero there means it was fed something different and never that it computed something different. **It is constructed to exercise containment and is not a failure anyone here has been bitten by**, unlike three of the four bugs beside it. Every step of the committed run, with the ablation beside it:

<!-- twicerun: containment -->
| step | contained | uncontained |
|---|---|---|
| 0 `generate_inputs` | 0 of 4 on all 10 | 0 of 4 on all 10 |
| 1 `daily_revenue` | 4 of 4 on all 10 | 4 of 4 on all 10 |
| 2 `customer_keys` | 4 of 4 x7, 3 of 4 x2, 2 of 4 x1, 1 of 4 x0, 0 of 4 x0 | 4 of 4 x7, 3 of 4 x3, 2 of 4 x0, 1 of 4 x0, 0 of 4 x0 |
| 3 `apply_price_updates` | 4 of 4 x2, 3 of 4 x1, 2 of 4 x4, 1 of 4 x2, 0 of 4 x1 | 4 of 4 x4, 3 of 4 x1, 2 of 4 x2, 1 of 4 x2, 0 of 4 x1 |
| 4 `append_audit_log` | 4 of 4 on all 10 | 4 of 4 on all 10 |
| 5 `mean_basket` | 4 of 4 on all 10 | 4 of 4 on all 10 |
| 6 `sparse_customer_keys` | 4 of 4 x4, 3 of 4 x2, 2 of 4 x2, 1 of 4 x0, 0 of 4 x2 | 4 of 4 x2, 3 of 4 x3, 2 of 4 x2, 1 of 4 x1, 0 of 4 x2 |
| 7 `roll_up_keys` | 0 of 4 on all 10 | 4 of 4 x7, 3 of 4 x3, 2 of 4 x0, 1 of 4 x0, 0 of 4 x0 |

The append with no unique key reports 15,812 extra rows uncontained against 3,953 contained, which is four reruns' worth of duplication charged to one step against what one rerun of it does.
<!-- /twicerun: containment -->

The bottom row is the one containment is for. Read the others as noise rather than as evidence either way: two steps in this pipeline are intermittent, so a contained pass and an uncontained pass differ mostly by which of them happened to fire. Counting steps-divergent-per-pass and subtracting was the first way this was measured and it was exactly that noise, which is why the quantity has to be per step. Across the four ten-trial runs before the artifact, `roll_up_keys` never once fired contained, over forty trials with containment on and forty with it off.

That one step is the whole difference. It is a small number and it is the honest one: on a pipeline where nothing reads a diverging artifact, containment removes no false step at all, and this pipeline had to be given a step that does.

There is a test that measures the same thing without the reference pipeline. On a three-step fixture where two steps do nothing but copy a wobbling step's output, the report goes from one finding to three with `--no-containment`.

## The cause axis

Class says what went wrong. Cause says where to look. Every step that fired is re-executed at `threads=1`, five times, and the report prints both rates:

```
  1 daily_revenue         PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1
  4 append_audit_log      PERSISTS_SINGLE_THREADED  4 of 4 at threads=10, 4 of 4 at threads=1
```

Same denominator on both sides, which is the only reason the two halves of that sentence can be read against each other. The bisect runs at the same N as the main loop for exactly that reason.

`PERSISTS_SINGLE_THREADED` is where the tool stops. It says the thread count is not the explanation and does not guess between a clock read, a data-dependent branch, appended state and something outside the pipeline.

**A zero at `threads=1` is not proof of anything, and the report says so on the line above the rates.** Four clean comparisons put a 95 percent one-sided upper bound of 53 percent on the per-comparison rate, which is the same arithmetic that argues for five runs rather than two, applied to the bisect's own evidence. It also assumes the comparisons are independent, and they are not: they share a process, a page cache and a machine. So `PARALLEL_ORDER` is a reading of two measured rates, not a finding that single-threaded execution cannot diverge, and a test asserts that no shape of report contains the words deterministic, stable, reproducible or passed. `STABLE_ON_THIS_INPUT` has one of those words inside it, so the test strips the token out of the text first and what is left still has to be clean, and a second test requires the token to be printed with the paragraph saying what it does not mean.

The bisect starts each single-threaded sequence from no carried state, exactly as run 1 of the main loop did, and its later runs carry run 1's artifacts with anything the bisect itself re-produced laid over the top. Both halves of that were wrong once and each cost the same mislabelling.

Seeding the sequence from run 1's state was the first implementation: all five single-threaded executions of `append_audit_log` then read the same log, agreed with each other, and the step came out `PARALLEL_ORDER`. Duplicating a log on rerun has nothing to do with threads. Carrying only what the bisect re-produced was the second: a step carrying state under a name a *non-divergent* step wrote found nothing there, because the bisect skips steps that did not fire, so all five runs fell back to the seed and agreed for the same empty reason. The regression test for the first shape could not catch the second, because its state name and its write name belong to one step.

## Amplification, because more runs cannot fix this

Running the pipeline more times lowers the chance of missing a step that diverges with probability `p`. It does not touch `p`. That is the ceiling, and the arithmetic states it plainly: at `p = 0.2` five runs still miss 41 percent of the time, and even twenty runs leave a 15 percent upper bound on `p` when nothing fires. No practical number of runs settles the question.

Amplification changes `p` instead, by handing a step an input built to make the mechanism fire. Every step the main loop found nothing in is re-executed against that substituted input, three runs each, escalating to the full five on a hit so the amplified rate and the main-loop rate share a denominator. Cost only rises where something was found.

Substituting the input is allowed because this tool is not checking that the answer is right. Reproducibility is a property of the code rather than of the data, so an input with the same types and the same row count is a fair question to ask of a step, and a step that only reproduces on the data it happened to be handed is fragile.

| amplifier | what it does | the bug it is aimed at |
|---|---|---|
| tie collapse | hashes a column's values into buckets until the artifact holds 500 rows per distinct value, leaving the most distinct column alone | `row_number() OVER (ORDER BY cust)`, which cannot disagree with itself unless `cust` has ties |
| thread count | re-executes on the same input at twice the machine's default, floor 4, ceiling 64 | parallel reduction and parallel scan order |
| row multiplication | duplicates every input row once | the append with no unique key, and the `MERGE`, whose source has to carry a repeated key before it can pick wrongly |

Two numbers in that table were measured rather than picked. The thread floor of 4 is where the float sum switches on: over eight comparisons each it fired once at `threads=2` and eight times at `threads=4`, and it is flat from there to 40, which is also why doubling a default of 10 was expected to find nothing.

500 rows per distinct value is the density at which the surrogate-key bug fired on every attempt of the standalone measurement this project started from. Inside the tool it roughly doubles that step's per-comparison rate, which is the table further down. A standalone re-measurement made while building this found much less of a gap, 10 of 12 comparisons at 2 rows per value against 11 of 12 at 500, and the distance between those two pictures is the rest of the pipeline running between one execution of the step and the next. Back to back in one process the bug fires most of the time at either density; inside a five-run loop over eight steps it does not.

**That 500 is also the density `reference.py` generates `customers` at**, which is the input to the loud version of the same bug: `(i % 1000)` over `range(500000)` is 500 rows per distinct value, and `TARGET_ROWS_PER_VALUE` in `amplify.py` is 500. So tie collapse is not raising the sparse step's firing probability by a general principle here. It is converting that step's input into the density the loud step already fires on every time, and I chose both numbers. A fixture built at a different density would need the amplifier retuned, or would be answering a different question.

The counterweight is that tie collapse is not reliably the best of the three on that step, and the table further down is one sample of which wins. Row multiplication is aimed at the append and the `MERGE` rather than at a surrogate key, and it led in eval runs D and E, at 0.87 against 0.80 and 0.95 against 0.88, both measured on 2026-08-26. Which of the two comes first moves between runs, and the amplifier that was not co-designed with this step wins about as often as the one that was.

Tie collapse replaces a non-NULL value with another real value from the same column, the minimum of its hash bucket, so the type, the row count, the NULL count and the value domain all survive. Casting a hash back to the column's type would have worked for integers and not for `DATE` or `DECIMAL`.

NULLs needed a special case rather than falling out of that. `hash(NULL)` is an ordinary non-NULL constant, so a NULL row lands in a bucket alongside real values and takes their representative: 300 NULLs in 3,000 rows came out as zero NULLs, while the report line beside it claimed the domain survived. Type and row count did survive, which is why it read as fine. A step that branches on NULL was being handed a different question from the one printed.

### The property that separates this from a chaos generator

**Each amplifier is built so that the bug's own fix survives it.** Tie collapse leaves the most distinct column alone, which is exactly what `ORDER BY cust, event_id` needs to stay decided. Row multiplication does not trouble a `CREATE OR REPLACE TABLE`, and a `MERGE` over a deduplicated source gives the same answer on twice the rows. An amplifier that made correct code fail would be measuring its own violence and the tool would be worthless.

That is checkable rather than assertable, because `pipelines/twins.py` is the one-line fix for every bug in the reference pipeline. Any non-zero exit there means the amplifiers are broken, not the twins:

```bash
uv run twicerun run pipelines/twins.py
```

Every twin pass of the committed eval run went through the same amplifiers, and this is what they did to correct code:

<!-- twicerun: twin-coverage -->
| amplifier | twin step-passes it ran on | comparisons | fired | raised |
|---|---|---|---|---|
| tie collapse | 20 of 60 | 40 | 0 | 0 |
| thread count | 60 of 60 | 120 | 0 | 0 |
| row multiplication | 50 of 60 | 100 | 0 | 0 |
<!-- /twicerun: twin-coverage -->

**The first column is the honest part of that table.** Tie collapse declines most of its chances, and the report says why each time rather than printing a zero. `orders.day` and `customers.cust` already hold 2,000 and 500 rows per value, which is at or past what the amplifier targets, so there is nothing for it to raise; `generate_inputs` reads no artifact at all, so two of the three amplifiers have nothing to substitute. A twin an amplifier never touched is not evidence that the amplifier is safe on it, and folding those into the zero would have made the table look twice as strong as it is.

Two larger samples of the same file, both hand-transcribed on 2026-08-26 and neither regenerated: 12 passes gave 24 of 72 for tie collapse, 72 of 72 for thread count and 60 of 72 for row multiplication, at zero fires; 40 passes in a fresh clone gave 240 twin step-passes, 1,040 amplified comparisons on correct code, zero fires and exit 0 on all 40, at the same coverage ratios.

**The twins only cover code that reproduces exactly.** `mean_basket` is the reference pipeline's correct-but-drifting float step, and there is no twin for it because there is nothing to fix: the drift is arithmetic. So this table says the amplifiers do not break code that gives the same answer twice. It says nothing about whether they widen drift that was already there, and the step that would answer that is the one step with no partner.

### The step this was built for

Step 6 of the reference pipeline is the surrogate-key bug at 2 rows per tie group, the density where it fires only sometimes. With the amplifiers pointed at that step whether or not the main loop had already caught it:

<!-- twicerun: gap-provenance -->
Generated by `twicerun report` from `results/gap-2026-08-27.json`, written 2026-08-27 by `uv run python scripts/amplification_gap.py 40 --json`. DuckDB 1.5.5 at `threads=10` on macOS-26.5.2-arm64-arm-64bit, 40 passes over `sparse_customer_keys`, 492s.
<!-- /twicerun: gap-provenance -->

<!-- twicerun: amplification-gap -->
|  | comparisons that fired | per-comparison rate | passes it found nothing on | passes it could not ask |
|---|---|---|---|---|
| the five-run loop | 84 of 160 | 0.53 | 5 of 40 | 0 |
| tie collapse | 149 of 160 | 0.93 | 0 of 40 | 0 |
| thread count | 73 of 128 | 0.57 | 16 of 40 | 0 |
| row multiplication | 137 of 156 | 0.88 | 2 of 40 | 0 |

The loop reported nothing on 5 of the 40 passes where it compared anything, and an amplifier fired on 5 of those 5.
<!-- /twicerun: amplification-gap -->

A larger sample will move all of those. The last two columns are three different things kept apart: an amplifier that declined to build an input and one that built an input the step wrote nothing from both count as could-not-ask, and only a comparison that happened and found the step clean is evidence about the step. A row with fewer comparisons than the others found nothing in its first two, because an amplifier is escalated to the full run count only on a hit, which is the cost argument working.

**`twicerun run` cannot produce that table and it is not meant to.** Amplification only touches steps the main loop found nothing in, so on most passes step 6 fires, never reaches an amplifier, and contributes nothing to the second half of the comparison. Waiting for the passes where it stays quiet means throwing most of them away, and the first row of the table above says how many that is. So the table comes from a script rather than from the tool, and the script ships:

```bash
uv run python scripts/amplification_gap.py 40 --json results/gap-2026-08-27.json
```

It points the amplifiers at that one step every pass. There is no flag for it in the tool, deliberately: a flag doing this would make every clean pipeline pay for evidence it does not need, and the argument for amplification's cost is precisely that it runs only where the loop came back quiet. What `twicerun run` does reproduce on its own is the first row and the passes-it-found-nothing-on column, since a pass where step 6 comes out `STABLE_ON_THIS_INPUT` is a pass where the loop said nothing and an amplifier did not.

The bottom line of that block is the gap the feature exists to close, measured rather than argued: a step that is definitely broken, a loop that says nothing about it on a fraction of passes, and an amplifier that fires on the passes the loop missed.

The thread-count amplifier was the one I expected to find nothing. This machine already runs DuckDB at 10 threads, and the float aggregate is flat from 4 threads upward, so doubling to 20 looked like a second look at the same thing. It is not: it fires on the surrogate-key step more often than not, and on the passes where `apply_price_updates` comes back quiet it is the amplifier that catches the `MERGE` bug, at `4 of 4` where the other two report nothing. That step's own input has no repeated key to collapse and duplicating its rows does not create one the merge can trip over, so thread count is the only one of the three with anything to say about it.

### The four statuses

| status | condition |
|---|---|
| `DIVERGENT` | the step fired on the real input, `k >= 1` of `m` |
| `STABLE_ON_THIS_INPUT` | 0 of `m` on the real input, `k >= 1` under at least one amplifier |
| `NO_DIVERGENCE_OBSERVED` | 0 of `m` on the real input and 0 under every amplifier that ran |
| `AMPLIFICATION_FAILED` | an amplified input made the step raise rather than diverge |

`STABLE_ON_THIS_INPUT` is not a milder `DIVERGENT` and it is not a pass. It is the case that a plain five-run loop reports as a clean zero, which is the case this whole tool is about, so the report prints it as loudly as anything else and prints the sentence saying what it does not mean:

```
  It says the step did not fire under these particular stresses, the ones named above, and
  nothing beyond that.
```

That sentence is enforced by a test, and so is the absence of the bare words deterministic, stable, reproducible and passed. The status token itself contains one of them, so the test strips the token out first and what is left still has to be clean.

`AMPLIFICATION_FAILED` exists so that a step which raises under amplification can never fall back to green. Collapsing a column can violate a downstream uniqueness constraint, and the honest report of that is the amplifier's name and the error, not a zero:

```
  1 insists_on_uniqueness    0 of 4  AMPLIFICATION_FAILED
      tie collapse        raised: ConstraintException: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "0"
      thread count        0 of 2, threads raised to 20 from the 10 in the loop above
      row multiplication  raised: ConstraintException: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "0"
```

Note the thread count line in the middle. One amplifier came back with a clean `0 of 2` and the step still does not get a clean status, because two of the three could not answer at all.

**The status is read off the measurement, never off the policy.** A policy decides whether a difference matters and must not be able to decide whether one happened, so a step whose drift was entirely downgraded still reads `DIVERGENT` with the `TOLERATED` count beside it. Judging one saved run under `strict`, under `reduction-order` and under `--tolerance-ulps 4` gives three different reports with the same status column, and there is a test that does exactly that and diffs it.

### What `NO_DIVERGENCE_OBSERVED` prints

Never alone, and never as a verdict. Always the rate, the bound in words, and the list of axes that were varied against the list that were not:

```
NO_DIVERGENCE_OBSERVED on 0 generate_inputs, 7 roll_up_keys. It is a fire rate and two lists, and
it is not a verdict about the code.
  0 of 4 on the real input, and 0 of 2 under each amplifier that ran. 4 clean comparisons rule out
  a per-comparison divergence probability above 53 percent, 95 percent one-sided. An amplifier's 2
  rule out one above 78 percent. That assumes an independence these runs do not have, since they
  share a process, a page cache and a machine.
  0 generate_inputs
      varied: how many times the pipeline ran, thread count
      not varied: tie collapse (this step reads no artifact, so there is no input to substitute),
      row multiplication (this step reads no artifact, so there is no input to substitute)
  7 roll_up_keys
      varied: how many times the pipeline ran, tie collapse, thread count, row multiplication
  Not varied anywhere: input distribution beyond what the amplifiers above changed, memory limit
  and spill behaviour, DuckDB version, wall clock, filesystem.
```

The pair of lists is the part that cannot be dropped. "Never fired in four comparisons" and "cannot fire" are different sentences, and naming what was moved against what was not is the only version of that difference a black-box tester can honestly produce. The lists are per step because they differ per step: a generator reads no artifact, so two of the three amplifiers have nothing to work with, and folding that into one summary put the same amplifier in both lists at once.

An amplifier that ran and compared no artifact goes in the second list too. That is the guard the main loop and the bisect already carry, arriving in the third loop: `any([])` is False, so a step that wrote nothing when re-executed on its own scores `0 of 2` out of two comparisons of nothing and reads exactly like two clean ones. The report shows the rate and disqualifies it on the same line.

## How the comparison works

Bit-exact equality reports around 600 findings on 1,000 groups of correct code, and a tool that fixes that by picking an epsilon until the demo passes is worse than one that ships the false positives. What sits between those is four steps.

**Partition the columns.** Integers, `VARCHAR`, `DATE`, `TIMESTAMP`, `UUID` and `DECIMAL` cannot come back different from a correct query run twice, so a difference in one of them is real. `FLOAT` and `DOUBLE` can. `LIST`, `STRUCT`, `MAP`, `UNION` and any type the classifier does not recognise are refused with the column named, because comparing a nested type badly is worse than refusing.

`DECIMAL` on the exact side is deliberate: it is the fix the tool recommends for the float aggregate bug, since DuckDB's decimal sum is fixed-point and reassociates exactly.

**Match rows on the exact columns.** This is the move that makes the whole thing tractable. Order-independent exact comparison is a multiset of hashes and is linear. Order-independent tolerant comparison is an assignment problem. Splitting the row into an exactly-matched key and tolerantly-compared values turns the assignment problem into a join.

**Preserve multiplicity.** Each side gets `row_number() OVER (PARTITION BY key ORDER BY values)` and the two are joined on key plus ordinal, with `IS NOT DISTINCT FROM` so a NULL matches a NULL. A key group with three rows in the reference and five in a later run pairs ordinals 1 to 3 and leaves 4 and 5 unpaired. Joining on the key alone would fan those out to fifteen pairs and report nothing wrong, which is why `append_audit_log` reads `MULTIPLICITY` rather than a row count that happens to differ.

**Measure the floats twice.** For every pair that moved, the tool reports both the ULP distance, which says whether the difference is last-bit noise, and the relative difference, which is the number someone with domain knowledge can judge. Neither decides anything on its own. `-0.0` and `0.0` are the same number, both NaN is not a difference, and an infinity against a finite number has no meaningful relative difference so it reports as infinite rather than as a NaN that would silently compare false against every threshold.

ULP distance is computed in SQL rather than by pulling rows into Python. DuckDB casts a `DOUBLE` to `BIT` giving the raw IEEE-754 layout and `BIT` to `BIGINT` giving the two's-complement reading of those bits; reflecting the negative half about the minimum turns sign-magnitude into a monotone integer ordering. It is checked against `struct.unpack` over zeros, subnormals, both infinities and a NaN.

## Attribution, and the tie it does not hide

For each key column, drop it and recount what failed to pair. On `customer_keys` that turns 491,520 unmatched rows into a sentence naming a column.

The counts alone do not always single one out, and finding that out changed the design. Dropping `event_id` works exactly as well as dropping `surrogate_id`, because each `cust` block keeps the same set of `surrogate_id` values and only the pairing to `event_id` shuffles inside it. Both descriptions of what moved are true and the arithmetic is symmetric, so the report says so rather than picking one and implying the numbers chose it.

What breaks the tie is something the storage interface already knows: which artifacts the step read. `event_id` came in from `customers`; `surrogate_id` did not exist until this step made it. A column the step invented is the better suspect. Across 10 passes the first column named was `surrogate_id` on both `row_number()` steps and `price_cents` on the merge step, every time.

## The tolerance is derived, and there is a pre-registered condition under which it fails

Reassociating a sum of `n` float64 terms moves the result by at most `gamma_n * sum(|x_i|)` with `gamma_n = n*u / (1 - n*u)` and `u = 2^-53`. Two orderings differ by at most twice that. The tool has the output and not the terms, so it substitutes `max(|a|, |b|)` for `sum(|x_i|)` and takes `n` from the row count the storage interface saw:

```
bound = 2 * gamma_n * max(|a|, |b|)
```

The magnitude drops out. The tool measures `|a - b| / max(|a|, |b|)`, so the same quantity sits on both sides of the comparison and the test reduces to `relative difference <= 2 * gamma_n`.

Before any of this was written, one condition was fixed: **observed maximum relative drift on the benign step must be at least 1000x below the computed bound**, or the bound is binding, the derivation is not conservative enough, and the design gets revisited rather than the threshold moved.

**It passes, and the margin is not where it looks.** Medians with an n, for the reason the results table gives: the drift figure is a maximum over 1,000 groups and its range grows with how long anyone looks.

<!-- twicerun: drift-bound -->
|  | `daily_revenue` | `mean_basket` |
|---|---|---|
| median furthest relative drift, n=10 | 4.7e-16 | 5.2e-16 |
| bound at `n` = 2,000,000 rows read | 4.44e-10 | 4.44e-10 |
| bound at `n` = 2,000 terms per output row | 4.44e-13 | 4.44e-13 |
| headroom at the loose `n`, and how often it cleared 1,000x | 946,168x, 10 of 10 | 859,629x, 10 of 10 |
| headroom at the tight `n`, and how often it cleared 1,000x | 946x, 1 of 10 | 860x, 0 of 10 |
<!-- /twicerun: drift-bound -->

Almost none of that margin comes from the drift being small. `rows_read` is the step's whole input, and each of the 1,000 output rows sums about 2,000 terms, so `n` is a thousand times larger than the quantity the bound is about. Take that slack out and the headroom lands in the hundreds, under the 1000x line. It is not always under it, which is why the last row carries a count rather than a verdict: earlier samples cleared the tight check on 3 of 56 step-passes, then 1 of 40, then 5 of 80. So the tight check fails most of the time rather than every time, and both the report and this table print how often it cleared on the run in front of you instead of quoting a number from this machine.

So the honest reading is between two and three orders of magnitude of headroom, not six, and every report prints both numbers so nobody has to take that from this file:

```
  5 mean_basket
      observed furthest relative drift 6.8129e-16
      n = 2,000,000 rows read by the step, bound 4.4409e-10, headroom 651,838x  CLEARS
      n = 2,000 terms per output row, bound 4.4409e-13, headroom 652x  FAILS
```

The threshold is not moving and neither is `n`. `rows_read` was the choice made in the spec before any of this was measured, and switching to whichever count clears the line after seeing the result is exactly what pre-registering is meant to stop. The number that would make the bound correct is the term count behind one output value, and getting it needs the query plan, which this tool does not have.

A calibration approach was considered and rejected: measure the drift distribution of a no-op control and set the tolerance from it. That is circular, because the no-op control is the thing under test.

### Where `rows_read` is wrong, and which way

Two errors, pointing opposite ways.

It undercounts. Only `ctx.read` and `ctx.state` add to it, so anything a step pulls in through `ctx.sql` is invisible and `apply_price_updates` records 300,000 while its two `CREATE TABLE AS` statements scan at least 300,000 more. That makes `n` too small, the bound too tight, and the tool reports a difference reassociation could in fact explain. A false positive, which is the direction to err in.

It also overcounts, in a different sense. "Brought into scope" is not "terms behind one output value", and for a group-by the gap is the group count. That makes the bound too loose and could tolerate a difference reassociation cannot explain, which is the unsafe direction, and for an aggregate it is the larger of the two by far. That is the gap between the two bound rows of the table above, and it is a factor of a thousand on this pipeline.

## The policy default is `strict`, and that is not a preference

Any difference at all counts as a divergence unless you ask otherwise. The argument for that is not first principles, it is how the failure actually surfaces.

Asked what difference between two runs of the same total he would have accepted in a pipeline he shipped, Vishal's answer was zero: any difference is a bug. Asked how he found out a pipeline had gone wrong, the answer was a downstream count that did not reconcile. Those fit together. If breakage reaches you as an exact reconciliation failing, a tool that quietly absorbs a small difference has hidden the thing you would have used to find the bug.

So correct code producing 630 findings under the default is the intended behaviour rather than an embarrassment. The user asked whether the pipeline gave the same answer twice, and it did not.

That also shapes the report. A build gate needs a verdict; someone tracing a count that did not add up needs the two numbers that disagreed, so the report prints the pair rather than a summary of it.

`--policy reduction-order` is the opt-in. It downgrades a `VALUE_DRIFT` finding to `TOLERATED` when three things hold at once: the step's only class across every comparison is `VALUE_DRIFT`, the step's fire rate at `threads=1` is 0 of m, and the magnitude is inside the bound. Any `ROW_MISSING`, `ROW_EXTRA`, `MULTIPLICITY` or `SCHEMA` finding anywhere in the step blocks it outright. A downgraded finding is still counted and still printed with its magnitudes.

The conjunction is the point, and each condition refuses something the other two would let through. A difference that vanishes single-threaded but is ten orders of magnitude larger than reassociation can account for is catastrophic cancellation or a genuinely different set of terms, and it is still reported. A difference small enough for the bound that survives `threads=1` is not reduction order whatever its size, and it is still reported, with that rate as the reason:

```
      drift not downgraded: the step still diverges at threads=1, 2 of 4, so the order of a parallel reduction is not what moved it
```

A step that was never bisected gets the same refusal. No evidence is not evidence, and the direction to err in is reporting a difference that reassociation might well have explained.

`--tolerance-rel` and `--tolerance-ulps` are deliberately not gated on the bisect. A threshold is a user saying a difference of that size does not matter in their domain, which is their claim to make and rests on no mechanism. The step line names which route downgraded it, because `TOLERATED on 4 of 4 by the derived reassociation bound` and `TOLERATED on 4 of 4 by a --tolerance threshold` are different claims and used to render identically.

They are an escape hatch, documented as one and never a default, and they still cannot excuse a missing or duplicated row.

**What the opt-in costs is one of the four broken steps, and it is the third column of the results table.** Everything else here is scored under `strict`, which is the default, so the price of `reduction-order` appeared nowhere at all until the eval started measuring it through the shipped `judge`. `daily_revenue` stops gating a release under it and the other three broken steps do not. That step is bug 1, a `sum()` over `DOUBLE`, and it is downgraded because its drift is float-only, inside the derived bound and gone at `threads=1`, which is the conjunction working exactly as designed. Under this policy the detector's sensitivity on the four broken steps is 3 of 4 by design, and the eval prints that in a section of its own.

## Measuring and deciding are separate stages

Numbers must not move when the policy does. If a tolerance can change a reported figure, everything downstream of it is negotiable and none of it is worth printing.

So `oracle.py` produces findings, `measurement.py` collects them per step, `policy.py` reads that and returns a verdict, and nothing writes back.

Running `twicerun run` twice under two policies does not show you that, because each invocation runs the pipeline again and the figures move between them for exactly the reason this tool exists. `twicerun judge` scores a run that already happened and executes nothing:

```
$ uv run twicerun run pipelines/reference.py --runs 3
$ uv run twicerun judge .twicerun/run-20260826-142816 --policy strict
  1 daily_revenue           2 of 2  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER
      daily_revenue: 602 of 1,000 paired rows moved on revenue
      furthest move over 2 comparisons: revenue, 3 ulp and 3.56e-16 relative
      490922.43387865723 against 490922.43387865706

$ uv run twicerun judge .twicerun/run-20260826-142816 --policy reduction-order
  1 daily_revenue           0 of 2  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER  TOLERATED on 2 of 2 by the derived reassociation bound
      daily_revenue: 602 of 1,000 paired rows moved on revenue
      furthest move over 2 comparisons: revenue, 3 ulp and 3.56e-16 relative
      490922.43387865723 against 490922.43387865706
```

Same 602, same 3 ulp, same 3.56e-16, same pair of values, and the same `DIVERGENT`. The fire rate moves, the `TOLERATED` note appears, and nothing else does. A test compares those detail lines rather than describing them.

`DIVERGENT` staying put under `reduction-order` next to a fire rate of `0 of 2` is the deliberate half. The step did produce two different answers; the policy decided the difference did not matter. Letting the policy rewrite the status would have put it in the same negotiable pile as every figure a tolerance touches.

The bisect and the amplifiers are measurement too, so they hold still as well. Judging one saved run under `strict`, under `reduction-order` and under `--tolerance-ulps 4` gives three different reports with an identical cause table, an identical amplification table and an identical status column, and only the fire rate and the `TOLERATED` count moving. `judge` re-derives both the `threads=1` rate and every amplifier's rate from the saved artifacts rather than reading a number out of the manifest, so all three go through the comparison code the run used.

## Running it

Needs [uv](https://docs.astral.sh/uv/) and nothing else. Python, DuckDB and the dev tools all come from `uv sync`.

```bash
git clone https://github.com/DataScienceVishal/twicerun && cd twicerun
uv sync --all-extras
./scripts/install-hooks.sh

uv run twicerun run pipelines/reference.py
uv run twicerun judge .twicerun/run-* --policy reduction-order   # newest, if the glob matches several
uv run twicerun run pipelines/reference.py --no-containment
uv run twicerun run pipelines/twins.py                           # the fixed pipeline: exit 0 or the amplifiers are broken

uv run python scripts/eval.py                                    # the eval, most of ten minutes and 1.3 GB
uv run python scripts/fetch_tlc.py && uv run twicerun run pipelines/tlc_backfill.py

uv run twicerun report results/*.json --format md                # the README's tables, to stdout
```

Regenerating this file's tables is three commands, and the last one is the only way a figure in them changes:

```bash
uv run python scripts/eval.py --json results/eval-2026-08-27.json
uv run python scripts/amplification_gap.py 40 --json results/gap-2026-08-27.json
uv run twicerun report results/eval-2026-08-27.json results/gap-2026-08-27.json --update README.md
```

`--update` refuses in both directions. A marker pair this file carries with no artifact behind it, and an artifact table this file does not carry, are both errors that name what is missing, because a block that silently stops being regenerated keeps whatever it last said. `tests/test_readme_tables.py` runs the same comparison in CI, so a hand edit inside a marked block fails the build rather than surviving to a reader.

The backfill exits 3 with a traceback if the partitions are not there, because the data being absent is a crash rather than a divergence, and 1 is the code a release gate keys on. The traceback names the script that fetches them.

| exit | meaning |
|---|---|
| 0 | nothing diverged |
| 1 | a step diverged on the real input |
| 2 | bad input, including a column the oracle refuses to compare, a `--key` naming a column that is not there, and a `report` whose artifacts and markdown do not line up |
| 3 | the run or the judge crashed |
| 4 | a step diverged only under an amplifier |
| 5 | an amplifier raised, so the check that would have tightened a zero did not run |

1 means divergence on your data and only that, so a release gate keyed on it does not also trip on a broken pipeline, and a comparison downgraded to `TOLERATED` does not set it.

**4 exists because 0 and 1 are both wrong for `STABLE_ON_THIS_INPUT`, and working that out took three attempts.** Zero is disqualifying: this tool's argument is that a green five-run loop lies about exactly that step, so returning green rebuilds the failure at the one channel a machine reads. Folding it into 1 is wrong for the reason everything else here is split in two. Measurement is kept apart from policy, `PARALLEL_ORDER` from `PERSISTS_SINGLE_THREADED`, the derived bound from a user's threshold, an amplifier that ran from one that measured, varied from not varied. "Your pipeline gave two answers on your data" and "your pipeline gave two answers on an input twicerun fabricated" are different claims with different urgency and different false-positive character, and the exit code was the one place they were about to be collapsed into a single integer.

Both are non-zero, so `set -e` and `run || fail` behave as they did. A gate that wants real-data divergence only asks for `== 1`. There is deliberately no flag to choose between them: a flag that lets a gate pick its exit code is a flag that lets a gate turn a finding green, and `--key` and `--tolerance` have each already done a version of that here.

**5 is the same argument one step further, and it took a second pass to see.** The first version had `AMPLIFICATION_FAILED` at 0, on the grounds that nothing there was seen giving two answers. That is true and it is not the question. Exit 0 was then carrying two meanings at once: "I checked and found nothing", and "the check that would have made that meaningful did not run". Those are further apart than 1 and 4 are, because this one is silent. A step sitting at `0 of 4` on real data leaves a 53 percent upper bound, amplification is the only thing here that tightens it, and an amplifier that raised leaves the weak bound with no signal at all. So it gets its own code, below 4, because a divergence you can reproduce is worth more than a check that did not happen.

`twicerun judge <run directory>` re-scores a saved run under a different policy without executing anything, which is how the paragraph above is checkable rather than assertable. It takes several directories and judges the newest, saying which on stderr, because the glob above matches one only while retention is 1 and `--keep 0` is a documented flag. It refuses a manifest whose artifacts retention has already dropped rather than reporting on files that are not there.

`--key artifact=col,col` matches rows of one artifact on a subset of its exact columns. The columns it leaves out stop deciding what makes a row a row and start being compared as values, which changes the class a difference gets without changing the difference. It is how you say that a surrogate key is not part of the answer.

It refuses a float column, and the reason is worth stating because the flag looks harmless. Matching on a float joins with bit equality, so one ulp of reassociation comes back as a missing row plus an extra row, which no policy can downgrade. The step then reports no drift, and a step with no drift has no reassociation bound to print, so `--key daily_revenue=day,revenue` used to delete the whole bound section including the headroom figure that fails. A flag that quietly removes the tool's own falsifiable check is worse than no flag.

`--no-containment` is the ablation described above. It is the only flag here that changes what gets measured rather than how it is judged, which is why `judge` does not have it: a saved run was executed one way or the other and cannot be re-scored into the other. `--no-amplify` is the second such flag, for the same reason.

`--no-amplify` turns the amplifiers off, and what it takes away with them is the point. No step gets a status, because three of the four are defined by what an amplifier did and the fourth needs a zero under every amplifier. So the report says that in place of the status, and prints the bound the status would have been resting on:

```
amplification is off (--no-amplify), so no status is printed for the 2 steps that never fired:
  0 generate_inputs, 7 roll_up_keys
  Each of those is 4 comparisons on the one input this pipeline was given and nothing else. 4
  clean comparisons rule out a per-comparison divergence probability above 53 percent, 95 percent
  one-sided. NO_DIVERGENCE_OBSERVED needs a zero under every amplifier as well, so it is not
  claimed, and neither is anything weaker.
```

That paragraph exists because two earlier flags failed the same audit. `--key` with a float column deleted the whole reassociation bound section, including the pre-registered check that is allowed to fail in it, and `--tolerance` deleted the sentence explaining why the mechanism story did not hold. A flag that quietly removes the tool's own falsifiable claim is worse than no flag, so there is a test asserting the bound sentence survives `--no-amplify`.

One pass writes a median 376 MB of Parquet under `.twicerun/`, which is gitignored, over 12 passes: 272 MB for the five runs and the single-threaded bisect, and 104 MB more for the amplified inputs and the runs over them. This file said 260 MB before that sample existed, which was the decomposition rather than a measurement. `--no-amplify` takes it back to 272 MB. Retention keeps one directory **per concurrent invocation**, so run it serially and the footprint stays there however many times you run it. `--keep 0` turns pruning off, and `rm -rf .twicerun` reclaims the lot.

Two things that follow from how retention works, both deliberate and neither obvious. Pruning happens at the end of a successful run, not the start, so a run that fails cannot delete the run you would have judged instead, and peak disk during a pass is one directory more than `--keep` says. And a run still writing is never a deletion candidate, because deleting it would pull the Parquet out from under another process.

That second one is a **peak**, not an end state, and this file had it wrong until someone ran it. Three parallel invocations do hold three directories at once, and each report says so in its header, which is the honest thing for a per-invocation view to say. But the last one to finish finds no live markers left, prunes the other two, and the count comes back to `--keep` by itself. So the header's line is true when it prints and stops being true a few seconds later, and the disk does not stay at three times a pass.

The tests run with no credentials and no network:

```bash
uv run pytest
uv run ruff check .
uv run python scripts/check_fingerprint.py
```

CI runs all three. The pre-commit hook runs the second and third, not the tests, because a suite that takes twenty seconds on every commit gets disabled within a day. So a change that passes locally and skips the hook can still fail on push, and the hook is the cheap half rather than the whole gate.

`--disable-socket` is in `addopts`, and `tests/test_socket_is_blocked.py` is what makes it live rather than configured. It opens a TCP socket and asserts `SocketBlockedError: A test tried to use socket.socket.`, which is raised where the call is written rather than after a timeout somewhere in the network stack, so deleting the flag fails the suite rather than quietly switching the network back on. Before that test existed the flag had been checked by hand once and the probe deleted, leaving a compiled `test_zz_socket_probe` in `tests/__pycache__` with no source beside it. It matters more since slice 5, because `scripts/fetch_tlc.py` now exists and does open a socket: `tests/test_fetch_tlc.py` imports seven names from it and calls `download()` in none of them, and the guard is what enforces that rather than the intention.

The hook resolves both tools out of `.venv/bin` and refuses to commit if ruff is missing rather than skipping it. That is not fussiness: it used `command -v ruff`, which on a machine following this README finds nothing at all, because `uv sync` does not put ruff on `PATH`. It skipped silently and let three lint errors through. Where `PATH` did have a ruff it was an unrelated 0.12.0 against the 0.16.4 pinned here, so the hook was running a different linter from CI and never said which. It prints its version now.

If you pipe that into anything, check `PIPESTATUS` or redirect instead. `uv run pytest | tail -5` reports the exit code of `tail`, which is 0 whatever pytest did, and twice during this build a slice was committed against a suite whose failure had been swallowed exactly that way. `uv run pytest >/dev/null 2>&1; echo $?` is what the pre-commit hook and CI effectively do.

The same shape caught someone checking the policy-separation claim by hand. `zsh` does not word-split an unquoted parameter expansion, so `FLAGS="--policy reduction-order"; twicerun judge "$RD" $FLAGS` passes one argument spelled `--policy reduction-order` in `bash` and something else in `zsh`. Their three reports had all run under the default policy and were identical for that reason rather than for the interesting one. Both failures look like a passing check, which is the only thing they have in common and the reason they are written down together.

To re-derive every DuckDB number quoted here on your own machine, which takes about two seconds:

```bash
uv run python scripts/measure_duckdb.py
```

Its counts will not match these, for the same reason the transcript above will not. What holds is the shape: parallel figures large, `threads=1` figures zero, `count()` stable, the single-word tiebreak fix clean, and the `MERGE` bug present at threads=8 and absent at threads=1. The script checks ten such invariants and exits non-zero if any of them breaks, so it fails loudly rather than printing numbers that mean something different from what they say.

## What this costs to run

Measured on 2026-08-26, because the spec estimated it by counting step executions and the estimate was three times out. None of the figures in this section is regenerated: the timings come from an instrumented script that is not committed, and wall clock on a laptop is not a quantity a committed artifact would make honest anyway.

A pass over the reference pipeline is five executions of eight steps, plus five single-threaded executions of each step that fired, plus three to five executions of each step that did not, once per amplifier. That is roughly 65 to 95 step executions against 8 for running the pipeline once. The spec called it 5x to 15x on that arithmetic and the arithmetic is right.

The wall clock is not. Over 20 passes, one pass took a **median 25 times as long as a single execution of the same pipeline**, the extremes being 21x and 43x. The denominator is run 1's own recorded time out of the manifest, because the tool cannot produce it: `--runs 1` exits 2, since one run has nothing to compare against. Where that goes, at the median:

| | share of a pass | in units of one plain execution |
|---|---|---|
| the five runs | 19% | 5.0x |
| the single-threaded bisect | 15% | 4.0x |
| comparing the artifacts | 65% | 16.6x |

**Two thirds of the cost is the oracle, not the re-execution.** Joining two 500,000-row artifacts on a composite key, four times per step, costs more than running the pipeline that produced them. Anyone reasoning about this tool's cost from the number of runs it does will be wrong in the same direction the spec was.

The spread is wide because that two thirds is IO-bound, and it moves with what else the machine is doing rather than with anything in the code. Two sessions of the same measurement gave medians of 25x and 37x on this laptop, and someone else's 28 passes gave 32.5x with the same 19/15/65 split inside it. Treat the multiplier as an order of magnitude, not a figure.

The bisect is the 15% row: 4.0x one execution. Containment added nothing measurable, since it changes which file a read opens and not how much work is done.

### What amplification added

Measured the same way, 12 fresh passes each, alternating so that whatever else the machine was doing lands on both:

| | multiple of one plain execution | slowest of the 12 | seconds | Parquet written |
|---|---|---|---|---|
| `--no-amplify` | median 28.0x | 36.6x | 6.7s | 272 MB |
| with the amplifiers | median 37.2x | 45.0x | 9.0s | 376 MB |

Medians over 12 passes each. The third column is the slowest pass that happened rather than a ceiling, and a thirteenth pass can exceed it: this is wall clock on a laptop with other things running, so it has no bound at all. **Amplification cost 1.33 times the pass**, which is a quarter of the amplified pass and more than the single-threaded bisect's 15 percent.

That ordering is not what the execution counts predict, and the reason is worth having. The bisect re-executed six steps five times each, 30 executions; amplification re-executed two steps three times against each of three amplifiers, 18. It still cost more, because which steps land in which loop is decided by the fire rate and not by what they cost, and on this pipeline the one expensive step is `generate_inputs`, which writes 205 MB, never fires, and therefore always lands in amplification.

The shape of the cost is the same surprise as the pass as a whole. Executing the amplified steps is a **median 6 percent of the amplified pass**, so most of the quarter is materialising the substituted inputs and then comparing the artifacts that come out of them. The oracle again. Reasoning about this tool's cost from the number of executions it does will be wrong in the same direction the spec was.

The two loops do partition the work. Amplification only touches steps that found nothing and the bisect only touches steps that did, so between them every step is re-executed once more and neither covers a step twice.

So this is a thing you run deliberately, before a release or on a schedule. `--runs` is the lever that moves it most, and it moves the miss rate with it. `--no-amplify` is the second lever and it is the one that costs the most evidence for what it saves.

## The condition under which this project is unnecessary

Pre-registered with the rest, and it belongs here rather than in a footnote: **if the naive baseline's false-positive count on correct code had come out at zero, the oracle would be more machinery than the problem needs**, and this section would say so.

It did not, and the baselines table has the current figure. Baseline 1 fired on `mean_basket` in 10 of 10 trials in every one of the five runs before the artifact too, at medians of 1,265, 1,270, 1,269, 1,208 and 1,347 rows that failed to pair, on a step where nothing is wrong. The count is across both sides, so it is out of 2,000 rather than 1,000: a group that differs loses a row each way.

The first four figures were published as `median 1,265 rows unmatched out of 1,000 on each side`, which is a magnitude larger than the ceiling it names, on a line whose own rule is that a ceiling cannot be beaten. The numerator summed the two sides and the denominator took one of them. The eval prints the two sides separately now, against the total of both, and the four figures are unchanged because the quantity was right and only its denominator was wrong.

`src/twicerun/compare.py` is that comparison, kept rather than deleted, and `scripts/eval.py` runs it against each trial's own runs 1 and 2.

**A second and narrower version of this condition was missing until run E, and it is the one that bites.** Baseline 1 gets one comparison where the oracle gets four, so the published gap between them was mostly a run count. Given the same five runs, bit-exact multiset equality agrees with the oracle on every cell of the sensitivity table and fires on no twin, which is the run-matched row of the baselines table. The sensitivity and specificity tables are therefore not evidence for the oracle, and the eval says so in the baseline 1 section on every run. The oracle earns its cost on what it produces beyond a yes or no: the class, the magnitudes, the attributed column, the bound and the cause. If those turn out not to be worth their runtime to anyone, the honest reading of that is the same as this section's.

The two-second version needs none of this repo: `scripts/measure_duckdb.py` re-derives it in raw DuckDB, and it is the better check, because it is a fact about DuckDB rather than about my code.

Stating the condition matters more than the outcome. A tool whose author cannot say what would have made it pointless has not tested the premise, and there are eight of these now, printed on every eval run whatever they say, with a ninth in the baseline 1 section that is not a pre-registered threshold because nobody thought to pre-register it.

## The limitation that goes first, not in a footnote

**twicerun only tests pipelines written against its storage interface.** It does not test arbitrary pipelines.

Watching a pipeline's writes without its cooperation needs either a kernel block-layer wrapper or system-call interception. Both are out of budget and both are worse on macOS. What is left is an interface the pipeline reads and writes through, which is portable, needs no root, and only sees pipelines that opted in.

What that buys back is why it is a design choice rather than a workaround. One abstraction carries five jobs: it is where artifacts get captured, where reads get redirected for containment, where input rows get counted for the tolerance bound, where the column names feeding attribution come from, and where amplified inputs get substituted. A step calling `ctx.write()` gets all five. A step calling `duckdb.execute("COPY ... TO ...")` behind its back gets none.

## What else it gets wrong

**Sorting to pair rows inside a key group is a heuristic once there is more than one float column.** The ordinal sorts both sides the same way, and for a single float column sorted-to-sorted pairing is the assignment that minimises total absolute difference, so it is optimal. With several, a lexicographic sort can pair the wrong two rows inside one key group. That can only understate a difference, so the failure mode is a bounded false negative confined to within-key-group permutations of float-only differences.

**A key group with no exact columns is one big group.** If every column is a float, there is no key, the whole artifact sorts as one group and rows pair by order alone. It is the weakest case here and it is where the heuristic above does the most work, so the report says `matched on no key` when it happens rather than leaving it to be inferred.

**NaN payloads are not distinguished.** The sign survives and the payload does not.

**Two of the published figures cannot go down when the detector gets worse.** Attribution is a rate over findings the oracle produced, and the bound check is a rate over float step-passes that drifted, so both are conditional on something having been found. A detector rewired to report no divergence at all still scores perfectly on both, because neither consults a fire count: they read `step.worst` and `step.rounds` directly. They are quality-of-explanation figures rather than detection figures, and the eval now says so on the attribution line and prints `NOT MEASURED` rather than a zero when there is nothing to score.

**Attribution can name more than one column, and sometimes should.** See above. The tie-break is a heuristic, not a proof.

**`rows_read` is not the term count the bound wants.** Measured above, both directions, with the size of the error printed in every report.

**A `threads=1` rate of 0 of 4 is four comparisons, not a property.** It is reported with its one-sided bound for that reason, and `PARALLEL_ORDER` should be read as the name of a pattern in two measured rates.

**The bisect resolves its reads against run 1 even under `--no-containment`.** A single step cannot be re-executed on its own without something to read, so the ablation ablates the main loop and not the bisect. A step that only inherited a divergence can therefore come out `PARALLEL_ORDER` in an uncontained report, and the report says so where it happens.

**Tie collapse declines more often than it applies.** It targets 500 rows per distinct value and refuses to touch a column that is already at or past that, since raising tie density is the whole job and there is nothing to raise. The twin coverage table above is the count of chances it took against the chances it had. The refusal is printed with its reason on the same line as the amplifier, and the declined amplifier goes in the not-varied list, but a reader skimming for zeros should know that most of the boxes tie collapse leaves are unticked rather than green.

**An amplifier only sees a step's `ctx.read` inputs.** State pulled in through `ctx.state` resolves against the previous run's copy rather than an upstream step's output, so there is nothing for an amplifier to substitute that the step's own last execution did not already decide. On the append bug that is the right answer and on some other shape of bug it may not be.

**Three amplifiers is three, and there is no argument that they are the right three.** Each is aimed at a bug that was measured here. A pipeline whose non-determinism comes from a clock read, a hash seed, a file listing order or a network response gets nothing from any of them, and the not-varied list is where the report admits it.

**The generated tables stop a number drifting and do nothing about the number being small.** `twicerun report` makes a cell a function of a committed measurement. It cannot tell you that ten trials is enough, and the sample above is ten where an earlier version of that table published forty. What it removes is one specific way a table goes false, which is the way all six entries in the section below went false, and the list of what it does not cover is in that section too.

**A committed artifact is a measurement of one laptop.** DuckDB 1.5.5, ten threads, macOS on Apple silicon. The stamp above every table says so, and nothing here calibrates against a second machine or a second DuckDB, which the not-varied list has said since slice 4.

## How the pieces fit

`src/twicerun/storage.py` is the interface. `ctx.read` and `ctx.write` address artifacts by `(run, step index, name)`. The method worth understanding is `ctx.state(name, initial)`, which resolves the *previous run's* copy of an artifact rather than this run's. Without it, a checker starts every run from an empty directory and can never see the two bugs that only exist because a pipeline runs against state its own last execution left behind.

`src/twicerun/oracle.py` is the comparison, and it is the project. Everything in "How the comparison works" above lives here.

`src/twicerun/policy.py` is the decision, kept apart from the comparison on purpose.

`src/twicerun/cause.py` is the second axis: the label vocabulary, the one-sided bound, and the reasoning about what a zero out of four can support. The execution that produces it lives in the runner, because re-executing a pipeline is the runner's job.

`src/twicerun/amplify.py` is the third: the three input transformations, the status vocabulary, and the list of axes this tool does not vary. Each amplifier is a pure function from one Parquet file to another, which is what lets the properties that have to hold every time be tested every time.

`src/twicerun/runner.py` is the loop. Run 1 is the reference and runs 2 to N are each compared against it, giving `k of m`. All-pairs clustering was the alternative, and it is rejected because tolerance-based equality is not transitive, so "how many distinct answers" stops being well defined the moment any tolerance exists.

`src/twicerun/tables.py` renders this file's results tables out of the committed artifacts. It is in the package rather than in `scripts/` because two of its functions are imported back by the eval: a fire-rate distribution and a tally of labels have to read the same in the terminal and in a table, or the two disagree about the same run.

## Why five runs and not two

A two-run tool cannot tell "deterministic" from "non-deterministic and lucky this time". Step 6 of the reference pipeline is the proof. It is the same `row_number()` bug as step 2 at a lower tie density, and its cell in the results table above spans four of the five values a rate out of four can take. At 1 of 4 a two-run checker reports nothing three times in four on a step that is definitely broken, and at a flat 0 of 4 even five runs miss it outright.

How often that flat zero comes up is the quantity nobody can pin down, which is the point rather than a gap in the measurement. Three of the first 10 passes ever taken of this step were flat zeros and none of the next 20 were, measured on 2026-08-26, with nothing changed between them but the sample. The gap table above is 40 more passes on 2026-08-27 and puts it at 5.

With `m` comparisons and a per-comparison divergence probability `p`, a step is missed with probability `(1-p)^m`. At `p = 0.5`, going from two runs to five takes the miss rate from 50 percent to 6.3 percent for 2.5 times the runtime. Going from five to ten takes it to 0.2 percent for twice as much again. The first trade is obviously worth making, the second is a judgement call, so five is the default and `--runs` moves it.

Those zero results are the argument for amplification, and it has a number in the gap table above. More runs lower the miss rate for a given `p` and do not change `p`. Stressing the input changes `p`, and on that step it roughly doubles it.

No practical number of runs proves determinism, which is why the tool never prints the word. There is a test asserting the report contains neither "deterministic" nor "stable".

## Six times a number in this README was beaten by a larger sample

The first five on 2026-08-26, the sixth on the 27th, all while the author was actively trying not to let it happen. They are listed because the tool's whole claim is that a small sample lies to you about a quantity that varies, and this is that claim demonstrated against its own documentation.

| # | what was published | what a larger sample gave |
|---|---|---|
| 1 | `apply_price_updates` fires 4 of 4 every time, from 5 invocations | a floor of 1 within 8 invocations, then 0 within 20 |
| 2 | the slice 2 results table, from 10 passes | 8 of its 10 quantities exceeded within 20 more passes; `daily_revenue` published at 3 to 6 ulp reached 19 |
| 3 | the slice 3 results table, from 20 passes | all 6 of its ranges exceeded within 28 more; `apply_price_updates` published at 1,344 to 42,304 rows reached 77,120 |
| 4 | the tight reassociation bound is under 1000x on 40 of 40 step-passes | it cleared on 3 of 56, then 1 of a later 40, then 5 of a later 80 |
| 5 | the slice 4 results table, from 20 passes | 13 of its figures exceeded by one 40-pass run that took six minutes, in a fresh clone |
| 6 | a fire-rate distribution over 40 passes, printed with the buckets that came up | a later ten-trial eval gave `apply_price_updates` the `0 of 4` the cell had not mentioned |

Number 4 is the worst of them, because it is the falsifiable check the tolerance section is built around, and because the report contradicted itself in the terminal: it printed that the tight check "is expected to fail" three lines above a step-pass where it cleared. That was fixed by replacing a wrong prediction with a number measured on this laptop and printed on every machine as "here", so the next reader got a report claiming 1,229x three lines above its own 1,245x. The same contradiction, twice, from two different attempts to state a historical fact in a live report. The report now counts the run in front of you and the history stays in this file where it can carry a date.

**Number 5 is the one that changed the method, because after four rounds of "the sample was too small again" that had stopped being a diagnosis.** The republished bracket that got beaten was `median 4 ulp [3 to 6]`, and three screens below it this very table already recorded that the same bracket had reached 19. The file contradicted itself, in writing, about the one quantity it had promised to be careful with.

The structural reason: **the ulp count and the relative drift are maxima**, over 1,000 groups times four comparisons. They are extreme-value statistics. The observed range of a maximum grows with the number of observations by construction, so there is no sample size at which a bracket on one becomes safe, and every previous fix had been to take a bigger sample and publish a wider bracket. That is the same move five times.

So the maxima carry a median and an n and no range at all, and the quantities with a real ceiling carry the ceiling instead: a step comparing 500,000 rows cannot lose more than 500,000 of them, and a fire rate out of four has five possible values, so the whole distribution goes in the cell.

**Number 6 is that method failing in a new shape**, which is why it is a row and not a footnote on number 5. The sentence that used to stand here read "the whole distribution goes in the cell and there is nothing left to beat". What went in the cell was every rate that *came up*, so `apply_price_updates` printed as `4 of 4 x27, 3 x5, 2 x5, 1 x3` and never mentioned zero. Four ten-trial runs of the eval later, one gave that step a flat `0 of 4`. Nothing about the earlier figures was wrong: those forty passes really did produce twenty-seven, five, five and three. The claim of completeness was wrong, because an omitted bucket reads as impossible when it only means unobserved. So every bucket prints now, including the ones at zero, and a step whose rate never varied collapses to `0 of 4 on all 10`, which accounts for every trial and leaves nothing unstated either.

**Six corrections, and every one of them was a figure retyped out of a terminal into a table.** That is the pattern the sixth one finally made visible, because the diagnosis for the first five was always the sample and the sixth had a correct sample and a wrong claim about it. A number that is transcribed can drift from the code that produced it in two ways at once: the code moves and nobody re-transcribes, or the sample grows and nobody notices the old cell is now false.

Slice 7 is the structural answer and it is narrower than it sounds. The tables above are rendered from `results/eval-2026-08-27.json` and `results/gap-2026-08-27.json` by `twicerun report`, and a test fails the build when this file stops matching them. That does not make a figure right and it does not make ten trials into forty. It removes transcription as a way for a table to become false, which is the failure mode that produced all six rows above, and it leaves the sample size as the one that is left.

### What is not regenerated, and when it was measured

Everything below is a measurement taken once, on a date, and transcribed. None of it comes out of a committed artifact, and each is here because deleting it would lose something a reader wants:

- **The transcripts**, 2026-08-26. The whole pass at the top of this file, and the four-line bound excerpt in the tolerance section. Both are illustrations of the output format rather than results, and both say so where they appear.
- **The five eval runs A to E**, 2026-08-26. Their JSON was never written, which is the defect this slice fixes. They are kept because five runs of the same code disagreeing about the headline number is the strongest evidence in the file for its own thesis.
- **The per-comparison progression of the append bug**, 3,953 to 15,812 across four comparisons, 2026-08-26, over 10 uncontained and 20 contained passes. The eval keeps the loudest comparison per step, so the progression is not in the artifact.
- **The twins over 12 and 40 passes**, 2026-08-26. The committed artifact's twin coverage is 10 trials of 6 steps.
- **Every `threads=1` figure was 0 of 4 or 4 of 4 and never anything between**, across 636 bisected step-passes counted on 2026-08-26: 166 during slice 3, 237 in a fresh clone, 233 in a 40-pass sweep. The count is transcribed; the property is not, because the third column of the results table above is generated and every cell in it is still a zero or a four. It is the one quantity here a larger sample has never moved, it was not designed, and it is not explained.
- **The cost measurements**, 2026-08-26. The 25x median multiplier, the 19/15/65 split, and the `--no-amplify` comparison over 12 passes each. Wall clock on a laptop, and the section says what that is worth.
- **The TLC backfill results and the row-count switch-on table**, 2026-08-26. These need the three TLC partitions, which are not committed and are not fetched in CI, so nothing here can regenerate them.
- **The DuckDB constructions in `scripts/measure_duckdb.py`**, re-derivable in about two seconds by running it. Its ten invariants fail loudly rather than printing figures that mean something else.

## The reference pipeline ships broken

A checker that finds nothing is indistinguishable from a checker that is broken. `pipelines/reference.py` carries four bugs, one step that drifts benignly, one step that fires intermittently, one control step that must never fire, and one step whose only job is to sit downstream of a bug so the containment ablation has something to measure.

`pipelines/twins.py` is the matched half: the same four bugs plus the intermittent one, each with the single line that fixes it. It is what the eval's specificity half scores against, at zero fires across every comparison in the table under "the eval" above, and it was already load-bearing before that, because an amplifier that made those twins fire would be a chaos generator rather than a detector.

Two of the eight steps are constructed rather than observed, and both say so where they are defined: the `MERGE` bug below, and `roll_up_keys`, which exists to be fed a diverging artifact. Three of the four bugs are failures Vishal has actually been bitten by running Databricks pipelines and SQL migrations: duplicate rows after a retry, IDs changing between runs, and totals not matching between runs. The fourth, the non-idempotent `MERGE`, is not a war story. It came out of an experiment for this project and he has never seen it. Its distinction is how narrow its window turned out to be: it needs a target somewhere around 100,000 to 125,000 rows, a source staged into a real table and `BIGINT` columns, and at 50,000 rows and again at 200,000 it gives the same answer every time.

Every configuration in the file was re-derived on this machine before it was written down, and two of the spec's claims did not survive that. The `MERGE` bug's cause is parallel order rather than the single-threaded persistence the spec predicted, because `threads=1` was clean at every scale tried. And at the three rows the spec proposed for it, it gives the same answer ten times out of ten.

## What this does not do

No `dbt`, Airflow, Dagster or Prefect adapter. No Spark. No syscall interception. No nested type comparison. No web UI. No fix generation.

No model calls either: no LLM adapter, no `openai` dependency, no `AZURE_OPENAI_*` configuration. A non-deterministic output layer on a tool whose premise is determinism is a contradiction that someone would notice, and nothing here needs one.

**No `(class, cause)` hint table, which the spec wanted and slice 6 was going to carry.** Two hints exist and both are one line of static text: `WALL_CLOCK` when a timestamp column moved, which is the commonest reason a step never reproduces, and a line naming an artifact one run wrote and the other did not. Mapping the five classes against the two causes to a suggested fix was fifteen lines of lookup, and it went with the slice.

## Data

The reference pipeline runs on generated data, and synthetic is the point here rather than a convenience. Its bugs are parameterised by tie density, group count and row count, and those parameters are the experiment. Real data would fix the tie density at whatever the file happens to contain and make the intermittency measurement impossible to produce.

Values come from `hash(i)` rather than `random()`, so all five runs read byte-identical inputs and any divergence is the pipeline's rather than the data's.

The other pipeline runs on NYC TLC trip records, which is the next section.

## NYC TLC, and the licence position stated rather than implied

**TLC publishes no licence for the trip records.** Searching the trip record page for the word finds only "base license number", which is a field in the data. What the page carries instead, verbatim, is:

> The data used in the attached datasets were collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP). The trip data was not created by the TLC, and TLC makes no representations as to the accuracy of these data.

That disclaims responsibility. It grants nothing, and it says nothing about redistribution either way.

So **no TLC bytes are committed here**. `scripts/fetch_tlc.py` is the only file in this repository that opens a socket. It fetches three monthly Parquet files and verifies a SHA-256 recorded on 2026-08-26 against each, so a reader can tell "the download was cut short" from "TLC restated the month", which are the same missing bytes to everything else. TLC does restate months: the yellow 2024-12 file was last modified 2025-02-21.

```bash
uv run python scripts/fetch_tlc.py                 # green, 3.6 MB
uv run python scripts/fetch_tlc.py --taxi yellow   # yellow, 181 MB
uv run python scripts/fetch_tlc.py --check         # verify what is on disk, fetch nothing
```

Nothing in `tests/` imports it for a download, and `--disable-socket` would fail the test if anything tried. What the suite runs against instead is three partitions generated from `pipelines/tlc_green_schema.sql`, which holds the real column lists read off the real Parquet with `DESCRIBE` rather than transcribed from the data dictionary PDF. `RatecodeID` is `BIGINT` and `PULocationID` is `INTEGER` in the same file, which is the kind of thing a hand transcription gets wrong.

Sources: the [trip record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and the [green data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf), updated 2025-03-18 to document `cbd_congestion_fee`.

### The schema change, which is real and was not invented here

TLC added `cbd_congestion_fee` to the Yellow, Green and High Volume FHV trip records for 2025 onward, carrying the Congestion Relief Zone charge that took effect on 5 January 2025. So a backfill over 2024-12, 2025-01 and 2025-02 spans a column that two of its three partitions have and one does not. Green goes from 20 columns to 21, yellow from 19 to 20.

```bash
uv run python scripts/tlc_schema.py
```

```
  green_tripdata_2024-12.parquet       53,994 rows  20 columns  cbd_congestion_fee: absent
  green_tripdata_2025-01.parquet       48,326 rows  21 columns  cbd_congestion_fee: present
  green_tripdata_2025-02.parquet       46,621 rows  21 columns  cbd_congestion_fee: present

read_parquet over the three as one list, which is what a backfill hands it:
  oldest partition first  20 columns  cbd_congestion_fee: DROPPED, silently
  newest partition first  21 columns  cbd_congestion_fee: present
```

**That is a revenue column disappearing on file order, with no error and no warning.** DuckDB takes the column set from the first file in the list. Put the 2024 partition first, which is the order a backfill walks its months in, and the fee is gone from the answer. Reverse the list and it is back. The two spellings that do not do that are `INSERT INTO` a target built from the older partition, which raises `Binder Error: table target has 20 columns but 21 values were supplied`, and `UNION ALL BY NAME`, which fills the missing column with NULL.

**twicerun cannot catch it, and that is the sharpest limitation this project has.** The wrong answer is wrong the same way on every run, so five runs agree with each other perfectly and the report is a clean zero. A rerun checker is blind to a deterministic wrong answer by construction. `scripts/tlc_schema.py` asserts its three facts and exits non-zero if any stops being true, which is the most this repository can do about a bug its main tool is structurally unable to see.

`UNION ALL BY NAME` costs something too, and the script prints it. After the union, a NULL fee on a 2024 row means the column did not exist, and a NULL fee on a 2025 row means the trip was never charged. 46,490 of 48,326 January rows carry a fee. Nothing distinguishes the two kinds of NULL afterwards.

### What twicerun found on the backfill

`pipelines/tlc_backfill.py` is four steps: land the three months keeping each one's own column set, union them by name, sum the fares by partition and pickup zone, then append to a cumulative ledger that never asks what is already in it.

```bash
uv run python scripts/fetch_tlc.py
uv run twicerun run pipelines/tlc_backfill.py
```

| | green, 148,941 rows | yellow, 10,721,140 rows |
|---|---|---|
| `land_partitions` | `0 of 4`, `NO_DIVERGENCE_OBSERVED` | `0 of 4`, `NO_DIVERGENCE_OBSERVED` |
| `unify_partitions` | `0 of 4`, `NO_DIVERGENCE_OBSERVED` | `0 of 4`, `NO_DIVERGENCE_OBSERVED` |
| `zone_revenue` | `0 of 4`, `NO_DIVERGENCE_OBSERVED` | `4 of 4`, `VALUE_DRIFT`, `PARALLEL_ORDER`, 498 of 776 zone totals moved |
| `revenue_ledger` | `4 of 4`, `MULTIPLICITY`, `PERSISTS_SINGLE_THREADED`, 657 extra rows | `4 of 4`, same, 776 extra rows |
| wall clock, disk | 3.7s, 59 MB | 124s, 3.0 GB |

**The negative result is the more useful half.** The parallel float reduction that this whole project is built around does not fire on a month of green taxi trips. It is not absent, it is under the row count at which DuckDB divides the work. Cross-joining the green union against `range(k)` and comparing `sum(total_amount)` grouped by pickup zone, four comparisons at each size, 236 groups:

| rows | groups that differed, four comparisons |
|---|---|
| 148,941 | 0, 0, 0, 0 |
| 297,882 | 41, 0, 0, 0 |
| 595,764 | 60, 61, 60, 77 |
| 1,191,528 | 103, 111, 113, 123 |
| 2,383,056 | 149, 156, 144, 151 |
| 4,766,112 | 189, 175, 167, 183 |
| 9,532,224 | 197, 196, 198, 195 |

Those are the observations rather than a summary of them. The switch-on sits between 148,941 and 595,764 rows on this machine, and **297,882 is exactly what the row-multiplication amplifier produces from the green input**, which is why the green report shows that amplifier at `0 of 2` on a step whose mechanism is present.

Two consequences worth being plain about. The reference pipeline's 2,000,000 rows are not decoration: at a tenth of that the same bug is silent. And the one bug of the four that fires on green is the ledger, because an append with no key duplicates whatever it is given at any scale, so a reader who runs the 3.6 MB dataset still sees a real bug caught on real data.

On yellow the surrogate-key mechanism is there too, though this pipeline does not build a surrogate key: `row_number() OVER (ORDER BY PULocationID)` over the 10,721,140 rows gave 10,720,819, 10,720,956 and 10,720,930 rows a different key across three comparisons at `threads=10`, and 0 at `threads=1`. Ordering by the pickup timestamp instead, which is the more natural thing to write, gave 3,401,785, 3,438,723 and 3,884,017. Adding a unique tiebreak gave 0 every time.

**3.0 GB is the yellow figure and it is not a typo.** Five runs of a step that writes 10.7 million rows, plus the bisect and the amplified inputs. Green is 59 MB. `rm -rf .twicerun` reclaims it.
