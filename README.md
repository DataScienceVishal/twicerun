# twicerun

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
artifacts  .twicerun/run-20260826-131018
retention  keeping 1 run directory

  0 generate_inputs         0 of 4
  1 daily_revenue           4 of 4  VALUE_DRIFT  cause PARALLEL_ORDER
      daily_revenue: 714 of 1,000 paired rows moved on revenue
      furthest move over 4 comparisons: revenue, 4 ulp and 4.77e-16 relative
      488567.08646427933 against 488567.0864642791
  2 customer_keys           4 of 4  ROW_MISSING ROW_EXTRA  cause PARALLEL_ORDER
      customer_keys: 491,520 of 500,000 reference rows and 491,520 later rows found no partner
      dropping surrogate_id from the key takes unmatched reference rows from 491,520 to 0
      event_id does the same, so surrogate_id is named first because it is the one no input to this step carries
  3 apply_price_updates     2 of 4  ROW_MISSING ROW_EXTRA  cause PARALLEL_ORDER
      prices: 18,720 of 125,000 reference rows and 18,720 later rows found no partner
      dropping price_cents from the key takes unmatched reference rows from 18,720 to 0
  4 append_audit_log        4 of 4  MULTIPLICITY  cause PERSISTS_SINGLE_THREADED
      audit_log: 3,953 later rows found no partner, against 3,953 reference rows
  5 mean_basket             4 of 4  VALUE_DRIFT  cause PARALLEL_ORDER
      mean_basket: 625 of 1,000 paired rows moved on mean_amount
      furthest move over 4 comparisons: mean_amount, 4 ulp and 4.61e-16 relative
      246.6479063732773 against 246.6479063732774
  6 sparse_customer_keys    1 of 4  ROW_MISSING ROW_EXTRA  cause PARALLEL_ORDER
      sparse_customer_keys: 237,280 of 500,000 reference rows and 237,280 later rows found no partner
      dropping surrogate_id from the key takes unmatched reference rows from 237,280 to 0
      event_id does the same, so surrogate_id is named first because it is the one no input to this step carries
  7 roll_up_keys            0 of 4

cause, from re-executing each divergent step 5 times at threads=1:
  1 daily_revenue         PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1
  2 customer_keys         PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1
  3 apply_price_updates   PARALLEL_ORDER            2 of 4 at threads=10, 0 of 4 at threads=1
  4 append_audit_log      PERSISTS_SINGLE_THREADED  4 of 4 at threads=10, 4 of 4 at threads=1
  5 mean_basket           PARALLEL_ORDER            4 of 4 at threads=10, 0 of 4 at threads=1
  6 sparse_customer_keys  PARALLEL_ORDER            1 of 4 at threads=10, 0 of 4 at threads=1

  Both rates are out of 4, which is what lets them be read against each other.
  PARALLEL_ORDER means the step stopped diverging with one thread. That is 4 clean comparisons and no
  more than that: the 95 percent one-sided upper bound it leaves on the per-comparison rate is
  53 percent. The bound also assumes an independence these runs do not have, since they
  share a process, a page cache and a machine.
  PERSISTS_SINGLE_THREADED means the thread count is not the explanation. The tool stops there rather
  than guessing between a clock read, a data-dependent branch, appended state and something
  outside the pipeline.

reassociation bound, computed rather than picked:
  1,000x of headroom was fixed before any of this was written and has not moved since.
  Below is that one check at two choices of n. The first is the count the spec settled on and carries a
  factor of the output row count in slack. The second is the terms behind one output value, has no slack
  in it, so it is normally the one that fails: it cleared on 1 of 40 step-passes here, at 1,229x. Both
  print so the slack is visible rather than described.
  1 daily_revenue
      observed furthest relative drift 4.7656e-16
      n = 2,000,000 rows read by the step, bound 4.4409e-10, headroom 931,868x  CLEARS
      n = 2,000 terms per output row, bound 4.4409e-13, headroom 932x  FAILS
  5 mean_basket
      observed furthest relative drift 4.6093e-16
      n = 2,000,000 rows read by the step, bound 4.4409e-10, headroom 963,468x  CLEARS
      n = 2,000 terms per output row, bound 4.4409e-13, headroom 963x  FAILS

6 of 8 steps diverged in 6.8s.
```

Step 5 is a correct float average. All 625 of those findings are the arithmetic behaving normally, and `--policy reduction-order` is the opt-in that says so.

Step 4 is the one to read twice. `cause PERSISTS_SINGLE_THREADED` next to `4 of 4 at threads=1` is the tool saying the thread count is not the problem, on the one bug in the file that a rerun causes rather than parallelism.

**Your numbers will not match that transcript, and neither will mine on the next run.** This is a tool about non-determinism and its own output is non-deterministic, so quoting any single figure as fixed would be the wrong thing to do twice over.

So the table below publishes **medians over 20 passes of the five-run loop**, which is 80 comparisons per step, on DuckDB 1.5.5 at `threads=10`. The spread each quantity showed is in brackets and **it is not a bound**. Every range this file has published has been beaten by a later sample, four times now, and the section at the end of this README lists all four with dates. Take the ranges as what 20 passes happened to produce and expect to go outside them in ten minutes.

| step | fires under `strict` | at `threads=1` | under `reduction-order` | what the oracle called it |
|---|---|---|---|---|
| 0 `generate_inputs` | 0 of 4, every pass | not bisected | 0 of 4 | the control, and it has never fired |
| 1 `daily_revenue` | 4 of 4, every pass | 0 of 4, every pass | 0 of 4 | `VALUE_DRIFT`, `PARALLEL_ORDER`, median 4 ulp [3 to 6] |
| 2 `customer_keys` | 4 of 4 [2 to 4] | 0 of 4, every pass | unchanged | `ROW_MISSING` `ROW_EXTRA`, `PARALLEL_ORDER`, median 368,640 rows [245,760 to 491,520] |
| 3 `apply_price_updates` | 3 of 4 [0 to 4] | 0 of 4 on all 19 that fired | unchanged | `ROW_MISSING` `ROW_EXTRA`, `PARALLEL_ORDER`, median 17,376 rows [1,056 to 40,608] |
| 4 `append_audit_log` | 4 of 4, every pass | **4 of 4, every pass** | unchanged | `MULTIPLICITY`, `PERSISTS_SINGLE_THREADED`, 3,953 extra rows in every comparison |
| 5 `mean_basket` | 4 of 4, every pass | 0 of 4, every pass | 0 of 4 | `VALUE_DRIFT`, `PARALLEL_ORDER`, median 4 ulp [4 to 6] |
| 6 `sparse_customer_keys` | 2 of 4 [0 to 4], three flat zeros | 0 of 4 on all 17 that fired | unchanged | `ROW_MISSING` `ROW_EXTRA`, `PARALLEL_ORDER`, median 237,280 rows [8,480 to 483,040] |
| 7 `roll_up_keys` | 0 of 4, every pass | not bisected | 0 of 4 | downstream of step 2, and contained, so it sees the same input every run |

The two float steps go to zero under `reduction-order` and nothing else moves. That is the whole claim for the oracle: the false positives disappear and the four real bugs are caught by the same code that dismissed them.

The `threads=1` column is what slice 3 added, and one row of it is not like the others. Six steps stop diverging with one thread and one does not, which is the difference between a step whose answer depends on how the work was divided and a step whose answer depends on it having run before. No number of runs separates those two; a second thread count does it in one column.

Every `threads=1` figure here was either 0 of 4 or 4 of 4, never anything between, across 166 bisected step-passes. That was not designed and it is not explained.

## Status

Slice 3 of 7. What runs today: the storage interface, the run layout, the artifact manifest, the five-run loop, the typed oracle, leave-one-out attribution, the policy layer, containment, and the single-threaded bisect.

What does not exist yet, in the order it arrives: input amplification and the four statuses (slice 4), the NYC TLC backfill and the eval numbers (slice 5), crash injection (slice 6), and the report generator that keeps this file's tables honest (slice 7).

Slice 2 shipped with a disclosure in the header of every report that downgraded anything, because `reduction-order` is a conjunction of three conditions and only two of them existed:

```
policy     reduction-order, so drift inside the reassociation bound is TOLERATED
           condition 2 of 3, that the step does not diverge at threads=1, is not implemented until slice 3. Every TOLERATED below rests on the other two
```

That line is gone, because the condition it stood in for is now measured and enforced. What a `TOLERATED` claims changed with it. It used to mean the drift was float-only and small enough that reassociation could account for it. It now means the drift also disappeared when the parallelism did, which is the difference between "small enough to be reassociation" and "demonstrably is reassociation". A step drifting inside the bound that keeps drifting at `threads=1` is now reported, with that rate as the reason.

## Containment, and what it is worth

Runs 2 to N resolve every read against run 1's artifacts. A step that reads a diverging step's output therefore reads the same bytes every time, so it reports what it did rather than what the step above it did.

The idea is [Spot's](https://academic.oup.com/gigascience/article/9/12/giaa106/5998300) (Salari, Kiar, Lewis, Evans and Glatard, GigaScience 9(12), [arXiv:2006.04684](https://arxiv.org/abs/2006.04684)), which compares two conditions of a neuroimaging pipeline "in a step-by-step execution that prevents the propagation of differences in the pipeline", and does it by copying the first condition's output files into the second. The borrowing is the idea and not the implementation: Spot's tool used ReproZip syscall interception, has not been touched since 2020, and works on a domain unrelated to this one. Here artifacts are already addressed by `(run, step index, name)`, so containment is a dictionary lookup rather than a file copy, and it costs nothing measurable.

`--no-containment` runs the ablation, so the number below is something you can reproduce rather than a claim to take on trust.

On the reference pipeline the step it changes most is `append_audit_log`, the append with no unique key. Two steps read their own last output through `ctx.state`, and this is the one where the difference is a clean number: `apply_price_updates` merges over its previous catalogue and the divergence is mostly overwritten each run, while an append keeps every copy. Extra rows reported per comparison, against a 3,953-row reference:

| | comparison 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| `--no-containment`, 10 passes | 3,953 | 7,906 | 11,859 | 15,812 |
| contained, 20 passes | 3,953 | 3,953 | 3,953 | 3,953 |

Both rows held on every pass. The uncontained row is four reruns' worth of duplication charged to one step: run 4 appends to run 3's log, which already had run 2's in it. The contained row is what one rerun of that step actually does.

Running the ablation shows you the loudest of those four figures, not all four: the report prints one comparison per step and 15,812 is the one it picks. The other three are in the manifest, and this prints the row counts they come from:

```bash
uv run python -c "import json,sys; print([s['artifacts'][0]['rows'] for r in json.load(open(sys.argv[1]))['runs'] for s in r['steps'] if s['name']=='append_audit_log'])" .twicerun/run-*/manifest.json
```

Uncontained that gives `[3953, 7906, 11859, 15812, 19765]`, one run per entry, and contained it gives `[3953, 7906, 7906, 7906, 7906]`. The bug is the same bug either way and the fire rate is 4 of 4 either way, so what containment bought here is the magnitude being a fact about the step rather than about how many times the tool ran.

The second number is the count of steps reported divergent, and getting it took a change to the reference pipeline that is worth being explicit about. Every step in that file read the control step's artifacts, which never differ, so nothing in it ever read a diverging artifact and the cascade could not happen. The ablation scored zero on the quantity slice 5's baseline 3 was pre-registered to use, which reads as evidence against a feature that was simply never exercised.

So `roll_up_keys` exists. It reads `customer_keys`, whose surrogate ids shuffle between runs, and computes an integer minimum that cannot reassociate, so a fire rate above zero there means it was fed something different and never that it computed something different. **It is constructed to exercise containment and is not a failure anyone here has been bitten by**, unlike three of the four bugs beside it. Over 20 contained and 10 ablated passes:

| | `roll_up_keys` fires | steps reported divergent |
|---|---|---|
| `--no-containment` | 4 of 4 [3 to 4], on all 10 passes | 7 of 8 on 9 passes, 6 of 8 on one |
| contained | 0 of 4 on all 20 passes | 6 of 8 on 16 passes, 5 of 8 on four |

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

**A zero at `threads=1` is not proof of anything, and the report says so on the line above the rates.** Four clean comparisons put a 95 percent one-sided upper bound of 53 percent on the per-comparison rate, which is the same arithmetic that argues for five runs rather than two, applied to the bisect's own evidence. It also assumes the comparisons are independent, and they are not: they share a process, a page cache and a machine. So `PARALLEL_ORDER` is a reading of two measured rates, not a finding that single-threaded execution cannot diverge, and a test asserts that neither shape of report contains the words deterministic, stable, reproducible or passed.

The bisect starts each single-threaded sequence from no carried state, exactly as run 1 of the main loop did, and its later runs carry run 1's artifacts with anything the bisect itself re-produced laid over the top. Both halves of that were wrong once and each cost the same mislabelling.

Seeding the sequence from run 1's state was the first implementation: all five single-threaded executions of `append_audit_log` then read the same log, agreed with each other, and the step came out `PARALLEL_ORDER`. Duplicating a log on rerun has nothing to do with threads. Carrying only what the bisect re-produced was the second: a step carrying state under a name a *non-divergent* step wrote found nothing there, because the bisect skips steps that did not fire, so all five runs fell back to the seed and agreed for the same empty reason. The regression test for the first shape could not catch the second, because its state name and its write name belong to one step.

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

**It passes, and the margin is not where it looks.** Over the same 20 passes, 40 float step-passes in all:

| | `daily_revenue` | `mean_basket` |
|---|---|---|
| observed furthest relative drift | 4.6e-16 to 2.2e-15 | 4.6e-16 to 8.1e-16 |
| bound at `n` = 2,000,000 rows read | 4.44e-10 | 4.44e-10 |
| headroom | roughly 200,000 to 1,000,000x | roughly 550,000 to 970,000x |
| bound at `n` = 2,000 terms per output row | 4.44e-13 | 4.44e-13 |
| headroom | roughly 200 to 960x | roughly 550 to 970x |

The check as pre-registered cleared 1000x on all 40 step-passes of the latest sample, at a median 931,534x. Almost none of that margin comes from the drift being small. `rows_read` is the step's whole input, and each of the 1,000 output rows sums about 2,000 terms, so `n` is a thousand times larger than the quantity the bound is about. Take that slack out and the median headroom is **932x, which is under the 1000x line**. It is not always under it: 1 of those 40 step-passes cleared, at 1,229x, and an earlier 56-step-pass sample cleared on 3. So the tight check fails most of the time rather than every time, and the report says that rather than predicting a failure it then contradicts three lines further down.

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

It also overcounts, in a different sense. "Brought into scope" is not "terms behind one output value", and for a group-by the gap is the group count. That makes the bound too loose and could tolerate a difference reassociation cannot explain, which is the unsafe direction, and for an aggregate it is the larger of the two by far. That is the error the second row of the table above measures.

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

## Measuring and deciding are separate stages

Numbers must not move when the policy does. If a tolerance can change a reported figure, everything downstream of it is negotiable and none of it is worth printing.

So `oracle.py` produces findings, `measurement.py` collects them per step, `policy.py` reads that and returns a verdict, and nothing writes back.

Running `twicerun run` twice under two policies does not show you that, because each invocation runs the pipeline again and the figures move between them for exactly the reason this tool exists. `twicerun judge` scores a run that already happened and executes nothing:

```
$ uv run twicerun run pipelines/reference.py --runs 3
$ uv run twicerun judge .twicerun/run-20260826-124119 --policy strict
  1 daily_revenue    2 of 2  VALUE_DRIFT PARALLEL_ORDER
      daily_revenue: 711 of 1,000 paired rows moved on revenue
      furthest move over 2 comparisons: revenue, 5 ulp and 5.88e-16 relative
      494720.69950346916 against 494720.6995034689

$ uv run twicerun judge .twicerun/run-20260826-124119 --policy reduction-order
  1 daily_revenue    0 of 2  VALUE_DRIFT PARALLEL_ORDER TOLERATED on 2 of 2
      daily_revenue: 711 of 1,000 paired rows moved on revenue
      furthest move over 2 comparisons: revenue, 5 ulp and 5.88e-16 relative
      494720.69950346916 against 494720.6995034689
```

Same 711, same 5 ulp, same 5.88e-16, same pair of values. The fire rate moves and nothing under it does. A test compares those detail lines rather than describing them.

The bisect is measurement too, so it holds still as well. Judging that one saved run under `strict`, under `reduction-order` and under `--tolerance-ulps 4` gave 21 identical detail lines and an identical cause table across all three, with only the fire rate and the `TOLERATED` count moving. `judge` re-derives the `threads=1` rate from the saved single-threaded artifacts rather than reading a number out of the manifest, so it goes through the same comparison code the run did.

## Running it

Needs [uv](https://docs.astral.sh/uv/) and nothing else. Python, DuckDB and the dev tools all come from `uv sync`. There is no published remote yet, so from the directory holding this file:

```bash
uv sync --all-extras
./scripts/install-hooks.sh

uv run twicerun run pipelines/reference.py
uv run twicerun judge .twicerun/run-* --policy reduction-order   # newest, if the glob matches several
uv run twicerun run pipelines/reference.py --no-containment
```

Exit codes are 0 for nothing diverged, 1 for something diverged, 2 for bad input and 3 for a crash. 2 covers a column the oracle refuses to compare and a `--key` naming a column that is not there, because both are facts about the pipeline's output rather than crashes. 1 means divergence and only divergence, so a release gate keyed on it does not also trip on a broken pipeline. A comparison downgraded to `TOLERATED` does not set it.

`twicerun judge <run directory>` re-scores a saved run under a different policy without executing anything, which is how the paragraph above is checkable rather than assertable. It takes several directories and judges the newest, saying which on stderr, because the glob above matches one only while retention is 1 and `--keep 0` is a documented flag. It refuses a manifest whose artifacts retention has already dropped rather than reporting on files that are not there.

`--key artifact=col,col` matches rows of one artifact on a subset of its exact columns. The columns it leaves out stop deciding what makes a row a row and start being compared as values, which changes the class a difference gets without changing the difference. It is how you say that a surrogate key is not part of the answer.

It refuses a float column, and the reason is worth stating because the flag looks harmless. Matching on a float joins with bit equality, so one ulp of reassociation comes back as a missing row plus an extra row, which no policy can downgrade. The step then reports no drift, and a step with no drift has no reassociation bound to print, so `--key daily_revenue=day,revenue` used to delete the whole bound section including the headroom figure that fails. A flag that quietly removes the tool's own falsifiable check is worse than no flag.

`--no-containment` is the ablation described above. It is the only flag here that changes what gets measured rather than how it is judged, which is why `judge` does not have it: a saved run was executed one way or the other and cannot be re-scored into the other.

One pass writes about 260 MB of Parquet under `.twicerun/`, which is gitignored: 205 MB for the five runs and 53 MB for the single-threaded bisect. Retention keeps one directory **per concurrent invocation**, so run it serially and the footprint stays at roughly 260 MB however many times you run it. `--keep 0` turns pruning off, and `rm -rf .twicerun` reclaims the lot.

Two things that follow from how retention works, both deliberate and neither obvious. Pruning happens at the end of a successful run, not the start, so a run that fails cannot delete the run you would have judged instead, and peak disk during a pass is one directory more than `--keep` says. And a run still writing is never a deletion candidate, because deleting it would pull the Parquet out from under another process, so three parallel invocations leave three directories and 778 MB. The header says when that happened rather than repeating a promise it suspended.

The tests run with no credentials and no network:

```bash
uv run pytest
uv run ruff check .
uv run python scripts/check_fingerprint.py
```

CI runs all three and so does the pre-commit hook, so a change that passes only the first will fail on push. `check_fingerprint.py` reads `BANNED.md` and refuses the writing tells listed there.

If you pipe that into anything, check `PIPESTATUS` or redirect instead. `uv run pytest | tail -5` reports the exit code of `tail`, which is 0 whatever pytest did, and twice during this build a slice was committed against a suite whose failure had been swallowed exactly that way. `uv run pytest >/dev/null 2>&1; echo $?` is what the pre-commit hook and CI effectively do.

To re-derive every DuckDB number quoted here on your own machine, which takes about two seconds:

```bash
uv run python scripts/measure_duckdb.py
```

Its counts will not match these, for the same reason the transcript above will not. What holds is the shape: parallel figures large, `threads=1` figures zero, `count()` stable, the single-word tiebreak fix clean, and the `MERGE` bug present at threads=8 and absent at threads=1. The script checks ten such invariants and exits non-zero if any of them breaks, so it fails loudly rather than printing numbers that mean something different from what they say.

## What this costs to run

Measured, because the spec estimated it by counting step executions and the estimate was three times out.

A pass over the reference pipeline is five executions of eight steps, plus five single-threaded executions of each step that fired. That is 65 to 70 step executions against 8 for running the pipeline once, so **8.1x to 8.8x by step count**. The spec called it 5x to 15x on that arithmetic and the arithmetic is right.

The wall clock is not. Over 20 passes, one pass took a **median 25 times as long as a single execution of the same pipeline**, the extremes being 21x and 43x. The denominator is run 1's own recorded time out of the manifest, because the tool cannot produce it: `--runs 1` exits 2, since one run has nothing to compare against. Where that goes, at the median:

| | share of a pass | in units of one plain execution |
|---|---|---|
| the five runs | 19% | 5.0x |
| the single-threaded bisect | 15% | 4.0x |
| comparing the artifacts | 65% | 16.6x |

**Two thirds of the cost is the oracle, not the re-execution.** Joining two 500,000-row artifacts on a composite key, four times per step, costs more than running the pipeline that produced them. Anyone reasoning about this tool's cost from the number of runs it does will be wrong in the same direction the spec was.

The spread is wide because that two thirds is IO-bound, and it moves with what else the machine is doing rather than with anything in the code. Two sessions of the same measurement gave medians of 25x and 37x on this laptop, and someone else's 28 passes gave 32.5x with the same 19/15/65 split inside it. Treat the multiplier as an order of magnitude, not a figure.

The bisect is the 15% row: 4.0x one execution. Containment added nothing measurable, since it changes which file a read opens and not how much work is done.

So this is a thing you run deliberately, before a release or on a schedule. `--runs` is the lever that moves it most, and it moves the miss rate with it.

## The condition under which this project is unnecessary

Pre-registered with the rest, and it belongs here rather than in a footnote: **if the naive baseline's false-positive count on correct code had come out at zero, the oracle would be more machinery than the problem needs**, and this section would say so.

It did not. Over 20 fresh passes, bit-exact comparison of `mean_basket` between two runs reported a median of 1,203 differing rows [1,110 to 1,554], which is about 600 of the 1,000 groups counted on both sides at once, on a step where nothing is wrong. `src/twicerun/compare.py` is that comparison, kept as the eval's baseline 1 rather than deleted.

It has no command of its own yet, so checking it through this repo means importing `compare` in four lines of Python. The raw-DuckDB version in `scripts/measure_duckdb.py` needs no such thing and takes two seconds, and it is the better check anyway: it is a fact about DuckDB rather than about my code. Baseline 1 gets a command when slice 5 builds the eval that runs it.

Stating the condition matters more than the outcome. A tool whose author cannot say what would have made it pointless has not tested the premise.

## The limitation that goes first, not in a footnote

**twicerun only tests pipelines written against its storage interface.** It does not test arbitrary pipelines.

Watching a pipeline's writes without its cooperation needs either a kernel block-layer wrapper or system-call interception. Both are out of budget and both are worse on macOS. What is left is an interface the pipeline reads and writes through, which is portable, needs no root, and only sees pipelines that opted in.

What that buys back is why it is a design choice rather than a workaround. One abstraction carries five jobs: it is where artifacts get captured, where reads get redirected for containment, where input rows get counted for the tolerance bound, where the column names feeding attribution come from, and where amplified inputs will be substituted. A step calling `ctx.write()` gets all five. A step calling `duckdb.execute("COPY ... TO ...")` behind its back gets none.

## What else it gets wrong

**Sorting to pair rows inside a key group is a heuristic once there is more than one float column.** The ordinal sorts both sides the same way, and for a single float column sorted-to-sorted pairing is the assignment that minimises total absolute difference, so it is optimal. With several, a lexicographic sort can pair the wrong two rows inside one key group. That can only understate a difference, so the failure mode is a bounded false negative confined to within-key-group permutations of float-only differences.

**A key group with no exact columns is one big group.** If every column is a float, there is no key, the whole artifact sorts as one group and rows pair by order alone. It is the weakest case here and it is where the heuristic above does the most work, so the report says `matched on no key` when it happens rather than leaving it to be inferred.

**NaN payloads are not distinguished.** The sign survives and the payload does not.

**Attribution can name more than one column, and sometimes should.** See above. The tie-break is a heuristic, not a proof.

**`rows_read` is not the term count the bound wants.** Measured above, both directions, with the size of the error printed in every report.

**A `threads=1` rate of 0 of 4 is four comparisons, not a property.** It is reported with its one-sided bound for that reason, and `PARALLEL_ORDER` should be read as the name of a pattern in two measured rates.

**The bisect resolves its reads against run 1 even under `--no-containment`.** A single step cannot be re-executed on its own without something to read, so the ablation ablates the main loop and not the bisect. A step that only inherited a divergence can therefore come out `PARALLEL_ORDER` in an uncontained report, and the report says so where it happens.

## How the pieces fit

`src/twicerun/storage.py` is the interface. `ctx.read` and `ctx.write` address artifacts by `(run, step index, name)`. The method worth understanding is `ctx.state(name, initial)`, which resolves the *previous run's* copy of an artifact rather than this run's. Without it, a checker starts every run from an empty directory and can never see the two bugs that only exist because a pipeline runs against state its own last execution left behind.

`src/twicerun/oracle.py` is the comparison, and it is the project. Everything in "How the comparison works" above lives here.

`src/twicerun/policy.py` is the decision, kept apart from the comparison on purpose.

`src/twicerun/cause.py` is the second axis: the label vocabulary, the one-sided bound, and the reasoning about what a zero out of four can support. The execution that produces it lives in the runner, because re-executing a pipeline is the runner's job.

`src/twicerun/runner.py` is the loop. Run 1 is the reference and runs 2 to N are each compared against it, giving `k of m`. All-pairs clustering was the alternative, and it is rejected because tolerance-based equality is not transitive, so "how many distinct answers" stops being well defined the moment any tolerance exists.

## Why five runs and not two

A two-run tool cannot tell "deterministic" from "non-deterministic and lucky this time". Step 6 of the reference pipeline is the proof. It is the same `row_number()` bug as step 2 at a lower tie density, and over 30 passes in total it fired between 0 and 4 times out of 4. At 1 of 4, which came up repeatedly, a two-run checker reports nothing three times in four on a step that is definitely broken. It was a flat 0 of 4 on 3 of those 30, so even five runs miss it outright about one time in ten.

That split, 3 flat zeros in the first 10 passes and none in the next 20, is itself the point. Nothing changed between them but the sample.

With `m` comparisons and a per-comparison divergence probability `p`, a step is missed with probability `(1-p)^m`. At `p = 0.5`, going from two runs to five takes the miss rate from 50 percent to 6.3 percent for 2.5 times the runtime. Going from five to ten takes it to 0.2 percent for twice as much again. The first trade is obviously worth making, the second is a judgement call, so five is the default and `--runs` moves it.

Those zero results are also the argument for slice 4. More runs lower the miss rate for a given `p`; they do not change `p`. Amplification changes `p`, by feeding the step an input built to make it fire.

No practical number of runs proves determinism, which is why the tool never prints the word. There is a test asserting the report contains neither "deterministic" nor "stable".

## Four times a number in this README was beaten by a larger sample

All four on 2026-08-26, all on this machine, all while the author was actively trying not to let it happen. They are listed because the tool's whole claim is that a small sample lies to you about a quantity that varies, and this is that claim demonstrated against its own documentation.

| # | what was published | what a larger sample gave |
|---|---|---|
| 1 | `apply_price_updates` fires 4 of 4 every time, from 5 invocations | a floor of 1 within 8 invocations, then 0 within 20 |
| 2 | the slice 2 results table, from 10 passes | 8 of its 10 quantities exceeded within 20 more passes; `daily_revenue` published at 3 to 6 ulp reached 19 |
| 3 | the slice 3 results table, from 20 passes | all 6 of its ranges exceeded within 28 more; `apply_price_updates` published at 1,344 to 42,304 rows reached 77,120 |
| 4 | the tight reassociation bound is under 1000x on 40 of 40 step-passes | it cleared on 3 of 56, and on 1 of a later 40, at 1,229x |

Number 4 is the worst of them, because it is the falsifiable check the tolerance section is built around, and because the report contradicted itself in the terminal: it printed that the tight check "is expected to fail" three lines above a step-pass where it cleared.

The response after the third one was to stop publishing ranges. Stating the sample size stops a range being a lie; it does not stop it being wrong, and a range a reader can beat in ten minutes should not be printed in a way that looks like a bound. The table above the fold is medians now, with the spread in brackets and this section next to it.

## The reference pipeline ships broken

A checker that finds nothing is indistinguishable from a checker that is broken. `pipelines/reference.py` carries four bugs, one step that drifts benignly, one step that fires intermittently, one control step that must never fire, and one step whose only job is to sit downstream of a bug so the containment ablation has something to measure.

Two of the eight steps are constructed rather than observed, and both say so where they are defined: the `MERGE` bug below, and `roll_up_keys`, which exists to be fed a diverging artifact. Three of the four bugs are failures Vishal has actually been bitten by running Databricks pipelines and SQL migrations: duplicate rows after a retry, IDs changing between runs, and totals not matching between runs. The fourth, the non-idempotent `MERGE`, is not a war story. It came out of an experiment for this project and he has never seen it. Its distinction is how narrow its window turned out to be: it needs a target somewhere around 100,000 to 125,000 rows, a source staged into a real table and `BIGINT` columns, and at 50,000 rows and again at 200,000 it gives the same answer every time.

Every configuration in the file was re-derived on this machine before it was written down, and two of the spec's claims did not survive that. The `MERGE` bug's cause is parallel order rather than the single-threaded persistence the spec predicted, because `threads=1` was clean at every scale tried. And at the three rows the spec proposed for it, it gives the same answer ten times out of ten.

## What this does not do

No `dbt`, Airflow, Dagster or Prefect adapter. No Spark. No syscall interception. No nested type comparison. No web UI. No fix generation.

No model calls either: no LLM adapter, no `openai` dependency, no `AZURE_OPENAI_*` configuration. A non-deterministic output layer on a tool whose premise is determinism is a contradiction, and the hint table in a later slice is fifteen lines of static lookup.

## Data

The reference pipeline runs on generated data, and synthetic is the point here rather than a convenience. Its bugs are parameterised by tie density, group count and row count, and those parameters are the experiment. Real data would fix the tie density at whatever the file happens to contain and make the intermittency measurement impossible to produce.

Values come from `hash(i)` rather than `random()`, so all five runs read byte-identical inputs and any divergence is the pipeline's rather than the data's. Slice 5 adds real NYC TLC trip records for the backfill and schema-change work.
