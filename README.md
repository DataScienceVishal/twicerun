# twicerun

Run a batch pipeline several times on the same input and report, per step, how often it failed to give the same answer.

The premise is one measurement, and it takes two seconds to check. `SELECT g, sum(v) FROM t GROUP BY g` over 2,000,000 rows in 1,000 groups, run twice at `threads=8` on DuckDB 1.5.5, disagrees on several hundred of the 1,000 groups. Ten runs on this machine gave 492, 497, 510, 575, 599, 611, 618, 690, 700 and 738. Nothing is wrong with the query. Parallel reduction adds the terms in whatever order the threads finish in, and float addition is not associative. At `threads=1` it disagrees on none of them, and `count()` over the same table never moves, which is the control: integer arithmetic cannot reassociate into a different answer.

So the naive version of this tool, run it twice and diff, reports hundreds of findings on correct code. The comparison is the project. The runner is forty lines.

```
$ uv run twicerun run pipelines/reference.py
pipeline   pipelines/reference.py
runs       5, run 1 is the reference, so 4 comparisons per step
policy     strict, so any difference at all is a divergence
duckdb     1.5.5, threads=10
platform   macOS-26.5.2-arm64-arm-64bit
artifacts  .twicerun/run-20260826-100133
retention  keeping 1 run directory, dropped 1

  0 generate_inputs         0 of 4
  1 daily_revenue           4 of 4  VALUE_DRIFT
      daily_revenue: 710 of 1,000 paired rows moved on revenue, up to 4 ulp and 4.78e-16 relative
      worst pair on revenue: 487167.6412606093 against 487167.64126060955
  2 customer_keys           4 of 4  ROW_MISSING ROW_EXTRA
      customer_keys: 491,520 of 500,000 reference rows and 491,520 later rows found no partner
      dropping surrogate_id from the key takes unmatched reference rows from 491,520 to 0
      event_id does the same, so surrogate_id is named first because it is the one no input to this step carries
  3 apply_price_updates     4 of 4  ROW_MISSING ROW_EXTRA
      prices: 36,160 of 125,000 reference rows and 36,160 later rows found no partner
      dropping price_cents from the key takes unmatched reference rows from 36,160 to 0
  4 append_audit_log        4 of 4  MULTIPLICITY
      audit_log: 15,812 later rows found no partner, against 3,953 reference rows
  5 mean_basket             4 of 4  VALUE_DRIFT
      mean_basket: 630 of 1,000 paired rows moved on mean_amount, up to 3 ulp and 3.48e-16 relative
      worst pair on mean_amount: 244.6697165778526 against 244.6697165778525
  6 sparse_customer_keys    1 of 4  ROW_MISSING ROW_EXTRA
      sparse_customer_keys: 245,760 of 500,000 reference rows and 245,760 later rows found no partner
      dropping surrogate_id from the key takes unmatched reference rows from 245,760 to 0
      event_id does the same, so surrogate_id is named first because it is the one no input to this step carries
```

Step 5 is a correct float average. All 630 of those findings are the arithmetic behaving normally, and `--policy reduction-order` is the opt-in that says so.

**Your numbers will not match that transcript, and neither will mine on the next run.** This is a tool about non-determinism and its own output is non-deterministic, so quoting any single figure as fixed would be the wrong thing to do twice over. Over 10 passes of five runs each:

| step | fires under `strict` | fires under `reduction-order` | what the oracle called it |
|---|---|---|---|
| 0 `generate_inputs` | 0 of 4, all 10 | 0 of 4, all 10 | the control, and it never fired |
| 1 `daily_revenue` | 4 of 4, all 10 | 0 of 4, all 10 | `VALUE_DRIFT`, 526 to 753 of 1,000 rows, 3 to 6 ulp |
| 2 `customer_keys` | 2 to 4 of 4 | unchanged | `ROW_MISSING` `ROW_EXTRA`, attributed to `surrogate_id` in 10 of 10 |
| 3 `apply_price_updates` | 1 to 4 of 4 | unchanged | `ROW_MISSING` `ROW_EXTRA`, attributed to `price_cents` in 10 of 10 |
| 4 `append_audit_log` | 4 of 4, all 10 | unchanged | `MULTIPLICITY`, 15,812 extra rows on a 3,953-row reference |
| 5 `mean_basket` | 4 of 4, all 10 | 0 of 4, all 10 | `VALUE_DRIFT`, 397 to 735 of 1,000 rows, 3 to 6 ulp |
| 6 `sparse_customer_keys` | 0 to 4 of 4, zero on 3 | unchanged | `ROW_MISSING` `ROW_EXTRA`, attributed to `surrogate_id` in 10 of 10 |

The two float steps go to zero and nothing else moves. That is the whole claim for the oracle: the false positives disappear and the four real bugs are caught by the same code that dismissed them.

Step 6 came back a flat 0 of 4 on 3 of those 10 passes, which means the tool ran a step that is definitely broken five times and reported nothing. That is not a defect in the measurement, it is the measurement, and it is the argument for the amplification work in slice 4.

## Status

Slice 2 of 7. What runs today: the storage interface, the run layout, the artifact manifest, the five-run loop, the typed oracle, leave-one-out attribution, and the policy layer.

What does not exist yet, in the order it arrives: containment and the single-threaded bisect (slice 3), input amplification and the confidence bound (slice 4), the NYC TLC backfill and the eval numbers (slice 5), crash injection (slice 6), and the report generator that keeps this file's tables honest (slice 7).

One consequence of that ordering is visible in every report that downgrades anything, and it is deliberate. `reduction-order` is a conjunction of three conditions and the middle one, that the step stops diverging at `threads=1`, needs the bisect from slice 3. So the report header says so rather than counting it as satisfied:

```
policy     reduction-order, so drift inside the reassociation bound is TOLERATED
           condition 2 of 3, that the step does not diverge at threads=1, is not implemented until slice 3. Every TOLERATED below rests on the other two
```

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

**It passes, and the margin is not where it looks.** Over 10 passes:

| | `daily_revenue` | `mean_basket` |
|---|---|---|
| observed max relative drift | 3.61e-16 to 5.88e-16 | 4.55e-16 to 7.85e-16 |
| bound at `n` = 2,000,000 rows read | 4.44e-10 | 4.44e-10 |
| headroom | 7.55e5 to 1.23e6x | 5.66e5 to 9.76e5x |
| bound at `n` = 2,000 terms per output row | 4.44e-13 | 4.44e-13 |
| headroom | 755 to 1,229x | 566 to 976x |

The check as pre-registered clears 1000x by six orders of magnitude on every pass. Almost all of that margin comes from `n` rather than from the drift being small. `rows_read` is the step's whole input, and each of the 1,000 output rows sums about 2,000 terms, so `n` is a thousand times larger than the quantity the bound is about. Take that slack out and the headroom lands at 566 to 1,229x, which is **under the 1000x line on 19 of the 20 step-passes measured**.

So the honest reading is three orders of magnitude of headroom, not six, and every report prints both numbers so nobody has to take that from this file:

```
  5 mean_basket
      n = 2,000,000 rows read by the step, so the bound is 4.4409e-10 relative
      observed max relative drift 4.5591e-16, which is 9.74e+05x inside the bound
      pre-registered check wanted 1000x of headroom and clears it
      at n = 2,000 terms per output row the bound is 4.4409e-13 and the headroom is 974x, which fails
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

`--policy reduction-order` is the opt-in. It downgrades a `VALUE_DRIFT` finding to `TOLERATED` when three things hold at once: the step's only class across every comparison is `VALUE_DRIFT`, the step stops diverging at `threads=1`, and the magnitude is inside the bound. Any `ROW_MISSING`, `ROW_EXTRA`, `MULTIPLICITY` or `SCHEMA` finding anywhere in the step blocks it outright. A downgraded finding is still counted and still printed with its magnitudes.

The conjunction is the point. A difference that vanishes single-threaded but is ten orders of magnitude larger than reassociation can account for is catastrophic cancellation or a genuinely different set of terms, and it is still reported.

`--tolerance-rel` and `--tolerance-ulps` are an escape hatch for someone who knows their domain, documented as one and never a default. They work under any policy because they are a claim about acceptable values rather than about a mechanism, and they still cannot excuse a missing or duplicated row.

## Measuring and deciding are separate stages

Numbers must not move when the policy does. If a tolerance can change a reported figure, everything downstream of it is negotiable and none of it is worth printing.

So `oracle.py` produces findings, `measurement.py` collects them per step, `policy.py` reads that and returns a verdict, and nothing writes back. The two transcripts of the same pipeline above carry the same ULP counts, the same relative magnitudes and the same row counts; only the fire rate and the `TOLERATED` line differ. There is a test that judges one measurement under both policies and asserts exactly that.

## Running it

Needs [uv](https://docs.astral.sh/uv/) and nothing else. Python, DuckDB and the dev tools all come from `uv sync`. There is no published remote yet, so from the directory holding this file:

```bash
uv sync --all-extras
./scripts/install-hooks.sh

uv run twicerun run pipelines/reference.py
uv run twicerun run pipelines/reference.py --policy reduction-order
```

Exit codes are 0 for nothing diverged, 1 for something diverged, 2 for bad input and 3 for a crash. 2 covers a column the oracle refuses to compare and a `--key` naming a column that is not there, because both are facts about the pipeline's output rather than crashes. 1 means divergence and only divergence, so a release gate keyed on it does not also trip on a broken pipeline. A comparison downgraded to `TOLERATED` does not set it.

`--key artifact=col,col` matches rows of one artifact on a subset of its exact columns. The columns it leaves out stop deciding what makes a row a row and start being compared as values, which changes the class a difference gets without changing the difference. It is how you say that a surrogate key is not part of the answer.

One pass writes about 200 MB of Parquet under `.twicerun/`, which is gitignored. By default only the current run directory is kept, so the footprint stays at roughly 200 MB however many times you run it. `--keep 0` turns pruning off, and `rm -rf .twicerun` reclaims the lot.

The tests run with no credentials and no network:

```bash
uv run pytest
```

To re-derive every DuckDB number quoted here on your own machine, which takes about two seconds:

```bash
uv run python scripts/measure_duckdb.py
```

Its counts will not match these, for the same reason the transcript above will not. What holds is the shape: parallel figures large, `threads=1` figures zero, `count()` stable, the single-word tiebreak fix clean, and the `MERGE` bug present at threads=8 and absent at threads=1. The script checks ten such invariants and exits non-zero if any of them breaks, so it fails loudly rather than printing numbers that mean something different from what they say.

## The condition under which this project is unnecessary

Pre-registered with the rest, and it belongs here rather than in a footnote: **if the naive baseline's false-positive count on correct code had come out at zero, the oracle would be more machinery than the problem needs**, and this section would say so.

It did not. Bit-exact comparison of `mean_basket` between two runs reports 1,078 to 1,374 differing rows over 10 passes, which is 539 to 687 of the 1,000 groups counted on both sides at once, on a step where nothing is wrong. `src/twicerun/compare.py` is that comparison, kept as the eval's baseline 1 rather than deleted. The equivalent raw-DuckDB measurement is in `scripts/measure_duckdb.py` and takes two seconds, so the premise can be checked without going through this repo's code at all.

Stating the condition matters more than the outcome. A tool whose author cannot say what would have made it pointless has not tested the premise.

## The limitation that goes first, not in a footnote

**twicerun only tests pipelines written against its storage interface.** It does not test arbitrary pipelines.

Watching a pipeline's writes without its cooperation needs either a kernel block-layer wrapper or system-call interception. Both are out of budget and both are worse on macOS. What is left is an interface the pipeline reads and writes through, which is portable, needs no root, and only sees pipelines that opted in.

What that buys back is why it is a design choice rather than a workaround. One abstraction carries five jobs: it is where artifacts get captured, where reads get redirected for containment, where input rows get counted for the tolerance bound, where the column names feeding attribution come from, and where amplified inputs will be substituted. A step calling `ctx.write()` gets all five. A step calling `duckdb.execute("COPY ... TO ...")` behind its back gets none.

## What else it gets wrong

**Sorting to pair rows inside a key group is a heuristic once there is more than one float column.** The ordinal sorts both sides the same way, and for a single float column sorted-to-sorted pairing is the assignment that minimises total absolute difference, so it is optimal. With several, a lexicographic sort can pair the wrong two rows inside one key group. That can only understate a difference, so the failure mode is a bounded false negative confined to within-key-group permutations of float-only differences.

**A key group with no exact columns is one big group.** If every column is a float, there is no key, the whole artifact sorts as one group and rows pair by order alone. The tool reports the key it used so this is visible, but it is the weakest case and it is where the heuristic above does the most work.

**NaN payloads are not distinguished.** The sign survives and the payload does not.

**Attribution can name more than one column, and sometimes should.** See above. The tie-break is a heuristic, not a proof.

**`rows_read` is not the term count the bound wants.** Measured above, both directions, with the size of the error printed in every report.

## How the pieces fit

`src/twicerun/storage.py` is the interface. `ctx.read` and `ctx.write` address artifacts by `(run, step index, name)`. The method worth understanding is `ctx.state(name, initial)`, which resolves the *previous run's* copy of an artifact rather than this run's. Without it, a checker starts every run from an empty directory and can never see the two bugs that only exist because a pipeline runs against state its own last execution left behind.

`src/twicerun/oracle.py` is the comparison, and it is the project. Everything in "How the comparison works" above lives here.

`src/twicerun/policy.py` is the decision, kept apart from the comparison on purpose.

`src/twicerun/runner.py` is the loop. Run 1 is the reference and runs 2 to N are each compared against it, giving `k of m`. All-pairs clustering was the alternative, and it is rejected because tolerance-based equality is not transitive, so "how many distinct answers" stops being well defined the moment any tolerance exists.

## Why five runs and not two

A two-run tool cannot tell "deterministic" from "non-deterministic and lucky this time". Step 6 of the reference pipeline is the proof. It is the same `row_number()` bug as step 2 at a lower tie density, and across 10 passes it fired between 0 and 4 times out of 4, with a flat 0 on 3 of them. At 1 of 4, a two-run checker reports nothing three times in four.

With `m` comparisons and a per-comparison divergence probability `p`, a step is missed with probability `(1-p)^m`. At `p = 0.5`, going from two runs to five takes the miss rate from 50 percent to 6.3 percent for 2.5 times the runtime. Going from five to ten takes it to 0.2 percent for twice as much again. The first trade is obviously worth making, the second is a judgement call, so five is the default and `--runs` moves it.

Those zero results are also the argument for slice 4. More runs lower the miss rate for a given `p`; they do not change `p`. Amplification changes `p`, by feeding the step an input built to make it fire.

No practical number of runs proves determinism, which is why the tool never prints the word. There is a test asserting the report contains neither "deterministic" nor "stable".

## The reference pipeline ships broken

A checker that finds nothing is indistinguishable from a checker that is broken. `pipelines/reference.py` carries four bugs, one step that drifts benignly, one step that fires intermittently, and one control step that must never fire.

Three of the four are failures Vishal has actually been bitten by running Databricks pipelines and SQL migrations: duplicate rows after a retry, IDs changing between runs, and totals not matching between runs. The fourth, the non-idempotent `MERGE`, is not a war story. It came out of an experiment for this project and he has never seen it. Its distinction is how narrow its window turned out to be: it needs a target somewhere around 100,000 to 125,000 rows, a source staged into a real table and `BIGINT` columns, and at 50,000 rows and again at 200,000 it gives the same answer every time.

Every configuration in the file was re-derived on this machine before it was written down, and two of the spec's claims did not survive that. The `MERGE` bug's cause is parallel order rather than the single-threaded persistence the spec predicted, because `threads=1` was clean at every scale tried. And at the three rows the spec proposed for it, it gives the same answer ten times out of ten.

## What this does not do

No `dbt`, Airflow, Dagster or Prefect adapter. No Spark. No syscall interception. No nested type comparison. No web UI. No fix generation.

No model calls either: no LLM adapter, no `openai` dependency, no `AZURE_OPENAI_*` configuration. A non-deterministic output layer on a tool whose premise is determinism is a contradiction, and the hint table in a later slice is fifteen lines of static lookup.

## Data

The reference pipeline runs on generated data, and synthetic is the point here rather than a convenience. Its bugs are parameterised by tie density, group count and row count, and those parameters are the experiment. Real data would fix the tie density at whatever the file happens to contain and make the intermittency measurement impossible to produce.

Values come from `hash(i)` rather than `random()`, so all five runs read byte-identical inputs and any divergence is the pipeline's rather than the data's. Slice 5 adds real NYC TLC trip records for the backfill and schema-change work.
