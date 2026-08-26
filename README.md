# twicerun

Run a batch pipeline several times on the same input and report, per step, how often it failed to give the same answer.

The premise is one measurement, and it takes two seconds to check. `SELECT g, sum(v) FROM t GROUP BY g` over 2,000,000 rows in 1,000 groups, run twice at `threads=8` on DuckDB 1.5.5, disagrees on several hundred of the 1,000 groups. Ten runs on this machine gave 492, 497, 510, 575, 599, 611, 618, 690, 700 and 738. Nothing is wrong with the query. Parallel reduction adds the terms in whatever order the threads finish in, and float addition is not associative. At `threads=1` it disagrees on none of them, and `count()` over the same table never moves, which is the control: integer arithmetic cannot reassociate into a different answer.

So the naive version of this tool, run it twice and diff, reports hundreds of findings on correct code. The comparison is the project. The runner is forty lines.

```
$ uv run twicerun run pipelines/reference.py
pipeline   pipelines/reference.py
runs       5, run 1 is the reference, so 4 comparisons per step
duckdb     1.5.5, threads=10
platform   macOS-26.5.2-arm64-arm-64bit
artifacts  .twicerun/run-20260826-083233
retention  keeping 1 run directory

  0 generate_inputs         0 of 4
  1 daily_revenue           4 of 4  daily_revenue: 684 rows only in the reference run, 684 only in the later run, of 1,000
  2 customer_keys           4 of 4  customer_keys: 491,520 rows only in the reference run, 491,520 only in the later run, of 500,000
  3 apply_price_updates     1 of 4  prices: 23,168 rows only in the reference run, 23,168 only in the later run, of 125,000
  4 append_audit_log        4 of 4  audit_log: 0 rows only in the reference run, 15,812 only in the later run, of 3,953
  5 mean_basket             4 of 4  mean_basket: 706 rows only in the reference run, 706 only in the later run, of 1,000
  6 sparse_customer_keys    3 of 4  sparse_customer_keys: 237,280 rows only in the reference run, 237,280 only in the later run, of 500,000

6 of 7 steps diverged in 9.2s.
Comparison is bit-exact over a canonical row hash, so a float aggregate that
reassociates under parallelism counts here exactly as a wrong answer does.
```

**Your numbers will not match that transcript, and neither will mine on the next run.** This is a tool about non-determinism and its own output is non-deterministic, so quoting any single figure as fixed would be the wrong thing to do twice over. Over 20 invocations of exactly that command:

| step | fire rate over 20 invocations | rows differing when it fired |
|---|---|---|
| 0 `generate_inputs` | 0 of 4, all 20 | the control, and it never fired |
| 1 `daily_revenue` | 4 of 4, all 20 | 625 to 787 of 1,000 |
| 2 `customer_keys` | 3 to 4 of 4 | 368,640 to 491,520 of 500,000 |
| 3 `apply_price_updates` | 0 to 4 of 4, zero on 1 | 9,248 to 35,104 of 125,000 |
| 4 `append_audit_log` | 4 of 4, all 20 | exactly 15,812 extra rows on a 3,953-row reference |
| 5 `mean_basket` | 4 of 4, all 20 | 610 to 762 of 1,000 |
| 6 `sparse_customer_keys` | 0 to 4 of 4, zero on 5 | 8,480 to 483,040 of 500,000 |

The summary line reads 4, 5 or 6 of 7 depending on the invocation, in 7.8 to 8.7 seconds. Only steps 1, 4 and 5 fire on every comparison. Steps 3 and 6 came back a flat 0 of 4 on 1 and 5 of those 20, which means the tool ran a step that is definitely broken five times and reported nothing at all. Step 6 does that a quarter of the time. That is not a defect in the measurement, it is the measurement, and it is the concrete argument for slice 4.

Step 5 is a correct float average. Every one of those 706 findings in the transcript is wrong, and the whole of slice 2 exists to fix it.

## Status

Slice 1 of 7. What runs today: the storage interface, the run directory layout, the artifact manifest, the five-run loop, and a bit-exact multiset comparison over a canonical row hash.

What does not exist yet, in the order it arrives: the typed oracle that stops the false positives (slice 2), containment and the single-threaded bisect (slice 3), input amplification and the confidence bound (slice 4), the NYC TLC backfill and the eval numbers (slice 5), crash injection (slice 6), and the report generator that keeps the results table in this file honest (slice 7).

## Running it

Needs [uv](https://docs.astral.sh/uv/) and nothing else. Python, DuckDB and the dev tools all come from `uv sync`. There is no published remote yet, so from the directory holding this file:

```bash
uv sync --all-extras
./scripts/install-hooks.sh

uv run twicerun run pipelines/reference.py
```

Exit codes are 0 for nothing diverged, 1 for something diverged, 2 for bad input and 3 for a crash. 1 means divergence and only divergence, so a release gate keyed on it does not also trip on a broken pipeline. On the reference pipeline it exits 1, and that includes the benign float drift, which is the point of this slice rather than a bug in it.

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

## The limitation that goes first, not in a footnote

**twicerun only tests pipelines written against its storage interface.** It does not test arbitrary pipelines.

Watching a pipeline's writes without its cooperation needs either a kernel block-layer wrapper or system-call interception. Both are out of budget and both are worse on macOS. What is left is an interface the pipeline reads and writes through, which is portable, needs no root, and only sees pipelines that opted in.

What that buys back is why it is a design choice rather than a workaround. One abstraction carries four jobs: it is where artifacts get captured, where reads get redirected for containment, where input rows get counted for the tolerance bound, and where amplified inputs get substituted. A step calling `ctx.write()` gets all four. A step calling `duckdb.execute("COPY ... TO ...")` behind its back gets none.

## How the pieces fit

Three files carry the idea.

`src/twicerun/storage.py` is the interface. `ctx.read` and `ctx.write` address artifacts by `(run, step index, name)`. The method worth understanding is `ctx.state(name, initial)`, which resolves the *previous run's* copy of an artifact rather than this run's. Without it, a checker starts every run from an empty directory and can never see the two bugs that only exist because a pipeline runs against state its own last execution left behind.

`src/twicerun/compare.py` is the comparison. Row order is not part of a pipeline's answer, so it hashes each row into a canonical digest and compares multisets. The encoding leans on DuckDB casting a DOUBLE to the shortest decimal string that reads back as the same double, which makes the text injective over the bit patterns. Checked rather than assumed: 50,000 consecutive doubles from 1.0 gave 50,000 distinct strings.

`src/twicerun/runner.py` is the loop. Run 1 is the reference and runs 2 to N are each compared against it, giving `k of m`. All-pairs clustering was the alternative, and it is rejected because tolerant equality is not transitive, so "how many distinct answers" stops being well defined the moment slice 2 adds any tolerance.

## Why five runs and not two

A two-run tool cannot tell "deterministic" from "non-deterministic and lucky this time". Step 6 of the reference pipeline is the proof. It is the same `row_number()` bug as step 2 at a lower tie density, and across 20 invocations it fired between 0 and 4 times out of 4. At 1 of 4, a two-run checker reports nothing three times in four. On 5 of those 20 invocations it fired 0 of 4, so a quarter of the time the five-run loop reported nothing at all on a step that is definitely broken.

With `m` comparisons and a per-comparison divergence probability `p`, a step is missed with probability `(1-p)^m`. At `p = 0.5`, going from two runs to five takes the miss rate from 50 percent to 6.3 percent for 2.5 times the runtime. Going from five to ten takes it to 0.2 percent for twice as much again. The first trade is obviously worth making, the second is a judgement call, so five is the default and `--runs` moves it.

Those five zero results are also the argument for what comes in slice 4, and they are why the count of runs cannot be the whole answer. More runs lower the miss rate for a given `p`; they do not change `p`. Amplification changes `p`, by feeding the step an input built to make it fire.

No practical number of runs proves determinism, which is why the tool never prints the word. There is a test asserting that the report contains neither "deterministic" nor "stable".

## The reference pipeline ships broken

A checker that finds nothing is indistinguishable from a checker that is broken. `pipelines/reference.py` carries four bugs, one step that drifts benignly, one step that fires intermittently, and one control step that must never fire.

Every configuration in it was re-derived on this machine before it was written down, and two of the spec's claims did not survive that. The `MERGE` bug is real but needs a target somewhere around 100,000 rows, a source staged into a real table, and BIGINT columns; at the three rows the spec proposed it gives the same answer ten times out of ten, and by 200,000 target rows it stops firing again. And its cause is parallel order, not the single-threaded persistence the spec predicted, because `threads=1` was clean at every scale tried.

## What this does not do

No `dbt`, Airflow, Dagster or Prefect adapter. No Spark. No syscall interception. No nested type comparison. No web UI. No fix generation.

No model calls either: no LLM adapter, no `openai` dependency, no `AZURE_OPENAI_*` configuration. A non-deterministic output layer on a tool whose premise is determinism is a contradiction, and the hint table in a later slice is fifteen lines of static lookup.

## Data

The reference pipeline runs on generated data, and synthetic is the point here rather than a convenience. Its bugs are parameterised by tie density, group count and row count, and those parameters are the experiment. Real data would fix the tie density at whatever the file happens to contain and make the intermittency measurement impossible to produce.

Values come from `hash(i)` rather than `random()`, so all five runs read byte-identical inputs and any divergence is the pipeline's rather than the data's. Slice 5 adds real NYC TLC trip records for the backfill and schema-change work.
