# twicerun

Run a batch pipeline several times on the same input and report, per step, how often it failed to give the same answer.

The premise is one measurement, and it takes twenty seconds to check. `SELECT g, sum(v) FROM t GROUP BY g` over 2,000,000 rows in 1,000 groups, run twice at `threads=8` on DuckDB 1.5.5, disagrees on 555 to 714 of the 1,000 groups. Nothing is wrong with the query. Parallel reduction adds the terms in whatever order the threads finish in, and float addition is not associative. At `threads=1` it disagrees on none of them, and `count()` over the same table never moves, which is the control: integer arithmetic cannot reassociate into a different answer.

So the naive version of this tool, run it twice and diff, reports hundreds of findings on correct code. The comparison is the project. The runner is forty lines.

```
$ twicerun run pipelines/reference.py
pipeline   pipelines/reference.py
runs       5, run 1 is the reference, so 4 comparisons per step
duckdb     1.5.5, threads=10
platform   macOS-26.5.2-arm64-arm-64bit
artifacts  .twicerun/run-20260826-081204

  0 generate_inputs         0 of 4
  1 daily_revenue           4 of 4  daily_revenue: 641 rows only in the reference run, 641 only in the later run, of 1,000
  2 customer_keys           4 of 4  customer_keys: 491,520 rows only in the reference run, 491,520 only in the later run, of 500,000
  3 apply_price_updates     4 of 4  prices: 26,560 rows only in the reference run, 26,560 only in the later run, of 100,000
  4 append_audit_log        4 of 4  audit_log: 0 rows only in the reference run, 15,812 only in the later run, of 3,953
  5 mean_basket             4 of 4  mean_basket: 710 rows only in the reference run, 710 only in the later run, of 1,000
  6 sparse_customer_keys    4 of 4  sparse_customer_keys: 245,760 rows only in the reference run, 245,760 only in the later run, of 500,000

6 of 7 steps diverged in 5.1s.
```

Step 5 is a correct float average. Every one of those 710 findings is wrong, and the whole of slice 2 exists to fix it.

## Status

Slice 1 of 7. What runs today: the storage interface, the run directory layout, the artifact manifest, the five-run loop, and a bit-exact multiset comparison over a canonical row hash.

What does not exist yet, in the order it arrives: the typed oracle that stops the false positives (slice 2), containment and the single-threaded bisect (slice 3), input amplification and the confidence bound (slice 4), the NYC TLC backfill and the eval numbers (slice 5), crash injection (slice 6), and the report generator that keeps the results table in this file honest (slice 7).

## Running it

```bash
git clone https://github.com/DataScienceVishal/twicerun.git
cd twicerun
uv sync --all-extras
./scripts/install-hooks.sh

uv run twicerun run pipelines/reference.py
```

Divergence exits 1, so it can gate a release. On the reference pipeline that includes the benign float drift, which is the point of this slice rather than a bug in it.

The tests run with no credentials and no network:

```bash
uv run pytest
```

To re-derive every DuckDB number quoted here on your own machine, which takes about two seconds:

```bash
uv run python scripts/measure_duckdb.py
```

Its counts will not match these, because the thing being measured is itself non-deterministic. What holds is the shape: parallel figures large, `threads=1` figures zero, `count()` stable. The script asserts those three and exits non-zero if any breaks.

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

A two-run tool cannot tell "deterministic" from "non-deterministic and lucky this time". Step 6 of the reference pipeline is the proof. It is the same `row_number()` bug as step 2 at a lower tie density, and across 20 invocations it fired between 0 and 4 times out of 4. At 1 of 4, a two-run checker reports nothing three times in four. On 2 of those 20 invocations it fired 0 of 4, so the five-run loop reported nothing at all on a step that is definitely broken.

With `m` comparisons and a per-comparison divergence probability `p`, a step is missed with probability `(1-p)^m`. At `p = 0.5`, going from two runs to five takes the miss rate from 50 percent to 6.3 percent for 2.5 times the runtime. Going from five to ten takes it to 0.2 percent for twice as much again. The first trade is obviously worth making, the second is a judgement call, so five is the default and `--runs` moves it.

Those two zero results are also the argument for what comes in slice 4, and they are why the count of runs cannot be the whole answer. More runs lower the miss rate for a given `p`; they do not change `p`. Amplification changes `p`, by feeding the step an input built to make it fire.

No practical number of runs proves determinism, which is why the tool never prints the word. There is a test asserting that the report contains neither "deterministic" nor "stable".

## The reference pipeline ships broken

A checker that finds nothing is indistinguishable from a checker that is broken. `pipelines/reference.py` carries four bugs, one step that drifts benignly, one step that fires intermittently, and one control step that must never fire.

Every configuration in it was re-derived on this machine before it was written down, and two of the spec's claims did not survive that. The `MERGE` bug is real but needs 100,000 target rows, a source staged into a real table, and BIGINT columns; at the three rows the spec proposed it gives the same answer ten times out of ten. And its cause is parallel order, not the single-threaded persistence the spec predicted, because `threads=1` was clean at every scale tried.

## What this does not do

No `dbt`, Airflow, Dagster or Prefect adapter. No Spark. No syscall interception. No nested type comparison. No web UI. No fix generation.

No model calls either: no LLM adapter, no `openai` dependency, no `AZURE_OPENAI_*` configuration. A non-deterministic output layer on a tool whose premise is determinism is a contradiction, and the hint table in a later slice is fifteen lines of static lookup.

## Data

The reference pipeline runs on generated data, and synthetic is the point here rather than a convenience. Its bugs are parameterised by tie density, group count and row count, and those parameters are the experiment. Real data would fix the tie density at whatever the file happens to contain and make the intermittency measurement impossible to produce.

Values come from `hash(i)` rather than `random()`, so all five runs read byte-identical inputs and any divergence is the pipeline's rather than the data's. Slice 5 adds real NYC TLC trip records for the backfill and schema-change work.
