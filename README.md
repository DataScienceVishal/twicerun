# twicerun

[![ci](https://github.com/DataScienceVishal/twicerun/actions/workflows/ci.yml/badge.svg)](https://github.com/DataScienceVishal/twicerun/actions/workflows/ci.yml)

Run a batch pipeline several times on the same input and report, per step, how often it failed to give the same answer.

The naive version of that is a two-run diff, and on correct code it reports hundreds of findings. DuckDB adds the terms of a parallel `sum()` in whatever order the threads finish in, and float addition is not associative. Running a pipeline five times is the easy half. Deciding whether two of its outputs are the same answer is the project.

```
$ uv run twicerun run pipelines/reference.py

  0 generate_inputs         0 of 4  NO_DIVERGENCE_OBSERVED
  1 daily_revenue           4 of 4  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER
  2 customer_keys           4 of 4  DIVERGENT  ROW_MISSING ROW_EXTRA  cause PARALLEL_ORDER
  3 apply_price_updates     2 of 4  DIVERGENT  ROW_MISSING ROW_EXTRA  cause PARALLEL_ORDER
  4 append_audit_log        4 of 4  DIVERGENT  MULTIPLICITY  cause PERSISTS_SINGLE_THREADED
  5 mean_basket             4 of 4  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER
  6 sparse_customer_keys    0 of 4  STABLE_ON_THIS_INPUT
  7 roll_up_keys            0 of 4  NO_DIVERGENCE_OBSERVED

5 of 8 steps diverged in 13.4s.
```

Four of those five are planted bugs. `mean_basket` is correct code, a float average whose answer moves in the last few bits. Step 6 is the fifth bug. It agreed with itself on all four comparisons, which is the case a plain five-run loop reports as a clean zero.

```bash
git clone https://github.com/DataScienceVishal/twicerun && cd twicerun
uv sync --all-extras
uv run twicerun run pipelines/reference.py
```

Needs [uv](https://docs.astral.sh/uv/) and nothing else. One pass writes a median 376 MB of Parquet under `.twicerun/`, which is gitignored.

<details>
<summary>The same run in full, with the per-step evidence the excerpt above drops</summary>

`reference.py` carries four bugs, one step that fires intermittently, one benign float step, one control that must never fire, and one step whose only job is to sit downstream of a bug.

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

</details>

<details>
<summary>The measurement the whole tool is built on: one `sum()`, run twice, disagreeing on 492 to 738 of 1,000 groups</summary>

The premise is one measurement, and it takes two seconds to check. `SELECT g, sum(v) FROM t GROUP BY g` over 2,000,000 rows in 1,000 groups, run twice at `threads=8` on DuckDB 1.5.5, disagrees on several hundred of the 1,000 groups. Ten runs on this machine gave 492, 497, 510, 575, 599, 611, 618, 690, 700 and 738. Nothing is wrong with the query. Parallel reduction adds the terms in whatever order the threads finish in, and float addition is not associative. At `threads=1` it disagrees on none of them, and `count()` over the same table never moves, which is the control: integer arithmetic cannot reassociate into a different answer.

```bash
uv run python scripts/measure_duckdb.py   # re-derives that measurement in raw DuckDB, about two seconds
```

Its counts will not match these, for the same reason the transcript above will not. What holds is the shape: parallel figures large, `threads=1` figures zero, the three `count()` runs agreeing with each other, the single-word tiebreak fix clean, and the `MERGE` bug present at threads=8 and absent at threads=1. The script checks ten such invariants and exits non-zero if any of them breaks, so it fails loudly rather than printing numbers that mean something different from what they say.

</details>

## What it cannot catch

<details>
<summary>A backfill that walks its months oldest first silently loses `cbd_congestion_fee`, and twicerun reports a clean zero</summary>

`pipelines/tlc_backfill.py` runs over three months of real NYC TLC green trip records. TLC added `cbd_congestion_fee` to the trip records for 2025 onward, so a backfill over 2024-12, 2025-01 and 2025-02 spans a column that two of its three partitions have and one does not. `scripts/tlc_schema.py` prints what DuckDB does with that:

```
read_parquet over the three as one list, which is what a backfill hands it:
  oldest partition first  20 columns  cbd_congestion_fee: DROPPED, silently
  newest partition first  21 columns  cbd_congestion_fee: present
```

DuckDB takes the column set from the first file in the list. Put the 2024 partition first, which is the order a backfill walks its months in, and a revenue column is gone from the answer with no error and no warning. Reverse the list and it is back.

**twicerun cannot catch it, and that is the sharpest limitation this project has.** The wrong answer is wrong the same way on every run, so five runs agree with each other perfectly and the report is a clean zero. A rerun checker is blind to a deterministic wrong answer by construction. `scripts/tlc_schema.py` asserts its three facts and exits non-zero if any stops being true, which is the most this repository can do about a bug its main tool is structurally unable to see. The two spellings that raise instead, and what `UNION ALL BY NAME` costs, are in [docs/data.md](docs/data.md).

</details>

<details>
<summary>Only pipelines written against `ctx.read` and `ctx.write` get tested at all</summary>

**twicerun only tests pipelines written against its storage interface.** It does not test arbitrary pipelines.

Watching a pipeline's writes without its cooperation needs either a kernel block-layer wrapper or system-call interception. Both are out of budget and both are worse on macOS. What is left is an interface the pipeline reads and writes through, which is portable, needs no root, and only sees pipelines that opted in.

What that buys back is why it is a design choice rather than a workaround. One abstraction carries five jobs: it is where artifacts get captured, where reads get redirected for containment, where input rows get counted for the tolerance bound, where the column names feeding attribution come from, and where amplified inputs get substituted. A step calling `ctx.write()` gets all five. A step calling `duckdb.execute("COPY ... TO ...")` behind its back gets none.

</details>

<details>
<summary>Everything else it gets wrong, including two published figures that cannot go down when the detector gets worse</summary>

**Sorting to pair rows inside a key group is a heuristic once there is more than one float column.** The ordinal sorts both sides the same way, and for a single float column sorted-to-sorted pairing is the assignment that minimises total absolute difference, so it is optimal. With several, a lexicographic sort can pair the wrong two rows inside one key group. That can only understate a difference, so the failure mode is a bounded false negative confined to within-key-group permutations of float-only differences.

A key group with no exact columns is one big group. If every column is a float, there is no key, the whole artifact sorts as one group and rows pair by order alone. It is the weakest case here and it is where the heuristic above does the most work, so the report says `matched on no key` when it happens rather than leaving it to be inferred.

NaN payloads are not distinguished. The sign survives and the payload does not.

**Two of the published figures cannot go down when the detector gets worse.** Attribution is a rate over findings the oracle produced, and the bound check is a rate over float step-passes that drifted, so both are conditional on something having been found. A detector rewired to report no divergence at all still scores perfectly on both, because neither consults a fire count: they read `step.worst` and `step.rounds` directly. They are quality-of-explanation figures rather than detection figures, and the eval now says so on the attribution line and prints `NOT MEASURED` rather than a zero when there is nothing to score.

`rows_read` is not the term count the bound wants. Both directions it is wrong in are measured in [docs/comparison.md](docs/comparison.md), and the size of the error prints in every report.

A `threads=1` rate of 0 of 4 is four comparisons, not a property. It is reported with its one-sided bound for that reason, and `PARALLEL_ORDER` should be read as the name of a pattern in two measured rates.

The bisect resolves its reads against run 1 even under `--no-containment`. A single step cannot be re-executed on its own without something to read, so the ablation ablates the main loop and not the bisect. A step that only inherited a divergence can therefore come out `PARALLEL_ORDER` in an uncontained report, and the report says so where it happens.

Tie collapse declines more often than it applies. It targets 500 rows per distinct value and refuses to touch a column that is already at or past that, since raising tie density is the whole job and there is nothing to raise. The twin coverage table below is the count of chances it took against the chances it had. The refusal is printed with its reason on the same line as the amplifier, and the declined amplifier goes in the not-varied list, but a reader skimming for zeros should know that most of the boxes tie collapse leaves are unticked rather than green.

An amplifier only sees a step's `ctx.read` inputs. State pulled in through `ctx.state` resolves against the previous run's copy rather than an upstream step's output, so there is nothing for an amplifier to substitute that the step's own last execution did not already decide. On the append bug that is the right answer and on some other shape of bug it may not be.

Three amplifiers is three, and there is no argument that they are the right three. Each is aimed at a bug that was measured here. A pipeline whose non-determinism comes from a clock read, a hash seed, a file listing order or a network response gets nothing from any of them, and the not-varied list is where the report admits it.

**A committed artifact is a measurement of one laptop.** DuckDB 1.5.5, ten threads, macOS on Apple silicon. The stamp above every table says so, and nothing here calibrates against a second machine or a second DuckDB, which every report's not-varied list says as well.

**Six figures published in this file were beaten by a longer run, and every one of the six was a number retyped out of a terminal into a table.** A read-through on 2026-08-27 found eight more of the same class in comments and docstrings, where no generator can reach. That is why the tables below are rendered from a committed measurement instead of typed, which removes transcription as a way for a cell to go false and does nothing whatever about ten trials being ten trials.

</details>

## Results

Every table below is rendered from a committed measurement by `twicerun report`, so no figure in one was typed by hand, and `tests/test_readme_tables.py` fails the build when this file stops matching the artifact.

<!-- twicerun: provenance -->
Generated by `twicerun report` from `results/eval-2026-08-27.json`, written 2026-08-27 by `uv run python scripts/eval.py --trials 10 --json`.

DuckDB 1.5.5 at `threads=10` on macOS-26.5.2-arm64-arm-64bit. 10 trials of 5 runs, so 4 comparisons per step per trial and 40 in all. 502s, a median 49s a trial.
<!-- /twicerun: provenance -->

<details>
<summary>Per step: what fired, what stopped firing at `threads=1`, and how far the numbers moved</summary>

Ten trials is fewer than the forty passes an earlier version of this table published, and that is the price. What it buys is the table being a function of a measurement rather than of a transcription, so running the eval again and committing what came out is the only way to move a cell.

**The ulp counts and the relative drift are maxima**, over 1,000 groups times four comparisons, so their observed range grows with how long anyone looks and a bracket on either can always be beaten by looking longer. That bracket is the one promise this file has already broken four times, so the maxima carry a median and a sample size and nothing else. The quantities with a real ceiling carry the ceiling instead: a step comparing 500,000 rows cannot lose more than 500,000 of them, and a fire rate out of four has five possible values, so the whole distribution fits in the cell.

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

A cell in the second column is a distribution. `4 of 4 x7, 3 of 4 x2, 2 of 4 x1, 1 of 4 x0, 0 of 4 x0` reads as seven of the ten trials at four fires out of four, two at three, one at two, and none at one or at nothing. **The buckets that came out zero print rather than being left out**, which they were not when this table first appeared: an omitted bucket reads as the value being impossible when it only means unobserved, and a later ten-trial run gave `apply_price_updates` the `0 of 4` its cell had never mentioned. A step whose rate never varied collapses to `0 of 4 on all 10` instead, which accounts for every trial and leaves nothing unstated either. The medians in the last column will move, and the ulp medians move by a whole ulp on a sample this size, which is why they carry an n.

The two float steps go to zero under `reduction-order` and nothing else moves. That is the whole claim for the oracle: the false positives disappear and the four real bugs are caught by the same code that dismissed them. The price is in the same column. `daily_revenue` is bug 1, a `sum()` over `DOUBLE`, and the policy downgrades it because the drift is float-only, inside the derived bound and gone at `threads=1`, so **sensitivity on the four broken steps is 3 of 4 by design under this policy** and a release stops being gated on that step.

The `threads=1` column is where one row is unlike the others. Five steps stop diverging with one thread and one does not, which is the difference between a step whose answer depends on how the work was divided and a step whose answer depends on it having run before. No number of runs separates those two; a second thread count does it in one column. A step that never fired is not bisected at all, and the cell says so rather than showing a zero that would be four comparisons of nothing.

</details>

<details>
<summary>Bit-exact multiset equality matched the oracle on every step trial it was scored on, so these tables hold no evidence for the oracle over it</summary>

Four baselines, implemented rather than named:

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

**Baseline 1 was handed one comparison and the oracle four, and that is most of the gap between them.** The run-matched row is the control that was missing: same runs, same bytes, same containment, and nothing but multiset equality over row hashes. It agrees with the oracle on every cell of the sensitivity table and fires on no twin, so **the sensitivity and specificity tables hold no evidence for the oracle over the cheap comparison**. What the oracle produces that multiset equality cannot is the divergence class, the ulp and relative magnitudes, the attributed column, the derived bound and the single-threaded cause, and none of those five is scored in either table. Baseline 2 catches four of the five matched pairs, which is more than the spec predicted, and [docs/eval.md](docs/eval.md) has the three things it still cannot do.

Eight conditions were pre-registered in the spec, each one a way for a piece of this project to turn out unnecessary:

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

**The committed run triggers the sensitivity condition, and it is the second of six ten-trial runs to do so.** The step is `apply_price_updates` and the shortfall is one trial. Nothing was changed in response, because the threshold was pre-registered and the eval exits 0 on a triggered condition by design. What the condition cannot distinguish is the detector missing a divergence from the fixture not producing one, and on this step the fixture is intermittent: the `MERGE` bug needs a target between roughly 100,000 and 125,000 rows and a source that happens to carry a repeated key.

Three of the eight used to be readable off an absence rather than off a measurement. A step that writes no artifact compares nothing, fires on none of the nothing it compared, and arrives at the conditions as a clean zero, which then prints TRIGGERED and declares a piece of this project unnecessary. Seven of the eight carry a third verdict now, `NOT MEASURED`, and the eighth is the wall clock, which is measured whatever the pipeline did.

</details>

<details>
<summary>Correct code under the same tool: no twin fired at all, and tie collapse declined most of the chances it had</summary>

`pipelines/twins.py` is the same pipeline with the single line that fixes each bug, and it is what the specificity half of the eval scores against.

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

Two things that denominator contains are worth naming. One of the six pairs is `generate_inputs`, the same function in both files, so a sixth of it is the sensitivity table's control row counted a second time. And `mean_basket` and `roll_up_keys` have no twin at all, so the correct-code step that fires in every trial is not in the specificity set.

<!-- twicerun: twin-coverage -->
| amplifier | twin step-passes it ran on | comparisons | fired | raised |
|---|---|---|---|---|
| tie collapse | 20 of 60 | 40 | 0 | 0 |
| thread count | 60 of 60 | 120 | 0 | 0 |
| row multiplication | 50 of 60 | 100 | 0 | 0 |
<!-- /twicerun: twin-coverage -->

**The first column is the honest part of that table.** Tie collapse declines most of its chances and the report says why each time rather than printing a zero: `orders.day` and `customers.cust` already hold 2,000 and 500 rows per value, at or past what the amplifier targets, and `generate_inputs` reads no artifact at all. A twin an amplifier never touched is not evidence that the amplifier is safe on it.

</details>

<details>
<summary>Containment: the step downstream of a bug fires on every uncontained trial and none of the contained ones</summary>

Contained means runs 2 to 5 read run 1's artifacts, so a divergence at one step cannot reach the next. Dropping it is baseline 3.

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

The bottom row is the one containment is for. `roll_up_keys` computes an integer minimum that cannot reassociate, so every uncontained fire there is inherited from the step it reads rather than its own. Read the other rows as noise either way: two steps in this pipeline are intermittent, so a contained pass and an uncontained pass differ mostly by which of them happened to fire. [docs/runs.md](docs/runs.md) has the ablation, the per-comparison progression, and where the idea was borrowed from.

</details>

<details>
<summary>The 1,000x of headroom in the tolerance check is really 1x, because my own test pipeline groups into 1,000 days</summary>

The tolerance is derived rather than picked. Reassociating a sum of `n` float64 terms moves the result by at most `gamma_n * sum(|x_i|)`, two orderings differ by at most twice that, and the tool substitutes `max(|a|, |b|)` for the sum it does not have. One condition was fixed before any of this was written: observed drift on the benign step must sit at least 1,000x inside that bound, or the derivation is not conservative enough and the design gets revisited rather than the threshold moved.

<!-- twicerun: drift-bound -->
|  | `daily_revenue` | `mean_basket` |
|---|---|---|
| median furthest relative drift, n=10 | 4.7e-16 | 5.2e-16 |
| bound at `n` = 2,000,000 rows read | 4.44e-10 | 4.44e-10 |
| bound at `n` = 2,000 terms per output row | 4.44e-13 | 4.44e-13 |
| headroom at the loose `n`, and how often it cleared 1,000x | 946,168x, 10 of 10 | 859,629x, 10 of 10 |
| headroom at the tight `n`, and how often it cleared 1,000x | 946x, 1 of 10 | 860x, 0 of 10 |
<!-- /twicerun: drift-bound -->

**On this fixture the two readings are the same check, and the 1000x is cancelled exactly.** The bound is linear in the term count in this regime, so the loose ratio is the tight ratio times the number of output rows, and `reference.py` groups into `(i % 1000)` days. The multiplier the check asks for is 1,000 and the free parameter of my own test pipeline is 1,000, so clearing the pre-registered check at the loose `n` is clearing the honest `n` with no margin at all. An otherwise identical pipeline grouping into 100 days would fail the same check on the same drift. What the check establishes is that the derivation is not wildly wrong; what it does not establish is a thousandfold safety margin, and the number was picked before anyone multiplied the factor out.

The last row is a count rather than a verdict because the tight check fails most of the time rather than every time. Both readings print in every report, so nobody has to take that from this file, and [docs/comparison.md](docs/comparison.md) has the derivation and the two directions `rows_read` is wrong in.

</details>

<details>
<summary>Why a nastier input beats more runs, measured head to head on the intermittent step</summary>

More runs lower the chance of missing a step that diverges with probability `p` and do nothing to `p`. Amplification changes `p`, by handing a step an input built to make the mechanism fire, and only steps the main loop found nothing in get one. Pointed at the intermittent step over 40 passes, whether or not the loop had already caught it:

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

`twicerun run` cannot produce that table and is not meant to. Amplification only touches steps the loop came back quiet on, so on most passes step 6 fires, never reaches an amplifier, and contributes nothing to the second half of the comparison, which is why the table comes from `scripts/amplification_gap.py` instead. The last two columns keep three things apart: an amplifier that declined to build an input and one that built an input the step wrote nothing from both count as could-not-ask, and only a comparison that happened and found the step clean is evidence about the step. [docs/amplification.md](docs/amplification.md) has the three amplifiers, the four statuses, and why an amplifier that broke correct code would make the tool worthless.

</details>

<details>
<summary>Naming the column that moved, where two thirds of the score is a sort key graded against itself</summary>

For each key column, drop it and recount what failed to pair. On `customer_keys` that turns 491,520 unmatched rows into a sentence naming a column.

<!-- twicerun: attribution -->
Leave-one-out named the column the step invented, rather than one it copied in, on 27 of 27 attributions. 18 of those 27 had two columns leaving the same count behind, so the counts chose nothing and the tie-break chose.
<!-- /twicerun: attribution -->

**Two thirds of that is a sort key scored against itself.** On both `row_number` steps, dropping `surrogate_id` and dropping `event_id` each take the unmatched count to zero, because the two columns are a bijection whose pairing moved, so the counts choose nothing and the `(remaining, from_input, column)` tie-break chooses. Preferring the column the step invented is right, and `tests/test_oracle.py::test_a_tie_goes_to_the_column_the_step_invented` asserts it as a unit test; re-scoring it ten times a run and calling the result an accuracy figure is not. `apply_price_updates` is the one step where the counts do the work.

</details>

## Running it

<details>
<summary>The full command list, from a clone to regenerating these tables</summary>

Python, DuckDB and the dev tools all come from `uv sync`.

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

The tests run with no credentials and no network:

```bash
uv run pytest
uv run ruff check .
uv run python scripts/check_fingerprint.py
```

CI runs all three. The pre-commit hook runs the second and third and not the tests, because a suite that takes twenty seconds on every commit gets disabled within a day, so a change that passes locally and skips the hook can still fail on push.

`--disable-socket` is in `addopts`, and `tests/test_socket_is_blocked.py` is what makes it live rather than configured. It opens a TCP socket and asserts `SocketBlockedError: A test tried to use socket.socket.`, which is raised where the call is written rather than after a timeout somewhere in the network stack, so deleting the flag fails the suite rather than quietly switching the network back on. It matters more now that `scripts/fetch_tlc.py` exists and does open a socket: `tests/test_fetch_tlc.py` imports seven names from it and calls `download()` in none of them, and the guard is what enforces that rather than the intention.

Regenerating this file's tables is three commands, and the last one is the only way a figure in them changes:

```bash
uv run python scripts/eval.py --json results/eval-2026-08-27.json
uv run python scripts/amplification_gap.py 40 --json results/gap-2026-08-27.json
uv run twicerun report results/eval-2026-08-27.json results/gap-2026-08-27.json --update README.md
```

`--update` refuses in both directions. A marker pair this file carries with no artifact behind it, and an artifact table this file does not carry, are both errors that name what is missing, because a block that silently stops being regenerated keeps whatever it last said. `tests/test_readme_tables.py` runs the same comparison in CI, so a hand edit inside a marked block fails the build rather than surviving to a reader.

</details>

<details>
<summary>Exit codes: 1 is your data, 4 is an input twicerun made up, 5 is a check that did not run</summary>

| exit | meaning |
|---|---|
| 0 | nothing diverged |
| 1 | a step diverged on the real input |
| 2 | bad input, including a column the oracle refuses to compare, a `--key` naming a column that is not there, and a `report` whose artifacts and markdown do not line up |
| 3 | the run or the judge crashed |
| 4 | a step diverged only under an amplifier |
| 5 | an amplifier raised, so the check that would have tightened a zero did not run |

1 means divergence on your data and only that, so a release gate keyed on it does not also trip on a broken pipeline, and a comparison downgraded to `TOLERATED` does not set it. **4 exists because 0 and 1 are both wrong for `STABLE_ON_THIS_INPUT`.** Zero is disqualifying, since this tool's argument is that a green five-run loop lies about exactly that step, and folding it into 1 would collapse "your pipeline gave two answers on your data" and "on an input twicerun fabricated" into one integer. 5 is the same argument one step further: exit 0 was carrying both "I checked and found nothing" and "the check that would have made that meaningful did not run", and those are further apart than 1 and 4 because the second one is silent.

</details>

<details>
<summary>Why `--key` refuses a float column, and why a run that fails is never cleaned up</summary>

`--key artifact=col,col` matches rows of one artifact on a subset of its exact columns, which is how you say that a surrogate key is not part of the answer. It refuses a float column, and the reason is worth stating because the flag looks harmless: matching on a float joins with bit equality, so one ulp of reassociation comes back as a missing row plus an extra row that no policy can downgrade, the step then reports no drift, and a step with no drift has no reassociation bound to print. `--key daily_revenue=day,revenue` used to delete this file's own falsifiable check, headroom figure and all.

`--no-containment` and `--no-amplify` are the two flags that change what gets measured rather than how it is judged, which is why `judge` has neither: a saved run was executed one way or the other and cannot be re-scored into the other.

One pass writes a median 376 MB of Parquet under `.twicerun/`, which is gitignored, and `--run-dir` puts it somewhere else. Retention keeps one directory **per concurrent invocation**, so run it serially and the footprint stays there however many times you run it. **A run that fails is never pruned**, because pruning is on the success path so that a failed run cannot delete the run you would have judged instead. Three `--key` typos in a row therefore leave three directories and 619 MB: each exits 2 after executing the pipeline and before comparing anything, and nothing will clear them but you. `--keep 0` turns pruning off and `rm -rf .twicerun` reclaims the lot.

</details>

## What it does not do

No `dbt`, Airflow, Dagster or Prefect adapter. No Spark. No syscall interception. No nested type comparison. No web UI. No fix generation.

<details>
<summary>No model calls, and the crash injection slice that a pre-registered condition cut</summary>

No model calls either: no LLM adapter, no `openai` dependency, no `AZURE_OPENAI_*` configuration. A non-deterministic output layer on a tool whose premise is determinism is a contradiction that someone would notice, and nothing here needs one.

**Crash injection was cut, by a contingency the spec wrote for it.** It was pre-declared droppable on one condition: that the sensitivity numbers miss their pre-registered threshold. Run B of the eval put `apply_price_updates` at 9 of 10 against a fixed 10 of 10, the condition fired, and the two days went to the detector and to the report generator instead. The committed run puts the same step short again, which is the one TRIGGERED in the conditions table above. The spec's own argument for crash injection was already that it was the weakest part of the project: 16 kill points across two write styles produced exactly one divergence, and that one was the `INSERT` bug that `grep` finds. So there is no `twicerun crash`, and the step-boundary schedule stays what it was in the spec, an argument about the shape of the project rather than a code path.

**No `(class, cause)` hint table, which the spec wanted and crash injection was going to carry.** Two hints exist and both are one line of static text: `WALL_CLOCK` when a timestamp column moved, which is the commonest reason a step never reproduces, and a line naming an artifact one run wrote and the other did not. Mapping the five classes against the two causes to a suggested fix was fifteen lines of lookup, and it went with the cut.

</details>

## How it works

The arguments behind the code, with the measurements that back them, are in [docs/comparison.md](docs/comparison.md) for the oracle and the derived tolerance, [docs/runs.md](docs/runs.md) for why five runs and what a pass costs, [docs/amplification.md](docs/amplification.md) for the three amplifiers, [docs/eval.md](docs/eval.md) for the trials and the baselines, and [docs/data.md](docs/data.md) for the fixture's bugs and the TLC licence position.

<details>
<summary>What each module owns, in the order the code runs them</summary>

`src/twicerun/storage.py` is the interface, and every other file assumes it. `ctx.read` and `ctx.write` address artifacts by `(run, step index, name)`. The method worth understanding is `ctx.state(name, initial)`, which resolves the *previous run's* copy of an artifact rather than this run's. Without it a checker starts every run from an empty directory and can never see the two bugs that only exist because a pipeline runs against state its own last execution left behind.

`src/twicerun/runner.py` is the loop: five runs, then the single-threaded bisect over the steps that fired, then the amplifiers over the steps that did not. Run 1 is the reference and runs 2 to N are each compared against it, giving `k of m`. All-pairs clustering was the alternative and is rejected because tolerance-based equality is not transitive, so "how many distinct answers" stops being well defined the moment any tolerance exists.

`src/twicerun/oracle.py` is the comparison and it is the project. Columns are partitioned into types that cannot legitimately change and types that can, so `DATE` moving is a finding and a `DOUBLE` moving is a measurement. Rows are matched on the exact columns, which turns an assignment problem into a join. `row_number() OVER (PARTITION BY key ORDER BY values)` on both sides preserves multiplicity, so a duplicated row reads as `MULTIPLICITY` rather than as a row count that happens to differ. Every float pair that moved is reported with both its ULP distance and its relative difference, and neither decides anything alone. `src/twicerun/sql.py` writes all of that out as DuckDB, including the ULP transform: `DOUBLE` to `BIT` gives the raw IEEE-754 layout, `BIT` to `BIGINT` reads those bits as two's complement, and reflecting the negative half about the minimum makes integer distance equal ulp distance.

`src/twicerun/measurement.py` collects findings per step and `src/twicerun/policy.py` decides what they are worth, with nothing written back. That is why `twicerun judge` can re-score one saved run under three policies and produce three reports whose figures are identical and whose fire rates are not.

`src/twicerun/cause.py` is the second axis: the label vocabulary, the one-sided bound, and the reasoning about what a zero out of four can support. The execution that produces it lives in the runner, because re-executing a pipeline is the runner's job.

`src/twicerun/amplify.py` is the third: the three input transformations, the status vocabulary, and the list of axes this tool does not vary. Each amplifier is a pure function from one Parquet file to another, which is what lets the properties that have to hold every time be tested every time.

`src/twicerun/tables.py` renders this file's results tables out of the committed artifacts. It is in the package rather than in `scripts/` because two of its functions are imported back by the eval: a fire-rate distribution and a tally of labels have to read the same in the terminal and in a table, or the two disagree about the same run.

</details>

<details>
<summary>Data: a generated fixture whose bugs are its parameters, and NYC TLC records that carry no licence</summary>

The reference pipeline runs on generated data, and synthetic is the point here rather than a convenience. Its bugs are parameterised by tie density, group count and row count, and those parameters are the experiment: real data would fix the tie density at whatever the file happens to contain and make the intermittency measurement impossible to produce. Values come from `hash(i)` rather than `random()`, so all five runs read byte-identical inputs and any divergence is the pipeline's rather than the data's. Three of its four bugs are failures Vishal has been bitten by on Databricks pipelines and SQL migrations: duplicate rows after a retry, IDs changing between runs, and totals not matching between runs. The fourth, a non-idempotent `MERGE`, came out of an experiment for this project and he has never seen it in production.

The second pipeline runs on NYC TLC trip records. **TLC publishes no licence for them**, only a disclaimer that it did not create the data and makes no representations about its accuracy, which grants nothing and says nothing about redistribution either way. So no TLC bytes are committed here: `scripts/fetch_tlc.py` fetches three monthly Parquet files and verifies a SHA-256 recorded on 2026-08-26 against each, and the suite runs against partitions generated from the real column lists in `pipelines/tlc_green_schema.sql`, read off the real Parquet with `DESCRIBE` rather than transcribed from the data dictionary. What the backfill found is in [docs/data.md](docs/data.md), including a revenue column that DuckDB drops silently on file order and that this tool is structurally unable to catch.

</details>

<details>
<summary>Every figure here that nobody can regenerate, listed with its date</summary>

Everything here was measured once, on a date, and transcribed. None of it comes out of a committed artifact, and each is kept because deleting it would lose something a reader wants:

- The transcripts, 2026-08-26: the pass at the top of this file, the bound excerpt and the two `judge` runs in [docs/comparison.md](docs/comparison.md), and the amplifier blocks in [docs/amplification.md](docs/amplification.md). All of them are illustrations of the output format rather than results, and each says so where it appears.
- The five ten-trial eval runs A to E and the sixth taken in a fresh clone, 2026-08-26, all in [docs/eval.md](docs/eval.md). Their JSON was never written, which is the defect the report generator exists to stop repeating. They are kept because five runs of the same code disagreeing about the headline number is the strongest evidence in this repository for its own thesis.
- The per-comparison progression of the append bug, 3,953 to 15,812 across four comparisons, 2026-08-26, over 10 uncontained passes and 20 contained ones. The eval keeps the loudest comparison per step, so the progression is not in the artifact.
- The twins over 12 and 40 passes, 2026-08-26. The committed artifact's twin coverage is 10 trials of 6 steps.
- **Every `threads=1` figure was 0 of 4 or 4 of 4 and never anything between**, across 636 bisected step-passes counted on 2026-08-26: 166 while the bisect was being built, 237 in a fresh clone, 233 in a 40-pass sweep. The count is transcribed; the property is not, because the third column of the results table above is generated and every cell in it is still a zero or a four. It is the one quantity here a larger sample has never moved, it was not designed, and it is not explained.
- The cost measurements, 2026-08-26: the 25x median multiplier, the 19/15/65 split and the `--no-amplify` comparison over 12 passes each, all in [docs/runs.md](docs/runs.md). Wall clock on a laptop, and that section says what it is worth.
- The TLC backfill results and the row-count switch-on table, 2026-08-26. These need the three TLC partitions, which are not committed and are not fetched in CI, so nothing here can regenerate them.
- The DuckDB constructions in `scripts/measure_duckdb.py`, re-derivable in about two seconds by running it. Its ten invariants fail loudly rather than printing figures that mean something else.
- The two measurements the amplifiers were tuned on, 2026-08-26, both in `amplify.py`: the float sum fired on 1 of 8 comparisons at `threads=2` and 8 of 8 at `threads=4` and is flat to 40, which is where the floor of 4 comes from, and the surrogate-key bug fired on 10 of 12 comparisons at 2 rows per tie group against 11 of 12 at 500.
- The 16 crash-injection kill points across two write styles that produced one divergence, 2026-08-26. That slice was cut, so nothing here can produce them again, which is what this list is for.
- Two illustrations in the prose that the tool does measure and the artifact does not carry: around 600 findings on the benign step under `strict`, and which column attribution named per step. The eval prints both on every run, and the generated cells beside them are the versions nobody typed.

</details>
