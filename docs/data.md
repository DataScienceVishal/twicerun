# Data

Two pipelines ship here. `pipelines/reference.py` runs on generated data and is the fixture the
[eval](eval.md) scores against. `pipelines/tlc_backfill.py` runs on NYC TLC trip records, which are
real, are not committed, and carry no licence.

## Why the fixture is generated

Synthetic is the point here rather than a convenience. The reference pipeline's bugs are
parameterised by tie density, group count and row count, and those parameters are the experiment.
Real data would fix the tie density at whatever the file happens to contain and make the
intermittency measurement impossible to produce.

Values come from `hash(i)` rather than `random()`, so all five runs read byte-identical inputs and
any divergence is the pipeline's rather than the data's.

## The reference pipeline ships broken

A checker that finds nothing is indistinguishable from a checker that is broken. `reference.py`
carries four bugs, one step that drifts benignly, one step that fires intermittently, one control
step that must never fire, and one step whose only job is to sit downstream of a bug so the
containment ablation has something to measure.

`pipelines/twins.py` is the matched half: the same four bugs plus the intermittent one, each with
the single line that fixes it. It is what the eval's specificity half scores against, at zero fires
across every comparison in the README's twins table, and it was already load-bearing before that,
because an amplifier that made those twins fire would be a chaos generator rather than a detector.

Two of the eight steps are constructed rather than observed, and both say so where they are defined:
the `MERGE` bug, and `roll_up_keys`, which exists to be fed a diverging artifact. Three of the four
bugs are failures I have actually been bitten by running Databricks pipelines and SQL migrations:
duplicate rows after a retry, IDs changing between runs, and totals not matching between runs. The
fourth, the non-idempotent `MERGE`, is not a war story. It came out of an experiment for this
project and I have never seen it. Its distinction is how narrow its window turned out to be: it
needs a target somewhere around 100,000 to 125,000 rows, a source staged into a real table and
`BIGINT` columns, and at 50,000 rows and again at 200,000 it gives the same answer every time.

Every configuration in the file was re-derived on this machine before it was written down, and two
of the spec's claims did not survive that. The `MERGE` bug's cause is parallel order rather than the
single-threaded persistence the spec predicted, because `threads=1` was clean at every scale tried.
And at the three rows the spec proposed for it, it gives the same answer ten times out of ten.

## NYC TLC trip records

**TLC publishes no licence for the trip records.** Searching the trip record page for the word finds
only "base license number", which is a field in the data. What the page carries instead, verbatim, is:

> The data used in the attached datasets were collected and provided to the NYC Taxi and Limousine
> Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement
> Programs (TPEP/LPEP). The trip data was not created by the TLC, and TLC makes no representations as
> to the accuracy of these data.

That disclaims responsibility. It grants nothing, and it says nothing about redistribution either
way.

So **no TLC bytes are committed here**. `scripts/fetch_tlc.py` is the only file in this repository
that opens a socket. It fetches three monthly Parquet files and verifies a SHA-256 recorded on
2026-08-26 against each, so a reader can tell "the download was cut short" from "TLC restated the
month", which are the same missing bytes to everything else. TLC does restate months: the yellow
2024-12 file was last modified 2025-02-21.

```bash
uv run python scripts/fetch_tlc.py                 # green, 3.6 MB
uv run python scripts/fetch_tlc.py --taxi yellow   # yellow, 181 MB
uv run python scripts/fetch_tlc.py --check         # verify what is on disk, fetch nothing
```

Nothing in `tests/` imports it for a download, and `--disable-socket` would fail the test if
anything tried. What the suite runs against instead is three partitions generated from
`pipelines/tlc_green_schema.sql`, which holds the real column lists read off the real Parquet with
`DESCRIBE` rather than transcribed from the data dictionary PDF. `RatecodeID` is `BIGINT` and
`PULocationID` is `INTEGER` in the same file, which is the kind of thing a hand transcription gets
wrong.

Sources: the [trip record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and
the [green data
dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf),
updated 2025-03-18 to document `cbd_congestion_fee`.

### The 2025 schema change

TLC added `cbd_congestion_fee` to the Yellow, Green and High Volume FHV trip records for 2025
onward, carrying the Congestion Relief Zone charge that took effect on 5 January 2025. So a backfill
over 2024-12, 2025-01 and 2025-02 spans a column that two of its three partitions have and one does
not. Green goes from 20 columns to 21, yellow from 19 to 20.

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

**That is a revenue column disappearing on file order, with no error and no warning.** DuckDB takes
the column set from the first file in the list. Put the 2024 partition first, which is the order a
backfill walks its months in, and the fee is gone from the answer. Reverse the list and it is back.
The two spellings that do not do that are `INSERT INTO` a target built from the older partition, which
raises `Binder Error: table target has 20 columns but 21 values were supplied`, and `UNION ALL BY
NAME`, which fills the missing column with NULL.

**twicerun cannot catch it, and that is the sharpest limitation this project has.** The wrong answer is
wrong the same way on every run, so five runs agree with each other perfectly and the report is a clean
zero. A rerun checker is blind to a deterministic wrong answer by construction. `scripts/tlc_schema.py`
asserts its three facts and exits non-zero if any stops being true, which is the most this repository
can do about a bug its main tool is structurally unable to see.

`UNION ALL BY NAME` costs something too, and the script prints it. After the union, a NULL fee on a
2024 row means the column did not exist, and a NULL fee on a 2025 row means the trip was never
charged. 46,490 of 48,326 January rows carry a fee. Nothing distinguishes the two kinds of NULL
afterwards.

### What twicerun found on the backfill

`pipelines/tlc_backfill.py` is four steps: land the three months keeping each one's own column set,
union them by name, sum the fares by partition and pickup zone, then append to a cumulative ledger
that never asks what is already in it.

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

Measured on 2026-08-26. None of this is regenerated, because the three partitions are not committed
and are not fetched in CI.

**The negative result is the more useful half.** The parallel float reduction that this whole project is
built around does not fire on a month of green taxi trips. It is not absent, it is under the row count
at which DuckDB divides the work. Cross-joining the green union against `range(k)` and comparing
`sum(total_amount)` grouped by pickup zone, four comparisons at each size, 236 groups:

| rows | groups that differed, four comparisons |
|---|---|
| 148,941 | 0, 0, 0, 0 |
| 297,882 | 41, 0, 0, 0 |
| 595,764 | 60, 61, 60, 77 |
| 1,191,528 | 103, 111, 113, 123 |
| 2,383,056 | 149, 156, 144, 151 |
| 4,766,112 | 189, 175, 167, 183 |
| 9,532,224 | 197, 196, 198, 195 |

Those are the observations rather than a summary of them. The switch-on sits between 148,941 and
595,764 rows on this machine, and **297,882 is exactly what the row-multiplication amplifier
produces from the green input**, which is why the green report shows that amplifier at `0 of 2` on a
step whose mechanism is present.

Two consequences worth being plain about. The reference pipeline's 2,000,000 rows are not
decoration: at a tenth of that the same bug is silent. And the one bug of the four that fires on
green is the ledger, because an append with no key duplicates whatever it is given at any scale, so
a reader who runs the 3.6 MB dataset still sees a real bug caught on real data.

On yellow the surrogate-key mechanism is there too, though this pipeline does not build a surrogate
key: `row_number() OVER (ORDER BY PULocationID)` over the 10,721,140 rows gave 10,720,819,
10,720,956 and 10,720,930 rows a different key across three comparisons at `threads=10`, and 0 at
`threads=1`. Ordering by the pickup timestamp instead, which is the more natural thing to write,
gave 3,401,785, 3,438,723 and 3,884,017. Adding a unique tiebreak gave 0 every time.

**3.0 GB is the yellow figure and it is not a typo.** Five runs of a step that writes 10.7 million
rows, plus the bisect and the amplified inputs. Green is 59 MB. `rm -rf .twicerun` reclaims it.
