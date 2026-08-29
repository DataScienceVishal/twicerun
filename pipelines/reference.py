"""A pipeline that ships broken, on purpose, with the bugs measured rather than assumed.

A checker that finds nothing is indistinguishable from a checker that is
broken, so this is what twicerun is pointed at. Every configuration below was
re-derived on this machine by `scripts/measure_duckdb.py` before it was written
down; the numbers in the docstrings are from duckdb 1.5.5 on a 10-core arm64
macOS box and they will move on yours.

Source values come from `hash(i)`, never `random()`, so all five runs read
byte-identical inputs and any divergence is the pipeline's rather than the
data's. There is a second reason for the shape of `generate_inputs`: DuckDB
splits an inline `range()` deterministically across threads, so a step that
aggregates straight out of `range()` reports no divergence at all. The values
have to land in a real file first.
"""

from __future__ import annotations

from twicerun.storage import StepContext

# hash() returns UBIGINT, so dividing by its maximum gives a deterministic
# double in [0, 1) without touching the RNG.
UNIT_HASH = "hash(i)::DOUBLE / 18446744073709551615.0"


def generate_inputs(ctx: StepContext) -> None:
    """The control. These four artifacts must be identical in every run.

    If this step ever shows a fire rate above zero, the comparison is broken
    and nothing below it means anything.
    """
    ctx.write(
        "orders",
        f"SELECT (i % 1000)::INTEGER AS day, i AS order_id, {UNIT_HASH} * 500 AS amount "
        f"FROM range(2000000) AS s(i)",
    )
    ctx.write(
        "customers",
        "SELECT (i % 1000)::INTEGER AS cust, i AS event_id FROM range(500000) AS s(i)",
    )
    ctx.write(
        "sparse_customers",
        "SELECT (i % 250000)::INTEGER AS cust, i AS event_id FROM range(500000) AS s(i)",
    )
    ctx.write(
        "price_updates",
        "SELECT (i % 100000)::BIGINT AS sku, (1000 + i)::BIGINT AS price_cents "
        "FROM range(200000) AS s(i)",
    )


def daily_revenue(ctx: StepContext) -> None:
    """Bug 1. sum() over a DOUBLE column, 2,000,000 rows in 1,000 groups.

    Parallel reduction associates the additions in whatever order the threads
    finish in, and float addition is not associative. Two runs at threads=8
    differed on a median 605 of the 1,000 groups over 10 attempts, and on 0 of
    1,000 at threads=1. That spread is not a bound, and the README says how many
    times a figure published there was later beaten by a longer run. The fix is
    DECIMAL(18,4), whose sum is fixed-point and reassociates exactly.
    """
    ctx.read("orders")
    ctx.write("daily_revenue", "SELECT day, sum(amount) AS revenue FROM orders GROUP BY day")


def customer_keys(ctx: StepContext) -> None:
    """Bug 2. A surrogate key from row_number() over a non-unique sort.

    500 rows share every value of `cust`, and nothing in the ORDER BY decides
    which of them comes first, so the parallel sort is free to order them
    differently on a rerun. Over 70 comparisons inside the five-run loop, a
    median 368,640 rows of 500,000 got a different key, the extremes being
    245,760 and 491,520. Sample size stated because the extremes are not
    bounds. The fix is one word: ORDER BY cust, event_id.

    Two queries in one session diverged on every attempt. Two separate runs
    reading the same Parquet file do not: across 13 invocations of the five-run
    loop this step fired 2 to 4 times out of 4, never fewer than 2 and not
    always 4. The spec has it firing every time, which was true of the narrower
    measurement it came from. Twenty passes of the contained loop gave the same
    2 to 4, and the single-threaded bisect gave 0 of 4 on every one of them, so
    the report reads PARALLEL_ORDER here.
    """
    ctx.read("customers")
    ctx.write(
        "customer_keys",
        "SELECT event_id, cust, row_number() OVER (ORDER BY cust) AS surrogate_id "
        "FROM customers",
    )


def apply_price_updates(ctx: StepContext) -> None:
    """Bug 3. A MERGE whose source holds two rows for one target key.

    Which candidate wins is not pinned by the statement, so a rerun can disagree
    with no error and no warning. No linter finds it, because it depends on the
    data rather than the query.

    The one bug in this file nobody here has been bitten by. The other three are
    failures Vishal hit running Databricks pipelines and SQL migrations; this one
    came out of an experiment for this project, and the window it fires in is
    narrow enough that most pipelines would never land in it.

    It needs scale. At 100,000 target rows and 200,000 source rows it gave 4 to
    8 distinct answers across 10 fresh connections at threads=8; at 50,000 it
    gave one answer every time, and at threads=1 it gave one answer at every
    scale tried. The spec predicted the opposite cause and had it firing at
    three target rows, which is why the first attempt to confirm it found
    nothing. The fix is to deduplicate the source before merging.

    Two details below are load-bearing and were found the hard way, by this step
    reporting 0 of 4 while the same merge diverged 8 times in 10 on its own. The
    source has to be staged into a real table: pointed straight at a view, over
    Parquet or over range(), the merge gave one answer in 10 of 10 at every
    thread count. And the columns have to be BIGINT. The identical merge on
    INTEGER columns gave 1 to 3 distinct answers where BIGINT gave 4 to 8.

    The catalogue holds 125,000 skus and the update source only covers the first
    100,000, so 25,000 rows carry through from the previous run untouched. That
    is deliberate: with full coverage the merge overwrites every row and
    ctx.state above becomes decoration, since rebuilding the seed each run would
    give the same artifact. It also happens to be where the bug is loudest, at 8
    distinct answers in 10 against 6 at full coverage. By 200,000 target rows it
    stops firing altogether, at any coverage, which is a narrower window than
    the "needs scale" story above suggests.

    In the pipeline it fires 0 to 4 times out of 4 over 20 invocations, and one
    of those 20 was a flat 0. So the five-run loop can report nothing at all on
    a step that is definitely broken. Magnitude when it does fire is a median
    17,376 rows of 125,000 over 20 contained passes. This docstring published a
    range here for three revisions and the paragraph below is every time the
    range was beaten, so it is a median with an n now.

    The floor has now been wrong twice. The first five invocations all gave 4 of
    4 and it went in as "every time"; the next eight put the floor at 1; twenty
    put it at 0. Each correction came from widening the sample, which is the
    same mistake this project exists to catch, one layer up.

    Under containment the merge always runs over run 1's catalogue rather than
    over a catalogue three runs of drift deep. Twenty contained passes fired a
    median 3 of 4 with one flat zero, at a median 17,376 rows of 125,000. Each
    resampling has moved the extremes outward: 9,248 to 35,104 first, then
    1,344 to 42,304, then 1,056 to 40,608 here and 77,120 on someone else's 28
    passes. The bisect gave 0 of 4 every time, which agrees with the standalone
    measurement: at threads=1 this merge gave one answer at every scale tried.
    """
    ctx.read("price_updates")
    ctx.state(
        "prices",
        "SELECT i::BIGINT AS sku, (500 + i)::BIGINT AS price_cents FROM range(125000) AS s(i)",
    )
    ctx.sql("CREATE TABLE catalogue AS SELECT * FROM prices")
    ctx.sql("CREATE TABLE staged_updates AS SELECT * FROM price_updates")
    ctx.sql(
        "MERGE INTO catalogue t USING staged_updates s ON t.sku = s.sku "
        "WHEN MATCHED THEN UPDATE SET price_cents = s.price_cents"
    )
    ctx.write("prices", "SELECT sku, price_cents FROM catalogue")


def append_audit_log(ctx: StepContext) -> None:
    """Bug 4. An append with no unique key, so a rerun duplicates every row.

    Deliberately kept even though `grep -c "INSERT INTO"` would find it. It is
    the one bug of the four a code reader gets for free, and the eval's static
    baseline exists to show that it is the only one.

    It is also the step containment changes most, because it is the only one
    whose input is its own last output. Uncontained, run 4 appends to run 3's
    log and the four comparisons report 3,953, 7,906, 11,859 and 15,812 extra
    rows: four reruns' worth of duplication, all of it attributed to one step.
    Contained, every run appends to run 1's log and all four report 3,953,
    which is what one rerun of this step actually does. Both figures held on
    every pass, 20 contained and 10 not.

    And it is the one step in this file the bisect labels
    PERSISTS_SINGLE_THREADED, 4 of 4 at threads=1 as well as at threads=10. An
    append with no unique key duplicates on rerun whatever the thread count,
    and that is the answer telling you threads are not your problem.
    """
    ctx.read("orders")
    ctx.state(
        "audit_log",
        "SELECT 0::BIGINT AS order_id, 0.0::DOUBLE AS amount WHERE false",
    )
    ctx.write(
        "audit_log",
        "SELECT order_id, amount FROM audit_log "
        "UNION ALL "
        "SELECT order_id, amount FROM orders WHERE amount > 499",
    )


def mean_basket(ctx: StepContext) -> None:
    """No bug here. A correct float average that legitimately reassociates.

    Bit-exact comparison reports several hundred of the 1,000 groups as
    differing and every one is a false positive, which is what the oracle was
    built for. Under the default policy it still reports them, with the ULP
    distance and the relative size attached, because the user asked whether the
    pipeline gave the same answer twice and it did not. Under
    --policy reduction-order the count goes to zero and the magnitudes stay.

    The downgrade is not allowed to stand on the divergence class alone, which
    it was until the mechanism became a condition of it: this step and
    daily_revenue both gave 0 of 4 at threads=1 on all 20 contained passes.
    """
    ctx.read("orders")
    ctx.write("mean_basket", "SELECT day, avg(amount) AS mean_amount FROM orders GROUP BY day")


def sparse_customer_keys(ctx: StepContext) -> None:
    """Bug 2's code at two rows per tie group, where it fires only sometimes.

    Eighteen standalone attempts landed on roughly 0, 8,480, or a quarter of
    the table, with zero coming up about a third of the time. Inside the
    five-run loop it fires 0 to 4 times out of 4 over 20 invocations, and it was
    a flat 0 on five of them.

    Twenty contained passes put that floor at 1 in 20 rather than 5, on the same
    machine and the same code, which is a reminder that a floor counted off 20
    samples is not a floor. Every pass where it fired gave 0 of 4 at threads=1.

    That floor is the real argument for amplification. Five runs are much better
    than two, but they are not enough: on 5 invocations in 20 this step is broken,
    the tool ran it five times, and the report said nothing. Raising the number
    of runs cannot fix that, because it lowers the miss rate for a given firing
    probability without changing the probability. Amplification changes the
    probability, which is why both exist.
    """
    ctx.read("sparse_customers")
    ctx.write(
        "sparse_customer_keys",
        "SELECT event_id, cust, row_number() OVER (ORDER BY cust) AS surrogate_id "
        "FROM sparse_customers",
    )


def roll_up_keys(ctx):
    """No bug of its own. It is here to be downstream of one.

    Every other step in this file reads `generate_inputs`, whose output is
    identical in every run, so nothing here read a diverging artifact and the
    cascade containment exists to stop could not happen. That made the
    containment ablation score zero on the one quantity the eval pre-registered
    for it, and a feature nothing exercises is a coverage gap rather than proof
    it is unnecessary. So this step is constructed, not a failure anyone here
    has been bitten by, and the README says which.

    It reads `customer_keys`, whose surrogate ids shuffle between runs, and
    buckets on `event_id / 1000` so that each bucket holds one row from every
    cust group. Two obvious bucketings do not work and both fail the same way.
    `cust` keeps the same block of surrogate ids per group and only shuffles
    the pairing inside it, which is the tie the attribution section of the
    README is about. `event_id % 1000` is worse: it equals `cust` exactly for
    this generator, so the first attempt at this step reported 0 of 4 with
    containment off and looked like evidence against the feature it was built
    to exercise.

    Contained, it reads run 1's copy every time and reports 0 of 4. Under
    --no-containment it reads its own run's and fires, which is the ablation
    number. The aggregate itself is an integer minimum and cannot reassociate
    into a different answer, so a fire rate above zero here means it was fed
    something different, never that it computed something different.
    """
    ctx.read("customer_keys")
    ctx.write(
        "key_buckets",
        "SELECT (event_id // 1000)::INTEGER AS bucket, min(surrogate_id) AS lowest_id "
        "FROM customer_keys GROUP BY bucket",
    )


STEPS = [
    generate_inputs,
    daily_revenue,
    customer_keys,
    apply_price_updates,
    append_audit_log,
    mean_basket,
    sparse_customer_keys,
    roll_up_keys,
]
