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
    finish in, and float addition is not associative. Measured at 555 to 714 of
    the 1,000 groups differing between two runs at threads=8, and 0 of 1,000 at
    threads=1. The fix is DECIMAL(18,4), whose sum is fixed-point and
    reassociates exactly.
    """
    ctx.read("orders")
    ctx.write("daily_revenue", "SELECT day, sum(amount) AS revenue FROM orders GROUP BY day")


def customer_keys(ctx: StepContext) -> None:
    """Bug 2. A surrogate key from row_number() over a non-unique sort.

    500 rows share every value of `cust`, and nothing in the ORDER BY decides
    which of them comes first, so the parallel sort is free to order them
    differently on a rerun. Measured at 275,280 to 491,520 rows of 500,000
    getting a different key, firing on every attempt. The fix is one word:
    ORDER BY cust, event_id.
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
    """
    ctx.read("price_updates")
    ctx.state(
        "prices",
        "SELECT i::BIGINT AS sku, (500 + i)::BIGINT AS price_cents FROM range(100000) AS s(i)",
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
    differing, and every one of those is a false positive. This step is the
    reason slice 2 exists.
    """
    ctx.read("orders")
    ctx.write("mean_basket", "SELECT day, avg(amount) AS mean_amount FROM orders GROUP BY day")


def sparse_customer_keys(ctx: StepContext) -> None:
    """Bug 2's code at two rows per tie group, where it fires only sometimes.

    Eighteen attempts on this machine landed on roughly 0, 8,480, or a quarter
    of the table, with zero coming up about a third of the time. A two-run
    checker would call this clean on a third of its attempts, which is the
    argument for running five times rather than twice, and the argument for
    slice 4's amplification on top of that.
    """
    ctx.read("sparse_customers")
    ctx.write(
        "sparse_customer_keys",
        "SELECT event_id, cust, row_number() OVER (ORDER BY cust) AS surrogate_id "
        "FROM sparse_customers",
    )


STEPS = [
    generate_inputs,
    daily_revenue,
    customer_keys,
    apply_price_updates,
    append_audit_log,
    mean_basket,
    sparse_customer_keys,
]
