"""The same pipeline with the bugs fixed, one line each. Nothing here may ever fire.

`reference.py` ships broken so that a checker finding nothing can be told apart
from a checker that is broken. This file is the other half of that pair, and it
has two jobs.

The first is the eval's specificity half: a detector that fires on everything
scores 50 percent against a matched set, so every twin here has to come out
quiet.

The second is what makes amplification a detector rather than a chaos generator,
and it is why the twins had to be written alongside the amplifiers rather than
later, with the eval that scores them. An amplifier
substitutes a step's input in order to raise the probability that a broken step
fires. If it also makes correct code fire, it is measuring its own violence and
the tool is worthless. So each amplifier is built so the fix survives it: tie
collapse leaves the most distinct column alone, which is what `ORDER BY cust,
event_id` needs, and row multiplication does not trouble a `CREATE OR REPLACE`
or a merge over a deduplicated source. Running this file is the check:

    uv run twicerun run pipelines/twins.py

Exit 0 means nothing diverged on the real input and no amplifier made anything
diverge either. Exit 1 here is a bug in the amplifiers, not in the twins.

The generator step is copied from `reference.py` unchanged, because both files
have to read the same bytes for the pairing to mean anything.
"""

from __future__ import annotations

from twicerun.storage import StepContext

UNIT_HASH = "hash(i)::DOUBLE / 18446744073709551615.0"


def generate_inputs(ctx: StepContext) -> None:
    """Byte-identical to the reference pipeline's, and the control for this one too."""
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


def daily_revenue_decimal(ctx: StepContext) -> None:
    """Bug 1's twin. The same sum with the accumulator changed to DECIMAL(18,4).

    Fixed-point addition is associative, so the order the threads finish in
    cannot change the answer. Measured on its own at threads 1, 2, 4, 10, 20 and
    40: 0 of 2 comparisons at every one of them, against 8 of 8 for the DOUBLE
    version from 4 threads upward.

    DECIMAL sits on the exact side of the oracle's type split for the same
    reason, so a DECIMAL that did move would be reported as a wrong answer at
    any magnitude rather than as drift.
    """
    ctx.read("orders")
    ctx.write(
        "daily_revenue",
        "SELECT day, sum(amount::DECIMAL(18,4)) AS revenue FROM orders GROUP BY day",
    )


def customer_keys_tiebreak(ctx: StepContext) -> None:
    """Bug 2's twin, and the one word of difference is `, event_id`.

    500 rows share every value of `cust`, so the sort has ties and the parallel
    sort is free to break them differently on a rerun. Adding a unique column to
    the ORDER BY leaves it nothing to decide.
    """
    ctx.read("customers")
    ctx.write(
        "customer_keys",
        "SELECT event_id, cust, row_number() OVER (ORDER BY cust, event_id) AS surrogate_id "
        "FROM customers",
    )


def apply_price_updates_deduped(ctx: StepContext) -> None:
    """Bug 3's twin. The source is collapsed to one row per key before the merge.

    The broken version's source holds two rows for each target key and nothing
    in the MERGE says which wins. `max(price_cents)` picks, and picks the same
    way every time.

    This is the twin row multiplication is aimed at. Duplicating the source
    manufactures a repeated key where the real input did not have one, which is
    what makes the broken version fire and what this GROUP BY absorbs.
    """
    ctx.read("price_updates")
    ctx.state(
        "prices",
        "SELECT i::BIGINT AS sku, (500 + i)::BIGINT AS price_cents FROM range(125000) AS s(i)",
    )
    ctx.sql("CREATE TABLE catalogue AS SELECT * FROM prices")
    ctx.sql(
        "CREATE TABLE staged_updates AS "
        "SELECT sku, max(price_cents) AS price_cents FROM price_updates GROUP BY sku"
    )
    ctx.sql(
        "MERGE INTO catalogue t USING staged_updates s ON t.sku = s.sku "
        "WHEN MATCHED THEN UPDATE SET price_cents = s.price_cents"
    )
    ctx.write("prices", "SELECT sku, price_cents FROM catalogue")


def replace_audit_log(ctx: StepContext) -> None:
    """Bug 4's twin. It writes what the run produced rather than appending to what it found.

    `ctx.write` is a COPY over the whole output, so not reading the previous
    copy is the whole fix: the artifact is replaced rather than grown. The
    broken version reads its own last output through `ctx.state` and unions,
    which duplicates every row on every rerun.

    Row multiplication doubles the rows this writes and changes nothing about
    whether two runs of it agree, which is the property that amplifier needs.
    """
    ctx.read("orders")
    ctx.write("audit_log", "SELECT order_id, amount FROM orders WHERE amount > 499")


def sparse_customer_keys_tiebreak(ctx: StepContext) -> None:
    """The intermittent step's twin, and the one tie collapse is aimed at.

    Two rows per value of `cust` here rather than 500, which is the density the
    broken version fires at only sometimes. Tie collapse takes it to 500 rows
    per value, which is where the broken version fires every time, and the
    tiebreak below still decides the order because `event_id` is the column tie
    collapse leaves alone.

    Measured standalone over 6 trials at 2 comparisons each, on the plain input
    and under all three amplifiers: 0 of 2 every time. The broken version over
    the same 18 trials fired on 33 of 36 comparisons.
    """
    ctx.read("sparse_customers")
    ctx.write(
        "sparse_customer_keys",
        "SELECT event_id, cust, row_number() OVER (ORDER BY cust, event_id) AS surrogate_id "
        "FROM sparse_customers",
    )


STEPS = [
    generate_inputs,
    daily_revenue_decimal,
    customer_keys_tiebreak,
    apply_price_updates_deduped,
    replace_audit_log,
    sparse_customer_keys_tiebreak,
]
