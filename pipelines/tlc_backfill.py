"""A four-step backfill over three real NYC TLC monthly partitions, one of which is a column short.

`reference.py` is synthetic because its bugs are parameterised by tie density,
group count and row count, and those parameters are the experiment. This file is
the other half of that argument: the same tool pointed at data nobody here
chose, with a schema change nobody here invented.

TLC added `cbd_congestion_fee` to the Yellow, Green and High Volume FHV trip
records for 2025 onward, carrying the Congestion Relief Zone charge that took
effect on 5 January 2025, and updated the data dictionary on 18 March 2025 to
document it. So the three months this backfills, 2024-12 then 2025-01 then
2025-02, span a column that two of the three partitions have and one does not:
green is 20 columns then 21, yellow 19 then 20. Nothing here manufactured that.

Fetch the partitions first, or the import below raises and names the script:

    uv run python scripts/fetch_tlc.py
    uv run twicerun run pipelines/tlc_backfill.py

Two of these four steps carry a bug and two do not, and which is which depends
on the taxi. The measured figures are in docs/data.md rather than in
these docstrings, because a step's behaviour here is a function of the row count
and this file runs on either dataset.
"""

from __future__ import annotations

import os
from pathlib import Path

from twicerun.storage import StepContext, quote

# TWICERUN_TLC_DIR moves where the partitions are read from. It is here for the
# offline test, which builds three Parquet files from `tlc_green_schema.sql` and
# points this at them, since no TLC bytes are committed and the suite may not
# open a socket. It is documented rather than private because someone keeping
# 181 MB of yellow taxi outside the working tree wants the same lever.
DATA = Path(os.environ.get("TWICERUN_TLC_DIR") or Path(__file__).resolve().parent.parent
            / "data" / "tlc")

# Ordered oldest first, which is the order a backfill walks its partitions in.
MONTHS = ("2024-12", "2025-01", "2025-02")

# What a revenue backfill keeps. Green and yellow disagree on the name of the
# pickup timestamp, `lpep_` against `tpep_`, so that one is resolved by suffix
# below rather than listed here.
CARRIED = (
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "congestion_surcharge",
)

# The column this file exists for. It is listed apart from the rest because it
# is the one that is absent from the 2024 partition, and the landing step has to
# ask each file whether it has it rather than assume.
NEW_IN_2025 = "cbd_congestion_fee"


def partitions() -> list[tuple[str, Path]]:
    """The monthly files on disk, oldest first, with a message if they are not there.

    Either taxi is accepted and neither is preferred: whichever
    `scripts/fetch_tlc.py` was pointed at is what this runs on. Both at once is
    refused rather than silently backfilling seven million yellow trips and a
    hundred and fifty thousand green ones into one ledger.
    """
    found = {
        month: sorted(DATA.glob(f"*_tripdata_{month}.parquet")) for month in MONTHS
    }
    absent = [month for month, paths in found.items() if not paths]
    if absent:
        raise FileNotFoundError(
            f"no TLC partition for {', '.join(absent)} under {DATA}. "
            f"Run: uv run python scripts/fetch_tlc.py"
        )
    taxis = {path.name.split("_")[0] for paths in found.values() for path in paths}
    if len(taxis) > 1:
        raise FileNotFoundError(
            f"{DATA} holds {' and '.join(sorted(taxis))} partitions and this backfill takes "
            f"one taxi. Move or delete the set you do not want."
        )
    return [(month, paths[0]) for month, paths in found.items()]


def artifact_for(month: str) -> str:
    return f"trips_{month.replace('-', '_')}"


def _columns_of(ctx: StepContext, path: Path) -> list[str]:
    described = ctx.sql(f"DESCRIBE SELECT * FROM read_parquet({quote(path)})").fetchall()
    return [name for name, *_ in described]


def land_partitions(ctx: StepContext) -> None:
    """One artifact per month, each keeping the column set its own file has.

    This is where the schema change becomes a fact in the manifest rather than a
    sentence in a docstring: the 2024-12 artifact is written without
    `cbd_congestion_fee` because the file does not have it, and the two 2025
    artifacts are written with it. A landing step that hardcoded the column list
    would raise on one of the three, which is the loud version of this problem
    and not the interesting one.

    It reads no artifact, only files, so it is this pipeline's control step in
    the same way `generate_inputs` is the reference pipeline's: if it ever shows
    a fire rate above zero, the comparison is broken. It is also the step two of
    the three amplifiers decline, for the same reason: there is no input
    artifact to substitute.
    """
    for month, path in partitions():
        available = _columns_of(ctx, path)
        pickup = next(c for c in available if c.endswith("_pickup_datetime"))
        projected = [f"'{month}' AS partition_month", f"{pickup} AS pickup", *CARRIED]
        if NEW_IN_2025 in available:
            projected.append(NEW_IN_2025)
        ctx.write(
            artifact_for(month),
            f"SELECT {', '.join(projected)} FROM read_parquet({quote(path)})",
        )


def unify_partitions(ctx: StepContext) -> None:
    """Concatenate the three months, coping with one of them being a column short.

    `UNION ALL BY NAME` is the handling, and it is the correct one: the 2024-12
    rows arrive with `cbd_congestion_fee` NULL, which is what that column means
    for a trip taken before the charge existed. The plain positional `UNION ALL`
    raises `Binder Error`, and the shape worth knowing about is neither of those.
    Handed the three files as a list, `read_parquet` takes its schema from the
    first one and drops `cbd_congestion_fee` without a word when the 2024
    partition leads. Reversing the list brings the column back. docs/data.md has
    that measurement, because a revenue column that disappears on file order is
    the failure this schema change actually causes and it is one this tool
    cannot see: it is wrong the same way on every run.
    """
    months = [month for month, _ in partitions()]
    for month in months:
        ctx.read(artifact_for(month))
    unioned = " UNION ALL BY NAME ".join(f"SELECT * FROM {artifact_for(month)}" for month in months)
    ctx.write("trips", unioned)


def zone_revenue(ctx: StepContext) -> None:
    """sum() over DOUBLE money columns, grouped by partition and pickup zone.

    The same mechanism as the reference pipeline's `daily_revenue`, on fares
    rather than on `hash(i)`: parallel reduction adds the terms in whatever order
    the threads finish in and float addition is not associative. Whether it fires
    depends on the row count, which is why this pipeline runs on either taxi and
    why docs/data.md publishes both.

    `trips` is the integer control sitting in the same artifact as the two float
    sums. A count cannot reassociate into a different answer, so if it ever moves
    the problem is not arithmetic.
    """
    ctx.read("trips")
    ctx.write(
        "zone_revenue",
        "SELECT partition_month, PULocationID AS zone, count(*) AS trips, "
        "sum(total_amount) AS revenue, "
        f"sum(coalesce({NEW_IN_2025}, 0)) AS cbd_fees "
        "FROM trips GROUP BY partition_month, PULocationID",
    )


def revenue_ledger(ctx: StepContext) -> None:
    """Append this backfill's months to the cumulative ledger, again, on every run.

    The bug is that the ledger has no key and the step never asks it what is
    already loaded. It decides that from the partition list it was handed, so a
    rerun of the backfill adds a second copy of every (month, zone) row and the
    revenue total doubles.

    This is the same shape as the reference pipeline's `append_audit_log` and it
    is here for a reason that only shows up on real data: of the four bugs in
    that file, it is the only one that does not need a row count to fire. The
    two parallel bugs are silent on a month of green taxi trips and loud on a
    month of yellow, and this one duplicates 700-odd ledger rows either way. A
    reader who runs the green dataset because it is 3.6 MB still sees one real
    bug caught on real data.

    `ctx.state` is what makes it visible at all. It resolves the *previous run's*
    ledger rather than this run's, which is the difference between rerunning a
    pipeline and calling a function twice. Under containment every run from 2 on
    reads run 1's ledger, so all four comparisons report one rerun's worth of
    duplication rather than one, two, three and four runs' worth.
    """
    ctx.read("zone_revenue")
    ctx.state(
        "revenue_ledger",
        "SELECT ''::VARCHAR AS partition_month, 0::INTEGER AS zone, 0::BIGINT AS trips, "
        "0.0::DOUBLE AS revenue, 0.0::DOUBLE AS cbd_fees WHERE false",
    )
    ctx.write(
        "revenue_ledger",
        "SELECT partition_month, zone, trips, revenue, cbd_fees FROM revenue_ledger "
        "UNION ALL "
        "SELECT partition_month, zone, trips, revenue, cbd_fees FROM zone_revenue",
    )


STEPS = [land_partitions, unify_partitions, zone_revenue, revenue_ledger]
