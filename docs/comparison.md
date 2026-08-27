# The oracle

`src/twicerun/oracle.py` is the comparison, and it is the project. Bit-exact equality reports around
600 findings on 1,000 groups of correct code, and a tool that fixes that by picking an epsilon until
the demo passes is worse than one that ships the false positives. What sits between those two is
this file, plus the policy layer that decides what a difference is worth, plus the derivation the
tolerance comes from.

## Four steps between bit-exact and an epsilon

**Partition the columns.** Integers, `VARCHAR`, `DATE`, `TIMESTAMP`, `UUID` and `DECIMAL` cannot come
back different from a correct query run twice, so a difference in one of them is real. `FLOAT` and
`DOUBLE` can. `LIST`, `STRUCT`, `MAP`, `UNION` and any type the classifier does not recognise are
refused with the column named, because comparing a nested type badly is worse than refusing.

`DECIMAL` on the exact side is deliberate: it is the fix the tool recommends for the float aggregate
bug, since DuckDB's decimal sum is fixed-point and reassociates exactly.

**Match rows on the exact columns.** This is the move that makes the whole thing tractable.
Order-independent exact comparison is a multiset of hashes and is linear. Order-independent tolerant
comparison is an assignment problem. Splitting the row into an exactly-matched key and
tolerantly-compared values turns the assignment problem into a join.

**Preserve multiplicity.** Each side gets `row_number() OVER (PARTITION BY key ORDER BY values)` and
the two are joined on key plus ordinal, with `IS NOT DISTINCT FROM` so a NULL matches a NULL. A key
group with three rows in the reference and five in a later run pairs ordinals 1 to 3 and leaves 4 and
5 unpaired. Joining on the key alone would fan those out to fifteen pairs and report nothing wrong,
which is why `append_audit_log` reads `MULTIPLICITY` rather than a row count that happens to differ.

**Measure the floats twice.** For every pair that moved, the tool reports both the ULP distance,
which says whether the difference is last-bit noise, and the relative difference, which is the
number someone with domain knowledge can judge. Neither decides anything on its own. `-0.0` and
`0.0` are the same number, both NaN is not a difference, and an infinity against a finite number has
no meaningful relative difference so it reports as infinite rather than as a NaN that would silently
compare false against every threshold.

ULP distance is computed in SQL rather than by pulling rows into Python. DuckDB casts a `DOUBLE` to
`BIT` giving the raw IEEE-754 layout and `BIT` to `BIGINT` giving the two's-complement reading of
those bits; reflecting the negative half about the minimum turns sign-magnitude into a monotone
integer ordering. It is checked against `struct.unpack` over zeros, subnormals, both infinities and
a NaN.

Two things this gets wrong are in the README's list of what it gets wrong: sorting to pair rows
inside a key group is a heuristic once there is more than one float column, and an artifact whose
every column is a float has no key at all and pairs by order.

## Which column moved

For each key column, drop it and recount what failed to pair. On `customer_keys` that turns 491,520
unmatched rows into a sentence naming a column.

The counts alone do not always single one out, and finding that out changed the design. Dropping
`event_id` works exactly as well as dropping `surrogate_id`, because each `cust` block keeps the same
set of `surrogate_id` values and only the pairing to `event_id` shuffles inside it. Both descriptions
of what moved are true and the arithmetic is symmetric, so the report says so rather than picking one
and implying the numbers chose it.

What breaks the tie is something the storage interface already knows: which artifacts the step read.
`event_id` came in from `customers`; `surrogate_id` did not exist until this step made it. A column
the step invented is the better suspect. Across 10 passes the first column named was `surrogate_id`
on both `row_number()` steps and `price_cents` on the merge step, every time.

`tests/test_oracle.py::test_a_tie_goes_to_the_column_the_step_invented` asserts the preference as a
unit test. What the [eval](eval.md) can and cannot conclude from re-scoring it ten times a run is a
separate question, and the answer is in that file.

## Where the tolerance comes from

Reassociating a sum of `n` float64 terms moves the result by at most `gamma_n * sum(|x_i|)` with
`gamma_n = n*u / (1 - n*u)` and `u = 2^-53`. Two orderings differ by at most twice that. The tool has
the output and not the terms, so it substitutes `max(|a|, |b|)` for `sum(|x_i|)` and takes `n` from
the row count the storage interface saw:

```
bound = 2 * gamma_n * max(|a|, |b|)
```

The magnitude drops out. The tool measures `|a - b| / max(|a|, |b|)`, so the same quantity sits on
both sides of the comparison and the test reduces to `relative difference <= 2 * gamma_n`.

Before any of this was written, one condition was fixed: **observed maximum relative drift on the
benign step must be at least 1000x below the computed bound**, or the bound is binding, the
derivation is not conservative enough, and the design gets revisited rather than the threshold moved.
The README carries the result, both readings of it, and the arithmetic that makes the margin smaller
than it looks.

Almost none of that margin comes from the drift being small. `rows_read` is the step's whole input,
and each of the 1,000 output rows sums about 2,000 terms, so `n` is a thousand times larger than the
quantity the bound is about. Take that slack out and the headroom lands in the hundreds, under the
1000x line. It is not always under it, which is why the README's table carries a count rather than a
verdict: earlier samples cleared the tight check on 3 of 56 step-passes, then 1 of 40, then 5 of 80.
So the tight check fails most of the time rather than every time, and both the report and that table
print how often it cleared on the run in front of you instead of quoting a number from this machine.

Every report prints both numbers so nobody has to take the reading from a document:

```
  5 mean_basket
      observed furthest relative drift 6.8129e-16
      n = 2,000,000 rows read by the step, bound 4.4409e-10, headroom 651,838x  CLEARS
      n = 2,000 terms per output row, bound 4.4409e-13, headroom 652x  FAILS
```

The threshold is not moving and neither is `n`. `rows_read` was the choice made in the spec before
any of this was measured, and switching to whichever count clears the line after seeing the result is
exactly what pre-registering is meant to stop. The number that would make the bound correct is the
term count behind one output value, and getting it needs the query plan, which this tool does not
have.

A calibration approach was considered and rejected: measure the drift distribution of a no-op control
and set the tolerance from it. That is circular, because the no-op control is the thing under test.

### Where `rows_read` is wrong

Two errors, pointing opposite ways.

It undercounts. Only `ctx.read` and `ctx.state` add to it, so anything a step pulls in through
`ctx.sql` is invisible and `apply_price_updates` records 300,000 while its two `CREATE TABLE AS`
statements scan at least 300,000 more. That makes `n` too small, the bound too tight, and the tool
reports a difference reassociation could in fact explain. A false positive, which is the direction to
err in.

It also overcounts, in a different sense. "Brought into scope" is not "terms behind one output
value", and for a group-by the gap is the group count. That makes the bound too loose and could
tolerate a difference reassociation cannot explain, which is the unsafe direction, and for an
aggregate it is the larger of the two by far. That is the gap between the two bound rows of the
README's table, and it is a factor of a thousand on this pipeline.

## The strict default and the one opt-in

Any difference at all counts as a divergence unless you ask otherwise. The argument for that is not
first principles, it is how the failure actually surfaces.

Asked what difference between two runs of the same total he would have accepted in a pipeline he
shipped, Vishal's answer was zero: any difference is a bug. Asked how he found out a pipeline had
gone wrong, the answer was a downstream count that did not reconcile. Those fit together. If
breakage reaches you as an exact reconciliation failing, a tool that quietly absorbs a small
difference has hidden the thing you would have used to find the bug.

So correct code producing around 600 findings under the default is the intended behaviour rather than
an embarrassment. Baseline 1's cell in the README's table is the version of that number nobody typed. The user asked whether the pipeline gave the same answer twice, and it did not.

That also shapes the report. A build gate needs a verdict; someone tracing a count that did not add
up needs the two numbers that disagreed, so the report prints the pair rather than a summary of it.

`--policy reduction-order` is the opt-in. It downgrades a `VALUE_DRIFT` finding to `TOLERATED` when
three things hold at once: the step's only class across every comparison is `VALUE_DRIFT`, the step's
fire rate at `threads=1` is 0 of m, and the magnitude is inside the bound. Any `ROW_MISSING`,
`ROW_EXTRA`, `MULTIPLICITY` or `SCHEMA` finding anywhere in the step blocks it outright. A downgraded
finding is still counted and still printed with its magnitudes.

The conjunction is the point, and each condition refuses something the other two would let through. A
difference that vanishes single-threaded but is ten orders of magnitude larger than reassociation can
account for is catastrophic cancellation or a genuinely different set of terms, and it is still
reported. A difference small enough for the bound that survives `threads=1` is not reduction order
whatever its size, and it is still reported, with that rate as the reason:

```
      drift not downgraded: the step still diverges at threads=1, 2 of 4, so the order of a parallel reduction is not what moved it
```

A step that was never bisected gets the same refusal. No evidence is not evidence, and the direction
to err in is reporting a difference that reassociation might well have explained.

The second of those three conditions did not exist until slice 3, and until it did, every report that
downgraded anything carried a line in its header saying which condition was missing and that every
`TOLERATED` below rested on the other two. What a `TOLERATED` claims changed when the line went. It
used to mean the drift was float-only and small enough that reassociation could account for it. It
now means the drift also disappeared when the parallelism did, which is the difference between "small
enough to be reassociation" and "demonstrably is reassociation".

`--tolerance-rel` and `--tolerance-ulps` are deliberately not gated on the bisect. A threshold is a
user saying a difference of that size does not matter in their domain, which is their claim to make
and rests on no mechanism. The step line names which route downgraded it, because `TOLERATED on 4 of
4 by the derived reassociation bound` and `TOLERATED on 4 of 4 by a --tolerance threshold` are
different claims and used to render identically.

They are an escape hatch, documented as one and never a default, and they still cannot excuse a
missing or duplicated row.

What the opt-in costs is one of the four broken steps, and it is the third column of the README's
results table. Everything else is scored under `strict`, which is the default, so the price of
`reduction-order` appeared nowhere at all until the eval started measuring it through the shipped
`judge`. `daily_revenue` stops gating a release under it and the other three broken steps do not.
That step is bug 1, a `sum()` over `DOUBLE`, and it is downgraded because its drift is float-only,
inside the derived bound and gone at `threads=1`, which is the conjunction working exactly as
designed. Under this policy the detector's sensitivity on the four broken steps is 3 of 4 by design.

## Measuring and deciding are separate stages

Numbers must not move when the policy does. If a tolerance can change a reported figure, everything
downstream of it is negotiable and none of it is worth printing.

So `oracle.py` produces findings, `measurement.py` collects them per step, `policy.py` reads that and
returns a verdict, and nothing writes back.

Running `twicerun run` twice under two policies does not show you that, because each invocation runs
the pipeline again and the figures move between them for exactly the reason this tool exists.
`twicerun judge` scores a run that already happened and executes nothing:

```
$ uv run twicerun run pipelines/reference.py --runs 3
$ uv run twicerun judge .twicerun/run-20260826-142816 --policy strict
  1 daily_revenue           2 of 2  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER
      daily_revenue: 602 of 1,000 paired rows moved on revenue
      furthest move over 2 comparisons: revenue, 3 ulp and 3.56e-16 relative
      490922.43387865723 against 490922.43387865706

$ uv run twicerun judge .twicerun/run-20260826-142816 --policy reduction-order
  1 daily_revenue           0 of 2  DIVERGENT  VALUE_DRIFT  cause PARALLEL_ORDER  TOLERATED on 2 of 2 by the derived reassociation bound
      daily_revenue: 602 of 1,000 paired rows moved on revenue
      furthest move over 2 comparisons: revenue, 3 ulp and 3.56e-16 relative
      490922.43387865723 against 490922.43387865706
```

Same 602, same 3 ulp, same 3.56e-16, same pair of values, and the same `DIVERGENT`. The fire rate
moves, the `TOLERATED` note appears, and nothing else does. A test compares those detail lines rather
than describing them.

`DIVERGENT` staying put under `reduction-order` next to a fire rate of `0 of 2` is the deliberate
half. The step did produce two different answers; the policy decided the difference did not matter.
Letting the policy rewrite the status would have put it in the same negotiable pile as every figure a
tolerance touches.

The bisect and the amplifiers are measurement too, so they hold still as well. Judging one saved run
under `strict`, under `reduction-order` and under `--tolerance-ulps 4` gives three different reports
with an identical cause table, an identical amplification table and an identical status column, and
only the fire rate and the `TOLERATED` count moving. `judge` re-derives both the `threads=1` rate and
every amplifier's rate from the saved artifacts rather than reading a number out of the manifest, so
all three go through the comparison code the run used.
