# Amplification

Running the pipeline more times lowers the chance of missing a step that diverges with probability
`p`. It does not touch `p`. That is the ceiling, and the arithmetic states it plainly: at `p = 0.2`
five runs still miss 41 percent of the time, and even twenty runs leave a 15 percent upper bound on
`p` when nothing fires. No practical number of runs settles the question.

Amplification changes `p` instead, by handing a step an input built to make the mechanism fire.
Every step the main loop found nothing in is re-executed against that substituted input, three runs
each, escalating to the full five on a hit so the amplified rate and the main-loop rate share a
denominator. Cost only rises where something was found.

Substituting the input is allowed because this tool is not checking that the answer is right.
Reproducibility is a property of the code rather than of the data, so an input with the same types
and the same row count is a fair question to ask of a step, and a step that only reproduces on the
data it happened to be handed is fragile.

## The three of them

| amplifier | what it does | the bug it is aimed at |
|---|---|---|
| tie collapse | hashes a column's values into buckets until the artifact holds 500 rows per distinct value, leaving the most distinct column alone | `row_number() OVER (ORDER BY cust)`, which cannot disagree with itself unless `cust` has ties |
| thread count | re-executes on the same input at twice the machine's default, floor 4, ceiling 64 | parallel reduction and parallel scan order |
| row multiplication | duplicates every input row once | the append with no unique key, and the `MERGE`, whose source has to carry a repeated key before it can pick wrongly |

Two numbers in that table were measured rather than picked. The thread floor of 4 is where the float
sum switches on: over eight comparisons each it fired once at `threads=2` and eight times at
`threads=4`, and it is flat from there to 40, which is also why doubling a default of 10 was
expected to find nothing.

500 rows per distinct value is the density at which the surrogate-key bug fired on every attempt of
the standalone measurement this project started from. Inside the tool it roughly doubles that step's
per-comparison rate, which is the README's gap table. A standalone re-measurement made while
building this found much less of a gap, 10 of 12 comparisons at 2 rows per value against 11 of 12 at
500, and the distance between those two pictures is the rest of the pipeline running between one
execution of the step and the next. Back to back in one process the bug fires most of the time at
either density; inside a five-run loop over eight steps it does not.

**That 500 is also the density `reference.py` generates `customers` at**, which is the input to the
loud version of the same bug: `(i % 1000)` over `range(500000)` is 500 rows per distinct value, and
`TARGET_ROWS_PER_VALUE` in `amplify.py` is 500. So tie collapse is not raising the sparse step's
firing probability by a general principle here. It is converting that step's input into the density
the loud step already fires on every time, and I chose both numbers. A fixture built at a different
density would need the amplifier retuned, or would be answering a different question.

The counterweight is that tie collapse is not reliably the best of the three on that step, and the
README's gap table is one sample of which wins. Row multiplication is aimed at the append and the
`MERGE` rather than at a surrogate key, and it led in eval runs D and E, at 0.87 against 0.80 and
0.95 against 0.88, both measured on 2026-08-26. Which of the two comes first moves between runs, and
the amplifier that was not co-designed with this step wins about as often as the one that was.

Tie collapse replaces a non-NULL value with another real value from the same column, the minimum of
its hash bucket, so the type, the row count, the NULL count and the value domain all survive.
Casting a hash back to the column's type would have worked for integers and not for `DATE` or
`DECIMAL`.

NULLs needed a special case rather than falling out of that. `hash(NULL)` is an ordinary non-NULL
constant, so a NULL row lands in a bucket alongside real values and takes their representative: 300
NULLs in 3,000 rows came out as zero NULLs, while the report line beside it claimed the domain
survived. Type and row count did survive, which is why it read as fine. A step that branches on NULL
was being handed a different question from the one printed.

The thread-count amplifier was the one I expected to find nothing. This machine already runs DuckDB
at 10 threads, and the float aggregate is flat from 4 threads upward, so doubling to 20 looked like
a second look at the same thing. It is not: it fires on the surrogate-key step more often than not,
and on the passes where `apply_price_updates` comes back quiet it is the amplifier that catches the
`MERGE` bug, at `4 of 4` where the other two report nothing. That step's own input has no repeated
key to collapse and duplicating its rows does not create one the merge can trip over, so thread
count is the only one of the three with anything to say about it.

## The bug's own fix has to survive it

Each amplifier is built so that the fix for the bug it targets still gives the same answer twice.
Tie collapse leaves the most distinct column alone, which is exactly what `ORDER BY cust, event_id`
needs to stay decided. Row multiplication does not trouble a `CREATE OR REPLACE TABLE`, and a
`MERGE` over a deduplicated source gives the same answer on twice the rows. An amplifier that made
correct code fail would be measuring its own violence and the tool would be worthless.

That is checkable rather than assertable, because `pipelines/twins.py` is the one-line fix for every
bug in the reference pipeline. Any non-zero exit there means the amplifiers are broken, not the
twins:

```bash
uv run twicerun run pipelines/twins.py
```

Every twin pass of the committed eval run went through the same amplifiers, and the README's
twin-coverage table is what they did to correct code. The first column of it is the honest part. Tie
collapse declines most of its chances, and the report says why each time rather than printing a
zero. `orders.day` and `customers.cust` already hold 2,000 and 500 rows per value, which is at or
past what the amplifier targets, so there is nothing for it to raise; `generate_inputs` reads no
artifact at all, so two of the three amplifiers have nothing to substitute. A twin an amplifier
never touched is not evidence that the amplifier is safe on it, and folding those into the zero
would have made the table look twice as strong as it is.

Two larger samples of the same file, both hand-transcribed on 2026-08-26 and neither regenerated: 12
passes gave 24 of 72 for tie collapse, 72 of 72 for thread count and 60 of 72 for row
multiplication, at zero fires; 40 passes in a fresh clone gave 240 twin step-passes, 1,040 amplified
comparisons on correct code, zero fires and exit 0 on all 40, at the same coverage ratios.

**The twins only cover code that reproduces exactly.** `mean_basket` is the reference pipeline's
correct-but-drifting float step, and there is no twin for it because there is nothing to fix: the
drift is arithmetic. So that table says the amplifiers do not break code that gives the same answer
twice. It says nothing about whether they widen drift that was already there, and the step that would
answer that is the one step with no partner.

## Pointing them at the one step

Step 6 of the reference pipeline is the surrogate-key bug at 2 rows per tie group, the density where
it fires only sometimes. The README's gap table is the amplifiers pointed at that step whether or
not the main loop had already caught it, and it comes from a script rather than from the tool:

```bash
uv run python scripts/amplification_gap.py 40 --json results/gap-2026-08-27.json
```

`twicerun run` cannot produce that table and it is not meant to. Amplification only touches steps
the main loop found nothing in, so on most passes step 6 fires, never reaches an amplifier, and
contributes nothing to the second half of the comparison. Waiting for the passes where it stays
quiet means throwing most of them away, and the first row of the table says how many that is.

There is no flag for it in the tool, deliberately: a flag doing this would make every clean pipeline
pay for evidence it does not need, and the argument for amplification's cost is precisely that it
runs only where the loop came back quiet. What `twicerun run` does reproduce on its own is the first
row and the passes-it-found-nothing-on column, since a pass where step 6 comes out
`STABLE_ON_THIS_INPUT` is a pass where the loop said nothing and an amplifier did not.

The last two columns of that table are three different things kept apart: an amplifier that declined
to build an input and one that built an input the step wrote nothing from both count as
could-not-ask, and only a comparison that happened and found the step clean is evidence about the
step. A row with fewer comparisons than the others found nothing in its first two, because an
amplifier is escalated to the full run count only on a hit, which is the cost argument working.

## The four statuses

| status | condition |
|---|---|
| `DIVERGENT` | the step fired on the real input, `k >= 1` of `m` |
| `STABLE_ON_THIS_INPUT` | 0 of `m` on the real input, `k >= 1` under at least one amplifier |
| `NO_DIVERGENCE_OBSERVED` | 0 of `m` on the real input and 0 under every amplifier that ran |
| `AMPLIFICATION_FAILED` | an amplified input made the step raise rather than diverge |

`STABLE_ON_THIS_INPUT` is not a milder `DIVERGENT` and it is not a pass. It is the case that a plain
five-run loop reports as a clean zero, which is the case this whole tool is about, so the report
prints it as loudly as anything else and prints the sentence saying what it does not mean:

```
  It says the step did not fire under these particular stresses, the ones named above, and
  nothing beyond that.
```

That sentence is enforced by a test, and so is the absence of the bare words deterministic, stable,
reproducible and passed. The status token itself contains one of them, so the test strips the token
out first and what is left still has to be clean.

`AMPLIFICATION_FAILED` exists so that a step which raises under amplification can never fall back to
green. Collapsing a column can violate a downstream uniqueness constraint, and the honest report of
that is the amplifier's name and the error, not a zero:

```
  1 insists_on_uniqueness    0 of 4  AMPLIFICATION_FAILED
      tie collapse        raised: ConstraintException: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "0"
      thread count        0 of 2, threads raised to 20 from the 10 in the loop above
      row multiplication  raised: ConstraintException: Constraint Error: PRIMARY KEY or UNIQUE constraint violation: duplicate key "0"
```

Note the thread count line in the middle. One amplifier came back with a clean `0 of 2` and the step
still does not get a clean status, because two of the three could not answer at all.

**The status is read off the measurement, never off the policy.** A policy decides whether a
difference matters and must not be able to decide whether one happened, so a step whose drift was
entirely downgraded still reads `DIVERGENT` with the `TOLERATED` count beside it. Judging one saved
run under `strict`, under `reduction-order` and under `--tolerance-ulps 4` gives three different
reports with the same status column, and there is a test that does exactly that and diffs it.

## What NO_DIVERGENCE_OBSERVED prints

Never alone, and never as a verdict. Always the rate, the bound in words, and the list of axes that
were varied against the list that were not:

```
NO_DIVERGENCE_OBSERVED on 0 generate_inputs, 7 roll_up_keys. It is a fire rate and two lists, and
it is not a verdict about the code.
  0 of 4 on the real input, and 0 of 2 under each amplifier that ran. 4 clean comparisons rule out
  a per-comparison divergence probability above 53 percent, 95 percent one-sided. An amplifier's 2
  rule out one above 78 percent. That assumes an independence these runs do not have, since they
  share a process, a page cache and a machine.
  0 generate_inputs
      varied: how many times the pipeline ran, thread count
      not varied: tie collapse (this step reads no artifact, so there is no input to substitute),
      row multiplication (this step reads no artifact, so there is no input to substitute)
  7 roll_up_keys
      varied: how many times the pipeline ran, tie collapse, thread count, row multiplication
  Not varied anywhere: input distribution beyond what the amplifiers above changed, memory limit
  and spill behaviour, DuckDB version, wall clock, filesystem.
```

The pair of lists is the part that cannot be dropped. "Never fired in four comparisons" and "cannot
fire" are different sentences, and naming what was moved against what was not is the only version of
that difference a black-box tester can honestly produce. The lists are per step because they differ
per step: a generator reads no artifact, so two of the three amplifiers have nothing to work with,
and folding that into one summary put the same amplifier in both lists at once.

An amplifier that ran and compared no artifact goes in the second list too. That is the guard the
main loop and the bisect already carry, arriving in the third loop: `any([])` is False, so a step
that wrote nothing when re-executed on its own scores `0 of 2` out of two comparisons of nothing and
reads exactly like two clean ones. The report shows the rate and disqualifies it on the same line.

`--no-amplify` turns the amplifiers off, and what it takes away with them is the point. No step gets
a status, because three of the four are defined by what an amplifier did and the fourth needs a zero
under every amplifier. So the report says that in place of the status, and prints the bound the
status would have been resting on:

```
amplification is off (--no-amplify), so no status is printed for the 2 steps that never fired:
  0 generate_inputs, 7 roll_up_keys
  Each of those is 4 comparisons on the one input this pipeline was given and nothing else. 4
  clean comparisons rule out a per-comparison divergence probability above 53 percent, 95 percent
  one-sided. NO_DIVERGENCE_OBSERVED needs a zero under every amplifier as well, so it is not
  claimed, and neither is anything weaker.
```

That paragraph exists because two earlier flags failed the same audit. `--key` with a float column
deleted the whole reassociation bound section, including the pre-registered check that is allowed to
fail in it, and `--tolerance` deleted the sentence explaining why the mechanism story did not hold.
A flag that quietly removes the tool's own falsifiable claim is worse than no flag, so there is a
test asserting the bound sentence survives `--no-amplify`.
