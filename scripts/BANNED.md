# Words this project does not use in prose

Enforced by `scripts/check_fingerprint.py`, which parses the fenced blocks below.
The pre-commit hook runs it over the staged diff and CI runs it over the tree.

Punctuation and emoji are matched by pattern in the checker rather than listed here.

Everything above the change log that is not in a fence is there because no fence
would hold it. The checker never sees those, which is the reason they each got
through several readings.

## Single words

```banned-words
delve
delves
delved
delving
leverage
leverages
leveraging
leveraged
robust
robustly
seamless
seamlessly
comprehensive
comprehensively
cutting-edge
harness
harnesses
harnessed
harnessing
unlock
unlocks
unlocked
unlocking
elevate
elevates
elevated
elevating
streamline
streamlines
streamlined
streamlining
empower
empowers
empowered
empowering
showcase
showcases
showcased
showcasing
boast
boasts
boasting
underscore
underscores
underscored
underscoring
pivotal
meticulous
meticulously
realm
realms
landscape
landscapes
tapestry
tapestries
```

## Phrases

```banned-phrases
at its core
it is worth noting
it's worth noting
a testament to
navigating the complexities
in today's fast-paced
let us dive in
let's dive in
in conclusion
the key takeaway
dive deep
deep dive
game changer
game-changing
best-in-class
state-of-the-art
paradigm shift
```

## Patterns

Python regex, case-insensitive, applied to prose files and to comments and string literals in code.

```banned-regex
not only .{1,60}? but also
it is not just .{1,60}?, it is
it's not just .{1,60}?, it's
whether you (are|'re) .{1,60}? or
in the (ever[- ]evolving|rapidly changing) .{0,30}(world|landscape|realm)
```

## Exceptions

Some of the words above have ordinary technical uses. The checker skips a hit when the surrounding
text matches one of these. Keep the list short: every exception is a hole in the net.

```allowed-contexts
eval harness
evaluation harness
test harness
agent harness
harness/
harness.py
_harness
underscore-separated
leading underscore
trailing underscore
double underscore
financial leverage
```

## Three the checker cannot see

**Bold carrying the rhythm instead of the finding.** The README ran 16
paragraph-initial bold lead-ins across 19 folds, nearly all of them admissions
arriving in the same shape. The eighth one no longer said "read this", it said
"another of those", and the three that genuinely outranked the rest were spending
their emphasis on a crowd. `94a9ac9` cut it to those three. One of the three sits
mid-paragraph, on the sentence holding the finding rather than the sentence
setting it up, which is where bold belongs when the two are not the same sentence.
Test it by deleting the asterisks and asking what was lost.

**A figure typed by hand where an artifact already holds one.** The README quoted
491,520 unmatched rows. The number is real and it is the top of the observed
range; `results/eval-2026-08-27.json` records `unmatched_median` as 444,840 of
500,000. This file's whole argument is that a spread is not a bound, so quoting
the loudest observation as the figure concedes the argument in passing, and six
other places had copied it from the first. `e48d40f` put the prose on the
artifact's median. A figure that cannot be generated does not go in the README,
and one that has to go in anyway goes in the disclosure list with its date.

**A fold written for a reader who opened a different fold.** Twenty-one folds,
all collapsed until clicked, so the ordinary reader has opened one. Six was the
count of ten-trial runs in one fold and part of an ordinal meaning something else
360 lines down in another, which contradicts itself for anyone holding both and
says nothing at all to anyone holding one. `1d2afc1` settled which of the two was
right. The sibling repo had the plainer form, pronouns whose antecedent sat two
folds back.

## Change log

Dated so the list has provenance instead of growing by taste.

- 2026-09-19: Added the three above. Two are this repository's own and name the
  commit that fixed them. The third came from the sibling audit repo and is worth
  carrying because 21 folds is more than that repo has and the failure mode scales
  with the count.
