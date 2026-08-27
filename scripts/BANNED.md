# Words this project does not use in prose

`check_fingerprint.py`, beside this file, parses the fenced blocks below. The pre-commit hook runs
it over the staged diff and CI runs it over the whole tree, so the list has one home and both
callers read the same one.

Adding a term is cheap. Removing one needs a reason written next to it in the commit message.

None of this makes writing good. It catches a specific kind of bad: words that sound like a claim
and are not one. A file can score zero here and still hedge, repeat itself and pad a list to six
items where three would do.

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

`eval harness` and `agent harness` are here because they are the ordinary names for those two
things and no synonym reads as well. `underscore` is here because it names a character that comes
up constantly in Python discussion.

What stays caught is bare `harness` as a verb, as in "harness the power of embeddings".
`test_check_fingerprint.py` asserts that, because an exception that quietly widens into a blanket
pass is worse than no exception.

## Punctuation and symbols

Enforced in code rather than listed as words.

Em dash `U+2014`, en dash `U+2013` and horizontal bar `U+2015`, in any form anywhere. Use a hyphen,
a comma, a colon, or restructure the sentence. En dash is included because the usual defence for
it, numeric ranges, reads fine with a hyphen and the exception would get abused.

Emoji anywhere: the standard blocks plus variation selector `U+FE0F` and the zero-width joiner
sequences. Includes the ones that look like punctuation, such as the check mark `U+2705` and the
cross mark `U+274C`, which turn up in generated tables.

## House style the checker cannot enforce

Prose:

- every section the same length
- exactly three bullets under every heading
- a bold lead-in on every bullet in a list, with no bullet that just says its thing
- a rhetorical question followed immediately by its own answer
- a closing paragraph that restates the opening
- headings that all share one grammatical shape
- a feature list padded to six items where three of them are trivial
- badge walls
- "Contributions are welcome" on a personal project nobody is contributing to

Code:

- comments that restate the line below them
- docstrings on trivial one-line functions
- `# Initialize variables` and its relatives
- `except Exception as e: print(f"Error: {e}")`
- variables named `data`, `result`, `temp`, `output`, `processed_data`
- an abstract base class with exactly one implementation
- a config dataclass with one field
- `Optional[Any]` type hints

## Change log

Additions go here with the date and what caught them, so the list has a provenance rather than
growing by vibes.

- 2026-08-25: seeded with the single words, the phrases and the regexes above. Added `dive deep`,
  `deep dive`, `game changer`, `best-in-class`, `state-of-the-art` and `paradigm shift` on top of
  the first list, plus the exceptions block for `eval harness` and `underscore`.
- 2026-08-25: added the missing inflections after `delves` walked through a check that caught
  `delve`. Also `robustly`, `harnessed`, `unlocked`, `elevated`, `streamlined`, `empowered`,
  `showcased`, `boast`, `boasting`, `underscored`, `realms`, `landscapes`, `tapestries`.
- 2026-08-26: added `agent harness` to the allowed contexts. It was a false positive on the phrase
  "agent harness and trace schema", which is the ordinary noun rather than the marketing verb.
  Bare `harness` stays banned.
