# Banned language

A living file. `check_fingerprint.py` parses the fenced blocks below, so the pre-commit hook and CI always run the current list. When `voice-auditor` or `code-smell-auditor` catches a new tell, add it here and the enforcement follows automatically.

Adding a term is cheap. Removing one needs a reason written next to it in the commit message.

## Why the list is the smaller half of the job

Every word here is a symptom. The disease is writing that commits to nothing. `voice-auditor` runs this list first because it is mechanical and fast, then does the part that matters: reading for hedged non-claims, uniform paragraph rhythm, and sentences that would survive being pasted into a different project's README unchanged.

A document can score zero hits on this file and still be obviously generated. Do not treat a clean run as a pass.

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

Written as Python regex, case-insensitive, applied to prose files only.

```banned-regex
not only .{1,60}? but also
it is not just .{1,60}?, it is
it's not just .{1,60}?, it's
whether you (are|'re) .{1,60}? or
in the (ever[- ]evolving|rapidly changing) .{0,30}(world|landscape|realm)
```

## Exceptions

Some of the banned words have legitimate technical uses. The checker skips a hit when the surrounding text matches one of these. Keep this list short: every exception is a hole in the net.

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

`eval harness` is here because Vishal uses the phrase himself throughout the brief and it is the ordinary name for the thing. `agent harness` earns its place the same way, and half the slate involves running an agent, so it would otherwise recur constantly. `underscore` is here because it names a character that appears constantly in Python discussion.

Note what stays caught: bare `harness` as a verb, as in "harness the power of embeddings". There is a test for exactly that in `test_check_fingerprint.py`, because an exception that quietly widens into a blanket pass is worse than no exception.

## Punctuation and symbols

Enforced in code, not listed as words.

**Em dash family**, banned in any form anywhere: `U+2014` (em dash), `U+2013` (en dash), `U+2015` (horizontal bar). Use a hyphen, a comma, a colon, or restructure the sentence. En dash is included because the usual defence for it, numeric ranges, reads fine with a hyphen and the exception would be abused.

**Emoji**, banned anywhere: the standard emoji blocks plus variation selector `U+FE0F` and the zero-width joiner sequences. Includes the ones that look like punctuation, such as the check mark `U+2705` and the cross mark `U+274C`, which turn up constantly in generated README tables.

## Structural tells

Not machine-checkable. `voice-auditor` reads for these and reports with line numbers.

- Every section the same length
- Exactly three bullets under every heading
- A bold lead-in on every bullet in a list, with no bullet that just says its thing
- A rhetorical question followed immediately by its own answer
- A closing paragraph that restates the opening
- Headings that all share one grammatical shape, for example all gerunds or all noun phrases
- Feature lists padded to six items where three of them are trivial
- Badge walls
- "Contributions are welcome" on a personal project nobody is contributing to

## Code tells

`code-smell-auditor` owns these. Also not machine-checkable, mostly.

- Comments that restate the line below them
- Docstrings on trivial one-line functions
- `# Initialize variables` and its relatives
- `except Exception as e: print(f"Error: {e}")`
- Variables named `data`, `result`, `temp`, `output`, `processed_data`
- An abstract base class with exactly one implementation
- A config dataclass with one field
- `Optional[Any]` type hints
- Files suspiciously similar in length
- Every function carrying an identically shaped docstring

The last one is the tell that survives the others. Uniform, evenly spaced perfection is itself the signal.

## Change log

Additions go here with the date and what caught them, so the list has a provenance rather than growing by vibes.

- 2026-08-25: Seeded from the brief in `Github_Project/Prompt.md`. Added `dive deep`, `deep dive`, `game changer`, `best-in-class`, `state-of-the-art`, `paradigm shift` on top of the original list, and the exceptions block for `eval harness` and `underscore`.
- 2026-08-25: Added missing inflections after `delves` walked through a check that caught `delve`. Also `robustly`, `harnessed`, `unlocked`, `elevated`, `streamlined`, `empowered`, `showcased`, `boast`, `boasting`, `underscored`, `realms`, `landscapes`, `tapestries`.
- 2026-08-26: Added `agent harness` to allowed contexts. Caught as a false positive in `SLATE.md` on the phrase "agent harness and trace schema", which is the ordinary noun rather than the marketing verb. Bare `harness` stays banned.
