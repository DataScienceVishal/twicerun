"""The second axis of a fire rate: what is left of it at threads=1.

Class says what went wrong, cause says where to look. A step that diverges 4 of
4 with the machine's default thread count and 0 of 4 with one thread points at
parallel reduction or parallel scan order; one that diverges either way points
somewhere else entirely, and the tool names that and stops rather than guessing
between a clock read, a data-dependent branch and appended state.

Both rates come from the same N, which is the only reason they can sit in one
sentence. "4 of 4 at threads=10, 0 of 4 at threads=1" is a comparison. The same
line with a 3-run bisect underneath a 5-run main loop is two unrelated fractions.

The zero is the part worth being careful about, because it is the half of the
sentence that looks like a result. Four clean comparisons at threads=1 are four
comparisons: they put a 95 percent one-sided upper bound of about 53 percent on
the per-comparison rate and they cannot do better however confident the label
beside them reads. So PARALLEL_ORDER is a reading of two measured rates, not a
finding that single-threaded execution cannot diverge, and the report prints the
bound next to the label to keep those two apart.
"""

from __future__ import annotations

from dataclasses import dataclass

# The mechanism under test is the order a parallel reduction adds its terms in,
# and one thread has no order to get wrong.
BISECT_THREADS = 1

PARALLEL_ORDER = "PARALLEL_ORDER"
PERSISTS_SINGLE_THREADED = "PERSISTS_SINGLE_THREADED"

CONFIDENCE = 0.95


def upper_bound(comparisons: int) -> float:
    """The one-sided bound on the per-comparison rate when nothing fired in `comparisons` tries.

    Solving `(1 - p)^m = 0.05` for p. At m = 4 it is 0.53, which is the figure
    that stops a zero being read as a never. It assumes the comparisons are
    independent, and they are not: they share a process, a page cache and a
    machine. The report says so rather than taking the credit quietly.
    """
    if comparisons < 1:
        return 1.0
    return 1.0 - (1.0 - CONFIDENCE) ** (1.0 / comparisons)


@dataclass(frozen=True)
class Bisect:
    """One step re-executed N times at threads=1, and how often it disagreed with itself.

    The reference is the first single-threaded execution, not run 1 of the main
    loop. Comparing a single-threaded run against a parallel one would report a
    reassociation difference on every float step by construction and would say
    nothing at all about the mechanism.
    """

    comparisons: int
    fired: int
    threads: int = BISECT_THREADS

    @property
    def label(self) -> str:
        return PERSISTS_SINGLE_THREADED if self.fired else PARALLEL_ORDER

    @property
    def bound(self) -> float:
        return upper_bound(self.comparisons)
