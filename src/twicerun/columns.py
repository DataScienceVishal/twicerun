"""Splitting a row into the part a difference can be trusted on and the part it cannot.

The split has to happen before any rows are read, because it decides the join
key. Integers, strings, dates and DECIMAL cannot come back different from a
correct query run twice: no reordering of a fixed-point addition or a string
concatenation changes the answer, so a difference in one of those columns is
real. FLOAT and DOUBLE can, because addition is not associative and a parallel
reduction picks whatever order the threads finish in.

DECIMAL sitting on the exact side is the fix this tool recommends for the float
aggregate bug, not an accident of classification. DuckDB's decimal sum is
fixed-point and reassociates exactly.

The third partition is the one worth arguing about. LIST, STRUCT, MAP and UNION
are refused rather than compared, because every cheap way of comparing them is
wrong in a way the report would not show: casting to VARCHAR makes a struct
holding 1.0 differ from one holding 1.00, and comparing element counts ignores
the elements. Anything the classifier does not recognise lands here too, so a
type added by a future DuckDB release stops the run instead of being compared by
whatever the fallback branch happened to be. Nothing in the reference pipeline
needs a nested type, so refusing costs nothing today.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

INTEGERS = frozenset(
    {
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
        "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
    }
)
CLOCK = frozenset(
    {
        "DATE", "TIME", "TIME WITH TIME ZONE",
        "TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
    }
)
EXACT_TYPES = INTEGERS | CLOCK | frozenset({"BOOLEAN", "VARCHAR", "BLOB", "UUID", "INTERVAL"})
APPROX_TYPES = frozenset({"FLOAT", "REAL", "DOUBLE"})
# DuckDB renders these with their parameters attached: DECIMAL(18,4), ENUM('a','b').
PARAMETERISED_EXACT = frozenset({"DECIMAL", "NUMERIC", "ENUM"})
NESTED = frozenset({"STRUCT", "MAP", "UNION"})


class Partition(StrEnum):
    EXACT = "EXACT"
    APPROX = "APPROX"
    UNSUPPORTED = "UNSUPPORTED"


class UnsupportedColumn(TypeError):
    """A column the oracle will not compare, named so the user can drop it."""


class UnknownKeyColumn(LookupError):
    """--key asked for a column the artifact does not have."""


class FloatInKey(ValueError):
    """--key tried to match rows on a float, which would be bit equality."""


def classify(sql_type: str) -> Partition:
    kind = sql_type.strip().upper()
    # INTEGER[] is a list and INTEGER[3] a fixed-size array; both end in a bracket
    # and neither is comparable here, so one test covers both.
    if kind.endswith("]"):
        return Partition.UNSUPPORTED
    head = kind.split("(", 1)[0].strip()
    if head in NESTED:
        return Partition.UNSUPPORTED
    if head in PARAMETERISED_EXACT:
        return Partition.EXACT
    if kind in APPROX_TYPES:
        return Partition.APPROX
    if kind in EXACT_TYPES:
        return Partition.EXACT
    return Partition.UNSUPPORTED


def is_clock(sql_type: str) -> bool:
    """A timestamp that moved is almost always the wall clock, and saying so is free."""
    return sql_type.strip().upper() in CLOCK


@dataclass(frozen=True)
class ColumnPlan:
    types: Mapping[str, str]
    partition: Mapping[str, Partition]
    key: tuple[str, ...]

    @property
    def values(self) -> tuple[str, ...]:
        return tuple(c for c in self.partition if c not in self.key)

    def approx_values(self) -> tuple[str, ...]:
        return tuple(c for c in self.values if self.partition[c] is Partition.APPROX)

    def exact_values(self) -> tuple[str, ...]:
        return tuple(c for c in self.values if self.partition[c] is Partition.EXACT)

    def ordinal_order(self) -> tuple[str, ...]:
        """What the ordinal inside a key group sorts by.

        The approx columns first, which is the spec's clause and the only part
        that matters with the default key, then any exact column --key pushed
        out of the key so the ordinal stays reproducible rather than falling to
        whatever order the scan produced.
        """
        return self.approx_values() + self.exact_values()

    def float_width(self, column: str) -> int:
        """Bits in the IEEE-754 layout, which the ULP transform needs.

        A FLOAT one step away from its neighbour is one FLOAT ulp apart and
        about 2^29 DOUBLE ulps apart, so widening before measuring would turn
        last-bit noise into a headline number.
        """
        return 32 if self.types[column].strip().upper() in {"FLOAT", "REAL"} else 64


def plan(columns: Mapping[str, str], key_override: Sequence[str] | None = None) -> ColumnPlan:
    """Partition an artifact's columns and pick the join key.

    The default key is every exact column. That is the move making an
    order-independent tolerant comparison tractable at all: exact comparison of
    unordered rows is a multiset of hashes and is linear, tolerant comparison of
    unordered rows is an assignment problem, and matching the exact part first
    turns the assignment problem into a join.

    Floats stay out of the key and --key cannot put one there. Matching rows on
    a float means joining on bit equality, which is the comparison this module
    exists to replace, and the damage is not confined to one artifact: a float
    moved into the key stops being measured as drift, so its last-bit
    reassociation arrives as ROW_MISSING and ROW_EXTRA that no policy can
    downgrade, the step reports no drift at all, and the whole reassociation
    bound section drops out of the report along with the pre-registered check
    that is allowed to fail in it. A flag that deletes a falsifiable claim is
    worse than no flag.
    """
    where = {column: classify(kind) for column, kind in columns.items()}
    refused = [
        f"{column} ({columns[column]})"
        for column, partition in where.items()
        if partition is Partition.UNSUPPORTED
    ]
    if refused:
        raise UnsupportedColumn(
            "cannot compare " + ", ".join(refused) + ". Nested and unrecognised types are "
            "refused rather than compared badly. Drop the column from the artifact, or "
            "unnest it, and rerun"
        )

    if key_override is None:
        key = tuple(c for c, partition in where.items() if partition is Partition.EXACT)
    else:
        key = _checked_override(key_override, where, columns)
    return ColumnPlan(types=dict(columns), partition=where, key=key)


def _checked_override(
    requested: Sequence[str], where: Mapping[str, Partition], types: Mapping[str, str]
) -> tuple[str, ...]:
    missing = [c for c in requested if c not in where]
    if missing:
        raise UnknownKeyColumn(
            f"--key named {', '.join(missing)}, which is not in this artifact. "
            f"It has {', '.join(where)}"
        )
    key = tuple(dict.fromkeys(requested))
    floats = tuple(c for c in key if where[c] is Partition.APPROX)
    if floats:
        named = ", ".join(f"{c} ({types[c]})" for c in floats)
        raise FloatInKey(
            f"--key put {named} in the key, and a float cannot be matched on. The join "
            f"would use bit equality, so one ulp of reassociation would come back as a "
            f"missing row and an extra row rather than as drift, and the step would "
            f"report no drift for the reassociation bound to be checked against. Leave "
            f"it out of --key and it is compared as a value, which is where its size "
            f"gets measured"
        )
    return key


def clock_columns(columns: Mapping[str, str], among: Iterable[str]) -> tuple[str, ...]:
    return tuple(c for c in among if is_clock(columns[c]))


def schema_difference(
    reference: Mapping[str, str], candidate: Mapping[str, str]
) -> str | None:
    """Every way two column sets differ, not the first one found.

    A run that drops one column and widens another has done two things and the
    report should say both.
    """
    if reference == candidate:
        return None
    gone = sorted(set(reference) - set(candidate))
    added = sorted(set(candidate) - set(reference))
    retyped = [
        f"{column} {reference[column]} to {candidate[column]}"
        for column in reference
        if column in candidate and reference[column] != candidate[column]
    ]

    parts = []
    if gone:
        parts.append(f"columns dropped {', '.join(gone)}")
    if added:
        parts.append(f"columns added {', '.join(added)}")
    if retyped:
        parts.append(f"types changed {', '.join(retyped)}")
    return "schema changed: " + "; ".join(parts)
