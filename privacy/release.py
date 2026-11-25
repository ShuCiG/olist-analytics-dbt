"""Checked release of grouped results: k-anonymity, l-diversity and differencing protection.

`GuardedDataset` holds one row per person (key column `token`). Every query goes through `run`:

1. rows are filtered and grouped;
2. a group with fewer than `k` distinct people is dropped, and so is a group whose sensitive column shows fewer
   than `l` distinct values (the row disappears, its size is not published);
3. the people behind every released group are compared with the groups released earlier. If two groups differ by
   1 to k-1 people, subtracting their aggregates would isolate those people, so the query is refused and nothing
   is released or logged.

Totals are never added to a release: a total next to suppressed groups would give them back by subtraction.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from privacy.config import PrivacyConfig

Scalar = str | int | float | bool | None
Record = Mapping[str, Scalar]

PERSON = "token"


class QueryRefused(Exception):
    """The query would, with an earlier release, single out fewer than k people."""


@dataclass(frozen=True)
class Query:
    group_by: tuple[str, ...] = ()
    filters: Mapping[str, Scalar] = field(default_factory=dict[str, Scalar])  # column == value, all must hold
    measure: str | None = None  # numeric column to average per group
    sensitive: str | None = None  # column that needs l distinct values per group


@dataclass(frozen=True)
class Release:
    rows: tuple[dict[str, Scalar], ...]
    suppressed_groups: int  # how many groups were dropped, never their sizes


def _mean(values: Sequence[Scalar], column: str) -> float | None:
    numbers: list[float] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise TypeError(f"measure column {column!r} holds a non-numeric value {value!r}")
        numbers.append(float(value))
    return round(sum(numbers) / len(numbers), 4) if numbers else None


class GuardedDataset:
    def __init__(self, records: Sequence[Record], config: PrivacyConfig | None = None) -> None:
        self.config = config or PrivacyConfig()
        seen: set[Scalar] = set()
        for record in records:
            person = record.get(PERSON)
            if not isinstance(person, str) or not person:
                raise ValueError(f"every record needs a non-empty {PERSON!r}")
            if person in seen:
                raise ValueError("one record per person: duplicate token (aggregate per person first)")
            seen.add(person)
        self._records = tuple(records)
        self._released: set[frozenset[str]] = set()  # the people behind each released group

    def run(self, query: Query) -> Release:
        self._check_columns(query)
        groups: dict[tuple[Scalar, ...], list[Record]] = {}
        for record in self._records:
            if all(record.get(col) == want for col, want in query.filters.items()):
                groups.setdefault(tuple(record.get(col) for col in query.group_by), []).append(record)

        suppressed = 0
        kept: list[tuple[tuple[Scalar, ...], list[Record]]] = []
        for key, members in sorted(groups.items(), key=lambda item: tuple(str(v) for v in item[0])):
            if self._is_small(members, query):
                suppressed += 1
            else:
                kept.append((key, members))

        cells = [frozenset(str(m[PERSON]) for m in members) for _, members in kept]
        self._refuse_if_differencing(cells)
        self._released.update(cells)

        rows: list[dict[str, Scalar]] = []
        for (key, members), cell in zip(kept, cells, strict=True):
            row: dict[str, Scalar] = dict(zip(query.group_by, key, strict=True))
            row["n"] = len(cell)
            if query.measure is not None:
                row[f"mean_{query.measure}"] = _mean([m.get(query.measure) for m in members], query.measure)
            rows.append(row)
        return Release(rows=tuple(rows), suppressed_groups=suppressed)

    def _check_columns(self, query: Query) -> None:
        if not self._records:
            return
        known = set(self._records[0])
        wanted = {*query.group_by, *query.filters, *(c for c in (query.measure, query.sensitive) if c)}
        if PERSON in wanted:
            raise ValueError(f"{PERSON!r} cannot be grouped, filtered or measured")
        if missing := wanted - known:
            raise ValueError(f"unknown columns: {sorted(missing)}")

    def _is_small(self, members: Sequence[Record], query: Query) -> bool:
        if len(members) < self.config.k:  # one record per person, so rows = distinct people
            return True
        if query.sensitive is None:
            return False
        return len({m.get(query.sensitive) for m in members}) < self.config.l

    def _refuse_if_differencing(self, cells: Sequence[frozenset[str]]) -> None:
        k = self.config.k
        for cell in cells:
            for earlier in self._released:
                gap = len(cell ^ earlier)
                if 0 < gap < k:
                    raise QueryRefused(
                        f"refused: a released group and this one differ by {gap} people (fewer than k={k}); "
                        "subtracting them would isolate those people"
                    )
