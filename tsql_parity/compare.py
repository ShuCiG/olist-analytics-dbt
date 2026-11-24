"""Row-level comparison of two result sets on a business key.

Three buckets (left only, right only, values differ), per-column differences with a decimal
tolerance, and a row-count deviation with a threshold.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

Row = dict[str, Any]


@dataclass
class Comparison:
    name: str
    left_rows: int
    right_rows: int
    left_only: int = 0
    right_only: int = 0
    differ: int = 0
    column_diffs: dict[str, int] = field(default_factory=lambda: {})
    examples: list[str] = field(default_factory=lambda: [])
    row_threshold_pct: float = 0.0

    @property
    def row_deviation_pct(self) -> float:
        if self.left_rows == 0:
            return 0.0 if self.right_rows == 0 else 100.0
        return abs(self.right_rows - self.left_rows) / self.left_rows * 100

    @property
    def ok(self) -> bool:
        return (
            self.left_only == 0
            and self.right_only == 0
            and self.differ == 0
            and self.row_deviation_pct <= self.row_threshold_pct
        )


def values_equal(left: Any, right: Any, tolerance: Decimal) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, int | float | Decimal) and isinstance(right, int | float | Decimal):
        return abs(Decimal(str(left)) - Decimal(str(right))) <= tolerance
    return left == right


def compare(
    name: str,
    left: list[Row],
    right: list[Row],
    keys: list[str],
    tolerance: Decimal = Decimal(0),
    row_threshold_pct: float = 0.0,
) -> Comparison:
    """FULL OUTER JOIN on `keys` in plain Python; a duplicate key counts as a difference."""

    def index(rows: list[Row]) -> dict[tuple[Any, ...], Row]:
        return {tuple(r[k] for k in keys): r for r in rows}

    li, ri = index(left), index(right)
    result = Comparison(name, len(left), len(right), row_threshold_pct=row_threshold_pct)
    if len(li) != len(left) or len(ri) != len(right):
        result.differ += 1
        result.examples.append("duplicate business key")
    result.left_only = len(li.keys() - ri.keys())
    result.right_only = len(ri.keys() - li.keys())
    for key in li.keys() & ri.keys():
        bad = [c for c in li[key] if c not in keys and not values_equal(li[key][c], ri[key].get(c), tolerance)]
        if bad:
            result.differ += 1
            for c in bad:
                result.column_diffs[c] = result.column_diffs.get(c, 0) + 1
            if len(result.examples) < 3:
                result.examples.append(f"{key}: " + ", ".join(f"{c} {li[key][c]!r} vs {ri[key].get(c)!r}" for c in bad))
    return result
