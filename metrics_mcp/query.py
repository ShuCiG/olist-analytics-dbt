"""Checks between an agent's arguments and MetricFlow. No MetricFlow import, so plain pytest covers it.

`mf`'s where parameter is a SQL string: `(select 1) = 1` reaches the warehouse as written.
So the server never takes one. A filter is a dimension, an operator and values, and
where_constraint() writes the string from those, after checking each of them.
"""

import datetime
import decimal
import re
from collections.abc import Collection
from typing import Literal

from pydantic import BaseModel, ConfigDict

Value = bool | int | float | str
MAX_ROWS = 1000
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_LIST_OPERATORS = {"in", "not in"}


class Filter(BaseModel):
    """One condition on a dimension of the queried metrics."""

    model_config = ConfigDict(extra="forbid")

    dimension: str
    operator: Literal["=", "!=", "<", "<=", ">", ">=", "in", "not in"]
    value: Value | list[Value]


def _literal(value: Value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    if "{" in value or "}" in value:  # the where string goes through Jinja
        raise ValueError(f"Filter value {value!r}: braces are not allowed")
    return "'" + value.replace("'", "''") + "'"


def where_constraint(condition: Filter, dimensions: Collection[str], time_grains: dict[str, str]) -> str:
    """The where string for one filter; the dimension must belong to the queried metrics."""
    name = condition.dimension
    if name not in dimensions:
        raise ValueError(f"Unknown dimension {name!r} for these metrics. Available: {', '.join(sorted(dimensions))}")
    if name in time_grains:
        column = f"{{{{ TimeDimension('{name}', '{time_grains[name]}') }}}}"
    else:
        column = f"{{{{ Dimension('{name}') }}}}"

    if condition.operator in _LIST_OPERATORS:
        if not isinstance(condition.value, list) or not condition.value:
            raise ValueError(f"Operator {condition.operator!r} takes a non-empty list of values")
        return f"{column} {condition.operator} ({', '.join(_literal(v) for v in condition.value)})"
    if isinstance(condition.value, list):
        raise ValueError(f"Operator {condition.operator!r} takes one value")
    return f"{column} {condition.operator} {_literal(condition.value)}"


def check_metrics(requested: list[str], known: Collection[str]) -> None:
    if not requested:
        raise ValueError("Name at least one metric.")
    unknown = [name for name in requested if name not in known]
    if unknown:
        raise ValueError(f"Unknown metric(s) {unknown}. Available: {', '.join(sorted(known))}")


def check_group_by(names: list[str]) -> None:
    """MetricFlow resolves the names and reports unknown ones; this keeps anything else out."""
    for name in names:
        if not _NAME.match(name):
            raise ValueError(f"Invalid group by name {name!r}")


def check_order_by(names: list[str], metrics: list[str], group_by: list[str]) -> None:
    allowed = {*metrics, *group_by}
    for name in names:
        if name.removeprefix("-") not in allowed:
            raise ValueError(f"Order by {name!r}: use a queried metric or group by, '-' for descending")


def check_limit(limit: int) -> None:
    if not 1 <= limit <= MAX_ROWS:
        raise ValueError(f"Limit must be between 1 and {MAX_ROWS}")


def json_value(value: object) -> object:
    """A warehouse value as JSON."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, datetime.date):  # datetime included
        return value.isoformat()
    return value
