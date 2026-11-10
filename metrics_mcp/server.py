"""MCP server over the MetricFlow semantic layer, on the local DuckDB marts (D104).

An agent picks metrics, dimensions and filters; MetricFlow writes the SQL. No tool takes
SQL, and the connection is read-only (dbt target `mcp`). Run over stdio:

    uv run python -m metrics_mcp.server

Reads transform/target/semantic_manifest.json, so run `dbt parse` on target dev first.
`explain_change` (D110) splits a metric's change between two periods by dimension members;
its maths lives in alerting/metric_drivers.py, shared with the Airflow run email.
"""

import contextlib
import datetime
import functools
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from alerting import metric_drivers
from metrics_mcp import query

if TYPE_CHECKING:
    from metricflow.engine.metricflow_engine import MetricFlowEngine

REPO = Path(__file__).resolve().parents[1]
TRANSFORM = REPO / "transform"
MAX_ERROR_CHARS = 1500
_READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)

server = MCPServer(
    "olist-metrics",
    instructions=(
        "Governed metrics of the Olist e-commerce marts. Every number comes from a metric defined in "
        "the semantic layer. If no metric or dimension fits a question, say so; there is no way to run SQL."
    ),
)


@functools.cache
def _engine() -> "MetricFlowEngine":
    """Loaded on first use: the imports take about 10 s and would delay the MCP handshake."""
    os.environ["DBT_TARGET"] = "mcp"
    os.environ["DBT_PROJECT_DIR"] = str(TRANSFORM)
    os.environ["DBT_PROFILES_DIR"] = str(TRANSFORM)
    # profiles.yml has a path relative to transform/; a client starts the server from anywhere
    os.environ.setdefault("DBT_DUCKDB_PATH", str(REPO / "data" / "olist.duckdb"))
    with contextlib.redirect_stdout(sys.stderr):  # stdout belongs to the MCP protocol
        from dbt_metricflow.cli.cli_configuration import CLIConfiguration

        configuration = CLIConfiguration()
        configuration.setup(configure_file_logging=False)
        return configuration.mf


@contextlib.contextmanager
def _guarded() -> Iterator[None]:
    """Stdout stays with the protocol; a failure reaches the agent as text it can act on.

    The SDK hides the text of any exception other than ToolError, and "unknown metric, these
    exist" is what lets an agent correct itself or refuse.
    """
    with contextlib.redirect_stdout(sys.stderr):
        try:
            yield
        except Exception as error:
            raise ToolError(str(error)[:MAX_ERROR_CHARS]) from error


def _dimensions(metrics: list[str]) -> list[Any]:
    return list(_engine().list_dimensions(metric_names=metrics))


def _time_grains(dimensions: list[Any]) -> dict[str, str]:
    """Grain of each time dimension, which a filter on it has to name."""
    return {d.dunder_name: d.type_params.time_granularity.value for d in dimensions if d.type.value == "time" and d.type_params}


def _known_metrics() -> set[str]:
    return {metric.name for metric in _engine().list_metrics(include_dimensions=False)}


@server.tool(annotations=_READ_ONLY)
def list_metrics() -> list[dict[str, str | None]]:
    """All governed metrics: name, type and business definition. Start here."""
    with _guarded():
        return [
            {"name": m.name, "type": m.type.value, "description": m.description}
            for m in _engine().list_metrics(include_dimensions=False)
        ]


@server.tool(annotations=_READ_ONLY)
def list_dimensions(metrics: list[str]) -> list[dict[str, str | None]]:
    """Dimensions you can group or filter these metrics by; with several metrics, only the shared ones.

    Group time dimensions at any coarser grain with a suffix: metric_time__month, __quarter, __year.
    """
    with _guarded():
        query.check_metrics(metrics, _known_metrics())
        return [{"name": d.dunder_name, "type": d.type.value, "description": d.description} for d in _dimensions(metrics)]


@server.tool(annotations=_READ_ONLY)
def dimension_values(metric: str, dimension: str) -> list[object]:
    """Distinct values of a categorical dimension, for exact filter values (e.g. customer__state = 'SP')."""
    with _guarded():
        query.check_metrics([metric], _known_metrics())
        dimensions = _dimensions([metric])
        if dimension not in {d.dunder_name for d in dimensions if d.type.value == "categorical"}:
            raise ValueError(f"{dimension!r} is not a categorical dimension of {metric!r}")
        return list(_engine().get_dimension_values(metric_names=[metric], get_group_by_values=dimension))


@server.tool(annotations=_READ_ONLY)
def query_metrics(
    metrics: list[str],
    group_by: list[str] | None = None,
    start_date: Annotated[datetime.date | None, Field(description="First day of metric_time, inclusive")] = None,
    end_date: Annotated[datetime.date | None, Field(description="Last day of metric_time, inclusive")] = None,
    filters: list[query.Filter] | None = None,
    order_by: Annotated[list[str] | None, Field(description="Queried metric or group by; prefix '-' for descending")] = None,
    limit: Annotated[int, Field(description=f"Rows to return, up to {query.MAX_ROWS}")] = 100,
    include_sql: bool = False,
) -> dict[str, Any]:
    """Values of metrics, optionally by dimensions, for a period, with filters. MetricFlow writes the SQL.

    Without group_by the result is one row over all the data. Use start_date and end_date for the
    period; use filters for other dimensions. `truncated` tells when more rows exist than were returned.
    """
    from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

    group_by = group_by or []
    order_by = order_by or []
    with _guarded():
        query.check_metrics(metrics, _known_metrics())
        query.check_group_by(group_by)
        query.check_order_by(order_by, metrics, group_by)
        query.check_limit(limit)
        where: list[str] = []
        if filters:
            dimensions = _dimensions(metrics)
            names = {d.dunder_name for d in dimensions}
            where = [query.where_constraint(f, names, _time_grains(dimensions)) for f in filters]
        result = _engine().query(
            MetricFlowQueryRequest.create(
                metric_names=metrics,
                group_by_names=group_by or None,
                time_constraint_start=datetime.datetime.combine(start_date, datetime.time.min) if start_date else None,
                time_constraint_end=datetime.datetime.combine(end_date, datetime.time.min) if end_date else None,
                where_constraints=where or None,
                order_by_names=order_by or None,
                limit=limit + 1,  # one more than asked: tells whether rows were cut
            )
        )
    table = result.result_df
    if table is None:
        raise ValueError("MetricFlow returned no result table")
    rows = list(table.rows)
    answer: dict[str, Any] = {
        "rows": [
            {column: query.json_value(value) for column, value in zip(table.column_names, row, strict=True)}
            for row in rows[:limit]
        ],
        "truncated": len(rows) > limit,
    }
    if include_sql:
        answer["sql"] = result.sql
    return answer


@server.tool(annotations=_READ_ONLY)
def explain_change(
    metric: str,
    before_start: Annotated[datetime.date, Field(description="First day of the earlier period, inclusive")],
    before_end: Annotated[datetime.date, Field(description="Last day of the earlier period, inclusive")],
    after_start: Annotated[datetime.date, Field(description="First day of the later period, inclusive")],
    after_end: Annotated[datetime.date, Field(description="Last day of the later period, inclusive")],
    dimensions: Annotated[
        list[str] | None, Field(description="Categorical dimensions to split the change by; default: the usual ones the metric has")
    ] = None,
    top: Annotated[int, Field(description="Members to list per dimension; the rest are summed as 'other'")] = metric_drivers.TOP_MEMBERS,
) -> dict[str, Any]:
    """Why a metric changed between two periods: the change split by dimension members, largest first.

    Per dimension the rows (top members plus 'other') add up to the total change. For a ratio
    metric each member also has `mix` (it got bigger or smaller) and `rate` (its own ratio moved).
    Only sums, counts and ratios of those can be split; distinct counts and averages are refused.
    """
    top_limit = 10
    with _guarded():
        query.check_metrics([metric], _known_metrics())
        for first, last in ((before_start, before_end), (after_start, after_end)):
            if first > last:
                raise ValueError(f"Period {first} to {last} ends before it starts")
        if not 1 <= top <= top_limit:
            raise ValueError(f"top must be between 1 and {top_limit}")
        manifest = json.loads((TRANSFORM / "target" / "semantic_manifest.json").read_text(encoding="utf-8"))
        specs = metric_drivers.specs_from_manifest(manifest)
        metric_drivers.spec_for(specs, metric)  # refuses what cannot be split, with the reason
        categorical = {d.dunder_name for d in _dimensions([metric]) if d.type.value == "categorical"}
        chosen = dimensions or [d for d in metric_drivers.EMAIL_DIMENSIONS if d in categorical]
        unknown = [d for d in chosen if d not in categorical]
        if unknown or not chosen:
            raise ValueError(f"Unknown or non-categorical dimension(s) {unknown}. Available: {', '.join(sorted(categorical))}")
        explanation = metric_drivers.explain(
            metric_drivers.metricflow_runner(_engine()),
            specs,
            metric,
            metric_drivers.Period(before_start, before_end),
            metric_drivers.Period(after_start, after_end),
            chosen,
            top,
        )
        return metric_drivers.to_dict(explanation)


if __name__ == "__main__":
    server.run("stdio")
