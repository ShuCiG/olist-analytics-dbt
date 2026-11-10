"""The server on built marts, through a real MCP client over stdio.

Needs built marts and a parsed dev manifest, like the dashboard tests: uv run pytest metrics_mcp
"""

import json
import os
import sys
from pathlib import Path
from typing import Any

import anyio
import duckdb
import pytest
from mcp import Client, StdioServerParameters

REPO = Path(__file__).resolve().parents[2]
TOOLS = {"list_metrics", "list_dimensions", "dimension_values", "query_metrics", "explain_change"}


def call_all(calls: list[tuple[str, dict[str, Any]]]) -> tuple[set[str], dict[str, Any], list[Any]]:
    """One server process for all calls: starting one takes about 10 s."""
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-m", "metrics_mcp.server"],
        cwd=str(REPO),
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    async def run() -> tuple[set[str], dict[str, Any], list[Any]]:
        async with Client(parameters) as client:
            tools = await client.list_tools()
            results = [await client.call_tool(name, arguments) for name, arguments in calls]
            return {t.name for t in tools.tools}, {t.name: t for t in tools.tools}, results

    return anyio.run(run)


def payload(result: Any) -> Any:
    assert not result.is_error, result.content
    return result.structured_content or json.loads(result.content[0].text)


CHANGE = {"before_start": "2017-10-01", "before_end": "2017-10-31", "after_start": "2017-11-01", "after_end": "2017-11-30"}

CALLS: list[tuple[str, dict[str, Any]]] = [
    ("list_metrics", {}),
    ("list_dimensions", {"metrics": ["revenue"]}),
    ("dimension_values", {"metric": "revenue", "dimension": "customer__state"}),
    ("query_metrics", {"metrics": ["orders"]}),
    (
        "query_metrics",
        {
            "metrics": ["revenue"],
            "group_by": ["customer__state"],
            "start_date": "2017-01-01",
            "end_date": "2017-12-31",
            "filters": [{"dimension": "customer__state", "operator": "in", "value": ["SP", "RJ"]}],
            "order_by": ["-revenue"],
            "include_sql": True,
        },
    ),
    ("query_metrics", {"metrics": ["revenue"], "group_by": ["customer__state"], "limit": 2}),
    ("query_metrics", {"metrics": ["profit"]}),
    ("query_metrics", {"metrics": ["revenue"], "group_by": ["seller__state"]}),
    ("query_metrics", {"metrics": ["revenue"], "filters": [{"dimension": "(select 1)", "operator": "=", "value": 1}]}),
    ("query_metrics", {"metrics": ["revenue"], "sql": "select 1"}),
    ("explain_change", {**CHANGE, "metric": "revenue", "dimensions": ["customer__state"], "top": 2}),
    ("explain_change", {**CHANGE, "metric": "late_delivery_rate", "dimensions": ["customer__state"]}),
    ("explain_change", {**CHANGE, "metric": "customers"}),
    ("explain_change", {**CHANGE, "metric": "revenue", "dimensions": ["seller__state"]}),
]


@pytest.fixture(scope="module")
def session() -> tuple[set[str], dict[str, Any], list[Any]]:
    return call_all(CALLS)


def test_only_the_five_tools_and_no_sql_argument(session: tuple[set[str], dict[str, Any], list[Any]]) -> None:
    names, tools, _ = session
    assert names == TOOLS
    arguments = json.dumps([tool.input_schema.get("properties", {}) for tool in tools.values()])
    assert '"sql"' not in arguments
    assert '"where"' not in arguments


def test_catalog(session: tuple[set[str], dict[str, Any], list[Any]]) -> None:
    metrics, dimensions, values, *_ = session[2]
    assert "revenue" in {m["name"] for m in payload(metrics)["result"]}
    assert "customer__state" in {d["name"] for d in payload(dimensions)["result"]}
    assert "SP" in payload(values)["result"]


def test_query_returns_metricflow_rows(session: tuple[set[str], dict[str, Any], list[Any]]) -> None:
    _, _, results = session
    database = os.environ.get("DBT_DUCKDB_PATH", str(REPO / "data" / "olist.duckdb"))
    with duckdb.connect(database, read_only=True) as connection:
        (orders,) = connection.execute("select count(*) from marts.fct_orders").fetchone() or (None,)
    assert payload(results[3])["rows"] == [{"orders": orders}]
    filtered = payload(results[4])
    assert [row["customer__state"] for row in filtered["rows"]] == ["SP", "RJ"]
    assert filtered["truncated"] is False
    assert "fct_orders" in filtered["sql"]


def test_limit_reports_truncation(session: tuple[set[str], dict[str, Any], list[Any]]) -> None:
    limited = payload(session[2][5])
    assert len(limited["rows"]) == 2
    assert limited["truncated"] is True


@pytest.mark.parametrize(
    ("index", "message"),
    [(6, "Unknown metric"), (7, "seller__state"), (8, "Unknown dimension")],
)
def test_bad_requests_come_back_as_tool_errors(
    session: tuple[set[str], dict[str, Any], list[Any]], index: int, message: str
) -> None:
    result = session[2][index]
    assert result.is_error
    assert message in result.content[0].text


def test_sql_argument_does_not_reach_the_warehouse(session: tuple[set[str], dict[str, Any], list[Any]]) -> None:
    # Unknown arguments are ignored or refused, never used: the result is the plain metric
    result = session[2][9]
    assert result.is_error or "sql" not in payload(result)


def test_explain_change_adds_up_to_the_total(session: tuple[set[str], dict[str, Any], list[Any]]) -> None:
    revenue = payload(session[2][10])
    assert revenue["delta"] == pytest.approx(revenue["after"] - revenue["before"])
    (by_state,) = revenue["breakdowns"]
    rows = [*by_state["drivers"], *([by_state["other"]] if by_state["other"] else [])]
    assert len(by_state["drivers"]) == 2
    assert sum(row["delta"] for row in rows) == pytest.approx(revenue["delta"])

    rate = payload(session[2][11])
    assert rate["kind"] == "ratio"
    (by_state,) = rate["breakdowns"]
    assert sum(row["mix"] + row["rate"] for row in by_state["drivers"]) + by_state["other"]["delta"] == pytest.approx(rate["delta"])


@pytest.mark.parametrize(("index", "message"), [(12, "cannot be split"), (13, "seller__state")])
def test_explain_change_refuses_what_it_cannot_split(
    session: tuple[set[str], dict[str, Any], list[Any]], index: int, message: str
) -> None:
    result = session[2][index]
    assert result.is_error
    assert message in result.content[0].text
