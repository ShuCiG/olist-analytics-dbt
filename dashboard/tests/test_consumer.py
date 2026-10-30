"""The dashboard reads exactly what its consumer manifest declares.

data-product/consumers/olist-dashboard.yaml is the dashboard's data usage agreement
with the Olist commerce data product: which output port, contract version and
columns it relies on. This test renders every page, records each SQL query the
dashboard sends, resolves every column to its mart table (sqlglot, with the output
contract as the schema) and compares with the manifest:
- a column the dashboard reads but the contract doesn't promise fails qualification,
- a column it reads but didn't declare, or declared but no longer reads, fails the
  comparison, so the manifest stays an accurate list of what a contract change can break.

Needs built marts on DuckDB, like the smoke test: uv run pytest dashboard
"""

import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlglot import exp, parse_one
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import traverse_scope
from streamlit.testing.v1 import AppTest

DASHBOARD = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DASHBOARD))  # Streamlit-style imports

import kpis  # noqa: E402
import warehouse  # noqa: E402

PRODUCT_DIR = DASHBOARD.parent / "data-product"
MANIFEST = PRODUCT_DIR / "consumers" / "olist-dashboard.yaml"
PAGES = ["views/overview.py", "views/delivery.py", "views/customers.py", "views/cohorts.py"]


def load(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def agreement() -> dict[str, Any]:
    return load(MANIFEST)


@pytest.fixture(scope="module")
def captured_sql() -> list[str]:
    """Every query the app sends while rendering all pages."""
    if warehouse.BACKEND != "duckdb":
        pytest.skip("SQL is resolved in the DuckDB dialect; the queries are the same on BigQuery")
    sql: list[str] = []
    original = warehouse.query

    def recording(text: str):
        sql.append(text)
        return original(text)

    # Module attributes: the app and pages import `query` from these modules at run time
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(warehouse, "query", recording)
        mp.setattr(kpis, "query", recording)
        for page in PAGES:
            at = AppTest.from_file(str(DASHBOARD / "app.py"), default_timeout=60)
            at.switch_page(page)
            at.run()
            assert not at.exception, [e.value for e in at.exception]
    assert sql
    return sql


def contract_schema(agreement: dict[str, Any]) -> dict[str, object]:
    """{schema: {table: {column: type}}} of the contract the dashboard consumes."""
    contract_id = agreement["provider"]["contractId"]
    (contract,) = [
        c for c in map(load, (PRODUCT_DIR / "contracts").glob("*.odcs.yaml")) if c["id"] == contract_id
    ]
    tables = {
        obj["physicalName"]: {p["name"]: "varchar" for p in obj["properties"]} for obj in contract["schema"]
    }
    return {warehouse.MARTS_DATASET: tables}


def columns_read(sql: str, schema: dict[str, object]) -> set[tuple[str, str]]:
    """(table, column) pairs one query reads from mart tables (CTE columns resolved away)."""
    tree = qualify(parse_one(sql, dialect="duckdb"), schema=schema, dialect="duckdb")
    used = set()
    for scope in traverse_scope(tree):
        for column in scope.columns:
            source = scope.sources.get(column.table)
            if isinstance(source, exp.Table):
                used.add((source.name, column.name))
    return used


def test_dashboard_reads_exactly_the_declared_columns(
    agreement: dict[str, Any], captured_sql: list[str]
) -> None:
    schema = contract_schema(agreement)
    read: set[tuple[str, str]] = set()
    for sql in captured_sql:
        read |= columns_read(sql, schema)  # raises on a column the contract doesn't have
    declared = {(table, col) for table, cols in agreement["uses"].items() for col in cols}
    assert read == declared, (
        f"read but not declared: {sorted(read - declared)}\n"
        f"declared but not read: {sorted(declared - read)}"
    )
