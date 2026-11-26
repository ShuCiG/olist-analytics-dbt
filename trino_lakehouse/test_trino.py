"""Trino over the Delta tables (#363): the metric equals dbt's, and the analyst role cannot read PII columns.

Needs the Compose stack and the loaded tables:
    docker compose -f trino_lakehouse/docker-compose.yml up -d --wait
    (load: see trino_lakehouse/README.md)
    uv run pytest trino_lakehouse

The dbt value comes from the built DuckDB marts (`OLIST_DBT_DUCKDB`, default data/olist.duckdb); the test is skipped
when that file has no `marts.fct_orders`.
"""

import os
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from trino_lakehouse import client

ROOT = Path(__file__).parent.parent
DUCKDB = Path(os.environ.get("OLIST_DBT_DUCKDB", ROOT / "data" / "olist.duckdb"))

PII = {
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "orders": ["customer_id"],
}


@pytest.fixture(scope="module", autouse=True)
def stack() -> None:
    try:
        client.execute("select 1", "admin")
    except OSError:
        pytest.skip("Trino is not running (docker compose -f trino_lakehouse/docker-compose.yml up -d --wait)")


def dbt_paid_amount() -> Decimal:
    if not DUCKDB.exists():
        pytest.skip(f"no dbt DuckDB at {DUCKDB}")
    with duckdb.connect(str(DUCKDB), read_only=True) as con:
        try:
            row = con.sql("select sum(paid_amount) from marts.fct_orders").fetchone()
        except duckdb.CatalogException:
            pytest.skip("marts.fct_orders is not built in the DuckDB file")
    assert row is not None
    return Decimal(row[0])


def test_total_paid_through_trino_equals_dbt() -> None:
    """Revenue-side metric, same raw rows: Trino on Delta vs `sum(fct_orders.paid_amount)` from dbt."""
    rows = client.execute("select sum(payment_value) from delta.olist.order_payments", "analyst")
    assert Decimal(str(rows[0][0])) == dbt_paid_amount()


def test_total_paid_through_trino_equals_the_csv() -> None:
    """The same sum read straight from the dump by DuckDB (runs in CI, where no dbt build exists)."""
    csv_dir = os.environ.get("OLIST_CSV_DIR")
    if not csv_dir:
        pytest.skip("OLIST_CSV_DIR not set")
    path = Path(csv_dir) / "olist_order_payments_dataset.csv"
    total = duckdb.sql(f"select sum(payment_value::decimal(18,2)) from read_csv('{path.as_posix()}')").fetchone()
    assert total is not None
    rows = client.execute("select sum(payment_value) from delta.olist.order_payments", "analyst")
    assert Decimal(str(rows[0][0])) == Decimal(total[0])


def test_payment_type_split_adds_up_through_trino() -> None:
    by_type = client.execute(
        "select payment_type, count(*), sum(payment_value) from delta.olist.order_payments group by 1", "analyst"
    )
    total = client.execute("select count(*), sum(payment_value) from delta.olist.order_payments", "analyst")[0]
    assert sum(r[1] for r in by_type) == total[0]
    assert sum(Decimal(str(r[2])) for r in by_type) == Decimal(str(total[1]))


@pytest.mark.parametrize(("table", "column"), [(t, c) for t, cols in PII.items() for c in cols])
def test_analyst_is_denied_each_pii_column(table: str, column: str) -> None:
    with pytest.raises(client.TrinoError, match="Access Denied"):
        client.execute(f"select {column} from delta.olist.{table} limit 1", "analyst")


@pytest.mark.parametrize("table", list(PII))
def test_analyst_star_hides_pii_columns(table: str) -> None:
    """`select *` and DESCRIBE must not leak the column names' values either."""
    visible = [r[0] for r in client.execute(f"show columns from delta.olist.{table}", "analyst")]
    assert not set(PII[table]) & set(visible)


def test_analyst_still_reads_the_allowed_columns() -> None:
    rows = client.execute("select count(*), count(order_id) from delta.olist.orders", "analyst")
    assert rows[0][0] == rows[0][1] > 0


def test_admin_reads_the_pii_columns() -> None:
    rows = client.execute("select count(distinct customer_unique_id) from delta.olist.customers", "admin")
    assert rows[0][0] > 90_000
