"""The static dashboard publishes aggregates only, with attribution, and its queries work.

KPI = metric lives in test_metrics.py, next to the Streamlit comparisons. Needs built
marts on DuckDB, like the smoke test: uv run pytest dashboard
"""

import json
import re
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import export_site
import warehouse

SOURCE = export_site.HERE

# An exact list, not a ban list: a new published column has to be added here on purpose
PUBLISHED_COLUMNS = {
    "kpis": {
        "period_id", "period_label", "orders", "customers", "revenue", "avg_order_value",
        "delivered_orders", "avg_days_to_deliver", "late_delivery_rate", "avg_review_score",
        "new_customers", "retention_m1", "retention_m3", "ltv_3m",
    },
    "monthly": {"period_id", "yr", "mo", "orders", "revenue", "avg_days_to_deliver", "late_delivery_rate"},
    "categories": {"period_id", "category", "revenue", "orders"},
    "payments": {"period_id", "method", "amount", "orders", "share"},
    "states": {"period_id", "state", "orders", "late_delivery_rate"},
    "review_scores": {"period_id", "delivery", "review_score", "orders", "share"},
    "retention": {"period_id", "months_since_first_order", "customers", "retention_rate", "value_per_customer"},
}


def read(site: Path, name: str) -> pd.DataFrame:
    return duckdb.sql(f"select * from '{(site / 'data' / f'{name}.parquet').as_posix()}'").df()


def test_publishes_exactly_the_listed_columns(static_site: Path) -> None:
    files = {p.stem for p in (static_site / "data").glob("*.parquet")}
    assert files == set(PUBLISHED_COLUMNS)
    for name, columns in PUBLISHED_COLUMNS.items():
        assert set(read(static_site, name).columns) == columns, name


def test_every_row_covers_a_group(static_site: Path) -> None:
    for name, column in export_site.GROUP_SIZE.items():
        assert read(static_site, name)[column].min() >= export_site.MIN_GROUP, name


def test_refuses_to_publish_a_small_group(monkeypatch: pytest.MonkeyPatch) -> None:
    one_order = pd.DataFrame({"yr": [2017], "mo": [1], "orders": [1]})
    monkeypatch.setattr(export_site, "monthly", lambda period: one_order)
    with pytest.raises(SystemExit, match="would not be an aggregate"):
        export_site.tables()


def test_never_reads_bigquery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(warehouse, "BACKEND", "bigquery")
    with pytest.raises(SystemExit, match="DuckDB only"):
        export_site.tables()


def test_data_stays_small(static_site: Path) -> None:
    assert sum(p.stat().st_size for p in (static_site / "data").iterdir()) < 100_000


def test_page_queries_return_rows_for_every_period(static_site: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The same queries.json the page runs in DuckDB-WASM, where files sit at the root
    monkeypatch.chdir(static_site / "data")
    queries = json.loads((static_site / "queries.json").read_text(encoding="utf-8"))
    tables = {name for sql in queries.values() for name in re.findall(r"'(\w+)\.parquet'", sql)}
    assert tables == set(PUBLISHED_COLUMNS)
    page = (SOURCE / "page.js").read_text(encoding="utf-8")
    match = re.search(r"const TABLES = (\[[^\]]*\])", page)
    assert match and set(json.loads(match.group(1))) == tables
    con = duckdb.connect()
    for period_id in export_site.PERIODS:
        for name, sql in queries.items():
            assert con.execute(sql, [period_id]).fetchall(), (name, period_id)


def test_page_offers_the_exported_periods() -> None:
    page = (SOURCE / "page.js").read_text(encoding="utf-8")
    for period_id, (label, _) in export_site.PERIODS.items():
        assert f'["{period_id}", "{label}"]' in page


def test_attribution_and_licence_on_the_page_and_next_to_the_data(static_site: Path) -> None:
    page = (static_site / "index.html").read_text(encoding="utf-8")
    notice = (static_site / "data" / "NOTICE.txt").read_text(encoding="utf-8")
    for text in (page, notice):
        assert "Brazilian E-Commerce Public Dataset by Olist" in text
        assert "kaggle.com/datasets/olistbr/brazilian-ecommerce" in text
        assert "CC BY-NC-SA 4.0" in text
        assert "creativecommons.org/licenses/by-nc-sa/4.0/" in text
        assert "non-commercial use" in text


def test_query_engine_pinned_to_an_exact_version() -> None:
    page = (SOURCE / "page.js").read_text(encoding="utf-8")
    imports = re.findall(r'^import .* from "([^"]+)";', page, re.M)
    assert len(imports) == 1
    assert re.fullmatch(r"https://cdn\.jsdelivr\.net/npm/@duckdb/duckdb-wasm@\d+\.\d+\.\d+/\+esm", imports[0])
