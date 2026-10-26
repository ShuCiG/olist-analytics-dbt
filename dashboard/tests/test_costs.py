"""The cost page (BigQuery only, D71) renders from the cost mart's columns.

No warehouse: `warehouse.query` is replaced by canned frames, so this runs without
BigQuery and without built marts.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

DASHBOARD = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DASHBOARD))  # Streamlit-style imports

import warehouse  # noqa: E402

PAGE = DASHBOARD / "views" / "costs.py"


def fake_query(jobs: int):
    def query(sql: str) -> pd.DataFrame:
        assert warehouse.costs_table() in sql
        if "cache_rate" in sql:
            return pd.DataFrame({"jobs": [jobs], "billed": [3 * 1024**3 if jobs else 0], "commands": [4 if jobs else 0], "cache_rate": [0.25 if jobs else 0.0]})
        if "group by 1" in sql and "dbt_target," in sql:
            return pd.DataFrame({"dbt_target": ["prod", "airflow"], "jobs": [200, 40], "mib": [2500.0, 400.0]})
        if "as node" in sql:
            return pd.DataFrame({"node": ["model: fct_orders_v2", "test: unique_dim_customers"], "jobs": [3, 1], "mib": [50.0, 10.0], "slot_s": [4.2, 0.3]})
        return pd.DataFrame({
            "started_at": pd.to_datetime(["2026-10-26 07:00"]), "jobs": [120], "mib": [800.0],
            "slot_s": [60.5], "failed_jobs": [0], "dbt_invocation_id": ["a037c466"],
        })

    return query


def run_page(monkeypatch: pytest.MonkeyPatch, jobs: int) -> AppTest:
    monkeypatch.setattr(warehouse, "query", fake_query(jobs))
    at = AppTest.from_file(str(PAGE), default_timeout=30)
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    return at


def test_cost_page_renders(monkeypatch: pytest.MonkeyPatch) -> None:
    at = run_page(monkeypatch, jobs=240)
    assert [m.value for m in at.metric] == ["3.00 GiB", "240", "4", "25%"]
    assert len(at.dataframe) == 1


def test_cost_page_before_the_first_deploy(monkeypatch: pytest.MonkeyPatch) -> None:
    at = run_page(monkeypatch, jobs=0)
    assert [m.value for m in at.metric] == ["0.00 GiB", "0", "0", "0%"]
    assert len(at.info) == 1 and not at.dataframe


def test_cost_page_only_on_bigquery() -> None:
    app = (DASHBOARD / "app.py").read_text(encoding="utf-8")
    assert 'if warehouse.BACKEND == "bigquery":\n    pages.append(st.Page("views/costs.py"' in app
