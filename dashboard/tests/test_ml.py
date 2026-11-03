"""The BigQuery ML page (BigQuery only, D96) renders from the ML marts' columns.

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

PAGE = DASHBOARD / "views" / "ml.py"
BACKTEST = "ml_sales_forecast_backtest"
REPEAT = "ml_repeat_purchase"


def metrics_frame() -> pd.DataFrame:
    rows = []
    for category in ["all categories", "health_beauty"]:
        for measure in ["orders", "revenue"]:
            rows.append((BACKTEST, category, measure, None, "holdout_total_ape", 0.042))
            rows.append((BACKTEST, category, measure, None, "mean_absolute_percentage_error", 9.5))
    rows += [
        (REPEAT, None, None, None, "roc_auc", 0.6512),
        (REPEAT, None, None, None, "top_decile_lift", 2.43),
        (REPEAT, None, None, None, "eval_repeat_rate", 0.021),
        (REPEAT, None, None, "first_category", "attribution", 0.031),
        (REPEAT, None, None, "customer_state", "attribution", 0.012),
    ]
    return pd.DataFrame(rows, columns=["model_name", "category_name", "measure", "feature_name", "metric_name", "metric_value"])


def fake_query(empty: bool):
    def query(sql: str) -> pd.DataFrame:
        assert "`olist-analytics-dbt-472315.marts." in sql  # prod `marts` (ML_DATASET), not MARTS_DATASET
        if "fct_ml_model_metrics" in sql:
            return metrics_frame().iloc[0:0] if empty else metrics_frame()
        if "sum(predicted_value) as revenue" in sql:
            return pd.DataFrame({"revenue": [3_150_000.0]})
        if "distinct category_name" in sql:
            return pd.DataFrame({"category_name": ["all categories", "health_beauty"]})
        if "week_start_date" in sql:
            weeks = pd.date_range("2018-05-07", periods=6, freq="W-MON")
            return pd.DataFrame({
                "week_start_date": weeks,
                "actual_value": [100.0, 110.0, 105.0, 98.0, None, None],
                "predicted_value": [None, None, 101.0, 103.0, 104.0, 106.0],
                "predicted_lower_bound": [None, None, 90.0, 91.0, 88.0, 87.0],
                "predicted_upper_bound": [None, None, 112.0, 115.0, 120.0, 125.0],
            })
        if "probability_decile" in sql and "group by" in sql:
            return pd.DataFrame({"probability_decile": range(1, 11), "repeat_rate": [0.05] + [0.02] * 9, "people": [2000] * 10})
        assert "sample = 'unlabelled'" in sql
        return pd.DataFrame({"people": [1234]})

    return query


def run_page(monkeypatch: pytest.MonkeyPatch, empty: bool) -> AppTest:
    monkeypatch.setattr(warehouse, "query", fake_query(empty))
    at = AppTest.from_file(str(PAGE), default_timeout=30)
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    return at


def test_ml_page_renders(monkeypatch: pytest.MonkeyPatch) -> None:
    at = run_page(monkeypatch, empty=False)
    assert [m.value for m in at.metric] == ["R$ 3.1 M", "4.2%", "0.65", "2.4×"]
    assert at.selectbox[0].value == "all categories"
    assert len(at.dataframe) == 1
    assert "weekly MAPE 9.5%" in at.caption[1].value


def test_ml_page_before_the_first_deploy(monkeypatch: pytest.MonkeyPatch) -> None:
    at = run_page(monkeypatch, empty=True)
    assert len(at.info) == 1 and not at.metric


def test_ml_page_only_on_bigquery() -> None:
    app = (DASHBOARD / "app.py").read_text(encoding="utf-8")
    assert 'if warehouse.BACKEND == "bigquery":\n    pages.append(st.Page("views/ml.py"' in app
