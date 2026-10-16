"""Dashboard KPIs = dbt Semantic Layer metrics, on the same warehouse.

The pages compute their KPI tiles in their own SQL (dashboard/kpis.py); the
metric definitions live in transform/models/marts/_marts__semantic_models.yml.
This test asks MetricFlow (`mf query`) for the same numbers and compares, so a
change to either side that makes them disagree fails CI.

Needs built marts, like the smoke test: uv run pytest dashboard. The warehouse
follows the dashboard settings: DuckDB by default; DASHBOARD_BACKEND=bigquery
with MARTS_DATASET=marts / airflow_marts compares against dbt target prod / airflow.
"""

import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # Streamlit-style imports

import kpis  # noqa: E402
import warehouse  # noqa: E402

TRANSFORM = Path(__file__).resolve().parents[2] / "transform"
SCRIPTS = Path(sys.executable).parent  # dbt and mf of this venv


def _dbt_target() -> str:
    if warehouse.BACKEND != "bigquery":
        return "dev"
    return {"marts": "prod", "airflow_marts": "airflow"}[warehouse.MARTS_DATASET]


def _env() -> dict[str, str]:
    # mf has no --target flag: profiles.yml takes the default target from DBT_TARGET.
    # UTF-8: mf prints emoji, which a Windows console code page can't encode.
    return {**os.environ, "DBT_TARGET": _dbt_target(), "PYTHONIOENCODING": "utf-8"}


def _run(tool: str, *args: str) -> None:
    exe = shutil.which(tool, path=str(SCRIPTS))
    assert exe, f"{tool} not found next to {sys.executable}"
    done = subprocess.run(
        [exe, *args], cwd=TRANSFORM, env=_env(), capture_output=True, text=True, encoding="utf-8"
    )
    assert done.returncode == 0, f"{tool} {' '.join(args)}\n{done.stdout[-3000:]}\n{done.stderr[-3000:]}"


@pytest.fixture(scope="module", autouse=True)
def semantic_manifest() -> None:
    # The semantic manifest carries the relation names of the target it was parsed for
    _run("dbt", "parse", "--quiet")


def mf_query(tmp_path: Path, metrics: list[str], *args: str) -> dict[str, float]:
    """One row of metric values from `mf query`."""
    out = tmp_path / "mf.csv"
    _run("mf", "query", "--metrics", ",".join(metrics), "--csv", str(out), *args)
    with out.open(newline="", encoding="utf-8") as f:
        (row,) = list(csv.DictReader(f))
    return {name: float(value) for name, value in row.items()}


def period_args(period: kpis.Period) -> list[str]:
    # metric_time = each semantic model's time dimension (purchase date for orders)
    return ["--start-time", period[0].isoformat(), "--end-time", period[1].isoformat()]


def assert_same(dashboard: pd.Series, metrics: dict[str, float], pairs: dict[str, str]) -> None:
    values = dashboard.astype(float).to_dict()
    for column, metric in pairs.items():
        assert values[column] == pytest.approx(metrics[metric], rel=1e-9), (column, metric)


@pytest.fixture(scope="module")
def period() -> kpis.Period:
    # The dashboard's default period: a sub-range, so the date filter is exercised
    return warehouse.default_period(*warehouse.date_bounds())


def test_sales_kpis(tmp_path: Path, period: kpis.Period) -> None:
    pairs = {
        "orders": "orders",
        "customers": "customers",
        "revenue": "revenue",
        "avg_order_value": "average_order_value",
    }
    metrics = mf_query(tmp_path, list(pairs.values()), *period_args(period))
    assert_same(kpis.sales(period), metrics, pairs)


def test_delivery_kpis(tmp_path: Path, period: kpis.Period) -> None:
    pairs = {
        "orders": "delivered_orders",
        "avg_days": "avg_days_to_deliver",
        "late_rate": "late_delivery_rate",
        "avg_score": "avg_review_score",
    }
    # The page covers delivered orders only; late_delivery_rate and
    # delivered_orders are defined that way, the two averages need the filter
    delivered = "{{ Dimension('order__is_delivered') }} = true"
    metrics = mf_query(tmp_path, list(pairs.values()), *period_args(period), "--where", delivered)
    assert_same(kpis.delivery(period), metrics, pairs)


def test_customer_kpis(tmp_path: Path, period: kpis.Period) -> None:
    pairs = {"customers": "customers", "avg_spend": "revenue_per_customer"}
    metrics = mf_query(tmp_path, list(pairs.values()), *period_args(period))
    assert_same(kpis.customers(period), metrics, pairs)


def test_repeat_rate_over_all_data(tmp_path: Path) -> None:
    # The page counts repeat orders within the picked period; the metric is by
    # first order (cohort). Over all the data both are "ordered more than once".
    pairs = {"customers": "new_customers", "repeat_rate": "repeat_customer_rate"}
    metrics = mf_query(tmp_path, list(pairs.values()))
    assert_same(kpis.customers(warehouse.date_bounds()), metrics, pairs)
