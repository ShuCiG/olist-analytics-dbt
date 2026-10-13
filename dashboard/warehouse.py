"""Query layer: the same SQL runs on DuckDB (local) and BigQuery (prod marts).

Backend comes from env `DASHBOARD_BACKEND` (`duckdb` default, or `bigquery`).
On Streamlit Community Cloud, top-level secrets are exported as env vars, so the
deployed app sets it in its secrets.

SQL in the pages sticks to what both engines accept (`extract`, `date '...'`
literals, `count(distinct)`, plain joins), like the dbt models do.
"""

import datetime as dt
import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
import streamlit as st

BACKEND = os.environ.get("DASHBOARD_BACKEND", "duckdb")
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
# Same default and env var as transform/profiles.yml, resolved from the repo root
DUCKDB_PATH = os.environ.get(
    "DBT_DUCKDB_PATH", str(Path(__file__).resolve().parents[1] / "data" / "olist.duckdb")
)

# Default period: full months only. The dump starts with a few test-like orders
# (2016) and ends mid-way through Sep 2018, which reads as fake spikes and drops
DEFAULT_PERIOD = (dt.date(2017, 1, 1), dt.date(2018, 8, 31))

# Statuses that never turned into a sale: left out of revenue KPIs
LOST_STATUSES = ("canceled", "unavailable")


def table(name: str) -> str:
    """Fully qualified mart relation (dbt puts marts in schema/dataset `marts`)."""
    if BACKEND == "bigquery":
        return f"`{GCP_PROJECT}.marts.{name}`"
    return f"marts.{name}"


def _bigquery_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    # Deployed app: key in st.secrets. Locally: Application Default Credentials
    # (GOOGLE_APPLICATION_CREDENTIALS), as for dbt.
    try:
        info = dict(st.secrets["gcp_service_account"])
    except (KeyError, FileNotFoundError):
        return bigquery.Client(project=GCP_PROJECT, location="EU")
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=GCP_PROJECT, location="EU", credentials=credentials)


@st.cache_resource
def _client():
    return _bigquery_client()


def _to_plain_types(df: pd.DataFrame) -> pd.DataFrame:
    # BigQuery NUMERIC arrives as Decimal objects; charts and formatting want floats
    for col in df.columns:
        if df[col].dtype == object and bool(df[col].map(lambda v: isinstance(v, Decimal)).any()):
            df[col] = pd.to_numeric(df[col])
    return df


@st.cache_data(ttl="1h", show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    """Run a query, cached for an hour (BigQuery bills per scan)."""
    if BACKEND == "bigquery":
        df = _client().query(sql).to_dataframe()
    else:
        import duckdb

        # Short-lived read-only connection: doesn't hold the file lock that a
        # concurrent `dbt build` or dlt load needs
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            df = con.sql(sql).df()
    return _to_plain_types(df)


def date_bounds() -> tuple[dt.date, dt.date]:
    # As ISO strings: DuckDB returns datetime64, BigQuery a `dbdate` extension type
    df = query(f"""
        select cast(min(purchase_date) as string) as lo, cast(max(purchase_date) as string) as hi
        from {table('fct_orders')}
    """)
    return dt.date.fromisoformat(df["lo"].iloc[0]), dt.date.fromisoformat(df["hi"].iloc[0])


def period() -> tuple[dt.date, dt.date]:
    """Purchase-date range picked in the sidebar (app.py)."""
    return st.session_state["period"]


def period_filter(column: str = "purchase_date") -> str:
    # Values are datetime.date objects from st.date_input, never free text
    lo, hi = period()
    return f"{column} between date '{lo.isoformat()}' and date '{hi.isoformat()}'"


def monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Turn `yr`/`mo` columns (portable `extract`) into a `month` timestamp."""
    df = df.copy()
    yyyymm = df.pop("yr").astype(int) * 100 + df.pop("mo").astype(int)
    df["month"] = pd.to_datetime(yyyymm.astype(str), format="%Y%m")
    return df.sort_values("month")
