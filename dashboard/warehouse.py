"""Query layer: the same SQL runs on DuckDB (local) and BigQuery (prod marts).

Backend comes from `DASHBOARD_BACKEND` (`duckdb` default, or `bigquery`), the
marts schema / dataset from `MARTS_DATASET` (`marts` default; `airflow_marts` =
the marts fed hourly by the local Airflow replay): env vars locally, top-level
secrets on Streamlit Community Cloud.

SQL in the pages sticks to what both engines accept (`extract`, `date '...'`
literals, `count(distinct)`, plain joins), like the dbt models do.
"""

import datetime as dt
import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
import streamlit as st


def _setting(name: str, default: str) -> str:
    # Streamlit exports secrets to env vars only once st.secrets is first read,
    # which is after this module is imported: read them directly
    if name in os.environ:
        return os.environ[name]
    try:
        return str(st.secrets.get(name, default))
    except FileNotFoundError:  # no secrets.toml (local runs, CI)
        return default


BACKEND = _setting("DASHBOARD_BACKEND", "duckdb")
GCP_PROJECT = _setting("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
MARTS_DATASET = _setting("MARTS_DATASET", "marts")
# The cost mart (fct_dbt_job_costs, D71) is built in prod only, so the cost page
# reads prod `marts` also when the order pages read the Airflow feed
COSTS_DATASET = _setting("COSTS_DATASET", "marts")
# Same default and env var as transform/profiles.yml, resolved from the repo root
DUCKDB_PATH = os.environ.get(
    "DBT_DUCKDB_PATH", str(Path(__file__).resolve().parents[1] / "data" / "olist.duckdb")
)

# Default period: full months only. The dump starts with a few test-like orders
# (2016) and ends mid-way through Sep 2018, which reads as fake spikes and drops
DEFAULT_PERIOD = (dt.date(2017, 1, 1), dt.date(2018, 8, 31))

# Every distinct period is a new BigQuery job, and the app is public. All marts
# together are ~110 MB, so a query that would scan more is a bug or abuse: it
# fails instead of being billed. Cache size bounds memory across many periods.
MAX_BYTES_BILLED = 1_000_000_000
QUERY_TIMEOUT_MS = 60_000
CACHE_ENTRIES = 256

# Statuses that never turned into a sale: left out of revenue KPIs
LOST_STATUSES = ("canceled", "unavailable")


def table(name: str) -> str:
    """Fully qualified mart relation (schema / dataset `MARTS_DATASET`)."""
    if BACKEND == "bigquery":
        return f"`{GCP_PROJECT}.{MARTS_DATASET}.{name}`"
    return f"{MARTS_DATASET}.{name}"


def costs_table() -> str:
    """The cost mart on BigQuery (dataset `COSTS_DATASET`); no DuckDB twin."""
    return f"`{GCP_PROJECT}.{COSTS_DATASET}.fct_dbt_job_costs`"


def source_label() -> str:
    """Where the numbers come from, for the sidebar caption."""
    if BACKEND != "bigquery":
        return "DuckDB (local)"
    if MARTS_DATASET == "marts":
        return "BigQuery (prod)"
    return "BigQuery (hourly Airflow feed)"


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


def _job_config():
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED, job_timeout_ms=QUERY_TIMEOUT_MS)


@st.cache_data(ttl="1h", max_entries=CACHE_ENTRIES, show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    """Run a query, cached for an hour (BigQuery bills per scan)."""
    if BACKEND == "bigquery":
        # Results are chart-sized: plain REST, no BigQuery Storage API client
        job = _client().query(sql, job_config=_job_config())
        df = job.to_dataframe(create_bqstorage_client=False)
    else:
        import duckdb

        # Short-lived read-only connection: DuckDB lets one process write or several
        # read, so the lock is held only for this query. A query while `dbt build`
        # or a dlt load writes the file fails with a lock error (and vice versa).
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


def default_period(lo: dt.date, hi: dt.date) -> tuple[dt.date, dt.date]:
    """DEFAULT_PERIOD clipped to the data; all of it while the feed is still short."""
    start, end = max(lo, DEFAULT_PERIOD[0]), min(hi, DEFAULT_PERIOD[1])
    return (start, end) if start <= end else (lo, hi)


def last_load() -> str | None:
    """When the newest order lines were loaded (`_loaded_at`, UTC), minute precision."""
    df = query(f"select cast(max(_loaded_at) as string) as ts from {table('fct_order_items')}")
    ts = df["ts"].iloc[0]
    return None if ts is None or pd.isna(ts) else str(ts)[:16]


def period() -> tuple[dt.date, dt.date]:
    """Purchase-date range picked in the sidebar (app.py)."""
    return st.session_state["period"]


def date_filter(dates: tuple[dt.date, dt.date], column: str = "purchase_date") -> str:
    # Values are datetime.date objects (st.date_input, tests), never free text
    lo, hi = dates
    return f"{column} between date '{lo.isoformat()}' and date '{hi.isoformat()}'"


def period_filter(column: str = "purchase_date") -> str:
    """`date_filter` for the period picked in the sidebar."""
    return date_filter(period(), column)


def monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Turn `yr`/`mo` columns (portable `extract`) into a `month` timestamp."""
    df = df.copy()
    yyyymm = df.pop("yr").astype(int) * 100 + df.pop("mo").astype(int)
    df["month"] = pd.to_datetime(yyyymm.astype(str), format="%Y%m")
    return df.sort_values("month")
