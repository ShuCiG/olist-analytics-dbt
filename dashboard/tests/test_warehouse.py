"""BigQuery queries from the public app are capped: a runaway scan fails, it isn't billed."""

import datetime as dt
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

DASHBOARD = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DASHBOARD))  # Streamlit-style imports

import warehouse  # noqa: E402


class FakeJob:
    def to_dataframe(self, **_: Any) -> pd.DataFrame:
        return pd.DataFrame({"n": [1]})


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def query(self, sql: str, job_config: Any = None) -> FakeJob:
        self.calls.append((sql, job_config))
        return FakeJob()


def test_bigquery_jobs_have_byte_cap_and_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeClient()
    monkeypatch.setattr(warehouse, "BACKEND", "bigquery")
    monkeypatch.setattr(warehouse, "_client", lambda: fake)
    warehouse.query.clear()
    warehouse.query("select 1 as n")
    warehouse.query.clear()

    [(_, config)] = fake.calls
    assert config.maximum_bytes_billed == warehouse.MAX_BYTES_BILLED
    assert int(config.job_timeout_ms) == warehouse.QUERY_TIMEOUT_MS  # the API field is a string


D = dt.date


def test_default_period_dump_only_keeps_full_months() -> None:
    assert warehouse.default_period(D(2016, 9, 4), D(2018, 8, 29)) == (D(2017, 1, 1), D(2018, 8, 29))
    assert warehouse.default_period(D(2016, 9, 4), D(2018, 10, 17)) == (D(2017, 1, 1), D(2018, 8, 31))


def test_default_period_with_generated_months_ends_at_last_full_month() -> None:
    # Mid-month: the month in progress is left out
    assert warehouse.default_period(D(2016, 9, 4), D(2019, 3, 12), True) == (D(2017, 1, 1), D(2019, 2, 28))
    # The last day closes its month: kept
    assert warehouse.default_period(D(2016, 9, 4), D(2019, 2, 28), True) == (D(2017, 1, 1), D(2019, 2, 28))
    # Year boundary
    assert warehouse.default_period(D(2016, 9, 4), D(2019, 1, 5), True) == (D(2017, 1, 1), D(2018, 12, 31))
