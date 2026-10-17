"""BigQuery queries from the public app are capped: a runaway scan fails, it isn't billed."""

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
