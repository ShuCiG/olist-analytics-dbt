"""The static dashboard built once per test session, from the built marts on DuckDB."""

import sys
from pathlib import Path

import pytest

DASHBOARD = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(DASHBOARD), str(DASHBOARD / "static_site")]  # Streamlit-style imports

import export_site  # noqa: E402
import warehouse  # noqa: E402


@pytest.fixture(scope="session")
def static_site(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if warehouse.BACKEND != "duckdb":
        pytest.skip("The static dashboard is built from DuckDB only")
    out = tmp_path_factory.mktemp("static_site")
    export_site.build(out)
    return out
