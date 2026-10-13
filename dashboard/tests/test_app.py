"""Smoke test: every page renders against the warehouse without exceptions.

Needs built marts (CI runs it after `dbt build` on DuckDB): uv run pytest dashboard
"""

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parents[1] / "app.py"
PAGES = ["views/overview.py", "views/delivery.py", "views/customers.py"]


@pytest.mark.parametrize("page", PAGES)
def test_page_renders(page: str) -> None:
    at = AppTest.from_file(str(APP), default_timeout=60)
    at.switch_page(page)
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    # Four KPI tiles per page, all with values
    assert len(at.metric) == 4
    assert all(m.value not in ("", "nan") for m in at.metric)
