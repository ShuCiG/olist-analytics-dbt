"""Elementary settings that keep row-level data out of its public report (Pages)."""

from pathlib import Path

import yaml

_PROJECT = Path(__file__).resolve().parents[1] / "transform" / "dbt_project.yml"


def test_no_failing_row_samples() -> None:
    project_vars = yaml.safe_load(_PROJECT.read_text(encoding="utf-8"))["vars"]
    assert project_vars.get("test_sample_row_count") == 0
