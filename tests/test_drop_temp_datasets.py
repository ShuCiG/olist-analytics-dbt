""".github/scripts/drop_temp_datasets.py (#343, D137): which datasets the weekly cleanup may drop."""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "drop_temp_datasets", ROOT / ".github" / "scripts" / "drop_temp_datasets.py"
)
assert _spec and _spec.loader
cleanup = importlib.util.module_from_spec(_spec)
sys.modules["drop_temp_datasets"] = cleanup  # dataclasses resolve annotations through it
_spec.loader.exec_module(cleanup)

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)


def ds(name: str, age_days: float, labels: dict[str, str] | None = None):
    return cleanup.DatasetInfo(name, NOW - timedelta(days=age_days), labels or {})


@pytest.mark.parametrize(
    "name",
    [
        "raw",
        "raw_staging",
        "staging",
        "intermediate",
        "marts",
        "seeds",
        "snapshots",
        "elementary",
        "airflow_raw",
        "airflow_raw_staging",
        "airflow_marts",
        "airflow_elementary",
        "airflow",
        "analytics",
        "dr",
        "scale",
        "ci_pr_",
        "ci_pr_x_marts",
        "dr254",
        "drop_me_raw",
        "scaled_marts",
    ],
)
def test_long_lived_and_unknown_names_are_never_candidates(name: str) -> None:
    assert cleanup.min_age_days(name) is None
    p = cleanup.plan([ds(name, 1000)], NOW)
    assert p.drop == [] and p.keep == []


@pytest.mark.parametrize(
    ("name", "days"),
    [
        ("dr254_raw", 8),
        ("dr254b_marts", 8),
        ("scale10x_raw", 8),
        ("scale10x", 8),
        ("ci_pr_373_45798_marts", 1),
        ("ci_pr_373_45798", 1),
    ],
)
def test_throwaway_names_have_their_minimum_age(name: str, days: int) -> None:
    assert cleanup.min_age_days(name) == days


def test_young_dataset_waits_out_the_cooldown() -> None:
    # dr254_raw was created 11-19 13:13; BigQuery refused the drop until 11-26 13:14 (7 days)
    p = cleanup.plan([ds("dr254_raw", 7), ds("dr254b_marts", 8.5)], NOW)
    assert [d.dataset_id for d in p.drop] == ["dr254b_marts"]
    assert [(d.dataset_id, why) for d, why in p.keep] == [("dr254_raw", "7 d old, drops after 8 d")]


def test_label_keep_protects_an_old_dataset() -> None:
    p = cleanup.plan([ds("scale10x_marts", 100, {"keep": "true"})], NOW)
    assert p.drop == []
    assert p.keep[0][1] == "label keep"


def test_leftover_ci_dataset_goes_after_a_day() -> None:
    p = cleanup.plan([ds("ci_pr_373_1_marts", 0.5), ds("ci_pr_373_2_marts", 2)], NOW)
    assert [d.dataset_id for d in p.drop] == ["ci_pr_373_2_marts"]


def test_report_lists_drops_failures_and_kept() -> None:
    p = cleanup.plan([ds("dr254b_marts", 9), ds("dr254_raw", 3)], NOW)
    text = cleanup.report(p, True, {"dr254b_marts": "403 denied"}, NOW)
    assert "Dropped: 0" in text and "`dr254b_marts`" in text and "FAILED: 403 denied" in text
    assert "`dr254_raw`: 3 d old, drops after 8 d" in text
    assert cleanup.report(p, False, {}, NOW).startswith("## Throwaway BigQuery datasets (dry run")


def test_rules_stay_inside_the_terraform_prefixes() -> None:
    # janitor.tf: the account may delete dr*, scale*, ci_pr_* only; a rule outside them would fail at GCP
    tf = (ROOT / "infra" / "janitor.tf").read_text(encoding="utf-8")
    for name in ("dr254_raw", "scale10x_raw", "ci_pr_1_2_marts"):
        assert any(p.match(name) for p, _ in cleanup.RULES)
    for prefix in ('"dr"', '"scale"', '"ci_pr_"'):
        assert prefix in tf
