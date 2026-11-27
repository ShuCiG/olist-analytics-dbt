""".github/scripts/weekly_runs.py (D95): the missed-run window, no GitHub calls."""

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("weekly_runs", ROOT / ".github" / "scripts" / "weekly_runs.py")
assert _spec and _spec.loader
weekly_runs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(weekly_runs)

# Tuesday 05:00 UTC, the drift check's cron
NOW = datetime(2026, 11, 10, 5, 0, tzinfo=UTC)
MONDAY = datetime(2026, 11, 9, 6, 17, tzinfo=UTC)


def test_this_weeks_runs_pass() -> None:
    assert weekly_runs.stale({"databricks.yml": MONDAY}, NOW) == []


def test_last_weeks_run_counts_all_monday_while_the_drift_check_is_late() -> None:
    # Drift check on Monday at 13:00, before or after this week's dispatch
    assert weekly_runs.stale({"databricks.yml": MONDAY - timedelta(days=7)}, MONDAY.replace(hour=13, minute=0)) == []


def test_missed_monday_fails_on_tuesday() -> None:
    assert weekly_runs.stale({"databricks.yml": MONDAY - timedelta(days=7)}, NOW) == [
        "databricks.yml: last successful run 2026-11-02 06:17 UTC, more than 7 days, 12:00:00 ago"
    ]


def test_no_success_at_all_fails() -> None:
    assert weekly_runs.stale({"databricks.yml": None}, NOW) == ["databricks.yml: no successful run on main"]


def test_newest_success_skips_failed_and_running_runs() -> None:
    runs = [
        {"created_at": "2026-11-17T06:37:03Z", "conclusion": None},  # still running
        {"created_at": "2026-11-16T06:37:03Z", "conclusion": "success"},
        {"created_at": "2026-11-12T06:37:03Z", "conclusion": "failure"},
        {"created_at": "2026-11-09T10:05:31Z", "conclusion": "success"},
    ]
    assert weekly_runs.newest_success(runs) == datetime(2026, 11, 16, 6, 37, 3, tzinfo=UTC)
    assert weekly_runs.newest_success(runs[2:3]) is None
    assert weekly_runs.newest_success([]) is None
