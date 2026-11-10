"""Missed weekly engine runs (job `weekly-runs` in drift.yml, D95).

Cloud Scheduler dispatches databricks.yml and snowflake.yml every Monday
(infra/scheduler.tf). A dispatch can fail without any GitHub run to send the failure
email: an expired token, a disabled job, a workflow error. This fails when a weekly
workflow has no successful run on main within MAX_AGE, whatever started it
(the dispatch or a manual run). A failed run sends its own email (notify-failure).

Usage: python .github/scripts/weekly_runs.py
Env:   GH_TOKEN, GITHUB_REPOSITORY (set in Actions); standard library only
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import UTC, datetime, timedelta

WORKFLOWS = ["databricks.yml", "snowflake.yml"]
# A week plus 12 h: on Monday last week's run still counts until 18:17 UTC (the
# drift check itself starts hours late), a missed Monday fails on Tuesday
MAX_AGE = timedelta(days=7, hours=12)


def last_success(repo: str, workflow: str, token: str) -> datetime | None:
    """Start time of the latest successful run on main, None if there is none."""
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs?branch=main&status=success&per_page=1"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        runs = json.load(resp)["workflow_runs"]
    return datetime.fromisoformat(runs[0]["created_at"].replace("Z", "+00:00")) if runs else None


def stale(last: dict[str, datetime | None], now: datetime, max_age: timedelta = MAX_AGE) -> list[str]:
    """One line per workflow without a success in the window."""
    limit = now - max_age
    out = []
    for workflow, at in sorted(last.items()):
        if at is None:
            out.append(f"{workflow}: no successful run on main")
        elif at < limit:
            out.append(f"{workflow}: last successful run {at:%Y-%m-%d %H:%M} UTC, more than {max_age} ago")
    return out


def main() -> None:
    repo, token = os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"]
    last = {w: last_success(repo, w, token) for w in WORKFLOWS}
    for workflow, at in sorted(last.items()):
        print(f"{workflow}: last success {at:%Y-%m-%d %H:%M} UTC" if at else f"{workflow}: no success")
    found = stale(last, datetime.now(UTC))
    if found:
        for line in found:
            print(f"::error::{line}")
        print("Weekly dispatch missed: check Cloud Scheduler jobs and Workflows executions (docs/RUNNING.md, Weekly runs)")
        sys.exit(1)
    print(f"Every weekly workflow succeeded within {MAX_AGE}")


if __name__ == "__main__":
    main()
