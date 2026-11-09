"""Email the SLOs whose error budget burns too fast (job `notify-slo` in ci-cd.yml, D111).

The deploy job computes the SLOs (slo_report.py) and hands the flagged ones over as a
job output; this own job holds the SMTP secret, as with notify_schema.py.

Env:
    SLO_ALERTS    JSON list of Status dicts (alerting/slo.py), the deploy job's `alerts`
    SMTP_CONN, ALERT_EMAILS, GITHUB_* as in notify_failure.py
"""

import json
import os
import sys
from pathlib import Path

# Repo root: alerting/ is shared with the Airflow alert emails
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from alerting import slo  # noqa: E402
from notify_failure import config_error, send  # noqa: E402  (same folder)

PAGES = "https://shucig.github.io/olist-analytics-dbt/slo/"


def main() -> None:
    to = [a.strip() for a in os.environ.get("ALERT_EMAILS", "").split(",") if a.strip()]
    if error := config_error(to, os.environ.get("SMTP_CONN", "")):
        print(f"::error::No SLO email sent: {error}")
        sys.exit(1)
    repo = os.environ["GITHUB_REPOSITORY"]
    run_url = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{repo}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    built = slo.email(
        json.loads(os.environ.get("SLO_ALERTS") or "[]"),
        prefix="[olist prod]",
        links=[("Open the SLO page", PAGES), ("Open the run", run_url)],
        footer=f"{repo} · GitHub Actions · recipients: repo variable ALERT_EMAILS",
    )
    if built is None:
        print("No SLO burning: no email")
        return
    subject, html = built
    send(os.environ["SMTP_CONN"], to, subject, html)
    print(f"Sent: {subject}")


if __name__ == "__main__":
    main()
