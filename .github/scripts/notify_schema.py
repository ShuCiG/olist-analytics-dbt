"""Email the source columns a prod load added to raw or found missing from the source
(job `notify-schema` in ci-cd.yml, D61).

The deploy doesn't fail for them: dlt stored new columns in raw, staging selects its
columns by name, so prod marts are unchanged; a removed column gets nulls, which pass
every test but a NOT NULL one. The email asks a person to decide.

Env:
    NEW_COLUMNS   `new_columns` of the feed status pipeline.py printed, JSON
                  ({"table": ["column", ...]}), via the deploy job's output
    REMOVED_COLUMNS  `removed_columns` of the same status, same shape
    SMTP_CONN, ALERT_EMAILS, GITHUB_* as in notify_failure.py
"""

import json
import os
import sys
from pathlib import Path

# Repo root: alerting/ is shared with the Airflow alert emails
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from alerting import schema_changes  # noqa: E402
from notify_failure import config_error, send  # noqa: E402  (same folder)


def main() -> None:
    to = [a.strip() for a in os.environ.get("ALERT_EMAILS", "").split(",") if a.strip()]
    if error := config_error(to, os.environ.get("SMTP_CONN", "")):
        print(f"::error::No schema email sent: {error}")
        sys.exit(1)
    repo = os.environ["GITHUB_REPOSITORY"]
    run_url = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{repo}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    built = schema_changes.email(
        json.loads(os.environ.get("NEW_COLUMNS") or "{}"),
        json.loads(os.environ.get("REMOVED_COLUMNS") or "{}"),
        prefix="[olist prod]",
        where="BigQuery prod, dataset raw",
        links=[("Open the run", run_url)],
        footer=f"{repo} · GitHub Actions · recipients: repo variable ALERT_EMAILS",
    )
    if built is None:
        print("No source column changes: no email")
        return
    subject, html = built
    send(os.environ["SMTP_CONN"], to, subject, html)
    print(f"Sent: {subject}")


if __name__ == "__main__":
    main()
