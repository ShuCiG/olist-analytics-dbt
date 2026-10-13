"""Email reports for the olist ELT DAGs (D28): run failed, fresh data, stale data.

- Recipients: Airflow Variable `olist_alert_emails` (comma-separated, edit in the UI
  under Admin → Variables; no redeploy).
- Stale threshold: Variable `olist_stale_after_hours` (default 24 = the olist
  source's `warn_after` in sources.yml).
- Transport: connection `smtp_default`. docker-compose points it at Mailpit (UI on
  :8025); a real SMTP server goes in airflow/.env (`SMTP_CONN`, see .env.example).

One email per run at most: failures come from the DAG's `on_failure_callback` (not
per task: a failed producer fails ~50 sensors), the rest from the `report` task.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from html import escape
from urllib.parse import quote

from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import Variable

RECIPIENTS_VAR = "olist_alert_emails"
DEFAULT_RECIPIENTS = "data-team@olist.local"  # caught by Mailpit
STALE_AFTER_VAR = "olist_stale_after_hours"
DEFAULT_STALE_AFTER_HOURS = 24
UI_URL = "http://localhost:8080"


def failure_notifier() -> SmtpNotifier:
    """DAG-level failure email. A new instance per DAG: notifiers render in place."""
    run_url = f"{UI_URL}/dags/{{{{ dag.dag_id }}}}/runs/{{{{ dag_run.run_id | urlencode }}}}"
    return SmtpNotifier(
        to=f"{{{{ var.value.get('{RECIPIENTS_VAR}', '{DEFAULT_RECIPIENTS}') }}}}",
        subject="[{{ dag.dag_id }}] FAILED: run {{ dag_run.run_id }}",
        html_content=(
            "<p>Run <b>{{ dag_run.run_id }}</b> of <b>{{ dag.dag_id }}</b> failed"
            "{% if reason %}: {{ reason }}{% endif %}.</p>"
            f'<p><a href="{run_url}">Open the run</a> to see which task failed and its log. '
            "Re-run with a new manual run (with <code>until</code> after the last month), "
            "not with Clear: see airflow/README.md.</p>"
        ),
    )


def _send(subject: str, rows: dict[str, object], note: str) -> None:
    table = "".join(
        f"<tr><td>{escape(k)}</td><td><b>{escape(str(v))}</b></td></tr>" for k, v in rows.items()
    )
    with SmtpHook() as smtp:
        smtp.send_email_smtp(
            to=Variable.get(RECIPIENTS_VAR, default=DEFAULT_RECIPIENTS),
            subject=subject,
            html_content=f"<p>{note}</p><table>{table}</table>",
        )


def report(ti, dag_run, warehouse: str) -> dict[str, object]:
    """Email the run's outcome: fresh data, or a stale-data warning about once a day.

    Runs after dbt succeeded or was skipped (trigger rule `none_failed`). Reads the
    feed status that `pipeline.py` prints as dlt_ingest's last line (XCom).
    Returns what it decided, for the task's XCom / log.
    """
    status = json.loads(ti.xcom_pull(task_ids="dlt_ingest"))
    dbt_built = bool(ti.xcom_pull(task_ids="has_new_orders"))
    loaded = status["orders_loaded"]

    last_loaded_at = status["last_loaded_at"]
    age_h = (
        (datetime.now(UTC) - datetime.fromisoformat(last_loaded_at)).total_seconds() / 3600
        if last_loaded_at
        else float("inf")
    )
    stale_after = float(Variable.get(STALE_AFTER_VAR, default=DEFAULT_STALE_AFTER_HOURS))
    stale = age_h >= stale_after
    # Hourly runs: the first run past the threshold, then one per 24 h (stateless)
    remind = stale and (age_h == float("inf") or (age_h - stale_after) % 24 < 1)

    rows: dict[str, object] = {
        "Warehouse": warehouse,
        "Run": dag_run.run_id,
        "Orders loaded this run": loaded,
        "Orders in raw": status["orders_total"],
        "Latest order (purchase time)": status["last_order_at"],
        "Last new orders loaded at (UTC)": last_loaded_at,
        "Hours since new orders": round(age_h, 1),
        "dbt build": "success" if dbt_built else "skipped (nothing new)",
    }
    prefix = f"[olist_elt_{warehouse}]"
    month = (status["last_order_at"] or "")[:7]
    if loaded or dbt_built:
        kind = "OK"
        subject = f"{prefix} OK: +{loaded} orders, data up to {month}"
        note = "Fresh data loaded and the marts rebuilt."
        if stale:
            note += f" Note: no new orders for {age_h:.0f} h."
    elif remind:
        kind = "STALE"
        subject = f"{prefix} STALE: no new orders for {age_h:.0f} h (data up to {month})"
        note = (
            f"The feed brought no new orders for more than {stale_after:g} h. "
            "Expected once the Olist dump is used up; otherwise check the source. "
            "Repeats about once a day while stale."
        )
    else:
        kind = "none"
        subject = note = ""

    if kind != "none":
        note += f' <a href="{UI_URL}/dags/olist_elt_{warehouse}/runs/{quote(dag_run.run_id)}">Run in Airflow</a>'
        _send(subject, rows, note)
    print(f"email: {kind} {subject}".rstrip())
    return {"email": kind, "hours_since_new_orders": round(age_h, 1)}
