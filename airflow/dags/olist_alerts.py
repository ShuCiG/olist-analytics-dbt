"""Email reports for the olist ELT DAGs (D28): run failed, fresh data, stale data,
new source columns (D61).

- Recipients: Airflow Variable `olist_alert_emails` (comma-separated, edit in the UI
  under Admin → Variables; no redeploy).
- Stale threshold: Variable `olist_stale_after_hours` (default 24 = the olist
  source's `warn_after` in sources.yml).
- Last STALE email per DAG: Variable `olist_stale_reminded_at_<warehouse>` (set by
  the `report` task, UTC ISO time), so the reminder goes out about once a day
  whatever hours the machine runs.
- Transport: connection `smtp_default`. docker-compose points it at Mailpit (UI on
  :8025); a real SMTP server goes in airflow/.env (`SMTP_CONN`, see .env.example).

One report per run at most: failures come from the DAG's `on_failure_callback` (not
per task: a dbt failure fails the producer and the failed node's sensors), with the
failed task, its error and log tail; the rest from the `report` task. A load that
added source columns, or found raw columns missing from the source, also gets a
SCHEMA email from the `schema_report` task.

This module is the Airflow side (context, XCom, Variables, SMTP); what the emails
say is decided in alerting/airflow_emails.py (unit-tested, no Airflow imports).
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from airflow.configuration import conf
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.sdk import Variable

# Repo root (in the image): the alerting package is shared with the CI failure email
sys.path.insert(0, "/opt/project")
from alerting import airflow_emails  # noqa: E402

RECIPIENTS_VAR = "olist_alert_emails"
DEFAULT_RECIPIENTS = "data-team@olist.local"  # caught by Mailpit
STALE_AFTER_VAR = "olist_stale_after_hours"
DEFAULT_STALE_AFTER_HOURS = 24
REMINDED_AT_VAR = "olist_stale_reminded_at_{warehouse}"
# Built by airflow-init (`dbt parse`): test descriptions for the emails
DBT_MANIFEST = "/opt/project/transform/target_airflow/manifest.json"


def _log_dir() -> Path:
    """Task logs: JSON lines in the shared logs volume, readable from the
    dag-processor that runs the DAG callback."""
    return Path(conf.get("logging", "base_log_folder"))


def _send(email: airflow_emails.Email) -> None:
    with SmtpHook() as smtp:
        smtp.send_email_smtp(
            to=Variable.get(RECIPIENTS_VAR, default=DEFAULT_RECIPIENTS),
            subject=email.subject,
            html_content=email.html,
        )


def on_dag_failure(context) -> None:
    """DAG-level failure email (`on_failure_callback`): see airflow_emails.failure_email."""
    ti = context.get("ti")
    email = airflow_emails.failure_email(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        task_id=ti.task_id if ti is not None else None,
        try_number=ti.try_number if ti is not None else 0,
        params=dict(context.get("params") or {}),
        reason=context.get("reason"),
        log_dir=_log_dir(),
        manifest=DBT_MANIFEST,
    )
    _send(email)
    print(f"email: FAILED {email.subject} | {email.detail}")


def report(ti, dag_run, warehouse: str) -> dict[str, object]:
    """Email the run's outcome: fresh data, or a stale-data warning about once a day.

    Runs after dbt succeeded or was skipped (trigger rule `none_failed`). Reads the
    feed status that `pipeline.py` prints as dlt_ingest's last line (XCom).
    Returns what it decided, for the task's XCom / log.
    """
    reminded_var = REMINDED_AT_VAR.format(warehouse=warehouse)
    reminded_at = Variable.get(reminded_var, default=None)
    now = datetime.now(UTC)
    email, age_h = airflow_emails.report_email(
        warehouse=warehouse,
        run_id=dag_run.run_id,
        status=json.loads(ti.xcom_pull(task_ids="dlt_ingest")),
        dbt_built=bool(ti.xcom_pull(task_ids="has_new_orders")),
        now=now,
        stale_after=float(Variable.get(STALE_AFTER_VAR, default=DEFAULT_STALE_AFTER_HOURS)),
        log_dir=_log_dir(),
        manifest=DBT_MANIFEST,
        reminded_at=datetime.fromisoformat(reminded_at) if reminded_at else None,
    )
    if email:
        _send(email)
        if email.kind == "STALE":
            Variable.set(reminded_var, now.isoformat(), description="Last STALE email (olist_alerts.report)")
    print(f"email: {email.kind} {email.subject}" if email else "email: none")
    return {"email": email.kind if email else "none", "hours_since_new_orders": round(age_h, 1)}


def schema_report(ti, dag_run, warehouse: str) -> str:
    """Email the source columns the load added or found missing, if any (airflow_emails.schema_email)."""
    email = airflow_emails.schema_email(
        warehouse=warehouse,
        run_id=dag_run.run_id,
        status=json.loads(ti.xcom_pull(task_ids="dlt_ingest")),
    )
    if email:
        _send(email)
    print(f"email: {email.subject}" if email else "email: none (no source column changes)")
    return email.kind if email else "none"
