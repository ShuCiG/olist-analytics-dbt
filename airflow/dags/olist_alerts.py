"""Email reports for the olist ELT DAGs (D28): run failed, fresh data, stale data.

- Recipients: Airflow Variable `olist_alert_emails` (comma-separated, edit in the UI
  under Admin → Variables; no redeploy).
- Stale threshold: Variable `olist_stale_after_hours` (default 24 = the olist
  source's `warn_after` in sources.yml).
- Transport: connection `smtp_default`. docker-compose points it at Mailpit (UI on
  :8025); a real SMTP server goes in airflow/.env (`SMTP_CONN`, see .env.example).

One email per run at most: failures come from the DAG's `on_failure_callback` (not
per task: a dbt failure fails the producer and the failed node's sensors), with the
failed task, its error and log tail; the rest from the `report` task.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from urllib.parse import quote

from airflow.configuration import conf
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.sdk import Variable

# Repo root (mounted): alerting/dbt_summary.py is shared with the CI failure email
sys.path.insert(0, "/opt/project")
from alerting import dbt_summary, email_html  # noqa: E402

RECIPIENTS_VAR = "olist_alert_emails"
DEFAULT_RECIPIENTS = "data-team@olist.local"  # caught by Mailpit
STALE_AFTER_VAR = "olist_stale_after_hours"
DEFAULT_STALE_AFTER_HOURS = 24
UI_URL = "http://localhost:8080"
LOG_TAIL_LINES = 15
PRODUCER = "dbt.dbt_producer_watcher"
# Built by airflow-init (`dbt parse`): test descriptions for the emails
DBT_MANIFEST = "/opt/project/transform/target_airflow/manifest.json"
# Log records that are parsing / runner chatter, not the task's own output
LOG_NOISE_LOGGERS = ("airflow.dag_processing", "cosmos.converter", "cosmos.airflow", "cosmos.dbt.graph")
LOG_NOISE_EVENTS = ("::", "Worker startup parse complete", "Tmp dir root location", "Output:")
ANSI = re.compile(r"\x1b\[[0-9;]*m")  # dbt colors its output


def _task_log(dag_id: str, run_id: str, task_id: str, try_number: int) -> tuple[str, list[str]]:
    """(exception, output lines before it) from a task's log file.

    Logs are JSON lines in the shared logs volume, readable from the dag-processor
    that runs the DAG callback.
    """
    path = (
        Path(conf.get("logging", "base_log_folder"))
        / f"dag_id={dag_id}"
        / f"run_id={run_id}"
        / f"task_id={task_id}"
        / f"attempt={try_number}.log"
    )
    if not path.is_file():
        return f"(no log file at {path})", []
    output: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if record.get("error_detail"):
            detail = record["error_detail"][-1]
            return f"{detail.get('exc_type')}: {detail.get('exc_value')}", output
        event = ANSI.sub("", str(record.get("event", "")))
        if (
            not event.strip()
            or event.startswith(LOG_NOISE_EVENTS)
            or record.get("logger", "").startswith(LOG_NOISE_LOGGERS)
        ):
            continue
        output.append(event[:300])
    return "(no exception in the log)", output


def on_dag_failure(context) -> None:
    """DAG-level failure email: what failed, the error and the relevant log lines.

    `ti` is the task Airflow names as the cause (the callback's `last_ti`). For a dbt
    task that is often a symptom (a test sensor downstream of the broken model), so
    any dbt failure reports the producer's `dbt build` summary instead: it lists
    every failed node with dbt's own message, data tests in business words.
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    run_url = f"{UI_URL}/dags/{dag_id}/runs/{quote(run_id, safe='')}"
    ti = context.get("ti")
    rows: dict[str, object] = {"DAG": dag_id, "Run": run_id}
    error, body = "(unknown: no failed task in the callback context)", ""
    what = "?"
    links = [("Open the run", run_url)]
    if ti is not None:
        rows["Failed task (as reported)"] = ti.task_id
        rows["Attempt"] = ti.try_number
        what = ti.task_id
        log_task = PRODUCER if ti.task_id.startswith("dbt.") else ti.task_id
        error, output = _task_log(dag_id, run_id, log_task, 1 if log_task == PRODUCER else ti.try_number)
        if output:
            body = email_html.heading("Last log lines") + email_html.pre(
                "\n".join(output[-LOG_TAIL_LINES:]), "FAILED"
            )
        if log_task == PRODUCER:
            done, block = dbt_summary.summary(output)
            if done:
                what, error = "dbt build", done
                # Totals are already in the intro
                body = dbt_summary.to_html("", block, dbt_summary.test_docs(DBT_MANIFEST))
            if "ERROR=0" in done and ti.task_id != PRODUCER:
                # dbt passed, Airflow failed the task: e.g. a sensor's stale executor
                # event after a fast deferral, or a task killed on a restart
                what = f"{ti.task_id} (dbt passed)"
                error = f"dbt build passed ({done}); Airflow marked {ti.task_id} as failed"
                body = (
                    email_html.paragraph(
                        "An Airflow-side failure, not a data one: every dbt model and"
                        " test finished. See the task's log for why Airflow failed it."
                    )
                    + body
                )
                links.insert(0, (f"Log of {ti.task_id}", f"{run_url}/tasks/{ti.task_id}"))
        links.insert(0, (f"Log of {log_task}", f"{run_url}/tasks/{log_task}"))
    params = {k: v for k, v in (context.get("params") or {}).items() if v not in (None, False)}
    rows["Params"] = json.dumps(params) if params else "none (scheduled-style run)"
    rows["Reason"] = context.get("reason")

    errors = re.search(r"ERROR=\d+", error)
    count = f" ({errors[0]})" if errors and errors[0] != "ERROR=0" else ""
    title = f"{what} failed{count}"
    intro = (
        f"Run <b>{escape(run_id)}</b> of <b>{escape(dag_id)}</b> stopped at "
        f"<b>{escape(what)}</b>:<br><code>{escape(error)}</code>"
    )
    subject = f"[{dag_id}] FAILED: {what}{count} in run {run_id}"
    _send(
        subject,
        email_html.page(
            "FAILED",
            title,
            intro,
            body,
            rows,
            links,
            footer=f"{dag_id} · local Airflow · re-run with a new manual run (until), not Clear:"
            " airflow/README.md",
        ),
    )
    print(f"email: FAILED {subject} | {error}")


def _send(subject: str, html: str) -> None:
    with SmtpHook() as smtp:
        smtp.send_email_smtp(
            to=Variable.get(RECIPIENTS_VAR, default=DEFAULT_RECIPIENTS),
            subject=subject,
            html_content=html,
        )


def report(ti, dag_run, warehouse: str) -> dict[str, object]:
    """Email the run's outcome: fresh data, or a stale-data warning about once a day.

    Runs after dbt succeeded or was skipped (trigger rule `none_failed`). Reads the
    feed status that `pipeline.py` prints as dlt_ingest's last line (XCom).
    Returns what it decided, for the task's XCom / log.
    """
    status = json.loads(ti.xcom_pull(task_ids="dlt_ingest"))
    dbt_built = bool(ti.xcom_pull(task_ids="has_new_orders"))
    # dbt's totals + warnings from the producer's log of this run (same worker volume)
    dbt_done, dbt_block = "", []
    if dbt_built:
        _, output = _task_log(dag_run.dag_id, dag_run.run_id, PRODUCER, 1)
        dbt_done, dbt_block = dbt_summary.summary(output)
    warnings = re.search(r"WARN=(\d+)", dbt_done)
    n_warn = int(warnings[1]) if warnings else 0
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
        "Orders loaded this run": f"{loaded:,}",
        "Orders in raw": f"{status['orders_total']:,}",
        "Latest order (purchase time)": status["last_order_at"],
        "Last new orders loaded at (UTC)": (last_loaded_at or "")[:19].replace("T", " "),
        "Hours since new orders": round(age_h, 1),
        "dbt build": (dbt_done.removeprefix("Done. ") or "success") if dbt_built else "skipped (nothing new)",
    }
    prefix = f"[olist_elt_{warehouse}]"
    month = (status["last_order_at"] or "")[:7]
    body = ""
    if loaded or dbt_built:
        kind = "OK"
        title = f"+{loaded:,} orders · data up to {month}"
        subject = f"{prefix} OK: +{loaded} orders, data up to {month}" + (
            f", {n_warn} dbt warning{'s' if n_warn > 1 else ''}" if n_warn else ""
        )
        intro = "Fresh data loaded and the marts rebuilt; the dashboard shows it within the hour."
        if stale:
            intro += f" Note: before this run there were no new orders for {age_h:.0f} h."
        if dbt_block:
            # Data-quality tests with severity warn: nothing fails, so this is where they show
            body = dbt_summary.to_html(dbt_done, dbt_block, dbt_summary.test_docs(DBT_MANIFEST))
    elif remind:
        kind = "STALE"
        title = f"No new orders for {age_h:.0f} h · data up to {month}"
        subject = f"{prefix} STALE: no new orders for {age_h:.0f} h (data up to {month})"
        intro = (
            f"The feed brought no new orders for more than <b>{round(stale_after, 1):g} h</b>. Reports "
            "and the dashboard still show data up to the latest order below. Expected once "
            "the Olist dump is used up; otherwise check the source. Repeats about once a day "
            "while stale."
        )
    else:
        kind, subject = "none", ""

    if kind != "none":
        _send(
            subject,
            email_html.page(
                kind,
                title,
                intro,
                body,
                rows,
                [("Open the run", f"{UI_URL}/dags/olist_elt_{warehouse}/runs/{quote(dag_run.run_id, safe='')}")],
                footer=f"olist_elt_{warehouse} · local Airflow · settings: Admin → Variables",
            ),
        )
    print(f"email: {kind} {subject}".rstrip())
    return {"email": kind, "hours_since_new_orders": round(age_h, 1)}
