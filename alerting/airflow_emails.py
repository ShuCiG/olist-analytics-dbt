"""What the ELT DAGs' alert emails say, decided from plain inputs (D28, D29).

airflow/dags/olist_alerts.py is the Airflow side (Variables, XCom, task context,
SMTP) and calls these. No Airflow imports here, so the decisions are unit-tested
in the project venv (tests/test_airflow_emails.py): OK / STALE / no email, the
stale reminder schedule, and which log and summary a failure email shows.

Task logs are read from Airflow's log folder (JSON lines, one file per attempt).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from html import escape
from pathlib import Path
from urllib.parse import quote

from . import dbt_summary, email_html, schema_changes

UI_URL = "http://localhost:8080"
# Elementary reports of the DAGs' edr_report task (`elementary-reports` service)
REPORTS_URL = "http://localhost:8081"
LOG_TAIL_LINES = 15
PRODUCER = "dbt.dbt_producer_watcher"
# Log records that are parsing / runner chatter, not the task's own output
LOG_NOISE_LOGGERS = ("airflow.dag_processing", "cosmos.converter", "cosmos.airflow", "cosmos.dbt.graph")
LOG_NOISE_EVENTS = ("::", "Worker startup parse complete", "Tmp dir root location", "Output:")
ANSI = re.compile(r"\x1b\[[0-9;]*m")  # dbt colors its output
# STALE reminders: about one a day; half an hour of slack for hourly runs that start late
REMIND_EVERY_HOURS = 23.5


@dataclass
class Email:
    kind: str  # OK | STALE | FAILED | SCHEMA
    subject: str
    html: str
    detail: str = ""  # one line for the task log


def task_log(log_dir: Path, dag_id: str, run_id: str, task_id: str, try_number: int) -> tuple[str, list[str]]:
    """(exception, output lines before it) from a task's log file."""
    path = log_dir / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}" / f"attempt={try_number}.log"
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


def run_url(dag_id: str, run_id: str) -> str:
    return f"{UI_URL}/dags/{dag_id}/runs/{quote(run_id, safe='')}"


def failure_email(
    dag_id: str,
    run_id: str,
    task_id: str | None,
    try_number: int,
    params: dict[str, object],
    reason: object,
    log_dir: Path,
    manifest: str | Path,
) -> Email:
    """DAG-level failure email: what failed, the error and the relevant log lines.

    `task_id` is the task Airflow names as the cause (the callback's `last_ti`). For a
    dbt task that is often a symptom (a test sensor downstream of the broken model),
    so any dbt failure reports the producer's `dbt build` summary instead: it lists
    every failed node with dbt's own message, data tests in business words.
    """
    url = run_url(dag_id, run_id)
    rows: dict[str, object] = {"DAG": dag_id, "Run": run_id}
    error, body = "(unknown: no failed task in the callback context)", ""
    what = "?"
    links = [("Open the run", url)]
    if task_id is not None:
        rows["Failed task (as reported)"] = task_id
        rows["Attempt"] = try_number
        what = task_id
        log_task = PRODUCER if task_id.startswith("dbt.") else task_id
        # The producer never retries (attempt 2+ is skipped): its log is attempt 1
        error, output = task_log(log_dir, dag_id, run_id, log_task, 1 if log_task == PRODUCER else try_number)
        if output:
            body = email_html.heading("Last log lines") + email_html.pre(
                "\n".join(output[-LOG_TAIL_LINES:]), "FAILED"
            )
        if log_task == PRODUCER:
            done, block = dbt_summary.summary(output)
            if done:
                what, error = "dbt build", done
                # Totals are already in the intro
                body = dbt_summary.to_html("", block, dbt_summary.test_docs(manifest))
            if "ERROR=0" in done and task_id != PRODUCER:
                # dbt passed, Airflow failed the task: e.g. a sensor's stale executor
                # event after a fast deferral, or a task killed on a restart
                what = f"{task_id} (dbt passed)"
                error = f"dbt build passed ({done}); Airflow marked {task_id} as failed"
                body = (
                    email_html.paragraph(
                        "An Airflow-side failure, not a data one: every dbt model and"
                        " test finished. See the task's log for why Airflow failed it."
                    )
                    + body
                )
                links.insert(0, (f"Log of {task_id}", f"{url}/tasks/{task_id}"))
        links.insert(0, (f"Log of {log_task}", f"{url}/tasks/{log_task}"))
    set_params = {k: v for k, v in params.items() if v not in (None, False)}
    rows["Params"] = json.dumps(set_params) if set_params else "none (scheduled-style run)"
    rows["Reason"] = reason

    errors = re.search(r"ERROR=\d+", error)
    count = f" ({errors[0]})" if errors and errors[0] != "ERROR=0" else ""
    intro = (
        f"Run <b>{escape(run_id)}</b> of <b>{escape(dag_id)}</b> stopped at "
        f"<b>{escape(what)}</b>:<br><code>{escape(error)}</code>"
    )
    html = email_html.page(
        "FAILED",
        f"{what} failed{count}",
        intro,
        body,
        rows,
        links,
        footer=f"{dag_id} · local Airflow · re-run with a new manual run (until), not Clear:"
        " airflow/README.md",
    )
    return Email("FAILED", f"[{dag_id}] FAILED: {what}{count} in run {run_id}", html, error)


def hours_since(last_loaded_at: str | None, now: datetime) -> float:
    """Hours since new orders last arrived; infinite when none ever did."""
    if not last_loaded_at:
        return float("inf")
    return (now - datetime.fromisoformat(last_loaded_at)).total_seconds() / 3600


def is_reminder_due(age_h: float, stale_after: float, reminded_at: datetime | None, now: datetime) -> bool:
    """Stale, and no STALE email yet in this stale period or for about a day.

    `reminded_at` is when the last STALE email went out (the caller keeps it), so a
    machine that is off at some hours still gets one a day. A reminder from before
    the period began (the feed was fresh in between) doesn't count.
    """
    if age_h < stale_after:
        return False
    if reminded_at is None:
        return True
    since_h = (now - reminded_at).total_seconds() / 3600
    return since_h >= REMIND_EVERY_HOURS or since_h > age_h - stale_after


def report_email(
    warehouse: str,
    run_id: str,
    status: dict,
    dbt_built: bool,
    now: datetime,
    stale_after: float,
    log_dir: Path,
    manifest: str | Path,
    reminded_at: datetime | None = None,
) -> tuple[Email | None, float]:
    """(email or None, hours since new orders) for a run that did not fail.

    `status` is the feed status `pipeline.py` prints (orders_loaded, orders_total,
    last_order_at, last_loaded_at). OK when the run brought orders or rebuilt the
    marts; else STALE when a reminder is due (`reminded_at` = when the last STALE
    email went out, see is_reminder_due); else no email.
    """
    dag_id = f"olist_elt_{warehouse}"
    # dbt's totals + warnings from the producer's log of this run
    dbt_done, dbt_block = "", []
    if dbt_built:
        _, output = task_log(log_dir, dag_id, run_id, PRODUCER, 1)
        dbt_done, dbt_block = dbt_summary.summary(output)
    warnings = re.search(r"WARN=(\d+)", dbt_done)
    n_warn = int(warnings[1]) if warnings else 0
    loaded = status["orders_loaded"]
    last_loaded_at = status["last_loaded_at"]
    age_h = hours_since(last_loaded_at, now)
    stale = age_h >= stale_after

    rows: dict[str, object] = {
        "Warehouse": warehouse,
        "Run": run_id,
        "Orders loaded this run": f"{loaded:,}",
        "Orders in raw": f"{status['orders_total']:,}",
        "Latest order (purchase time)": status["last_order_at"],
        "Last new orders loaded at (UTC)": (last_loaded_at or "")[:19].replace("T", " "),
        "Hours since new orders": round(age_h, 1),
        "dbt build": (dbt_done.removeprefix("Done. ") or "success") if dbt_built else "skipped (nothing new)",
    }
    prefix = f"[{dag_id}]"
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
            body = dbt_summary.to_html(dbt_done, dbt_block, dbt_summary.test_docs(manifest))
    elif is_reminder_due(age_h, stale_after, reminded_at, now):
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
        return None, age_h

    html = email_html.page(
        kind,
        title,
        intro,
        body,
        rows,
        [("Open the run", run_url(dag_id, run_id))]
        # Rebuilt after every dbt build (also in the UI: Browse → Elementary)
        + ([("Elementary report", f"{REPORTS_URL}/airflow_{warehouse}/")] if dbt_built else []),
        footer=f"{dag_id} · local Airflow · settings: Admin → Variables",
    )
    return Email(kind, subject, html), age_h


def schema_email(warehouse: str, run_id: str, status: dict) -> Email | None:
    """SCHEMA email when the run's load added source columns to raw or found raw
    columns missing from the source (D61), else None.

    Its own email, not a line in OK: it calls for a person (source owner, contract,
    staging PR), and it is sent right after the load, so a later dbt failure doesn't
    swallow it. A feed status without the fields (older pipeline) means no change.
    """
    dag_id = f"olist_elt_{warehouse}"
    built = schema_changes.email(
        status.get("new_columns") or {},
        status.get("removed_columns") or {},
        prefix=f"[{dag_id}]",
        where=f"{warehouse} raw (Airflow)",
        links=[("Open the run", run_url(dag_id, run_id))],
        footer=f"{dag_id} · local Airflow · settings: Admin → Variables",
    )
    if built is None:
        return None
    subject, html = built
    return Email("SCHEMA", subject, html)
