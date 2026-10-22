"""alerting/airflow_emails.py: which email an ELT DAG run sends (OK / STALE / none,
failure email contents), from fake Airflow task logs in a temp log folder."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from alerting import airflow_emails
from alerting.airflow_emails import PRODUCER

NOW = datetime(2026, 10, 15, 12, 5, tzinfo=UTC)
DAG = "olist_elt_duckdb"
# Real run ids have colons (scheduled__2026-10-15T12:00:00+00:00): not a valid path on Windows
RUN = "manual__2026-10-15T120000"

DBT_OK_WITH_WARNINGS = [
    "12:04:01  Completed with 1 warning:",
    "12:04:01  ",
    "12:04:01  [WARNING]: in test relationships_customers_zip (models/staging/_stg.yml)",
    "12:04:01  [WARNING]: Got 278 results, configured to warn if != 0",
    "12:04:01  ",
    "12:04:01  Done. PASS=120 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=121",
]
DBT_FAILED = [
    "12:04:01  Completed with 1 error, 0 partial successes, and 0 warnings:",
    "12:04:01  [ERROR]: in model stg_olist__sellers (models/staging/stg_olist__sellers.sql)",
    '12:04:01    Binder Error: Referenced column "seller_town" not found',
    "12:04:01  Done. PASS=40 WARN=0 ERROR=1 SKIP=80 NO-OP=0 REUSED=0 TOTAL=121",
]


def write_log(
    log_dir: Path, task_id: str, attempt: int, events: list[str], error: tuple[str, str] | None = None
) -> None:
    """A task log as Airflow 3 writes it: JSON lines, exception in `error_detail`."""
    path = log_dir / f"dag_id={DAG}" / f"run_id={RUN}" / f"task_id={task_id}" / f"attempt={attempt}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, object]] = [
        {"event": "::group::Log message source details", "logger": "airflow"},  # runner noise
        {"event": "Filling up the DagBag", "logger": "airflow.dag_processing.dagbag"},
        *({"event": e, "logger": "task.stdout"} for e in events),
    ]
    if error:
        records.append({"event": "Task failed", "error_detail": [{"exc_type": error[0], "exc_value": error[1]}]})
    path.write_text("\n".join(json.dumps(r) for r in records) + "\nnot json\n", encoding="utf-8")


@pytest.fixture
def manifest(tmp_path: Path) -> Path:
    node = {
        "name": "relationships_customers_zip",
        "resource_type": "test",
        "description": "Customers whose zip code is missing.",
        "config": {"meta": {"alert_unit": "customers"}},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": {"test.a": node}}), encoding="utf-8")
    return path


def status(loaded: int, hours_ago: float | None) -> dict:
    """Feed status as pipeline.py prints it."""
    return {
        "orders_loaded": loaded,
        "orders_total": 1000,
        "last_order_at": "2017-03-31 23:59:00",
        "last_loaded_at": (NOW - timedelta(hours=hours_ago)).isoformat() if hours_ago is not None else None,
    }


# --- stale window ---------------------------------------------------------------


def test_hours_since() -> None:
    assert airflow_emails.hours_since((NOW - timedelta(hours=2.5)).isoformat(), NOW) == 2.5
    assert airflow_emails.hours_since(None, NOW) == float("inf")


@pytest.mark.parametrize(
    ("age_h", "expected"),
    [
        (23.9, False),  # fresh
        (24.0, True),  # first hourly run past the threshold
        (24.9, True),
        (25.0, False),  # then quiet ...
        (47.9, False),
        (48.2, True),  # ... until a day later
        (float("inf"), True),  # never any orders
    ],
)
def test_is_reminder_hour(age_h: float, expected: bool) -> None:
    assert airflow_emails.is_reminder_hour(age_h, 24) is expected


# --- report task (run did not fail) -----------------------------------------------


def report(tmp_path: Path, manifest: Path, feed: dict, dbt_built: bool):
    return airflow_emails.report_email(
        "duckdb", RUN, feed, dbt_built, NOW, stale_after=24, log_dir=tmp_path / "logs", manifest=manifest
    )


def test_new_orders_send_ok_with_dbt_warnings(tmp_path: Path, manifest: Path) -> None:
    write_log(tmp_path / "logs", PRODUCER, 1, DBT_OK_WITH_WARNINGS)
    email, age_h = report(tmp_path, manifest, status(loaded=1234, hours_ago=0.05), dbt_built=True)
    assert email is not None and email.kind == "OK"
    assert email.subject == f"[{DAG}] OK: +1234 orders, data up to 2017-03, 1 dbt warning"
    assert "278 customers" in email.html and "Customers whose zip code is missing." in email.html
    assert "PASS=120 WARN=1" in email.html
    assert round(age_h, 2) == 0.05


def test_rebuild_without_new_orders_is_ok(tmp_path: Path, manifest: Path) -> None:
    # Manual run with `until` over a loaded month: 0 orders, dbt still rebuilt
    write_log(tmp_path / "logs", PRODUCER, 1, ["12:04:01  Completed successfully", "12:04:01  Done. PASS=121 WARN=0 ERROR=0"])
    email, _ = report(tmp_path, manifest, status(loaded=0, hours_ago=30), dbt_built=True)
    assert email is not None and email.kind == "OK"
    assert email.subject.endswith("data up to 2017-03")  # no warnings suffix
    assert "no new orders for 30 h" in email.html


def test_nothing_new_and_fresh_sends_nothing(tmp_path: Path, manifest: Path) -> None:
    email, _ = report(tmp_path, manifest, status(loaded=0, hours_ago=3), dbt_built=False)
    assert email is None


def test_stale_reminder(tmp_path: Path, manifest: Path) -> None:
    email, _ = report(tmp_path, manifest, status(loaded=0, hours_ago=24.4), dbt_built=False)
    assert email is not None and email.kind == "STALE"
    assert email.subject == f"[{DAG}] STALE: no new orders for 24 h (data up to 2017-03)"
    assert "skipped (nothing new)" in email.html


def test_stale_between_reminders_sends_nothing(tmp_path: Path, manifest: Path) -> None:
    email, _ = report(tmp_path, manifest, status(loaded=0, hours_ago=30), dbt_built=False)
    assert email is None


def test_empty_raw_is_stale(tmp_path: Path, manifest: Path) -> None:
    feed = status(loaded=0, hours_ago=None) | {"orders_total": 0, "last_order_at": None}
    email, age_h = report(tmp_path, manifest, feed, dbt_built=False)
    assert email is not None and email.kind == "STALE"
    assert age_h == float("inf")


# --- failure callback ---------------------------------------------------------------


def failure(tmp_path: Path, manifest: Path, task_id: str | None, try_number: int = 1):
    return airflow_emails.failure_email(
        DAG,
        RUN,
        task_id,
        try_number,
        params={"until": None, "full_refresh": False},
        reason="task_failure",
        log_dir=tmp_path / "logs",
        manifest=manifest,
    )


def test_non_dbt_failure_shows_error_and_log_tail(tmp_path: Path, manifest: Path) -> None:
    events = ["Order chunk (2017-02, end]: 0 orders", "\x1b[31mloading raw.orders\x1b[0m"]
    write_log(tmp_path / "logs", "dlt_ingest", 2, events, ("PipelineStepFailed", "403 Forbidden"))
    email = failure(tmp_path, manifest, "dlt_ingest", try_number=2)
    assert email.kind == "FAILED"
    assert email.subject == f"[{DAG}] FAILED: dlt_ingest in run {RUN}"
    assert email.detail == "PipelineStepFailed: 403 Forbidden"
    assert "Last log lines" in email.html and "loading raw.orders" in email.html  # colors stripped
    assert "DagBag" not in email.html and "::group::" not in email.html  # runner noise
    assert "none (scheduled-style run)" in email.html  # no params set


def test_dbt_sensor_failure_reports_the_producers_dbt_summary(tmp_path: Path, manifest: Path) -> None:
    # Airflow names a downstream test sensor; the cause is in the producer's build
    write_log(tmp_path / "logs", PRODUCER, 1, DBT_FAILED, ("CosmosDbtRunError", "dbt build failed"))
    email = failure(tmp_path, manifest, "dbt.stg_olist__sellers.test", try_number=2)
    assert email.subject == f"[{DAG}] FAILED: dbt build (ERROR=1) in run {RUN}"
    assert email.detail.startswith("Done. PASS=40")
    assert "dbt errors" in email.html and "seller_town" in email.html
    assert f"tasks/{PRODUCER}" in email.html


def test_sensor_failure_when_dbt_passed_is_airflow_side(tmp_path: Path, manifest: Path) -> None:
    write_log(tmp_path / "logs", PRODUCER, 1, DBT_OK_WITH_WARNINGS)
    email = failure(tmp_path, manifest, "dbt.dim_date.test")
    assert email.subject == f"[{DAG}] FAILED: dbt.dim_date.test (dbt passed) in run {RUN}"
    assert "ERROR=" not in email.subject.split("(dbt passed)")[0]
    assert "An Airflow-side failure, not a data one" in email.html
    assert "tasks/dbt.dim_date.test" in email.html  # link to the sensor's own log


def test_failure_without_log_file(tmp_path: Path, manifest: Path) -> None:
    email = failure(tmp_path, manifest, "has_new_orders")
    assert email.detail.startswith("(no log file at ")


def test_failure_without_task(tmp_path: Path, manifest: Path) -> None:
    email = failure(tmp_path, manifest, None)
    assert email.subject == f"[{DAG}] FAILED: ? in run {RUN}"
    assert "no failed task in the callback context" in email.detail


# --- schema_report task (D61) ---------------------------------------------------------


def test_new_source_columns_send_schema_email() -> None:
    feed = {**status(loaded=10, hours_ago=0.05), "new_columns": {"orders": ["order_channel"], "sellers": ["a", "b"]}}
    email = airflow_emails.schema_email("duckdb", RUN, feed)
    assert email is not None and email.kind == "SCHEMA"
    assert email.subject == f"[{DAG}] SCHEMA: 3 new source columns in orders, sellers"
    assert "raw.orders.order_channel" in email.html and "raw.sellers.b" in email.html
    assert "olist-raw.odcs.yaml" in email.html  # the next step names the input contract


@pytest.mark.parametrize("feed", [status(loaded=10, hours_ago=0.05), {**status(loaded=0, hours_ago=3), "new_columns": {}}])
def test_no_new_columns_no_schema_email(feed: dict) -> None:
    # No field (an older pipeline's status) or an empty one: nothing to say
    assert airflow_emails.schema_email("duckdb", RUN, feed) is None


def test_schema_email_escapes_column_names() -> None:
    # Column names come from the source files' headers
    feed = {**status(loaded=1, hours_ago=0.05), "new_columns": {"orders": ["<script>x</script>"]}}
    email = airflow_emails.schema_email("duckdb", RUN, feed)
    assert email is not None
    assert "<script>" not in email.html and "&lt;script&gt;" in email.html
