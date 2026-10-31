"""CI check: parse the DAG folder the way the dag-processor does.

Runs inside the Airflow image (see the `airflow-dags` job in ci-cd.yml). Fails on
any import error, when an expected DAG is missing or has no dbt tasks, when the
Cosmos watcher producer is missing, when dbt is not gated by `has_new_orders`
(dlt_ingest -> has_new_orders -> elementary_tables -> producer: no rebuild once the
feed is used up, Elementary's tables exist before the build),
when the alerts are not wired (`report` after dbt with `none_failed`, a DAG-level
failure callback), when `edr_report` (Elementary's report) is not between the
dbt group and `report` with `none_skipped` and a skip exit code (a broken report
must not fail the run), when the dbt model lineage is not prepared by the producer
and sent by `openlineage` behind it (D87), when a dbt-running task gets Elementary's clock as `--vars`
instead of env OLIST_AS_OF (a changed `--vars` makes dbt drop its partial parse:
a full parse per run, D50), when retries are not as expected (BigQuery: every
task retries once, transient GCP errors, D53, with the producer retrying at once
and every node task waiting for it; DuckDB: none, a retried node task runs dbt
outside the pool), or
when the DuckDB pool holds anything but the writers (dlt, Elementary's tables,
producer, edr_report): a sensor in
the 1-slot pool waits for the producer it watches. A non-empty dummy key must be mounted so the BigQuery DAG is
rendered too; parsing never authenticates.
"""

import sys

from airflow.dag_processing.dagbag import DagBag

DAG_FOLDER = "/opt/project/airflow/dags"
# Airflow puts the DAG folder on sys.path (the DAG imports olist_alerts from there)
sys.path.insert(0, DAG_FOLDER)

EXPECTED = {"olist_elt_duckdb", "olist_elt_bigquery"}
PRODUCER = "dbt.dbt_producer_watcher"
GATE = "has_new_orders"
ELEMENTARY = "elementary_tables"
EDR_REPORT = "edr_report"
LINEAGE = "openlineage"
DUCKDB_WRITERS = {"dlt_ingest", ELEMENTARY, PRODUCER, EDR_REPORT}
RETRIES = {"olist_elt_duckdb": 0, "olist_elt_bigquery": 1}

bag = DagBag(dag_folder=DAG_FOLDER)

for path, error in bag.import_errors.items():
    print(f"Import error in {path}:\n{error}", file=sys.stderr)

missing = EXPECTED - set(bag.dag_ids)
if missing:
    print(f"Missing DAGs: {sorted(missing)}", file=sys.stderr)

for dag_id in sorted(EXPECTED & set(bag.dag_ids)):
    dag = bag.dags[dag_id]
    dbt_tasks = [t for t in dag.task_ids if t.startswith("dbt.")]
    print(f"{dag_id}: {len(dag.task_ids)} tasks ({len(dbt_tasks)} dbt)")
    if not dbt_tasks:
        print(f"{dag_id} has no dbt tasks", file=sys.stderr)
        missing.add(dag_id)
    if PRODUCER not in dag.task_ids:
        print(f"{dag_id} has no {PRODUCER} (not in watcher mode?)", file=sys.stderr)
        missing.add(dag_id)
    elif not {GATE, ELEMENTARY} <= set(dag.task_ids) or (
        dag.get_task(GATE).upstream_task_ids != {"dlt_ingest"}
        or dag.get_task(ELEMENTARY).upstream_task_ids != {GATE}
        or ELEMENTARY not in dag.get_task(PRODUCER).upstream_task_ids
    ):
        print(
            f"{dag_id}: dbt is not gated by dlt_ingest -> {GATE} -> {ELEMENTARY}",
            file=sys.stderr,
        )
        missing.add(dag_id)
    if (
        "report" not in dag.task_ids
        or dag.get_task("report").trigger_rule != "none_failed"
        or not dag.on_failure_callback
    ):
        print(f"{dag_id}: alerts not wired (report task / failure callback)", file=sys.stderr)
        missing.add(dag_id)
    if EDR_REPORT not in dag.task_ids or not (
        dag.get_task(EDR_REPORT).trigger_rule == "none_skipped"
        and dag.get_task(EDR_REPORT).skip_on_exit_code
        # After the dbt group's last tasks; `report` behind both (a failed dbt must
        # still hold back the email when edr_report ran)
        and dag.get_task(EDR_REPORT).upstream_task_ids
        and all(t.startswith("dbt.") for t in dag.get_task(EDR_REPORT).upstream_task_ids)
        and dag.get_task(EDR_REPORT).upstream_task_ids < dag.get_task("report").upstream_task_ids
        and EDR_REPORT in dag.get_task("report").upstream_task_ids
    ):
        print(f"{dag_id}: {EDR_REPORT} not wired (dbt -> {EDR_REPORT} -> report)", file=sys.stderr)
        missing.add(dag_id)
    # Lineage (D87): the producer prepares the per-model events, `openlineage` sends
    # them behind it, outside the run's main line
    if PRODUCER in dag.task_ids and (
        getattr(dag.get_task(PRODUCER)._post_execute_hook, "__name__", None) != "stash_dbt_lineage"
        or LINEAGE not in dag.task_ids
        or dag.get_task(LINEAGE).upstream_task_ids != {PRODUCER}
        or dag.get_task(LINEAGE).downstream_task_ids
    ):
        print(f"{dag_id}: lineage not wired ({PRODUCER} post_execute -> {LINEAGE})", file=sys.stderr)
        missing.add(dag_id)
    for task_id in (ELEMENTARY, PRODUCER):
        if task_id not in dag.task_ids:
            continue
        task = dag.get_task(task_id)
        if "OLIST_AS_OF" not in (task.env or {}) or "custom_run_started_at" in (
            f"{getattr(task, 'vars', None)} {getattr(task, 'bash_command', '')}"
        ):
            print(f"{dag_id}: {task_id} must get the clock as env OLIST_AS_OF", file=sys.stderr)
            missing.add(dag_id)
    # All or nothing: a sensor that retries behind a producer that doesn't leaves the
    # producer failed, and `report` never runs even when every node healed
    wrong = sorted(t.task_id for t in dag.tasks if t.retries != RETRIES[dag_id])
    if wrong:
        print(f"{dag_id}: retries != {RETRIES[dag_id]} on {wrong[:5]}", file=sys.stderr)
        missing.add(dag_id)
    if RETRIES[dag_id] and PRODUCER in dag.task_ids:
        producer = dag.get_task(PRODUCER)
        no_wait = [
            t.task_id
            for t in dag.tasks
            if t.task_id.startswith("dbt.") and t.task_id not in (PRODUCER, f"{PRODUCER}_done")
            and getattr(t._pre_execute_hook, "__name__", None) != "wait_for_producer"
        ]
        if producer.retry_delay.total_seconds() or not producer.execution_timeout or no_wait:
            print(
                f"{dag_id}: retry timing (producer retry_delay 0 + timeout, node tasks"
                f" wait for it) — not waiting: {no_wait[:5]}",
                file=sys.stderr,
            )
            missing.add(dag_id)
    if dag_id == "olist_elt_duckdb":
        pooled = {t.task_id for t in dag.tasks if t.pool == "duckdb"}
        if pooled != DUCKDB_WRITERS:
            print(
                f"{dag_id}: pool duckdb — unexpected {len(pooled - DUCKDB_WRITERS)} tasks,"
                f" missing {sorted(DUCKDB_WRITERS - pooled)}",
                file=sys.stderr,
            )
            missing.add(dag_id)

sys.exit(1 if bag.import_errors or missing else 0)
