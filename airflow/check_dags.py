"""CI check: parse the DAG folder the way the dag-processor does.

Runs inside the Airflow image (see the `airflow-dags` job in ci-cd.yml). Fails on
any import error, when an expected DAG is missing or has no dbt tasks, when the
Cosmos watcher producer is missing, when dbt is not gated by `has_new_orders`
(dlt_ingest -> has_new_orders -> elementary_tables -> producer: no rebuild once the
feed is used up, Elementary's tables exist before the build),
when the alerts are not wired (`report` after dbt with `none_failed`, a DAG-level
failure callback), or
when the DuckDB pool holds anything but the writers (dlt + producer): a sensor in
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
DUCKDB_WRITERS = {"dlt_ingest", ELEMENTARY, PRODUCER}

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
