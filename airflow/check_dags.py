"""CI check: parse the DAG folder the way the dag-processor does.

Runs inside the Airflow image (see the `airflow-dags` job in ci-cd.yml). Fails on
any import error (including Cosmos' `dbt ls`) and when an expected DAG is missing
or has no dbt tasks. A non-empty dummy key must be mounted so the BigQuery DAG is
rendered too; parsing never authenticates.
"""

import sys

from airflow.dag_processing.dagbag import DagBag

EXPECTED = {"olist_elt_duckdb", "olist_elt_bigquery"}

bag = DagBag(dag_folder="/opt/project/airflow/dags")

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

sys.exit(1 if bag.import_errors or missing else 0)
