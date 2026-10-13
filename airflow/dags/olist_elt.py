"""Olist ELT: dlt ingest of the next purchase month, then the dbt project via Cosmos.

One DAG per warehouse, built by the same factory:
- `olist_elt_duckdb`   — file data/airflow.duckdb (apart from the dev file)
- `olist_elt_bigquery` — datasets airflow_raw, airflow_staging, ... (apart from
  prod, which GitHub Actions CD owns); only created when a GCP key is mounted

Why two DAGs and not a `target` param: Cosmos renders the dbt graph with a fixed
profile at parse time, and dlt keeps a separate load cursor per destination, so
each warehouse needs its own run history.

dbt runs in Cosmos' watcher mode: one producer task runs a single `dbt build` for
the whole project (one parse, dbt's own threads), and every node is a sensor task
that mirrors its status from the producer's log. See the speed notes in
airflow/README.md.

Schedule: a simulated live feed (D28). Olist is a static 2016-2018 dump, so every
hourly run loads the next purchase month that has orders (`pipeline.py --next`,
cursor in dlt state): the whole history arrives in ~a day, then runs find nothing
new and skip dbt. A manual run with the `until` param loads up to that month and
always runs dbt (catch-up, recovery). See airflow/README.md.

Alerts (olist_alerts.py): one email per run at most — failed run, fresh data, or
stale data (no new orders for a day, repeated daily). Recipients in the Airflow
Variable `olist_alert_emails`.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG, Param
from airflow.task.trigger_rule import TriggerRule
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import (
    ExecutionMode,
    InvocationMode,
    LoadMode,
    SourceRenderingBehavior,
    TestBehavior,
    TestIndirectSelection,
)
from olist_alerts import on_dag_failure, report

PROJECT_DIR = Path("/opt/project")  # repo, mounted by docker-compose
DBT_PROJECT_DIR = PROJECT_DIR / "transform"
VENV_BIN = Path("/opt/project-venv/bin")  # dbt + dlt, built from uv.lock
DBT_EXECUTABLE = VENV_BIN / "dbt"
# Built by `dbt parse` in airflow-init (and in CI). Rebuild after changing models:
#   docker compose -f airflow/docker-compose.yml run --rm airflow-init
DBT_MANIFEST = DBT_PROJECT_DIR / "target_airflow" / "manifest.json"
GCP_KEY = Path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent"))

# Which orders to load: up to the `until` param if given, else the next month
CHUNK_FLAG = "{{ '--until ' ~ params.until if params.until else '--next' }}"


@dataclass(frozen=True)
class Warehouse:
    name: str  # DAG id suffix
    dbt_target: str
    dlt_args: list[str]
    env: dict[str, str] = field(default_factory=dict)
    pool: str | None = None  # for every task that writes to the warehouse


def no_freshness_skips(*_: object) -> list[tuple[str, str]]:
    """Watcher freshness callback: never skip nodes downstream of stale sources.

    Freshness is a warn-only signal here (D18): a month without new orders leaves the
    order tables honestly stale, and their models must still run.
    """
    return []


def has_new_orders(ti, params) -> bool:
    """Skip dbt when the feed brought nothing (dump used up): no rebuild for no data.

    Reads the feed status JSON that `pipeline.py` prints as the ingest's last line
    (its XCom). Manual runs with `until` or `full_refresh` always build: re-running a
    month whose dbt failed loads 0 new orders but still has to rebuild.
    """
    if params["until"] or params["full_refresh"]:
        return True
    return json.loads(ti.xcom_pull(task_ids="dlt_ingest"))["orders_loaded"] > 0


DUCKDB_FILE = PROJECT_DIR / "data" / "airflow.duckdb"

WAREHOUSES = [
    Warehouse(
        name="duckdb",
        dbt_target="dev",
        dlt_args=["--destination", "duckdb", "--duckdb-path", str(DUCKDB_FILE)],
        env={"DBT_DUCKDB_PATH": str(DUCKDB_FILE)},
        # One writer per DuckDB file: dlt and dbt tasks queue on a 1-slot pool
        pool="duckdb",
    ),
]
# Empty placeholder mounted when no key is configured (see docker-compose.yml)
if GCP_KEY.is_file() and GCP_KEY.stat().st_size > 0:
    WAREHOUSES.append(
        Warehouse(
            name="bigquery",
            dbt_target="airflow",
            dlt_args=["--destination", "bigquery", "--dataset", "airflow_raw"],
            env={"DBT_RAW_SCHEMA": "airflow_raw"},
        )
    )


def build_dag(wh: Warehouse) -> DAG:
    with DAG(
        dag_id=f"olist_elt_{wh.name}",
        description=f"dlt next month → dbt build on {wh.name}",
        # Simulated feed: one dump month per run (D28). A run takes ~6-9 min.
        schedule="@hourly",
        start_date=pendulum.datetime(2026, 10, 13, tz="UTC"),
        # Missed hours are not replayed: each run takes "the next month" anyway
        catchup=False,
        # dlt's cursor moves forward month by month: runs must not overlap
        max_active_runs=1,
        # No retries: a watcher producer never reruns (attempt > 1 is skipped), and
        # retried node tasks run their own dbt command instead, outside the DuckDB
        # pool (lock errors). Recover with a new run, not Clear (airflow/README.md).
        default_args={"retries": 0},
        # One email per failed run (not per task: a failed producer fails ~50 sensors)
        on_failure_callback=on_dag_failure,
        params={
            "until": Param(
                None,
                type=["null", "string"],
                pattern=r"^\d{4}-\d{2}$",
                description="Load orders up to this month (YYYY-MM) instead of the next one",
            ),
            "full_refresh": Param(
                False,
                type="boolean",
                description="Drop raw tables + dlt state, rebuild incremental models",
            ),
        },
        tags=["olist", "dlt", "dbt", wh.name],
    ) as dag:
        ingest = BashOperator(
            task_id="dlt_ingest",
            bash_command=(
                f"{VENV_BIN / 'python'} ingestion/pipeline.py {' '.join(wh.dlt_args)}"
                f" {CHUNK_FLAG}"
                " {{ '--full-refresh' if params.full_refresh else '' }}"
            ),
            cwd=str(PROJECT_DIR),
            env=wh.env,
            append_env=True,
            **({"pool": wh.pool} if wh.pool else {}),
        )

        dbt = DbtTaskGroup(
            group_id="dbt",
            project_config=ProjectConfig(
                DBT_PROJECT_DIR,
                manifest_path=DBT_MANIFEST,
                env_vars=wh.env,
                # Packages are installed once by airflow-init, not per task
                install_dbt_deps=False,
            ),
            profile_config=ProfileConfig(
                profile_name="olist",
                target_name=wh.dbt_target,
                profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
            ),
            execution_config=ExecutionConfig(
                # One `dbt build` per run in a producer task; node tasks are sensors
                # that read its per-node results. Per-task startup + parse (~40 s per
                # node in LOCAL mode) is paid once.
                execution_mode=ExecutionMode.WATCHER,
                dbt_executable_path=DBT_EXECUTABLE,
                invocation_mode=InvocationMode.SUBPROCESS,
                # Only tests whose parents are all in the node's selection; the
                # default (eager) runs relationships tests before the other parent
                # exists. Matters when a node task falls back to its own dbt run
                # (retry after a failed producer); the producer builds everything.
                test_indirect_selection=TestIndirectSelection.CAUTIOUS,
                # Producer only: the sensors don't write, and in the 1-slot pool they
                # would wait for the producer they are watching
                setup_operator_args={
                    "freshness_callback": no_freshness_skips,
                    # Unit tests check logic on fixtures: CI's job, not the data pipeline's
                    "dbt_cmd_flags": ["--exclude-resource-type", "unit_test"],
                    **({"pool": wh.pool} if wh.pool else {}),
                },
            ),
            render_config=RenderConfig(
                # Graph from the manifest, not `dbt ls`: Airflow 3 re-parses the DAG
                # file in every task, and Cosmos' dbt ls cache (Airflow Variables)
                # can't be written from tasks, so each task ran 2 uncached dbt ls
                # (minutes under load, past the import timeout: endless retries)
                load_method=LoadMode.DBT_MANIFEST,
                # Per model a run task + a test task, both sensors of the producer's
                # build (the watcher has no BUILD behavior: the build is the producer)
                test_behavior=TestBehavior.AFTER_EACH,
                # Multi-parent tests (relationships) stay in one parent's test task.
                # Detached, they become sensors the producer never reports (it reports
                # tests per model) and fail with "no information about the node"
                should_detach_multiple_parents_tests=False,
                # Source tasks run `dbt source freshness` (warn-only, D18)
                source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
            ),
            operator_args={"full_refresh": "{{ params.full_refresh }}"},
        )

        new_orders = ShortCircuitOperator(
            task_id="has_new_orders",
            python_callable=has_new_orders,
            # Skip only what follows by trigger rules: `report` still runs
            ignore_downstream_trigger_rules=False,
        )

        email_report = PythonOperator(
            task_id="report",
            python_callable=report,
            op_kwargs={"warehouse": wh.name},
            # After dbt succeeded or was skipped; a failed dbt fails the run instead
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        ingest >> new_orders >> dbt >> email_report
    return dag


for _wh in WAREHOUSES:
    build_dag(_wh)
