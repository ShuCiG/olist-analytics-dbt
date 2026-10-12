"""Olist ELT: dlt ingest of one purchase month, then the dbt project via Cosmos.

One DAG per warehouse, built by the same factory:
- `olist_elt_duckdb`   — file data/airflow.duckdb (apart from the dev file)
- `olist_elt_bigquery` — datasets airflow_raw, airflow_staging, ... (apart from
  prod, which GitHub Actions CD owns); only created when a GCP key is mounted

Why two DAGs and not a `target` param: Cosmos renders the dbt graph with a fixed
profile at parse time, and dlt keeps a separate load cursor per destination, so
each warehouse needs its own run history (and backfills).

Schedule: monthly over the dump's purchase months. A run for the interval starting
2017-03-01 loads orders up to 2017-03 (`pipeline.py --until 2017-03`). History is
replayed with a backfill, see airflow/README.md.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import (
    InvocationMode,
    LoadMode,
    SourceRenderingBehavior,
    TestBehavior,
    TestIndirectSelection,
)

PROJECT_DIR = Path("/opt/project")  # repo, mounted by docker-compose
DBT_PROJECT_DIR = PROJECT_DIR / "transform"
VENV_BIN = Path("/opt/project-venv/bin")  # dbt + dlt, built from uv.lock
DBT_EXECUTABLE = VENV_BIN / "dbt"
GCP_KEY = Path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent"))

# `--until <month>`: the `until` param if given, else the run's data interval.
# Neither (manual run without a logical date): no flag, load everything.
UNTIL_FLAG = (
    "{% set until = params.until or (data_interval_start.strftime('%Y-%m')"
    " if data_interval_start else none) %}"
    "{{ '--until ' ~ until if until else '' }}"
)


@dataclass(frozen=True)
class Warehouse:
    name: str  # DAG id suffix
    dbt_target: str
    dlt_args: list[str]
    env: dict[str, str] = field(default_factory=dict)
    pool: str | None = None  # for every task that writes to the warehouse


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
        description=f"dlt monthly chunk → dbt build on {wh.name}",
        # Purchase months in the dump: 2016-09 .. 2018-10
        schedule="@monthly",
        start_date=pendulum.datetime(2016, 9, 1, tz="UTC"),
        end_date=pendulum.datetime(2018, 10, 1, tz="UTC"),
        catchup=False,  # history is loaded on purpose, with a backfill
        # dlt's cursor moves forward month by month: runs must not overlap.
        # Backfills don't inherit this: pass `--max-active-runs 1` (airflow/README.md)
        max_active_runs=1,
        params={
            "until": Param(
                None,
                type=["null", "string"],
                pattern=r"^\d{4}-\d{2}$",
                description="Load orders up to this month (YYYY-MM) instead of the run's month",
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
                f" {UNTIL_FLAG}"
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
                dbt_executable_path=DBT_EXECUTABLE,
                invocation_mode=InvocationMode.SUBPROCESS,
                # Only tests whose parents are all in the node's selection; the
                # default (eager) runs relationships tests before the other parent exists
                test_indirect_selection=TestIndirectSelection.CAUTIOUS,
            ),
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                dbt_executable_path=DBT_EXECUTABLE,
                invocation_mode=InvocationMode.SUBPROCESS,
                # One `dbt build --select <node>` task per node: model + its tests
                test_behavior=TestBehavior.BUILD,
                # ...and multi-parent tests (relationships) get their own task after
                # all parents, otherwise cautious selection would never run them
                should_detach_multiple_parents_tests=True,
                # Source tasks run `dbt source freshness` (warn-only, D18)
                source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
            ),
            operator_args={
                "full_refresh": "{{ params.full_refresh }}",
                **({"pool": wh.pool} if wh.pool else {}),
            },
        )

        ingest >> dbt
    return dag


for _wh in WAREHOUSES:
    build_dag(_wh)
