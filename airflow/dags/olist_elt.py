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
new and skip the load (one status query, D49) and dbt. A manual run with the `until` param loads up to that month and
always runs dbt (catch-up, recovery). See airflow/README.md.

Alerts (olist_alerts.py): one report per run at most — failed run, fresh data, or
stale data (no new orders for a day, repeated daily) — plus a SCHEMA email when the
load added source columns to raw or found raw columns missing from the source (D61).
Recipients in the Airflow Variable
`olist_alert_emails`.
"""

from __future__ import annotations

import json
import os
import re
import time
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
from olist_alerts import on_dag_failure, report, schema_report
from olist_lineage import RawLoadOperator, send_dbt_lineage, stash_dbt_lineage

PROJECT_DIR = Path("/opt/project")  # repo, mounted by docker-compose
DBT_PROJECT_DIR = PROJECT_DIR / "transform"
VENV_BIN = Path("/opt/project-venv/bin")  # dbt + dlt, built from uv.lock
DBT_EXECUTABLE = VENV_BIN / "dbt"
# Built by `dbt parse` in airflow-init (and in CI). Rebuild after changing models:
#   docker compose -f airflow/docker-compose.yml run --rm airflow-init
DBT_MANIFEST = DBT_PROJECT_DIR / "target_airflow" / "manifest.json"
# Volume for dbt's partial parse caches (docker-compose.yml; Cosmos' own cache dir
# is set there too, AIRFLOW__COSMOS__CACHE_DIR): a recreated container keeps them
DBT_CACHE_DIR = Path("/opt/dbt-cache")
GCP_KEY = Path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent"))
# Elementary's CLI, own venv (Dockerfile). Reports land in the mounted data folder,
# one per warehouse; the plugin (plugins/elementary_reports.py) and the
# `elementary-reports` service (docker-compose.yml) serve them.
EDR_BIN = Path("/opt/edr-venv/bin")
REPORTS_DIR = PROJECT_DIR / "data" / "elementary"
# edr's exit code turned into a skipped task: a broken report must not fail the run
EDR_FAILED = 99

# Which orders to load: up to the `until` param if given, else the next month
CHUNK_FLAG = "{{ '--until ' ~ params.until if params.until else '--next' }}"
# Elementary's clock (D38): the last order loaded so far, pushed by has_new_orders.
# `or ''`: a missing XCom renders as the string "None", which dbt can't parse as a
# time (seen on a run started by the previous image); empty = the wall clock.
# Passed as env OLIST_AS_OF (read by the var in dbt_project.yml), not `--vars`:
# dbt drops its partial parse whenever `--vars` change, so a new month per run cost
# a full project parse (~20 s) in each of the run's three dbt calls; a changed
# env var read only in dbt_project.yml keeps it (D50).
AS_OF = "{{ ti.xcom_pull(task_ids='has_new_orders', key='as_of') or '' }}"
DBT_CLOCK_ENV = {"OLIST_AS_OF": AS_OF}
# Retries (warehouses with retries, D53): see wait_for_producer
PRODUCER_TASK_ID = "dbt.dbt_producer_watcher"
PRODUCER_TIMEOUT = pendulum.duration(minutes=20)
# Final states of the producer (Cosmos' PRODUCER_TERMINAL_STATES, a private module)
PRODUCER_DONE = {"success", "failed", "skipped", "upstream_failed", "removed"}
RETRY_DELAY = pendulum.duration(minutes=1)
# A retried node's own dbt run skips Elementary's on-run-end uploads: parallel runs
# (two dims healing at once) collided on its tables ("Transaction is aborted due to
# concurrent update", BigQuery DML). The producer's build still uploaded the run.
FALLBACK_FLAGS = [
    "--vars",
    json.dumps(
        {
            "disable_dbt_artifacts_autoupload": True,
            "disable_run_results": True,
            "disable_tests_results": True,
            "disable_dbt_invocation_autoupload": True,
        }
    ),
]


@dataclass(frozen=True)
class Warehouse:
    name: str  # DAG id suffix
    dbt_target: str
    dlt_args: list[str]
    # OpenLineage name of the raw tables dlt loads (olist_lineage.py): the dbt
    # integration's namespace for the profile, and database.schema of the tables
    lineage_namespace: str
    raw_dataset: str
    env: dict[str, str] = field(default_factory=dict)
    pool: str | None = None  # for every task that writes to the warehouse
    # Airflow retries of every task: heals transient network errors (see build_dag)
    retries: int = 0


def no_freshness_skips(*_: object) -> list[tuple[str, str]]:
    """Watcher freshness callback: never skip nodes downstream of stale sources.

    Freshness is a warn-only signal here (D18): a month without new orders leaves the
    order tables honestly stale, and their models must still run.
    """
    return []


def wait_for_producer(context) -> None:
    """Node sensors' `pre_execute`: on a retry, wait until the producer is over.

    A retried sensor reruns its node only once the producer is in a final state;
    while it still runs (or waits for its own retry) Cosmos reads the node's failed
    status again and the retry is spent. A fixed retry delay can't fit: a node fails
    while the producer still builds the rest, and the nodes dbt skipped behind it
    start after the producer is over, one graph level after another. The producer
    retries at once (attempt 2 skips itself) and is cut off after PRODUCER_TIMEOUT,
    so the wait is bounded; after it Cosmos decides as before.
    """
    ti = context["ti"]
    if ti.try_number == 1:
        return
    deadline = pendulum.now("UTC") + PRODUCER_TIMEOUT
    while pendulum.now("UTC") < deadline:
        states = ti.get_task_states(
            dag_id=ti.dag_id, task_ids=[PRODUCER_TASK_ID], run_ids=[ti.run_id]
        )
        state = states.get(ti.run_id, {}).get(PRODUCER_TASK_ID)
        if state in PRODUCER_DONE:
            return
        print(f"Retry: producer is {state}, waiting before this node's own dbt run")
        time.sleep(15)


def has_new_orders(ti, params) -> bool:
    """Skip dbt when the feed brought nothing (dump used up): no rebuild for no data.

    Reads the feed status JSON that `pipeline.py` prints as the ingest's last line
    (its XCom). Manual runs with `until` or `full_refresh` always build: re-running a
    month whose dbt failed loads 0 new orders but still has to rebuild. So does the
    run after a failed scheduled run: once the dump is used up no run brings orders,
    and a failed last month would otherwise stay unbuilt with a single email.

    Also pushes `as_of` (XCom), the last order loaded so far: Elementary's clock
    (D38), so its anomaly tests look back from the replayed month, not from today.
    """
    status = json.loads(ti.xcom_pull(task_ids="dlt_ingest"))
    # Rendered into the elementary_tables bash command: only a plain timestamp
    # (pipeline.py checks it too; same pattern as its TIMESTAMP)
    as_of = status["last_order_at"] or ""
    if as_of and not re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?", as_of):
        raise ValueError(f"last_order_at is not a timestamp: {as_of!r}")
    ti.xcom_push(key="as_of", value=as_of)
    if params["until"] or params["full_refresh"]:
        return True
    previous = ti.get_previous_dagrun()
    if previous is not None and previous.state == "failed":
        print(f"Previous run {previous.run_id} failed: building again")
        return True
    return status["orders_loaded"] > 0


DUCKDB_FILE = PROJECT_DIR / "data" / "airflow.duckdb"
# The tables dlt loads: the dbt project's sources
RAW_TABLES = sorted(
    source["identifier"]
    for source in json.loads(DBT_MANIFEST.read_text())["sources"].values()
    if source["source_name"] == "olist"
)

WAREHOUSES = [
    Warehouse(
        name="duckdb",
        dbt_target="dev",
        dlt_args=["--destination", "duckdb", "--duckdb-path", str(DUCKDB_FILE)],
        # dbt-duckdb names the database after the file
        lineage_namespace=f"duckdb://{DUCKDB_FILE}",
        raw_dataset=f"{DUCKDB_FILE.stem}.raw",
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
            lineage_namespace="bigquery",
            # Project as in profiles.yml
            raw_dataset=f"{os.environ.get('GCP_PROJECT_ID', 'olist-analytics-dbt-472315')}.airflow_raw",
            env={"DBT_RAW_SCHEMA": "airflow_raw"},
            # Transient GCP errors (an HTML 403 from Google's front end, SSLEOFError on
            # a job insert) failed whole runs. dlt is idempotent (merge + cursor), and
            # dbt nodes heal via Cosmos' fallback (build_dag); the client libraries
            # don't retry these (google-cloud-bigquery treats SSLError as permanent)
            retries=1,
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
        # Retries: a watcher producer never reruns (attempt > 1 skips itself), so a
        # retried node task runs its own `dbt run/test --select <node>` instead;
        # nodes dbt skipped after a failed parent fail too and retry the same way.
        # The producer needs the retry as well: it then ends skipped, not failed, and
        # `report` (behind the producer, `none_failed`) can still run. A real failure
        # (failing test, SQL error) fails again on the retry: one more dbt call, the
        # run still fails with one email. Timing: wait_for_producer. DuckDB: 0,
        # those node commands run outside the 1-slot pool (lock errors); recover
        # with a new run (airflow/README.md).
        default_args={"retries": wh.retries, "retry_delay": RETRY_DELAY},
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
        ingest = RawLoadOperator(
            task_id="dlt_ingest",
            lineage_namespace=wh.lineage_namespace,
            raw_tables=[f"{wh.raw_dataset}.{table}" for table in RAW_TABLES],
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
                # Templated per task (`env` is a template field of Cosmos operators)
                env_vars={**wh.env, **DBT_CLOCK_ENV},
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
                    # One OpenLineage event per model, sent by `openlineage` (olist_lineage.py)
                    "post_execute": stash_dbt_lineage,
                    # Unit tests check logic on fixtures: CI's job, not the data pipeline's
                    "dbt_cmd_flags": ["--exclude-resource-type", "unit_test"],
                    **({"pool": wh.pool} if wh.pool else {}),
                    **(
                        {
                            "retry_delay": pendulum.duration(0),
                            "execution_timeout": PRODUCER_TIMEOUT,
                            "pre_execute": None,
                        }
                        if wh.retries
                        else {}
                    ),
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
                # Elementary's tables are built by elementary_tables, before the build
                # (the producer gets this exclude too)
                exclude=["package:elementary"],
            ),
            operator_args={
                "full_refresh": "{{ params.full_refresh }}",
                # Sensors (the producer overrides it above)
                **(
                    {"pre_execute": wait_for_producer, "dbt_cmd_flags": FALLBACK_FLAGS}
                    if wh.retries
                    else {}
                ),
            },
        )

        # Elementary's tables (run/test history, metrics) before the build: on a fresh
        # warehouse its schema-change tests on sources would start before dbt creates
        # them. Incremental and filled by hooks, so a rebuild keeps the history (D38).
        # Own target path per warehouse: its partial parse is kept between runs, also
        # across recreated containers (volume, D55), and never mixes with the producer's.
        elementary_tables = BashOperator(
            task_id="elementary_tables",
            bash_command=(
                f"{DBT_EXECUTABLE} run --select package:elementary"
                f" --target {wh.dbt_target} --target-path {DBT_CACHE_DIR / f'elementary_{wh.name}'}"
            ),
            cwd=str(DBT_PROJECT_DIR),
            env={**wh.env, **DBT_CLOCK_ENV, "DBT_PROFILES_DIR": str(DBT_PROJECT_DIR)},
            append_env=True,
            **({"pool": wh.pool} if wh.pool else {}),
        )

        new_orders = ShortCircuitOperator(
            task_id="has_new_orders",
            python_callable=has_new_orders,
            # Skip only what follows by trigger rules: `report` still runs
            ignore_downstream_trigger_rules=False,
        )

        # Elementary's report (test and run history, anomaly charts) from this
        # warehouse's `elementary` schema, as CD does for prod (D38). Also after a
        # failed build, which the report shows; not when dbt was skipped (nothing
        # new). edr runs its own dbt project with the project's `elementary` profile.
        # Written next to the old report and moved over it: the servers never hand
        # out a half-written file. In the pool: on DuckDB edr's dbt opens the file.
        report_dir = REPORTS_DIR / f"airflow_{wh.name}"
        edr_dir = DBT_CACHE_DIR / f"edr_{wh.name}"
        edr_report = BashOperator(
            task_id="edr_report",
            bash_command=(
                f"mkdir -p {report_dir} {edr_dir}"
                f" && printf 'anonymous_usage_tracking: false\\n' > {edr_dir / 'config.yml'}"
                f" && {EDR_BIN / 'edr'} report --profiles-dir {DBT_PROJECT_DIR}"
                f" --profile-target {wh.dbt_target} --config-dir {edr_dir} --target-path {edr_dir}"
                f" --file-path {report_dir / 'index.tmp.html'} --env airflow_{wh.name}"
                " --days-back 30 --open-browser false"
                f" && mv {report_dir / 'index.tmp.html'} {report_dir / 'index.html'}"
                f" || exit {EDR_FAILED}"
            ),
            cwd=str(DBT_PROJECT_DIR),
            # edr calls `dbt` from PATH: the edr venv's must come first
            env={**wh.env, "PATH": f"{EDR_BIN}:{os.environ.get('PATH', '/usr/bin:/bin')}"},
            append_env=True,
            skip_on_exit_code=EDR_FAILED,
            trigger_rule=TriggerRule.NONE_SKIPPED,
            **({"pool": wh.pool} if wh.pool else {}),
        )

        email_report = PythonOperator(
            task_id="report",
            python_callable=report,
            op_kwargs={"warehouse": wh.name},
            # After dbt succeeded or was skipped; a failed dbt fails the run instead.
            # Behind edr_report too: the email links to the fresh report.
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        # Right after the load, beside the dbt branch: a dbt failure or a skipped
        # dbt run doesn't hold it back
        schema_email = PythonOperator(
            task_id="schema_report",
            python_callable=schema_report,
            op_kwargs={"warehouse": wh.name},
        )

        # The producer's model events to Marquez (olist_lineage.py), beside the rest:
        # Marquez takes 1-3 min for a build's events, nothing waits for it
        lineage = PythonOperator(
            task_id="openlineage",
            python_callable=send_dbt_lineage,
            op_kwargs={"producer_task_id": PRODUCER_TASK_ID},
        )

        ingest >> new_orders >> elementary_tables >> dbt >> edr_report >> email_report
        # Directly too: a failed dbt must hold back the email even if edr_report ran
        dbt >> email_report
        ingest >> schema_email
        dag.get_task(PRODUCER_TASK_ID) >> lineage
    return dag


for _wh in WAREHOUSES:
    build_dag(_wh)
