# Airflow + Cosmos

Local Airflow 3 (Docker Compose, LocalExecutor) that runs the ELT as an hourly
simulated feed: `dlt_ingest` (the next purchase month) → `has_new_orders` (skip dbt
when nothing arrived) → `dbt` task group → `report` (email, see below), where the
dbt group is rendered by
[Cosmos](https://github.com/astronomer/astronomer-cosmos) in watcher mode: one
producer task runs `dbt source freshness` + a single `dbt build` for the whole
project, and every node (model run / its tests, seed, snapshot, source) is a
sensor task that shows that node's result from the producer's log.

| DAG | Writes to | Notes |
|---|---|---|
| `olist_elt_duckdb` | `data/airflow.duckdb` | writers (dlt, dbt producer) share a 1-slot pool `duckdb` (single writer) |
| `olist_elt_bigquery` | datasets `airflow_raw`, `airflow_staging`, ... | only when a GCP key is mounted; prod stays with GitHub Actions CD |

## Run

```bash
cp airflow/.env.example airflow/.env      # optional: GCP_KEY_FILE for the BigQuery DAG
docker compose -f airflow/docker-compose.yml up -d --build
```

UI at http://localhost:8080 (no login: local only), alert emails at
http://localhost:8025 (Mailpit). DAGs start paused.

## Schedule: a simulated live feed

Olist is a static 2016-2018 dump, so "new data" is replayed (D28). The DAGs run
`@hourly` (`catchup=False`, `max_active_runs=1`) and every run loads **the next
purchase month that has orders** (`pipeline.py --next`; the cursor is in dlt
state, empty months such as 2016-11 are skipped):

| Run | Loads | dbt |
|---|---|---|
| 1st after unpausing an empty warehouse | 2016-09 (4 orders) | builds |
| next ones | 2016-10, 2016-12, 2017-01, ... one month each | builds |
| after 2018-10 (~25 runs, about a day) | nothing, `orders_loaded=0` | skipped by `has_new_orders` |

Once the dump is used up the runs stay green, dbt is skipped (no rebuild for no
data, no BigQuery cost) and the order tables' `_loaded_at` stops moving: source
freshness goes stale, which the `STALE` email reports (see below).

Catch up faster with a manual run that loads up to a month at once (the `until`
param always runs dbt):

```bash
docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger olist_elt_duckdb --conf '{"until": "2017-06"}'
```

Start the feed over: a manual run with `{"full_refresh": true}` drops the raw
tables + dlt state and loads the first month (add `until` to load more).

## Email alerts

At most one email per run ([dags/olist_alerts.py](dags/olist_alerts.py)):

| When | Subject | Sent by |
|---|---|---|
| a task failed | `[olist_elt_duckdb] FAILED: dlt_ingest in run <run_id>` with the error and the task's last output lines; any dbt failure → `FAILED: dbt build (ERROR=1)` with dbt's end-of-run summary (each failed node + message, warnings); params, links to the log and the run | DAG `on_failure_callback` (once per run, not per task) |
| new orders loaded (or a manual `until` / `full_refresh` rebuild) | `OK: +800 orders, data up to 2017-01` + summary table | `report` task |
| no new orders for `olist_stale_after_hours` (default 24, = source `warn_after`) | `STALE: no new orders for 25 h (data up to 2018-10)` | `report` task, then about once a day while stale |
| nothing new, not stale yet | no email | |

The summary comes from the feed status that `pipeline.py` prints as the ingest's
last line (orders loaded, orders in raw, latest order, when orders last arrived).

Settings (Admin → Variables in the UI, no restart; `airflow-init` creates them with
the defaults when missing and never overwrites them):

- `olist_alert_emails` — comma-separated recipients (default `data-team@olist.local`)
- `olist_stale_after_hours` — stale threshold in hours (default 24)

Transport: connection `smtp_default`, from env `AIRFLOW_CONN_SMTP_DEFAULT`. By
default it points at **Mailpit** (service in docker-compose): it catches every
email, nothing leaves the machine, inbox at http://localhost:8025. Real email:
set `SMTP_CONN` in `airflow/.env` (Gmail example with an app password in
[.env.example](.env.example)) and `up -d` again.

Components share a Fernet key (`AIRFLOW__CORE__FERNET_KEY`): without it each
container encrypts Variables with its own random key, and a Variable set in one
reads as missing in the others (recipients silently fell back to the default).

## Failures and re-runs

Mid-feed, a failed dbt step heals itself: the next hourly run loads the next month
and builds everything, including the failed month's rows (the incremental model's
`_loaded_at` cursor is still behind them). After the last month there is no run
with new data, so re-run with a **new** manual run with `until`, not with Clear:

```bash
docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger olist_elt_duckdb --conf '{"until": "2018-10"}'
```

Why: a Cosmos watcher producer never runs twice — on attempt 2+ it skips itself,
and each node task then runs its own `dbt` command for its node. On DuckDB those
run outside the 1-slot pool and fail on the file lock (the data is safe, the run
is not). A new run starts at attempt 1. It is idempotent: dlt's cursor is already
past that month (nothing is reloaded), dbt rebuilds tables/views and merges the
incremental model. Retries are off (`retries=0`) for the same reason.

## BigQuery DAG

Put the service-account key path in `airflow/.env` (`GCP_KEY_FILE=...`, see `.env.example`) and
recreate the stack (`up -d`). The DAG writes `airflow_raw` (dlt) and `airflow_staging`, `airflow_marts`,
... (dbt target `airflow`), never prod. Reset it (drops all `airflow*` datasets, guarded to targets
`ci` / `airflow`):

```bash
cd transform && uv run dbt run-operation drop_isolated_datasets --target airflow
```

The dlt cursor lives in `airflow_raw` and in the `dlt-pipelines` volume; after dropping the datasets also
clear it: `docker compose -f airflow/docker-compose.yml exec airflow-scheduler rm -rf /home/airflow/.dlt/pipelines/olist_bigquery_airflow_raw`.

## Params

Params on a manual run: `until` (`YYYY-MM`, load up to that month at once instead
of the next one; dbt always runs) and `full_refresh` (drop raw tables + dlt state,
rebuild incremental models). A manual run without params behaves like a scheduled
one.

## Design notes

- dbt and dlt run from `/opt/project-venv`, built from `uv.lock` (same versions as
  local dev and CI, no clash with Airflow's dependencies). Cosmos calls dbt as a
  subprocess (`ExecutionMode.WATCHER`, `InvocationMode.SUBPROCESS`).
- Watcher mode, measured on months 2016-09 and 2016-10 from an empty warehouse:
  DuckDB 29.1 / 28.0 → 5.6 / 5.7 min per month, BigQuery 18.9 → 8.9 / 6.1 min.
  One-task-per-node (`ExecutionMode.LOCAL`) paid dbt startup + project parse in
  every task (~36 s per task, one at a time on DuckDB); now it's paid once per run,
  and dbt's own threads run independent nodes in parallel (one process = one
  DuckDB writer). Sensor tasks are deferrable: running → deferred → running while
  the producer works is expected.
- Watcher specifics in the DAG: no `TestBehavior.BUILD` (the build *is* the
  producer) → `AFTER_EACH`, a run + a test sensor per model; multi-parent tests
  not detached (the producer reports tests per model, a detached test sensor gets
  no status); freshness never skips nodes (`no_freshness_skips`: warn-only, D18);
  unit tests excluded (CI checks them).
- The repo is mounted at `/opt/project`: tasks always run the current models. The
  DAG *graph* comes from `transform/target_airflow/manifest.json` (Cosmos
  `LoadMode.DBT_MANIFEST`, no `dbt ls` per parse); after adding/removing models
  or tests, rebuild it: `docker compose -f airflow/docker-compose.yml run --rm airflow-init`.
- `airflow-init` migrates the metadata DB, creates the `duckdb` pool, runs
  `dbt deps` and `dbt parse` (the manifest).
