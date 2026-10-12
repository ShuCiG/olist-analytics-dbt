# Airflow + Cosmos

Local Airflow 3 (Docker Compose, LocalExecutor) that runs the ELT month by month:
`dlt_ingest` (one purchase month) → `dbt` task group rendered by
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

UI at http://localhost:8080 (no login: local only). DAGs start paused.

## Backfill the history

The DAGs are `@monthly` from 2016-09 to 2018-10 with `catchup=False`: history is
replayed on purpose, with a backfill. Each run loads its own month (dlt keeps
the cursor), so runs must go one at a time:

```bash
docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
  airflow backfill create --dag-id olist_elt_duckdb \
  --from-date 2016-09-01 --to-date 2017-01-01 --max-active-runs 1
```

`--max-active-runs 1` is required: an Airflow 3 backfill has its own limit
(default 10) and ignores the DAG's `max_active_runs=1`. Parallel months race on
the dlt cursor and the Kaggle download cache.

## Failures and re-runs

Re-run a failed month with a **new** run, not with Clear (or a backfill with
`--reprocess-behavior`, which reuses the run id):

```bash
docker compose -f airflow/docker-compose.yml exec airflow-scheduler   airflow dags trigger olist_elt_duckdb --conf '{"until": "2016-10"}'
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

Params on a manual run: `until` (`YYYY-MM`, load up to that month at once) and
`full_refresh` (drop raw tables + dlt state, rebuild incremental models).

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
