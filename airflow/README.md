# Airflow + Cosmos

Local Airflow 3 (Docker Compose, LocalExecutor) that runs the ELT month by month:
`dlt_ingest` (one purchase month) → `dbt` task group rendered by
[Cosmos](https://github.com/astronomer/astronomer-cosmos), one `dbt build` task
per model / seed / snapshot, plus `dbt source freshness` tasks.

| DAG | Writes to | Notes |
|---|---|---|
| `olist_elt_duckdb` | `data/airflow.duckdb` | dbt tasks share a 1-slot pool `duckdb` (single writer) |
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
  subprocess (`ExecutionMode.LOCAL`, `InvocationMode.SUBPROCESS`).
- The repo is mounted at `/opt/project`: tasks always run the current models. The
  DAG *graph* comes from `transform/target_airflow/manifest.json` (Cosmos
  `LoadMode.DBT_MANIFEST`, no `dbt ls` per parse); after adding/removing models
  or tests, rebuild it: `docker compose -f airflow/docker-compose.yml run --rm airflow-init`.
- `airflow-init` migrates the metadata DB, creates the `duckdb` pool, runs
  `dbt deps` and `dbt parse` (the manifest).
