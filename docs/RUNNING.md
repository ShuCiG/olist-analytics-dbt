# Running the project

Everything runs locally on DuckDB with no account or key; BigQuery, Databricks and Airflow are optional.
Requirements: [uv](https://docs.astral.sh/uv/) (Python 3.12 is installed by it), Docker for Airflow.

- [Local build (DuckDB)](#local-build-duckdb)
- [Data arriving month by month](#data-arriving-month-by-month)
- [SCD2 snapshot demo](#scd2-snapshot-demo)
- [Data product checks](#data-product-checks)
- [Semantic layer queries](#semantic-layer-queries)
- [Observability report (Elementary)](#observability-report-elementary)
- [Project evaluator](#project-evaluator)
- [Dashboard](#dashboard)
- [BigQuery](#bigquery)
- [Databricks](#databricks)
- [Airflow](#airflow)
- [Development hooks](#development-hooks)
- [Security review (`/audit`)](#security-review-audit)

## Local build (DuckDB)

```bash
uv sync                               # Python 3.12 + dependencies
uv run python ingestion/pipeline.py   # download Olist (Kaggle, no token needed), load data/olist.duckdb schema raw

cd transform
uv run dbt deps
uv run dbt build                      # seed + 21 models + snapshot + 108 data tests + 5 unit tests
uv run dbt docs generate && uv run dbt docs serve
cd ..
uv run pytest                         # Python tests: pipeline, dbt summary parser, alert decisions (no data needed)
uv run pytest data-product dashboard  # data product checks, dashboard tests (need the built marts)
```

## Data arriving month by month

The dump is static; dlt state remembers the last loaded month, so loads can be replayed in chunks:

```bash
uv run python ingestion/pipeline.py --full-refresh --until 2017-06   # orders up to June 2017
uv run python ingestion/pipeline.py --until 2017-07                  # adds July 2017 only
uv run python ingestion/pipeline.py --next                           # adds the next month with orders
uv run python ingestion/pipeline.py                                  # adds the rest; re-run = no-op
cd transform && uv run dbt source freshness                          # checks raw `_loaded_at`
uv run dbt build        # after each chunk: fct_order_items merges only the new rows
```

## SCD2 snapshot demo

`snap_sellers` keeps the history of seller location (`check` strategy on zip / city / state,
`hard_deletes: new_record`). Sellers never change in the dump, so prod holds one version each. To see versions:

```bash
# change a city and delete a seller in raw, then snapshot
uv run python -c "import duckdb; c = duckdb.connect('data/olist.duckdb'); c.execute(\"update raw.sellers set seller_city = 'campinas' where seller_id = '0015a82c2db000af6aaaf3ae2ecb0532'\"); c.execute(\"delete from raw.sellers where seller_id = '001cca7ae9ae17fb1caed9dfb1094831'\")"
cd transform && uv run dbt snapshot && cd ..
uv run python ingestion/pipeline.py && cd transform && uv run dbt snapshot   # reload restores both
```

| seller | city | dbt_valid_from | dbt_valid_to | dbt_is_deleted |
|---|---|---|---|---|
| 0015a8… | santo andre | 08:28:05 | 08:28:53 | False |
| 0015a8… | campinas | 08:28:53 | 08:29:23 | False |
| 0015a8… | santo andre | 08:29:23 | *null* | False |
| 001cca… | cariacica | 08:28:05 | 08:28:53 | False |
| 001cca… | cariacica | 08:28:53 | 08:29:23 | True |
| 001cca… | cariacica | 08:29:23 | *null* | False |

A reload with no changes adds no versions (dlt metadata columns are not compared).

## Data product checks

```bash
uv run datacontract test data-product/contracts/olist-raw.odcs.yaml --server local    # input contract on raw
uv run datacontract test data-product/contracts/olist-marts.odcs.yaml --server local  # output contract on marts
# --server prod / live: BigQuery datasets marts / airflow_marts (needs BigQuery Read Session User)
uv run python data-product/check_compatibility.py origin/main   # versioning rule, as on a PR
uv run python data-product/build_site.py site/data-product      # product page + contract catalog
```

On Windows set `PYTHONUTF8=1` (the CLI prints emoji the console code page can't encode).

## Semantic layer queries

```bash
cd transform
uv run dbt parse     # writes target/semantic_manifest.json
uv run mf list metrics
uv run mf query --metrics revenue,average_order_value --group-by metric_time__month --start-time 2018-01-01
uv run mf query --saved-query late_delivery_by_state --order -delivered_orders
uv run mf query --metrics revenue --group-by metric_time__year --explain   # show the generated SQL
uv run mf validate-configs                                                  # every metric queried on the warehouse
DBT_TARGET=prod uv run dbt parse && DBT_TARGET=prod uv run mf query --metrics orders   # BigQuery prod
```

`mf` has no `--target` flag: `profiles.yml` takes the default target from `DBT_TARGET`.

## Observability report (Elementary)

Every `dbt build` records its run and test results in the `elementary` schema (on-run-end hooks); the anomaly and
schema-change tests run with the other tests. The report is rendered by Elementary's CLI, `edr`, which is not in
`uv.lock` (it would downgrade `networkx`), so it runs in its own `uvx` environment pinned to the project's dbt versions:

```bash
cd transform
mkdir -p ../data/elementary
# edr runs its own dbt project from its install folder: the DuckDB path must be absolute
DBT_DUCKDB_PATH="$(cd ../data && pwd)/olist.duckdb" \
  uvx --python 3.12 --from "elementary-data[duckdb]==0.26.1" \
    --with dbt-core==1.12.5 --with dbt-duckdb==1.11.0 --with duckdb==1.5.6 \
  edr report --profiles-dir . --file-path ../data/elementary/index.html
```

- BigQuery prod: `--from "elementary-data[bigquery]==0.26.1" --with dbt-core==1.12.5 --with dbt-bigquery==1.12.1`
  and `--profile-target prod` (CD publishes this one at `/elementary/` on GitHub Pages).
- Windows: `edr`'s internal dbt project is deep inside uv's cache and passes the 260-character path limit
  (`No such file or directory: 'dbt_packages\...'`); point uv at a short cache, e.g. `UV_CACHE_DIR=C:/uvc`.
- The anomaly tests' clock is the last order in the data (var `custom_run_started_at`, env `OLIST_AS_OF`), not
  today: try `OLIST_AS_OF=2017-11-30T23:59:59 uv run dbt test --select test_name:volume_anomalies` to see
  Black Friday 2017 flagged.

## Project evaluator

dbt_project_evaluator is off in normal runs (its ~50 nodes stay out of builds and Airflow); CI turns it on:

```bash
cd transform
DBT_PROJECT_EVALUATOR=true uv run dbt build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions
```

## Dashboard

```bash
uv run streamlit run dashboard/app.py                                   # DuckDB: data/olist.duckdb
DASHBOARD_BACKEND=bigquery uv run streamlit run dashboard/app.py        # BigQuery prod marts (ADC key)
DASHBOARD_BACKEND=bigquery MARTS_DATASET=airflow_marts uv run streamlit run dashboard/app.py  # Airflow-fed marts
uv run pytest dashboard                                                 # pages render, KPIs = metrics, SQL = agreement
```

Deployed on Streamlit Community Cloud from `main` (entrypoint `dashboard/app.py`; it installs the slim
`dashboard/requirements.txt`, not the dbt/dlt project). Access is service account `dashboard-reader`:
*BigQuery Job User* on the project + *BigQuery Data Viewer* on datasets `marts` and `airflow_marts` only
(granted in [`infra/iam.tf`](../infra/iam.tf)).
App secrets:

```toml
DASHBOARD_BACKEND = "bigquery"
MARTS_DATASET = "airflow_marts"   # default "marts" (prod)

[gcp_service_account]   # fields of the service account's JSON key
type = "service_account"
project_id = "..."
# ...
```

Settings are read once, when the app process starts: after editing the secrets, **Reboot app** in the
Streamlit Cloud workspace (saving them alone keeps the old values).

## BigQuery

Needs a GCP project with billing and a service account with *BigQuery Data Editor* + *BigQuery Job User*
(+ *BigQuery Read Session User* for `datacontract test` on BigQuery). This project's datasets, service accounts,
grants, budget and quota are Terraform in [`infra/`](../infra/README.md) (`terraform plan` / `apply` with your own
Google account; for another project set `project_id`). GitHub Actions authenticates without keys (Workload Identity
Federation, `infra/wif.tf`); keys for local runs, Airflow and Streamlit are made in the console.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json   # Application Default Credentials
export GCP_PROJECT_ID=<your-project>
uv run python ingestion/pipeline.py --destination bigquery
cd transform && uv run dbt build --target prod
```

## Databricks

Free Edition works (no cloud account, no card). Create a catalog `olist` (`CREATE CATALOG olist` in the SQL editor)
and a personal access token, then set:

```bash
export DATABRICKS_HOST=dbc-xxxxxxxx-xxxx.cloud.databricks.com   # SQL warehouse → Connection details
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxxxxxxxxxx
export DATABRICKS_TOKEN=...                                       # Settings → Developer → Access tokens
uv run python ingestion/pipeline.py --destination databricks
cd transform && uv run dbt run --target databricks --select package:elementary
uv run dbt build --target databricks --exclude package:elementary
```

The scheduled CI (`.github/workflows/databricks.yml`, target `databricks_ci`) runs as a service principal with
`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` instead of a token; setup in D58.

## Airflow

```bash
docker compose -f airflow/docker-compose.yml up -d --build      # UI: http://localhost:8080, Mailpit: :8025
docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger olist_elt_duckdb --conf '{"until": "2017-06"}'   # catch up to a month at once
```

Or run a published image instead of building: `OLIST_AIRFLOW_IMAGE=ghcr.io/shucig/olist-airflow:<sha>` in
`airflow/.env`. BigQuery key, DAG params, alert settings, reset and re-running a failed month:
[airflow/README.md](../airflow/README.md).

## Development hooks

```bash
uv run pre-commit install   # whitespace / YAML / secrets checks, no commits to main,
                            # sqlfluff on changed SQL, pyright + pytest on Python changes
uv run sqlfluff lint transform/models transform/tests
uv run pyright
```

How a change goes: an issue (Task or Bug form) → a branch per issue → a PR whose description has `Closes #N`.
New issues and PRs land on the [roadmap board](https://github.com/users/ShuCiG/projects/2) by themselves; merging
the PR closes the issue and moves both to Done.

## Security review (`/audit`)

In Claude Code, run `/audit`: four agents review the repo (D39) and write their reports to `.audit/<date>/`
(`thief.md`, `vandal.md`, `mayor.md`, `chief.md`). The folder stays local: gitignored, blocked by a
pre-commit hook and by CI. Helpers, also usable on their own:

```bash
uv run python .claude/skills/audit/audit_tools.py check                 # .audit/ ignored and untracked
uv run python .claude/skills/audit/audit_tools.py prepare .audit/<dir>  # context pack for the agents
uv run python .claude/skills/audit/audit_tools.py redact .audit/<dir>   # mask secrets in the reports
```

## Terraform drift alert

`.github/workflows/drift.yml` plans `infra/` against live GCP daily (D60). A failure email titled
*Terraform drift* means one of:

- **Drift** — someone changed a managed resource in the console (an IAM grant, a dataset setting, the budget,
  the quota). The email lists the resources; the run's job summary has the full plan. Either undo the change in
  the console, or, if it should stay, put it into `infra/` through a PR and `terraform apply`.
- **Unapplied code** — an `infra/` change was merged but not applied. Run `terraform plan` / `apply` locally
  (`infra/README.md`).
- **Error** (no resource list) — usually permissions or auth: the plan account `terraform-plan` lacks a viewer role
  for a newly managed resource type (add it in `infra/drift.tf`), or the environment secret `BILLING_ACCOUNT` is
  missing.

Not caught: changes outside what Terraform owns. A label or an extra IAM grant added in the console keeps the run
green (labels are non-authoritative in the provider, grants are additive `*_iam_member`); review those in the
console's IAM page.

Run it on demand: `gh workflow run drift.yml` (main only). Once fixed, the next run is green.
