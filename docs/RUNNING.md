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
- [dbt v2 parse check](#dbt-v2-parse-check)
- [Dashboard](#dashboard)
- [BigQuery](#bigquery) (cost per dbt node, PR data diff)
- [Databricks](#databricks)
- [Airflow](#airflow)
- [Development hooks](#development-hooks)
- [Security review (`/audit`)](#security-review-audit)
- [Terraform drift alert](#terraform-drift-alert)
- [Service account keys](#service-account-keys)
- [SCHEMA email (source column changes)](#schema-email-source-column-changes)

## Local build (DuckDB)

```bash
uv sync                               # Python 3.12 + dependencies
uv run python ingestion/pipeline.py   # download Olist (Kaggle, no token needed), load data/olist.duckdb schema raw

cd transform
uv run dbt deps
uv run dbt build                      # seed + 25 models + snapshot + 150 data tests + 13 Elementary tests + 8 unit tests
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
cd transform && uv run dbt build -s snap_sellers+ && cd ..
uv run python ingestion/pipeline.py --full-refresh   # reload restores both (without the flag: "load skipped", no new months)
cd transform && uv run dbt build -s snap_sellers+    # snapshot + dim_sellers_history + the point-in-time fact
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

`dim_sellers_history` turns the snapshot into contiguous ranges (2026-10-24 run, times local):

| seller | version | city | valid_from | valid_to | is_current | is_deleted |
|---|---|---|---|---|---|---|
| 0015a8… | 1 | santo andre | 1900-01-01 | 10:11:33 | false | false |
| 0015a8… | 2 | campinas | 10:11:33 | 10:13:36 | false | false |
| 0015a8… | 3 | santo andre | 10:13:36 | 9999-12-31 | true | false |
| 001cca… | 1 | cariacica | 1900-01-01 | 10:11:33 | false | false |
| 001cca… | 2 | cariacica | 10:11:33 | 10:13:36 | false | true |
| 001cca… | 3 | cariacica | 10:13:36 | 9999-12-31 | true | false |

The first version starts at 1900-01-01, not at the snapshot time (`recorded_at`): the snapshot started years after
the orders. `fct_order_items_seller_as_of` gives the 242 items of these two sellers (orders from 2017–2018)
version 1, with `is_seller_version_current = false`; `fct_order_items` → `dim_sellers` shows today's version.

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
    --with dbt-core==1.12.6 --with dbt-duckdb==1.11.1 --with duckdb==1.5.6 \
  edr report --profiles-dir . --file-path ../data/elementary/index.html
```

- BigQuery prod: `--from "elementary-data[bigquery]==0.26.1" --with dbt-core==1.12.6 --with dbt-bigquery==1.12.1`
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

## dbt v2 parse check

dbt-core installs the v2 engine (dbt-oss) as `dbt-core-experimental-parser`; CI parses the project with it on
every PR, as a warning only ([D83](DECISIONS.md)). Same check locally:

```bash
cd transform
uv run dbt parse --use-v2-parser --target-path target-v2
../.venv/Scripts/dbt-core-experimental-parser build --exclude dim_customer_rfm   # the v2 engine itself (Windows path)
```

On Windows set `PYTHONUTF8=1`: dbt-core reads the v2 manifest with the console code page otherwise and fails.
The second line runs dbt-oss directly; point `DBT_DUCKDB_PATH` at a copy named `olist.duckdb` (the file name is the
catalog name), not at `data/olist.duckdb`.

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
# COSTS_DATASET = "marts"         # cost page (BigQuery only): where fct_dbt_job_costs lives, default prod "marts"

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

### Cost per dbt node

Every query dbt sends to BigQuery carries job labels (`dbt_target`, `dbt_resource_type`, `dbt_package`, `dbt_node`;
`macros/query_comment.sql`). The prod deploy builds `fct_dbt_job_costs` from the project's job history after the
main build (D71); the dashboard's cost page reads it. The models are enabled only for target `prod`: reading
`INFORMATION_SCHEMA.JOBS_BY_PROJECT` needs `bigquery.jobs.listAll`, which `dbt-runner` holds through the custom role
`jobHistoryViewer` (`infra/job_history.tf`) and the project owner holds anyway. To try it as the owner in throwaway
datasets:

```bash
unset GOOGLE_APPLICATION_CREDENTIALS          # your own account (ADC), not a service account key
export DBT_CI_SCHEMA=ci_cost_check            # datasets ci_cost_check_staging / _marts
cd transform && uv run dbt build --target ci --select +fct_dbt_job_costs --vars '{job_costs: true}'
cd .. && DASHBOARD_BACKEND=bigquery COSTS_DATASET=ci_cost_check_marts uv run streamlit run dashboard/app.py
cd transform && uv run dbt run-operation drop_isolated_datasets --target ci   # drop them afterwards
```

The model ignores `--full-refresh` (its rows outlive the 180 days of job history); drop the table to rebuild it.

### PR data diff

What slim CI posts on a PR (D73), run locally as `olist-ci`: build the changed models into throwaway datasets
deferred to the last prod deploy's manifest, compare the marts that built with prod, drop the datasets.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/olist-ci.json DBT_CI_SCHEMA=ci_diff_check
run_id=$(gh api "repos/ShuCiG/olist-analytics-dbt/actions/artifacts?name=prod-manifest" \n  --jq '[.artifacts[] | select(.workflow_run.head_branch == "main")][0].workflow_run.id')
gh run download "$run_id" --name prod-manifest --dir prod-state
cd transform && uv run dbt run --target ci --select package:elementary
uv run dbt build --target ci --select state:modified+ --exclude package:elementary --defer --state ../prod-state
cd .. && uv run python .github/scripts/data_diff.py prod-state/manifest.json data-diff.md   # Markdown of the comment
cd transform && uv run dbt run-operation drop_isolated_datasets --target ci
```

The macro behind it (`dbt run-operation data_diff`, `macros/data_diff.sql`) also runs on DuckDB, e.g. against a
hand-edited copy of a mart in another schema.

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
`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` instead of a token; setup in D58. It also tests both data
contracts against server `databricks-ci` (D64). To run that check on your own load, point the server's variables at it
(datacontract-cli takes the warehouse and credentials from its own `DATACONTRACT_DATABRICKS_*` variables):

```bash
export DATACONTRACT_DATABRICKS_HTTP_PATH=$DATABRICKS_HTTP_PATH DATACONTRACT_DATABRICKS_TOKEN=$DATABRICKS_TOKEN
export DBT_RAW_SCHEMA=ci_dbx_local_raw DBT_CI_SCHEMA=ci_dbx_local     # schemas in catalog olist_ci
uv run datacontract ci data-product/contracts/olist-raw.odcs.yaml --server databricks-ci
uv run datacontract ci data-product/contracts/olist-marts.odcs.yaml --server databricks-ci
```

## Snowflake

A trial account works for 30 days (no external access: see D85). Set up the account with Terraform once
([infra/snowflake/README.md](../infra/snowflake/README.md): key pairs, `bootstrap.sql`, `terraform apply`), then as
service user `OLIST_SVC`:

```bash
export SNOWFLAKE_ACCOUNT=<org>-<account>                           # SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME()
export SNOWFLAKE_PRIVATE_KEY_PATH=~/.snowflake/keys/olist_svc.p8   # dbt on Windows: a Windows path
uv run python ingestion/pipeline.py --destination snowflake
cd transform && uv run dbt run --target snowflake --select package:elementary
uv run dbt build --target snowflake --exclude package:elementary && cd ..
```

dbt Projects on Snowflake and the task graph (same variables; Snowflake CLI comes pinned through `uvx`):

```bash
uv run python snowflake/deploy.py      # upload transform/ + packages as OLIST.DBT_PROJECTS.OLIST, apply tasks.sql
```

```sql
EXECUTE DBT PROJECT OLIST.DBT_PROJECTS.OLIST ARGS = 'build --exclude package:elementary';  -- one run, target native
EXECUTE TASK OLIST.DBT_PROJECTS.DBT_ELEMENTARY;                                         -- the whole graph now
SELECT SYSTEM$GET_DBT_LOG('<query id>');                                                -- log of a failed run
ALTER TASK OLIST.DBT_PROJECTS.DBT_ELEMENTARY SUSPEND;                                   -- stop the daily schedule
```

The weekly CI (`.github/workflows/snowflake.yml`, target `snowflake_ci`) needs environment `snowflake-ci` with
variable `SNOWFLAKE_ACCOUNT` and secret `SNOWFLAKE_PRIVATE_KEY` (the PEM text of `olist_svc.p8`); without the
secret it skips. When the trial ends: delete the secret, `terraform destroy` in `infra/snowflake` (the database has
`prevent_destroy`: remove it first), or keep everything on a paid account.

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

Grants added next to Terraform's don't show in a plan (additive `*_iam_member`), so the second job,
**IAM outside Terraform** (D62), compares the live IAM with the state and `infra/iam_allowlist.yaml`. Its lines:

- `binding  <scope>  <role>  <member>` — a grant nobody declared. If it was a mistake or unknown, remove it in the
  console (IAM page, dataset sharing, bucket permissions). If it should stay: as a Terraform grant in `infra/iam.tf`
  (preferred), or, when Terraform can't own it, as an allowlist entry with a `why`.
- `service account not in Terraform` — an account made in the console: delete it, or add it to
  `infra/service_accounts.tf`.
- `labels on dataset/<id>` — remove the label, or manage labels in `infra/datasets.tf`.

User emails show masked (`user:s***@example.com`, the log is public); the console shows the full one.

The third job, **Service account keys** (D63), lists the user-managed keys of every service account:

- `key older than 90 days  <account>  <key id>  <age>` — rotate it (below).
- `key on an account that should have none  <account>  <key id>` — a key nobody declared: delete it, or, if the
  account really needs one, add it to `infra/sa_keys.yaml` with where it is used.

Run it on demand: `gh workflow run drift.yml` (main only). Once fixed, the next run is green.

## Service account keys

GitHub Actions needs no keys (D57). Three accounts keep a JSON key made in the console, listed in
`infra/sa_keys.yaml`; the drift check fails once one is older than 90 days. Rotation, the same for each:
create a new key, switch the place that uses it, check it works, delete the old key.

```bash
P=olist-analytics-dbt-472315
SA=dbt-runner@$P.iam.gserviceaccount.com          # or dashboard-reader@..., olist-ci@...
gcloud iam service-accounts keys list --iam-account $SA --managed-by=user        # the old key's id
gcloud iam service-accounts keys create ~/.gcp/$P-new.json --iam-account $SA     # keep it outside the repo
# switch (table below) and check, then:
gcloud iam service-accounts keys delete <old key id> --iam-account $SA
```

| Account | Used by | Switch | Check |
|---|---|---|---|
| `dbt-runner` | local Airflow, BigQuery DAG | `GCP_KEY_FILE` in `airflow/.env`, then `docker compose -f airflow/docker-compose.yml up -d` | trigger `olist_elt_bigquery` in the UI |
| `dashboard-reader` | Streamlit Community Cloud | app settings → Secrets: replace the `[gcp_service_account]` fields with the new file's, then **Reboot app** | the app's pages load |
| `olist-ci` | local BigQuery runs and tests | `GOOGLE_APPLICATION_CREDENTIALS` | `cd transform && uv run dbt debug --target ci` |

Delete the old key file too. A key you can't place: disable it first (`keys disable`), wait a day for something to
break, then delete it.

## SCHEMA email (source column changes)

Sent when a load found columns the source didn't have before (D61): from Airflow's `schema_report` task, or from
CD's `notify-schema` job after a prod deploy. The column is already in raw (as text); staging, marts and the
dashboard are unchanged, so nothing is broken. To do:

1. Ask the source owner what the column means and whether it stays.
2. Add it to the input contract, `data-product/contracts/olist-raw.odcs.yaml`.
3. If the business needs it: add it to the staging model (cast, rename) and its `.yml` in a PR, then further up as
   needed.

Each column is reported once: the next load knows it. The sellers snapshot keeps new `sellers` columns too (it
reads the source).

**Removed column** (`... source column removed in <table>`): raw has a column the source file lacks. dlt keeps it and
loads nulls, so staging, marts and the dashboard show nulls for it; `dbt build` stays green unless a NOT NULL test
covers the column. Every load reports it until fixed. To do:

1. Ask the source owner whether it was dropped on purpose.
2. By mistake: the source restores it. Rows loaded meanwhile keep their nulls until a reload
   (`uv run python ingestion/pipeline.py --full-refresh`, plus `--destination` / `--dataset` as for that warehouse).
3. On purpose: remove it from the staging model and its `.yml` and from the input contract in a PR, then reload with
   `--full-refresh` so raw drops the column (until then the email repeats).
