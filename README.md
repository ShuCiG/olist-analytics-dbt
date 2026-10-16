# Olist Analytics — dbt project

[![CI/CD](https://github.com/ShuCiG/olist-analytics-dbt/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/ShuCiG/olist-analytics-dbt/actions/workflows/ci-cd.yml)
[![dbt docs](https://img.shields.io/badge/dbt_docs-lineage-FF694B?logo=dbt&logoColor=white)](https://shucig.github.io/olist-analytics-dbt/)
[![Dashboard](https://img.shields.io/badge/dashboard-live-FF4B4B?logo=streamlit&logoColor=white)](https://olist-analytics-dbt.streamlit.app/)

An end-to-end ELT project on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(~100k orders, 2016–2018, 8 related tables). Raw data is loaded with **dlt** into **DuckDB** (local dev) and
**BigQuery** (prod) and modeled with **dbt Core** into a tested star schema. Every pull request is built in CI on
DuckDB and, slim, on BigQuery. Every merge deploys to BigQuery prod and publishes the dbt docs. A local
Airflow replays the dump hourly as a live feed into its own BigQuery datasets, and a Streamlit dashboard
reads those marts.

**📖 Live dbt docs with lineage graph:** https://shucig.github.io/olist-analytics-dbt/<br>
**📊 Live dashboard (BigQuery, fed hourly by Airflow):** https://olist-analytics-dbt.streamlit.app/

Built with AI pair programming (Claude Code); the architecture, design decisions and code review are mine.
The reasoning behind each choice is in the [decision log](docs/DECISIONS.md).

[![Dashboard: delivery & reviews](docs/images/dashboard-delivery.png)](https://olist-analytics-dbt.streamlit.app/)

## Stack

| Layer | Tool |
|---|---|
| Ingestion (EL) | [dlt](https://dlthub.com/) — Kaggle CSV → DuckDB `raw` schema or BigQuery `raw` dataset; order tables arrive in monthly chunks (`merge`, cursor in dlt state) |
| Warehouse | DuckDB (local dev), BigQuery (`prod` / `ci` targets, EU) — same models, identical test results |
| Transformation (T) | dbt Core 1.12 + dbt-duckdb, `dbt_utils`, `dbt_expectations`, `dbt_date` |
| Data product | Descriptor in the Open Data Product Standard (owner, input / output ports) + ODCS data contracts per port, enforced in CI with `datacontract-cli` |
| Semantic layer | dbt Semantic Layer / MetricFlow: business metrics defined once in YAML, queried with `mf` on DuckDB and BigQuery; dashboard KPIs tested against them |
| Orchestration | Airflow 3 + [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos) in Docker Compose: hourly DAG `dlt ingest → dbt` replaying the dump as a live feed, one month per run (watcher mode: one `dbt build` per run, one Airflow task per dbt node); email alerts (failed / fresh / stale) via SMTP, Mailpit locally |
| Dashboard | Streamlit + Altair on Streamlit Community Cloud, reads the Airflow-fed BigQuery marts with a read-only service account; declared as a dbt exposure |
| Code quality | sqlfluff (dbt templater), pyright, pytest, pre-commit hooks, [dbt_project_evaluator](https://github.com/dbt-labs/dbt-project-evaluator) (project structure + governance rules) |
| CI/CD | GitHub Actions. PR: `sqlfluff lint` + `pyright` + `pytest` + dbt project evaluator + Airflow DAG parse → full `dbt build` on DuckDB (+ data contract tests, semantic layer validation, dashboard tests) + slim CI on BigQuery (`state:modified+ --defer` to prod) in per-run datasets. Merge: dlt → BigQuery, `dbt build --target prod`, docs → GitHub Pages. Any failure → email with the failed step and log excerpt (`ALERT_EMAILS`) |
| Environment | uv, Python 3.12 |

## Architecture

```mermaid
flowchart LR
    kaggle[(Kaggle<br/>Olist CSV)] -->|dlt| raw
    subgraph wh[DuckDB dev / BigQuery prod]
        raw[raw<br/>8 tables, all strings] --> staging
        raw --> snapshots[snapshots<br/>SCD2 sellers]
        seed[seed:<br/>category translation] --> marts
        staging[staging<br/>8 views: typed, renamed] --> intermediate
        intermediate[intermediate<br/>6 views: joins, rollups, dedup] --> marts
        marts[marts<br/>star schema tables]
    end
    marts --> docs[dbt docs<br/>GitHub Pages]
    marts --> dash[Streamlit dashboard<br/>dbt exposure]
```

## Lineage

```mermaid
flowchart LR
    classDef stg fill:#e8f1fb,stroke:#4a7ab5
    classDef int fill:#fdf3e1,stroke:#c48a1c
    classDef mart fill:#e6f4ea,stroke:#3a8a4f
    classDef seed fill:#f1e8fb,stroke:#7a4ab5
    classDef snap fill:#f3f3f3,stroke:#777777

    stg_orders[stg_olist__orders]:::stg
    stg_items[stg_olist__order_items]:::stg
    stg_payments[stg_olist__order_payments]:::stg
    stg_reviews[stg_olist__order_reviews]:::stg
    stg_customers[stg_olist__customers]:::stg
    stg_products[stg_olist__products]:::stg
    stg_sellers[stg_olist__sellers]:::stg
    stg_geo[stg_olist__geolocation]:::stg
    translation[product_category_name_translation]:::seed
    src_sellers[(source: sellers)]
    snap_sellers:::snap

    int_pay[int_payments_pivoted_to_orders]:::int
    int_items[int_order_items_aggregated_to_orders]:::int
    int_delivery[int_orders_delivery_metrics]:::int
    int_reviews[int_order_reviews_deduplicated]:::int
    int_zip[int_zip_codes_located]:::int
    int_oc[int_orders_joined_to_customers]:::int

    fct_orders:::mart
    fct_order_items:::mart
    dim_customers:::mart
    dim_sellers:::mart
    dim_products:::mart
    dim_date:::mart

    stg_payments --> int_pay
    stg_items --> int_items
    stg_orders --> int_delivery
    stg_reviews --> int_reviews
    stg_geo --> int_zip
    stg_orders --> int_oc
    stg_customers --> int_oc

    int_oc --> fct_orders
    int_items --> fct_orders
    int_pay --> fct_orders
    int_delivery --> fct_orders
    int_reviews --> fct_orders

    stg_items --> fct_order_items
    int_oc --> fct_order_items

    int_oc --> dim_customers
    int_pay --> dim_customers
    int_zip --> dim_customers

    stg_sellers --> dim_sellers
    int_zip --> dim_sellers

    stg_products --> dim_products
    translation --> dim_products

    src_sellers --> snap_sellers
```

## Data model

```mermaid
erDiagram
    fct_orders }o--|| dim_customers : customer_unique_id
    fct_orders }o--|| dim_date : purchase_date
    fct_order_items }o--|| fct_orders : order_id
    fct_order_items }o--|| dim_products : product_id
    fct_order_items }o--|| dim_sellers : seller_id
    fct_order_items }o--|| dim_customers : customer_unique_id
    fct_order_items }o--|| dim_date : purchase_date
```

| Model | Grain | Notes |
|---|---|---|
| `fct_orders` | order | item, payment, delivery and review measures. Versioned: v2 (latest, table `fct_orders`), v1 deprecated (`fct_orders_v1`) |
| `fct_order_items` | order line | reconciles exactly with `fct_orders.order_amount` (singular test) |
| `dim_customers` | **person** (`customer_unique_id`) | Olist issues a new `customer_id` per order. Location comes from the latest order. Includes LTV and repeat flag |
| `dim_products` | product | category translated to English, fallback `unknown` |
| `dim_sellers` | seller | zip-prefix centroid coordinates |
| `dim_date` | day | `dbt_date.get_date_dimension`, 2016–2019 |

## Testing & data quality

**108 data tests** run on every PR (`dbt build`): primary/foreign keys, accepted values, ranges, composite keys, a row-count parity test, 2 singular tests and a custom generic test `is_not_before` (timestamp chronology).

**5 unit tests** (dbt ≥1.8 `unit_tests:`) check transformation logic on hand-made fixtures, independent of the data:
payment pivot by type, delivery lateness rules (date vs timestamp comparison, undelivered orders),
`dim_customers` lifetime value (canceled orders excluded) and location (latest order, deterministic tie-break),
and the incremental filter of `fct_order_items` (`is_incremental()` overridden, `this` as a fixture).

**Python tests** (`pytest`, [tests/](tests/), no warehouse or download needed) cover the code around dbt:
the dlt pipeline end to end on a tiny fake dump in a temp DuckDB (month cursor, `--next` skipping empty months,
used-up dump, `--until`, re-runs, `--full-refresh`, the feed-status JSON Airflow reads), the parser of dbt's
end-of-run summary, and the alert decisions (OK / STALE / no email, the daily reminder window, which log a
failure email quotes) on fake Airflow task logs. The Streamlit pages have separate tests on built marts: every page
renders, and its KPI tiles equal the [semantic layer](#semantic-layer-metricflow) metrics.

Known defects in the source data are **surfaced as `warn` tests, not hidden**:

| Finding | Handling |
|---|---|
| `review_id` is not unique; 547 orders have several reviews | composite key `(review_id, order_id)`; latest review per order in `int_order_reviews_deduplicated` |
| 278 customer zip prefixes missing from geolocation | `relationships` test, `severity: warn`; dims left-join the centroids |
| 249 orders where amount paid ≠ items + freight (> 1 BRL) | singular test `assert_payments_match_order_amount`, `severity: warn` |
| 2 payments with 0 installments | range test, `severity: warn` |
| 2 product categories have no English translation | `coalesce` fallback to the Portuguese name |
| 1,359 orders handed to the carrier before payment approval (166 before purchase) | `is_not_before`, `severity: warn` |
| 23 orders delivered to the customer before carrier pickup | `is_not_before`, `severity: warn` |

Raw data is loaded as **all-VARCHAR**: zip prefixes keep their leading zeros, and all type casting happens in staging,
where it is visible and tested. Casts use cross-database macros (`dbt.type_timestamp()`, `dbt.datediff`, ...) so
the same models can run on BigQuery.

Mart models have **enforced contracts**: every column is declared with its type, and the build fails if the SQL
returns anything else. Money columns are cast explicitly by the `to_money()` macro (per adapter via
`adapter.dispatch`): `numeric(18, 2)` on DuckDB, `round(x, 2)` as `NUMERIC` on BigQuery, which forbids
parameterized types in `CAST`. Shared column descriptions live in doc blocks (`models/_docs.md`) and are
reused across models.

## Governance

Set up the way a mart would be shared across teams (dbt Mesh), even in a one-repo project:

- **Groups and access** (`models/_groups.yml`): every intermediate and mart model belongs to a domain
  group, `orders` or `parties` (customers and sellers). Marts are `public` and carry enforced contracts;
  intermediate models used inside one group are `private` (a `ref` from another group fails at parse
  time); shared ones stay `protected`.
- **Model versions**: `fct_orders` v2 renamed `payment_value` to `paid_amount`, a breaking change for
  consumers. v1 stays available until its `deprecation_date` as a projection of v2 (one copy of the logic).
  `ref('fct_orders')` resolves to the latest version, which keeps the unsuffixed table name (`alias`);
  a consumer pins the old shape with `ref('fct_orders', v=1)` or the table `fct_orders_v1`.
- **[dbt_project_evaluator](https://github.com/dbt-labs/dbt-project-evaluator)** runs as a CI check on every PR (off in
  all other runs): naming and folder conventions, DAG shape (fan-out, rejoins, root models),
  documentation and primary-key test coverage, public models without contracts, exposures that read
  non-public models. Its first run made the refactor that added `int_orders_joined_to_customers` (three
  marts each joined orders to customers); the two accepted findings are listed with their reasons in
  `seeds/dbt_project_evaluator_exceptions.csv`.

```bash
cd transform
DBT_PROJECT_EVALUATOR=true uv run dbt build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions
```

## Data product

The project is packaged as one data product, [`olist-commerce`](data-product/olist-commerce.odps.yaml)
([ODPS](https://bitol-io.github.io/open-data-product-standard/) v1.1): a Product Owner, a support channel, what it is
for and its limits, an **input port** (the raw Olist tables) and **output ports** (the star schema, the metrics).
Each data port is bound to a versioned [ODCS](https://bitol-io.github.io/open-data-contract-standard/) data contract:

- [`olist-raw`](data-product/contracts/olist-raw.odcs.yaml): what the product expects from its source (tables, keys,
  allowed values, freshness);
- [`olist-marts`](data-product/contracts/olist-marts.odcs.yaml): what it promises to consumers (five marts column for
  column, keys and foreign keys, value rules, freshness SLA, classification of person-related columns), on three
  servers: local DuckDB, BigQuery prod, BigQuery live feed.

dbt is the implementation, the contract is the promise, and CI holds them together: the input contract is tested
right after the ingest, the output contract after `dbt build` (`datacontract ci`, ~370 checks on the data), and
`pytest data-product` checks that the descriptor is valid ODPS, its ports pin existing contract versions, and the
contract matches the public dbt models column for column. Changing a mart without changing its contract fails the PR.

```bash
uv run datacontract test data-product/contracts/olist-marts.odcs.yaml --server local   # or prod / live (BigQuery)
uv run pytest data-product
```

## Semantic layer (MetricFlow)

Business metrics are defined once, in [`_marts__semantic_models.yml`](transform/models/marts/_marts__semantic_models.yml),
on top of the marts. **Semantic models** describe `fct_orders` and `dim_customers`: entities (join keys: `order`,
`customer`), dimensions (status, delivery flags, customer state) and measures (counts, sums, averages).
**Metrics** are built on the measures: simple ones with filters (`revenue` = items + freight of orders not canceled
or unavailable), ratios (`average_order_value`, `late_delivery_rate`, `repeat_customer_rate`). `dim_date` is the
time spine. MetricFlow writes the SQL for the current target, joins included: `late_delivery_rate` by
`customer__state` joins orders to customers by itself.

| Metric | Definition |
|---|---|
| `orders`, `sold_orders` | orders placed; of them, not canceled or unavailable |
| `revenue` | items + freight of sold orders, BRL |
| `average_order_value` | revenue / sold orders |
| `customers`, `revenue_per_customer` | people who ordered in the period; revenue / them |
| `delivered_orders`, `late_deliveries`, `late_delivery_rate` | delivered orders; of them, after the promised date; the share |
| `avg_days_to_deliver`, `avg_review_score` | purchase → delivery, days; latest review score |
| `new_customers`, `repeat_customers`, `repeat_customer_rate` | people by first order (cohort); of them, who ordered again; the share |

```bash
cd transform
uv run dbt parse     # writes target/semantic_manifest.json
uv run mf list metrics
uv run mf query --metrics revenue,average_order_value --group-by metric_time__month --start-time 2018-01-01
uv run mf query --saved-query late_delivery_by_state --order -delivered_orders
uv run mf query --metrics revenue --group-by metric_time__year --explain   # show the generated SQL
DBT_TARGET=prod uv run dbt parse && DBT_TARGET=prod uv run mf query --metrics orders   # BigQuery prod
```

The dashboard keeps its own SQL (it runs on Streamlit Cloud without dbt), and a test holds both sides together:
[`dashboard/tests/test_metrics.py`](dashboard/tests/test_metrics.py) computes every KPI tile and asks `mf query`
for the matching metric on the same data and period; CI runs it after `dbt build`, next to
`mf validate-configs` (every dimension, measure and metric queried against the warehouse). Same numbers on DuckDB
and BigQuery prod (99,441 orders, R$ 15,735,527.03 revenue, 6.77 % late, 3.12 % repeat customers).

## How to run

Requirements: [uv](https://docs.astral.sh/uv/). Kaggle credentials are not needed.

```bash
uv sync                               # Python 3.12 + dependencies
uv run python ingestion/pipeline.py   # download Olist, load into data/olist.duckdb (schema raw)

cd transform
uv run dbt deps
uv run dbt build                      # seed + 21 models + snapshot + 108 data tests + 5 unit tests
uv run dbt docs generate && uv run dbt docs serve
cd .. && uv run pytest                # Python tests (pipeline, alert logic)
```

Simulate data arriving month by month (the dump is static; dlt state remembers the last loaded month):

```bash
uv run python ingestion/pipeline.py --full-refresh --until 2017-06   # orders up to June 2017
uv run python ingestion/pipeline.py --until 2017-07                  # adds July 2017 only
uv run python ingestion/pipeline.py --next                           # adds the next month with orders
uv run python ingestion/pipeline.py                                  # adds the rest; re-run = no-op
cd transform && uv run dbt source freshness                          # checks raw `_loaded_at`
uv run dbt build        # after each chunk: fct_order_items merges only the new rows
```

`snap_sellers` keeps SCD2 history of seller location (`check` strategy on zip / city / state, `hard_deletes: new_record`).
Sellers never change in the dump, so prod holds one version each. To see versions locally:

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

BigQuery (needs a GCP project with billing and a service account with *BigQuery Data Editor* + *BigQuery Job User*):

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json   # Application Default Credentials
export GCP_PROJECT_ID=<your-project>
uv run python ingestion/pipeline.py --destination bigquery
cd transform && uv run dbt build --target prod
```

## Dashboard (Streamlit)

`dashboard/` is a Streamlit app over the marts, declared in dbt as the exposure
`olist_dashboard` (visible in the docs lineage graph; `dbt ls --select +exposure:olist_dashboard`
lists the models it depends on). Three pages: sales overview, delivery & reviews, customers & sellers;
a sidebar date filter applies to every query; the sidebar shows the latest order and when it was loaded.
Live: https://olist-analytics-dbt.streamlit.app/

| Sales overview | Customers & sellers |
|---|---|
| ![Sales overview](docs/images/dashboard-overview.png) | ![Customers & sellers](docs/images/dashboard-customers.png) |

```bash
uv run streamlit run dashboard/app.py                                   # DuckDB: data/olist.duckdb
DASHBOARD_BACKEND=bigquery uv run streamlit run dashboard/app.py        # BigQuery prod marts (ADC key)
DASHBOARD_BACKEND=bigquery MARTS_DATASET=airflow_marts uv run streamlit run dashboard/app.py  # Airflow-fed marts
uv run pytest dashboard                                                 # pages render, KPIs = metrics (also in CI)
```

The pages send plain SQL that both engines accept, so DuckDB and BigQuery give the same numbers
(checked KPI by KPI). Results are cached for an hour, so BigQuery scans stay small.

Deployed on Streamlit Community Cloud from `main` (entrypoint `dashboard/app.py`; it installs the slim
`dashboard/requirements.txt`, not the dbt/dlt project). It reads dataset `airflow_marts`, which the local
Airflow's BigQuery DAG refreshes every hour (see [Orchestration](#orchestration-airflow--cosmos)); prod
`marts` is still built by CD on every merge, and the dashboard can point at either (`MARTS_DATASET`). Since
that Airflow runs on a PC, the data stops moving while the PC is off; the sidebar date and the stale-data
email show it. Access is service account `dashboard-reader`: *BigQuery Job User* on the project +
*BigQuery Data Viewer* on datasets `marts` and `airflow_marts` only, so it cannot see raw data or write
anywhere. App secrets:

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

## Orchestration (Airflow + Cosmos)

Local Airflow 3 replays the dump as a live feed: an **hourly** DAG run loads the next purchase month with
dlt (`--next`, cursor in dlt state), skips dbt when nothing new arrived (dump used up), else runs the dbt
project as a Cosmos task group in **watcher
mode**: a producer task runs `dbt source freshness` and one `dbt build` for the whole project (dbt threads
run independent nodes in parallel), while one sensor task per node (model run, model tests, seed, snapshot,
source) mirrors that node's result from the producer's log. The Airflow graph still shows every dbt node;
dbt starts and parses the project once per run instead of once per task: **~29 → ~6 min per month on
DuckDB, ~19 → ~6–9 min on BigQuery**.

```mermaid
flowchart LR
    ingest[dlt_ingest<br/>next month] --> gate{has_new_orders}
    gate -- yes --> producer[dbt_producer_watcher<br/>freshness + dbt build]
    gate -. no: skip dbt .-> report
    producer -. node results .-> sensors
    sensors --> report[report<br/>email: OK / STALE]
    subgraph sensors[sensor tasks, same graph as dbt]
        direction LR
        src[8 × source] --> stg[8 × stg_*<br/>run + test] --> int[5 × int_*<br/>run + test] --> marts[6 × marts<br/>run + test]
        src --> snap[snap_sellers]
    end
```

**Alerts:** at most one email per run — `FAILED` (DAG failure callback: the error and log tail; for dbt, dbt's
summary naming each failed node), `OK: +800 orders, data up to 2017-01, 3 dbt warnings` (summary, dbt totals,
the warn-level tests), or `STALE: no new orders for 25 h` (about once a day). Recipients and the stale threshold
are Airflow Variables; SMTP goes to a local Mailpit inbox (http://localhost:8025) unless a real server is set in
`airflow/.env`. Failed GitHub Actions runs (PR checks, prod deploy) are emailed too: job `notify-failure` sends
the failed jobs, step and log excerpt to repo variable `ALERT_EMAILS`. Emails are styled HTML, and data-test
findings read in business words ("278 customers — zip code missing from the geolocation reference, so maps leave
them out ..."), taken from each warn test's `description` in the dbt yml.

Two DAGs from one factory: `olist_elt_duckdb` (own file `data/airflow.duckdb`, dlt and the dbt producer
serialized by a 1-slot pool) and `olist_elt_bigquery` (isolated `airflow_*` datasets; prod belongs to CD).
Unpause a DAG and the history arrives in ~25 hourly runs; a manual run with `until` catches up at once:

```bash
docker compose -f airflow/docker-compose.yml up -d --build      # UI: http://localhost:8080
docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger olist_elt_duckdb --conf '{"until": "2017-06"}'
```

The image is prod-like: code, dbt packages and the dbt manifest are baked in and the image is labelled with its
git commit; only `data/` is mounted. Every merge publishes it to GHCR (`ghcr.io/shucig/olist-airflow:<sha>`,
`:latest`), so a version can be pulled or rolled back by tag. A dev compose override mounts the repo for live edits.

Details (BigQuery key, params, alerts, reset, re-running a failed month) in [airflow/README.md](airflow/README.md).
CI builds the image and checks that both DAGs parse in watcher mode with only the writers in the DuckDB pool,
dbt gated by `has_new_orders` and the alerts wired.

## Project structure

```
├── ingestion/pipeline.py        # dlt: Kaggle CSV → DuckDB / BigQuery raw, monthly chunks
├── alerting/                    # alert emails (Airflow + CI): dbt summary parser, decisions, HTML layout
├── tests/                       # pytest: pipeline, dbt summary, alert decisions
├── airflow/                     # Docker Compose, Dockerfile, DAG factory (Cosmos)
├── data-product/                # ODPS descriptor, ODCS contracts (input / output), their tests
├── dashboard/                   # Streamlit app over the marts (DuckDB / BigQuery), AppTest + KPI tests
├── transform/                   # dbt project
│   ├── models/
│   │   ├── staging/olist/       # 1:1 with sources: cast, rename
│   │   ├── intermediate/        # reusable rollups and dedup
│   │   └── marts/               # star schema + semantic models and metrics (MetricFlow)
│   ├── seeds/                   # category translation
│   ├── snapshots/               # snap_sellers (SCD2, YAML-defined)
│   ├── tests/                   # singular tests + generic test is_not_before
│   └── macros/                  # to_money, generate_schema_name, drop_isolated_datasets (CI / Airflow cleanup)
├── .github/workflows/ci-cd.yml  # CI on PR (DuckDB + slim BigQuery), prod deploy + docs on merge
└── docs/                        # DECISIONS.md (architecture decision log), dashboard screenshots
```

## Development workflow

Feature branch → PR → CI must pass → squash merge → CD.

- **PR:** `sqlfluff lint` + `pyright` + `pytest` + Airflow DAG parse check (built image), then in parallel a full `dbt build` on DuckDB (fresh dlt ingest) and slim CI on BigQuery:
  only modified models and their children are built, in datasets `ci_pr_<N>_<run_id>_*`; unmodified parents are read
  from prod (`--defer` to the manifest of the last deploy). The datasets are dropped after the run.
- **Merge to `main`:** dlt → BigQuery `raw` (new months only), `dbt source freshness`, `dbt build --target prod`, dbt docs (generated from prod) → GitHub Pages,
  `manifest.json` stored as an artifact for slim CI, Airflow image (code + manifest baked in) → GHCR.

Locally, `uv run pre-commit install` enables the same lint, type check and Python tests plus hygiene hooks before every commit.
`main` is protected. Design decisions are logged in [docs/DECISIONS.md](docs/DECISIONS.md).

## Roadmap

- [x] **Phase 1 — MVP:** dlt ingestion, staging / intermediate / marts, data tests, CI/CD, docs on Pages
- [x] **Phase 2 — Quality:** dbt unit tests, model contracts, custom generic test, sqlfluff + pre-commit
- [x] **Phase 3 — BigQuery:** prod/CI targets, slim CI (`state:modified+ --defer`) in ephemeral datasets, CD on merge
- [x] **Phase 4 — Incremental:** month-by-month loads, incremental fact, SCD2 snapshot, source freshness
- [x] **Phase 5 — Orchestration:** Airflow 3 + Astronomer Cosmos (Docker), DAG factory for both warehouses, watcher mode
- [x] **Phase 6 — Dashboard:** Streamlit over the marts (DuckDB / BigQuery), dbt exposure, public deploy
- [x] **Phase 7 — Live feed:** hourly replay of the dump in Airflow, email alerts, dashboard on the Airflow-fed marts
