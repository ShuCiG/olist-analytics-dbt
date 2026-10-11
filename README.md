# Olist Analytics — dbt project

[![CI/CD](https://github.com/ShuCiG/olist-analytics-dbt/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/ShuCiG/olist-analytics-dbt/actions/workflows/ci-cd.yml)
[![dbt docs](https://img.shields.io/badge/dbt_docs-lineage-FF694B?logo=dbt&logoColor=white)](https://shucig.github.io/olist-analytics-dbt/)

An end-to-end ELT project on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(~100k orders, 2016–2018, 8 related tables). Raw data is loaded with **dlt** into **DuckDB** and modeled with
**dbt Core** into a tested star schema. Every pull request rebuilds the whole pipeline in CI. Every merge
publishes the dbt docs.

**📖 Live dbt docs with lineage graph:** https://shucig.github.io/olist-analytics-dbt/

## Stack

| Layer | Tool |
|---|---|
| Ingestion (EL) | [dlt](https://dlthub.com/) — Kaggle CSV → DuckDB `raw` schema or BigQuery `raw` dataset |
| Warehouse | DuckDB (local dev), BigQuery (`prod` / `ci` targets, EU) — same models, identical test results |
| Transformation (T) | dbt Core 1.12 + dbt-duckdb, `dbt_utils`, `dbt_expectations`, `dbt_date` |
| Code quality | sqlfluff (dbt templater) + pre-commit hooks |
| CI/CD | GitHub Actions: `sqlfluff lint` → full `dbt build` on every PR, docs → GitHub Pages on merge |
| Environment | uv, Python 3.12 |

## Architecture

```mermaid
flowchart LR
    kaggle[(Kaggle<br/>Olist CSV)] -->|dlt| raw
    subgraph duckdb[DuckDB]
        raw[raw<br/>8 tables, all VARCHAR] --> staging
        seed[seed:<br/>category translation] --> marts
        staging[staging<br/>8 views: typed, renamed] --> intermediate
        intermediate[intermediate<br/>5 views: rollups, dedup] --> marts
        staging --> marts
        marts[marts<br/>star schema tables]
    end
    marts --> docs[dbt docs<br/>GitHub Pages]
```

## Lineage

```mermaid
flowchart LR
    classDef stg fill:#e8f1fb,stroke:#4a7ab5
    classDef int fill:#fdf3e1,stroke:#c48a1c
    classDef mart fill:#e6f4ea,stroke:#3a8a4f
    classDef seed fill:#f1e8fb,stroke:#7a4ab5

    stg_orders[stg_olist__orders]:::stg
    stg_items[stg_olist__order_items]:::stg
    stg_payments[stg_olist__order_payments]:::stg
    stg_reviews[stg_olist__order_reviews]:::stg
    stg_customers[stg_olist__customers]:::stg
    stg_products[stg_olist__products]:::stg
    stg_sellers[stg_olist__sellers]:::stg
    stg_geo[stg_olist__geolocation]:::stg
    translation[product_category_name_translation]:::seed

    int_pay[int_payments_pivoted_to_orders]:::int
    int_items[int_order_items_aggregated_to_orders]:::int
    int_delivery[int_orders_delivery_metrics]:::int
    int_reviews[int_order_reviews_deduplicated]:::int
    int_zip[int_zip_codes_located]:::int

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

    stg_orders --> fct_orders
    stg_customers --> fct_orders
    int_items --> fct_orders
    int_pay --> fct_orders
    int_delivery --> fct_orders
    int_reviews --> fct_orders

    stg_items --> fct_order_items
    stg_orders --> fct_order_items
    stg_customers --> fct_order_items

    stg_customers --> dim_customers
    stg_orders --> dim_customers
    int_pay --> dim_customers
    int_zip --> dim_customers

    stg_sellers --> dim_sellers
    int_zip --> dim_sellers

    stg_products --> dim_products
    translation --> dim_products
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
| `fct_orders` | order | item, payment, delivery and review measures |
| `fct_order_items` | order line | reconciles exactly with `fct_orders.order_amount` (singular test) |
| `dim_customers` | **person** (`customer_unique_id`) | Olist issues a new `customer_id` per order. Location comes from the latest order. Includes LTV and repeat flag |
| `dim_products` | product | category translated to English, fallback `unknown` |
| `dim_sellers` | seller | zip-prefix centroid coordinates |
| `dim_date` | day | `dbt_date.get_date_dimension`, 2016–2019 |

## Testing & data quality

**93 data tests** run on every PR (`dbt build`): primary/foreign keys, accepted values, ranges, composite keys, a row-count parity test, 2 singular tests and a custom generic test `is_not_before` (timestamp chronology).

**4 unit tests** (dbt ≥1.8 `unit_tests:`) check transformation logic on hand-made fixtures, independent of the data:
payment pivot by type, delivery lateness rules (date vs timestamp comparison, undelivered orders),
and `dim_customers` lifetime value (canceled orders excluded) and location (latest order, deterministic tie-break).

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
returns anything else. Money columns are cast explicitly to `numeric(18, 2)` (`to_money()` macro). Shared column
descriptions live in doc blocks (`models/_docs.md`) and are reused across models.

## How to run

Requirements: [uv](https://docs.astral.sh/uv/). Kaggle credentials are not needed.

```bash
uv sync                               # Python 3.12 + dependencies
uv run python ingestion/pipeline.py   # download Olist, load into data/olist.duckdb (schema raw)

cd transform
uv run dbt deps
uv run dbt build                      # seed + 19 models + 93 data tests + 4 unit tests
uv run dbt docs generate && uv run dbt docs serve
```

BigQuery (needs a GCP project with billing and a service account with *BigQuery Data Editor* + *BigQuery Job User*):

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json   # Application Default Credentials
export GCP_PROJECT_ID=<your-project>
uv run python ingestion/pipeline.py --destination bigquery
cd transform && uv run dbt build --target prod
```

## Project structure

```
├── ingestion/pipeline.py        # dlt: Kaggle CSV → DuckDB raw
├── transform/                   # dbt project
│   ├── models/
│   │   ├── staging/olist/       # 1:1 with sources: cast, rename
│   │   ├── intermediate/        # reusable rollups and dedup
│   │   └── marts/               # star schema
│   ├── seeds/                   # category translation
│   ├── tests/                   # singular tests
│   └── macros/                  # generate_schema_name (dev/prod vs CI isolation)
├── .github/workflows/ci-cd.yml  # CI on PR, docs deploy on merge
└── docs/                        # PROGRESS.md, DECISIONS.md (architecture decision log)
```

## Development workflow

Feature branch → PR → CI (`sqlfluff lint`, then `dbt build` on real data) must pass → squash merge → dbt docs redeployed.
Locally, `uv run pre-commit install` enables the same lint plus hygiene hooks before every commit.
`main` is protected. Design decisions are logged in [docs/DECISIONS.md](docs/DECISIONS.md).

## Roadmap

- [x] **Phase 1 — MVP:** dlt ingestion, staging / intermediate / marts, data tests, CI/CD, docs on Pages
- [x] **Phase 2 — Quality:** dbt unit tests, model contracts, custom generic test, sqlfluff + pre-commit
- [ ] **Phase 3 — BigQuery:** prod/CI targets, slim CI (`state:modified+ --defer`) in ephemeral datasets
- [ ] **Phase 4 — Incremental:** month-by-month loads, incremental fact, SCD2 snapshot, source freshness
- [ ] **Phase 5 — Orchestration:** Airflow + Astronomer Cosmos (Docker)
- [ ] **Phase 6 — Dashboard:** Streamlit over marts, dbt exposures
