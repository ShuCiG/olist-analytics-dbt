# Progress

**Current phase:** 1 — MVP
**Next step:** intermediate models (int_payments_pivoted, int_order_items_enriched, int_orders_delivery_metrics, int_zip_codes_located), then marts.

## Phases

### Phase 1 — MVP (local, DuckDB) ⏳
Goal: honest "dbt Core, hands-on project" on resume.
- [x] git, uv, Python 3.12, deps, .gitignore
- [x] CLAUDE.md, docs/DECISIONS.md, docs/PROGRESS.md
- [x] Download Olist CSV (kagglehub, no token needed; cached in ~/.cache/kagglehub)
- [x] dlt pipeline `ingestion/pipeline.py`: CSV → `data/olist.duckdb` schema `raw`, all columns VARCHAR, replace
- [x] dbt project in `transform/` (run dbt from there), profile duckdb dev, dbt_utils 1.4.1 + dbt_expectations 0.10.10, custom generate_schema_name
- [x] sources.yml (+ descriptions)
- [x] seed: product_category_name_translation (+ tests)
- [x] staging: 8 models + 45 tests (PASS 52, WARN 2 expected)
- [ ] intermediate: int_payments_pivoted, int_order_items_enriched, int_orders_delivery_metrics, int_zip_codes_located
- [ ] marts: dim_customers, dim_sellers, dim_products, dim_date, fct_orders, fct_order_items
- [ ] singular test(s), `dbt build` green, `dbt docs generate`
- [ ] README (EN): goal, stack, lineage screenshot, how to run
- [ ] Push to GitHub (public)

### Phase 2 — Quality & CI
Unit tests, model contracts, custom generic test, macros, sqlfluff + pre-commit, GitHub Actions CI (lint + `build --empty` + unit tests), docs → GitHub Pages.

### Phase 3 — BigQuery + CD
GCP project, service account, `ci`/`prod` targets, dlt → BigQuery, slim CI with ephemeral datasets, deploy on merge.

### Phase 4 — Incremental realism
dlt monthly-chunk loads, incremental `fct_order_items`, snapshot `snap_sellers` (SCD2), source freshness.

### Phase 5 — Airflow + Cosmos
Docker Compose, DAG: dlt ingest → dbt build (Cosmos task group), backfill.

### Phase 6 — Dashboard
Streamlit over marts, dbt exposures, README screenshots.

## Session log
- 2026-10-10 — Planning, stack chosen (D1–D7). Phase 1 setup: git, uv, deps installed (dbt-core 1.12.5, dbt-duckdb 1.11.0, dbt-bigquery 1.12.1, dlt 1.31.0, duckdb 1.5.6).
- 2026-10-10 — dlt pipeline done: 8 raw tables loaded (orders 99,441; order_items 112,650; geolocation 1,000,163). Raw = all VARCHAR, typing in staging. Translation CSV (has BOM) → dbt seed. No commits yet.
- 2026-10-10 — dbt staging done. Data quirks found (→ tests): review_id not unique (use review_id+order_id; 547 orders have >1 review); customer_id is per-order, person = customer_unique_id (96,096); 278 customer zips missing in geolocation (warn); 2 categories lack translation; 1 order without payments; 2 payments with 0 installments (warn).
