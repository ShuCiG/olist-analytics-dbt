# Progress

**Current phase:** 2 — Quality
**Next step:** PR 2.2 `feat/unit-tests` — dbt unit tests for int_payments_pivoted_to_orders, int_orders_delivery_metrics, dim_customers.

## Phases

### Phase 1 — MVP (local, DuckDB) ✅
Goal: honest "dbt Core, hands-on project" on resume.
- [x] git, uv, Python 3.12, deps, .gitignore
- [x] CLAUDE.md, docs/DECISIONS.md, docs/PROGRESS.md
- [x] Download Olist CSV (kagglehub, no token needed; cached in ~/.cache/kagglehub)
- [x] dlt pipeline `ingestion/pipeline.py`: CSV → `data/olist.duckdb` schema `raw`, all columns VARCHAR, replace
- [x] dbt project in `transform/` (run dbt from there), profile duckdb dev, dbt_utils 1.4.1 + dbt_expectations 0.10.10, custom generate_schema_name
- [x] sources.yml (+ descriptions)
- [x] seed: product_category_name_translation (+ tests)
- [x] staging: 8 models + 45 tests (PASS 52, WARN 2 expected)
- [x] intermediate (5): int_payments_pivoted_to_orders, int_order_items_aggregated_to_orders, int_orders_delivery_metrics, int_order_reviews_deduplicated, int_zip_codes_located
- [x] marts (6): dim_customers, dim_sellers, dim_products, dim_date (dbt_date), fct_orders, fct_order_items
- [x] 2 singular tests; `dbt build` PASS 103, WARN 3 (expected), 19 models; `dbt docs generate` ok
- [x] Git flow: branches + PRs, user merges; branch protection on main; CI/CD (dbt build on PR, docs → GitHub Pages on merge)
- [x] README (EN): badges, architecture + lineage + ER diagrams (Mermaid), data-quality findings, how to run, roadmap
- [x] Push to GitHub (public): https://github.com/ShuCiG/olist-analytics-dbt (pushed early, commit per step)

### Phase 2 — Quality & CI ⏳
- [x] PR 2.1: sqlfluff (dbt templater) + pre-commit + lint job in CI (gates dbt build)
- [ ] PR 2.2: unit tests
- [ ] PR 2.3: contracts on marts, custom generic test, doc blocks
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
- 2026-10-10 — Intermediate + marts done (19 models, 106 tests). Sanity: 96,096 customers, 2,997 repeat (~3%), LTV total ~R$15.7M, avg delivery 12.5 days, 6.8% late. 249 orders paid ≠ items+freight → warn singular test. Repo pushed public.
- 2026-10-10 — Switched to PR flow (user request): feature branches, user merges, CI/CD in GitHub Actions pulled forward from phase 2 (full dbt build with real data in CI; docs → Pages on main).
- 2026-10-10 — PR #1 merged (CI/CD live, docs at https://shucig.github.io/olist-analytics-dbt/). README written (Mermaid lineage generated from manifest.json). Phase 1 done on merge.
- 2026-10-10 — PR #2 (README) merged → Phase 1 done. PR 2.1: sqlfluff + pre-commit + CI lint gate.
