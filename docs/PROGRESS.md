# Progress

**Current phase:** 4 — Incremental realism (in progress)
**Next step:** PR 4.3 — snapshot `snap_sellers` (SCD2).

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

### Phase 2 — Quality & CI ✅
- [x] PR 2.1: sqlfluff (dbt templater) + pre-commit + lint job in CI (gates dbt build)
- [x] PR 2.2: unit tests (4: payments pivot, delivery lateness, dim_customers LTV + location)
- [x] PR 2.3: contracts on 5 marts (+ PK constraints), `to_money()` macro, generic test `is_not_before`, doc blocks + `__overview__`
Unit tests, model contracts, custom generic test, macros, sqlfluff + pre-commit, GitHub Actions CI (lint + `build --empty` + unit tests), docs → GitHub Pages.

### Phase 3 — BigQuery + CD ✅
- [x] GCP project `olist-analytics-dbt-472315` (EU), billing + $1 budget alert, SA `dbt-runner`, key outside repo, `GOOGLE_APPLICATION_CREDENTIALS` (user env)
- [x] PR 3.1: `prod`/`ci` BigQuery targets, dlt `--destination bigquery`, cross-DB fixes (seed types, money/float contract types). BQ and DuckDB both PASS 111, WARN 6
- [x] PR 3.2: secret `GCP_SA_KEY`; CI job `dbt build (BigQuery)` on PRs in `ci_pr_<N>_<run_id>_*` datasets, dropped after (`drop_ci_datasets`, guarded to target `ci`)
- [x] PR 3.3 (+3.4 merged into it): CD on main (dlt → BQ, `dbt build --target prod`, docs from prod, manifest artifact) + slim CI (`state:modified+ --defer`, full-build fallback)

### Phase 4 — Incremental realism
- [x] PR 4.1: dlt monthly chunks (`--until`, state cursor, merge for order tables), `_loaded_at`, source freshness (CI + CD), pyright clean
- [x] PR 4.1b: pyright in pre-commit + CI (gates `dbt build`)
- [x] PR 4.2: incremental `fct_order_items` (merge, `_loaded_at` cursor, BQ partitions, unit test of incremental branch), CD `full_refresh` input
- [ ] PR 4.3: snapshot `snap_sellers` (SCD2)

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
- 2026-10-11 — PR #3 merged. PR 2.2: 4 dbt unit tests; mutation-checked (flipped sort / timestamp-vs-date compare → tests fail). `dbt build` PASS 107, WARN 3. README test count fixed (106 was build nodes; 86 data tests).
- 2026-10-11 — PR #4 merged. PR 2.3: contracts on marts (DuckDB checks decimal precision → money cast to numeric(18,2)); `is_not_before` found 3 new source defects (warn). `dbt build` PASS 111, WARN 6. Phase 2 done.
- 2026-10-11 — PR #5 merged → Phase 2 done. Phase 3 started: GCP setup with user (sandbox blocked dlt DML → billing). PR 3.1: BigQuery targets + cross-DB type fixes; `dbt build` PASS 111, WARN 6 on BigQuery and DuckDB.
- 2026-10-11 — PR #6 merged. PR 3.2: BigQuery CI job with per-run datasets + cleanup macro; secret set and $1 budget alert confirmed by user.
- 2026-10-11 — PR #7 merged. PR 3.3: CD to BigQuery prod + slim CI. Local check: unchanged state → empty selection; touched `dim_sellers` → 4 nodes, parents deferred to prod datasets.
- 2026-10-11 — PR #8 merged. First prod deploy on main green (ingest, build, docs from BigQuery, `prod-manifest` artifact). Smoke-test PR #9 (closed unmerged): slim CI built 4/117 nodes, parents deferred to prod. Phase 3 done.
- 2026-10-11 — Phase 4 started. PR 4.1: monthly-chunk dlt loads (checked: 14,940 + 4,026 + 80,475 = 99,441 orders, re-run = 0), freshness warn-only; verified on scratch BigQuery datasets (chunks + migration of old `raw`). Pylance issues fixed (pyarrow-stubs, typed credentials).
- 2026-10-11 — PR 4.1b (stacked on 4.1, user request): pyright config shared with Pylance, pre-commit hook + CI job; mutation-checked (injected type error → hook fails).
- 2026-10-11 — PR 4.2: incremental `fct_order_items`; chunked incremental result = full refresh on DuckDB and BigQuery. `dbt build` PASS 115, WARN 6 (121 nodes). Prod needs one-off full refresh of `fct_order_items` (old table has no `_loaded_at`).
