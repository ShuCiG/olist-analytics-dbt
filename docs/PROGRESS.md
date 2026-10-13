# Progress

**Current phase:** 7 — Live feed + alerts (D28)
**Next step:** PR 7.3 dashboard on `airflow_marts` (needs the user's GCP step: `dashboard-reader` Data Viewer on `airflow_marts`). Ask before starting.

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

### Phase 4 — Incremental realism ✅
- [x] PR 4.1: dlt monthly chunks (`--until`, state cursor, merge for order tables), `_loaded_at`, source freshness (CI + CD), pyright clean
- [x] PR 4.1b: pyright in pre-commit + CI (gates `dbt build`)
- [x] PR 4.2: incremental `fct_order_items` (merge, `_loaded_at` cursor, BQ partitions, unit test of incremental branch), CD `full_refresh` input
- [x] PR 4.3: snapshot `snap_sellers` (SCD2, check strategy, hard deletes), CI cleanup covers `_snapshots`

### Phase 5 — Airflow + Cosmos
Docker Compose, DAG: dlt ingest → dbt build (Cosmos task group), backfill.
- [x] PR 5.1: Airflow 3.3.2 + Cosmos 1.15.1 in Docker Compose; DAG factory `olist_elt_duckdb` / `olist_elt_bigquery` (monthly, `--until` = interval month); DuckDB pool; backfill 2016-09..11 verified
- [x] PR 5.2: CI job `airflow DAGs parse` (built image), Cosmos `DBT_MANIFEST` (fixes endless dbt-ls retries), BigQuery DAG verified (2016-09..10 = DuckDB results, prod untouched), `drop_isolated_datasets` (ci + airflow), task logs in UI (shared secret_key + logs volume), README section
- [x] PR 5.3: Cosmos `ExecutionMode.WATCHER` (D25): DuckDB ~29 → ~6 min/month, BigQuery ~19 → 6–9 min; same data; CI check asserts watcher + DuckDB pool = writers only

### Phase 6 — Dashboard
Streamlit over marts, dbt exposures, README screenshots.
- [x] PR 6.1: Streamlit app `dashboard/` (3 pages, DuckDB/BigQuery backends, D26), exposure `olist_dashboard`, pyright + AppTest smoke test in CI
- [x] PR 6.2: deploy prep (D27): SA `dashboard-reader` (Job User + Data Viewer on `marts`), `dashboard/requirements.txt`, settings from `st.secrets`; app live at https://olist-analytics-dbt.streamlit.app/ (deploys from `main` on push)
- [x] PR 6.3: exposure `url`, README screenshots + live link

### Phase 7 — Live feed + alerts
User question: new data must flow on a schedule, up to the dashboard, with email reports (D28).
- [x] PR 7.1: hourly replay — `pipeline.py --next` (next month that has orders), DAGs `@hourly` without `end_date`, `has_new_orders` short-circuit skips dbt once the dump is used up; CI parse check asserts the gate
- [x] PR 7.2: email alerts (D29) — `report` task (OK / STALE, ~daily) + DAG failure callback, Mailpit in Compose + optional real SMTP (`SMTP_CONN`), Variables `olist_alert_emails` / `olist_stale_after_hours`; pipeline prints feed status JSON; shared Fernet key
- [x] PR 7.2 (cont., user request): OK email shows dbt totals + warn-level tests; GitHub Actions `notify-failure` job (D30) emails failed jobs + log excerpt to `vars.ALERT_EMAILS` via secret `SMTP_CONN`
- [ ] PR 7.3: dashboard on `airflow_marts` (setting `MARTS_DATASET`, "data as of" caption); user grants `dashboard-reader` Data Viewer on `airflow_marts`

## Backlog (ideas, not scheduled)
- **Airflow, further speed-up** (after watcher, D25): one dbt invocation in the producer (freshness is a second dbt start + parse, ~40–60 s); seed a per-target partial parse so the first run of a container doesn't reparse (Cosmos picks the host's `target/partial_parse.msgpack` otherwise); dbt's opt-in Rust parser (check BigQuery/package compatibility first).
- **Airflow, prod-like image:** bake the project into the image instead of the bind mount (loses live edit).

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
- 2026-10-12 — PR #13 merged; auto deploy failed as predicted on old prod `fct_order_items` (no `_loaded_at`); user ran CD with `full_refresh=true` → green, prod table MONTH-partitioned + clustered, 112,650 rows. Lesson: drop/migrate before merge, the deploy starts right away.
- 2026-10-12 — PR 4.3: `snap_sellers`; demo change/delete/restore → correct versions; reload without changes = no versions. `dbt build` PASS 122, WARN 6 (128 nodes). Phase 4 done on merge.
- 2026-10-12 — PR #14 merged → Phase 4 done. Phase 5 started (user: both warehouses → two DAGs from one factory, D22). C: full broke the image build; user moved Docker's disk to D:. Fixes found by running: Cosmos drops package-lock.yml (declared dbt_date), eager test selection (→ cautious + detached multi-parent tests), backfill ignores DAG max_active_runs (→ `--max-active-runs 1`). Backfill 2016-09..11: raw/fct_orders = CSV (4, 324, 0), fct_order_items 369 rows over 2 loads, snapshot 1 version/seller. ~30 s/task, ~25 min/month on DuckDB.
- 2026-10-12 — PR #15 merged (prod deploy green). PR 5.2: CI parse job; BigQuery DAG run exposed endless retries (Airflow 3 re-parses the DAG per task; Cosmos dbt-ls cache unwritable from tasks → 2 uncached dbt ls per task, >3 min under load) → `DBT_MANIFEST`. BigQuery backfill 2016-09..10: 46/46 tasks per run, raw/fct_orders 4 + 324, fct_order_items 369 over 2 loads, snapshot 1 version/seller, prod `fct_orders` still 99,441; datasets dropped after (macro now also drops dlt's `_raw_staging`). UI task logs fixed (shared `[api] secret_key` + logs volume). Speed analysis → backlog.
- 2026-10-12 — PR #16 merged (Phase 5 done). PR `fix/ci-warnings`: prod deploy log cleaned up (D24), only 6 expected test WARNs + BigQuery numeric notice remain. Airflow analysis: tasks start from the host's (Windows) `partial_parse.msgpack` → dbt errors and fully reparses (~60 s); plan = WATCHER mode (one `dbt build` per run), DuckDB pool on the producer only.
- 2026-10-12 — PR #17 merged; deploy log clean (only 6 expected test WARNs + 3 numeric notices). PR 5.3 watcher mode: baseline on current code DuckDB 29.1 / 28.0 min (2016-09/10); watcher 5.6 / 5.7; BigQuery 8.9 / 6.1 (was 18.9). Found by running: no BUILD behavior in watcher, detached tests get no status, default freshness callback would skip models, producer skips on attempt 2+ → sensors fall back outside the DuckDB pool (lock errors) → retries=0 + re-run = new DAG run. Docker Desktop updated mid-session (29.8.2). Backups `*.bak-*` of airflow.duckdb / dlt state left in data/ and the dlt-pipelines volume for the user to delete.
- 2026-10-12 — PR #18 merged (watcher). PR `chore/project-cleanup`: project-wide review — all model/seed/snapshot columns documented (was ~25 undocumented in staging/intermediate/seed), README (CI/CD row, money types per engine, snapshot in diagrams, structure), CLAUDE.md layout/CI/commands, overview doc block, airflow README re-run command, workflow whitespace, unused `analyses` path, backlog tidied. Verified: pre-commit all files, `dbt build` PASS 122 WARN 6, Airflow `full_refresh` param in watcher mode (dlt reload 328 orders, `dbt build --full-refresh`, 53/53, 6.1 min).
- 2026-10-13 — Cleanup PR merged. Phase 6 started (user: both backends + public deploy, 3 pages). PR 6.1: Streamlit app + exposure; KPIs identical on DuckDB and BigQuery prod (99,092 orders / R$ 15.68M / 6.8 % late / 3.1 % repeat for 2017-01..2018-08); screenshots checked light + dark. Found by looking: Vega hid every other bar label, edge months (2016, Sep–Oct 2018) faked spikes → full-month default period.
- 2026-10-13 — PR #20 merged (deploy green). PR 6.2: user created SA `dashboard-reader` (key `~/.gcp/dashboard-reader.json`); verified via AppTest secrets: all pages on BigQuery with the key from `st.secrets`, no ADC; marts readable, `raw` / writes Forbidden. Found: settings read at import missed secrets-as-env (Streamlit exports them lazily) → read `st.secrets` directly. Slim requirements checked in a clean venv.
- 2026-10-13 — PR #21 merged. User authorized Streamlit Community Cloud on GitHub (reviewed its OAuth scopes: `public_repo` includes write; accepted, main is protected, revocable) and created the app from `main` (Python 3.12, secrets from the reader key; temp TOML deleted). Live app checked in a browser: 3 pages on BigQuery prod, KPIs = local. PR 6.3: README live link + badge + screenshots (`docs/images/`, taken on BigQuery), exposure `url`, dashboard in stack + architecture diagram. Phase 6 done on merge.
- 2026-10-13 — PR #22 merged → Phase 6 done. Phase 7 planned with the user (D28: replay hourly, dashboard on `airflow_marts`, Mailpit + real SMTP). PR 7.1: `--next` + hourly DAGs. Found: the old `@monthly` DAG with `end_date` 2018-10 never ran by itself. Empty month 2016-11 would freeze a "next calendar month" cursor → next month *with orders*. Verified: the first scheduled run fired by itself on unpause (DuckDB: 2016-12, 1 order; BigQuery from empty: 2016-09, 4); manual `--next` 2016-10 (324); `until=2018-10` catch-up; exhausted run → `orders_loaded=0`, 52 dbt tasks skipped (42 s DuckDB / 77 s BigQuery). Both warehouses = prod: 99,441 orders / 112,650 items (3 incremental loads) / 96,096 customers. CI check mutation-tested. BigQuery DAG left unpaused (hourly runs now only reload reference tables, dbt skipped).
- 2026-10-13 — PR #23 merged. PR 7.2 alerts. Verified in real runs (Mailpit, 2 recipients): FAILED (`until=2016-13` → dlt fails, one email for 55 failed/upstream_failed tasks), OK (`full_refresh` on DuckDB: "+4 orders, data up to 2016-09", 55/55), STALE on BigQuery (threshold 1 h, 1.5 h since orders); exhausted + fresh → no email; a run past the reminder window → no email (by design). Found: recipients fell back to the default — per-container random Fernet keys, `Can't decrypt _val` in dag-processor / api-server → shared key; `airflow tasks test` hid it. dlt `pipeline.dataset()` broken with installed sqlglot → `sql_client()`. DuckDB feed restarted from 2016-09 by the test (hourly OK emails for ~a day); BigQuery feed stays exhausted. Then (user feedback): Gmail SMTP set up by the user (`SMTP_CONN`, app password) and verified; `airflow-init` creates the alert Variables when missing; failure email rewritten — error + log tail, dbt failures show dbt's summary (checked with a broken `stg_olist__sellers`: "Binder Error: column seller_town not found", reverted). Then: OK email carries dbt totals + warnings (dry-run on the 14:00 run: "3 dbt warnings"); CI/CD failure emails (D30): script tested locally on the old failed deploy (email sent); repo variable `ALERT_EMAILS` set; secret `SMTP_CONN` must be set by the user (auto mode blocks secret writes).
