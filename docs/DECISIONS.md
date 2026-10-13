# Decisions log

Short ADR-style entries. Newest last. Format: context → decision → why.

## D1 · 2026-10-10 · Dataset: Olist Brazilian e-commerce (Kaggle)
Real data, 9 related tables, natural star schema. Rejected: jaffle_shop / TPC-H (too common, synthetic), NYC taxi (less business-like).

## D2 · 2026-10-10 · Warehouse: DuckDB (dev) + BigQuery (CI/prod, phase 3)
DuckDB = zero infra, fast local loop. BigQuery adds a real cloud DWH and enables true CD. Two targets force cross-DB SQL.
Rejected: Snowflake (30-day trial → project dies), Postgres (OLTP, weak for DWH showcase), Databricks (heavier).

## D3 · 2026-10-10 · Tooling
- uv + Python 3.12 pinned (dbt does not support 3.14)
- dbt-core 1.12, dbt-duckdb 1.11, dbt-bigquery 1.12
- dlt for ingestion (ELT pattern dlt + dbt)
- sqlfluff (dbt templater) + pre-commit
- Airflow 3 + Astronomer Cosmos in Docker (chosen over Dagster: more common in job postings)
- Streamlit dashboard (chosen over Evidence: Python-only, no Node)

## D4 · 2026-10-10 · CI/CD on GitHub Actions
PR: lint → slim CI (`state:modified+ --defer`) into ephemeral BigQuery dataset `ci_pr_<n>` → unit tests; drop dataset on close.
Merge to main: `dbt build --target prod`, store manifest artifact, publish dbt docs to GitHub Pages.
Before BigQuery exists: lint + `dbt build --empty` + unit tests.

## D5 · 2026-10-10 · Phased delivery, MVP first
Each phase must leave the repo in a presentable state. See PROGRESS.md for phase scope.

## D6 · 2026-10-10 · Realistic increments (phase 4)
Olist is a static dump. dlt will load by purchase-month chunks to simulate daily batches, so incremental models, snapshots (SCD2) and Airflow backfill behave like production.

## D7 · 2026-10-10 · Repo
Public `github.com/ShuCiG/olist-analytics-dbt`. GCP service-account key lives only in GitHub Secrets; never in repo.

## D8 · 2026-10-10 · Raw layer is all-VARCHAR
dlt loads every column as string; dbt staging casts types. Why: zip prefixes have leading zeros, raw mirrors source exactly, casting logic is visible and tested in dbt.

## D9 · 2026-10-10 · dbt conventions
- dbt runs from `transform/`; `profiles.yml` committed (no secrets; DuckDB path via `DBT_DUCKDB_PATH`).
- Custom `generate_schema_name`: dev/prod → plain schema (`staging`, `marts`); other targets (CI) → `<target_schema>_<schema>` for isolation.
- Test syntax uses `arguments:` block (dbt ≥1.10 style).
- Known data defects get `severity: warn` instead of being hidden — documents data quality.
- dim_customers grain = `customer_unique_id` (person), not order-level `customer_id`.

## D10 · 2026-10-10 · Mart modeling choices
- `order_amount` (items + freight) and `payment_value` (actually paid) kept separate; mismatch >1 BRL is a warn test, not "fixed".
- Latest review per order wins (`int_order_reviews_deduplicated`).
- Geolocation collapsed to zip centroid (avg lat/lng); dims left-join it (278 customer zips unmatched).
- Missing category → English translation → Portuguese name → 'unknown'.
- `dim_date` via `dbt_date.get_date_dimension` (2016–2019).

## D11 · 2026-10-10 · PR-based git flow, CI/CD pulled into MVP
Context: direct pushes to main were not how a real team works.
Decision: feature branches → PR → user merges; `main` protected (PR + green `dbt build` check required, no direct push).
CI runs the full pipeline with real data (kagglehub needs no token; dataset cached), not `--empty` — data is small (~45MB, ~1 min).
CD: on merge, dbt docs (static, with lineage) deployed to GitHub Pages.
Supersedes the CI part of D4 until BigQuery (phase 3).

## D12 · 2026-10-10 · SQL linting
- sqlfluff with **dbt templater** (renders real macros), dialect duckdb, max line 120.
- Templater uses a dedicated `lint` target (DuckDB `:memory:`): no data or DB file needed, works in CI and from repo root.
- ST06 (column order by expression type) excluded: marts group columns by business meaning.
- Jinja loop bodies indented one level; avoid whitespace control (`-%}`) inside select lists, it confuses LT02.
- CI: `sqlfluff lint` job gates `dbt build` (`needs:`). Both are required checks (a skipped required job would otherwise count as passing).
- pre-commit: standard hygiene hooks + `no-commit-to-branch main` + local sqlfluff hook via `uv run` (same version as CI).

## D13 · 2026-10-11 · Unit tests
- Native dbt `unit_tests:` (≥1.8), in `_<layer>__unit_tests.yml` next to model ymls.
- Unit-test business rules, not SQL: only models with real logic (pivot, lateness, LTV/location). Staging renames are covered by data tests.
- Fixtures list only columns the model reads; `expect` lists only asserted columns.
- Fixture timestamps avoid midnight-crossing intervals: `dbt.datediff` counts boundaries on DuckDB, BigQuery semantics may differ.
- New tests are mutation-checked once (break the rule, confirm failure).
- Unit tests run inside `dbt build` (before the model is built; parents must exist). Prod runs (phase 3) will add `--exclude-resource-type unit_test`.

## D14 · 2026-10-11 · Contracts, money type, generic test, doc blocks
- Enforced contracts on project-owned marts (`dim_customers`, `dim_sellers`, `dim_products`, `fct_orders`, `fct_order_items`). Not on `dim_date`: its ~37 columns are generated by dbt_date.
- `data_type` uses names both engines accept (`string`, `bigint`, `integer`, `boolean`, `date`, `timestamp`, `numeric(18, 2)`); exception `double` for lat/lng → phase 3 must switch to `float64` on BigQuery (e.g. target-conditional Jinja).
- DuckDB contract check compares decimal precision/scale. Money columns in marts are cast explicitly via `to_money()` = `numeric(18, 2)` (BRL cents) instead of inheriting DECIMAL(28..38, 6) from arithmetic.
- PKs get `not_null` + `primary_key` constraints (enforced on DuckDB, informational on BigQuery) and keep `unique`/`not_null` data tests, which work on every engine.
- Generic test `is_not_before(earlier_column)`: logical invariants = error, known source defects (carrier timestamps) = warn.
- Shared column descriptions in doc blocks (`models/_docs.md`), `__overview__` replaces the default docs-site landing page.

## D15 · 2026-10-11 · BigQuery targets and cross-DB types
- GCP project `olist-analytics-dbt-472315`, location `EU`, billing enabled (sandbox forbids DML; dlt state tables and phase-4 `merge` need it). Budget alert $1.
- Auth everywhere = Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`): dbt `method: oauth`, dlt default credentials, CI via `google-github-actions/auth`. Key lives outside the repo; no path in config.
- Service account `dbt-runner`: BigQuery Data Editor + Job User (project level).
- Targets: `prod` (datasets `staging`, `intermediate`, `marts`, `seeds`), `ci` (datasets prefixed with `DBT_CI_SCHEMA`, see `generate_schema_name`). `dev`/`lint` stay DuckDB.
- dlt: `--destination duckdb|bigquery`, separate pipeline state per destination (`olist_<destination>`).
- BigQuery forbids parameterized types in CAST, and contracts cast. Money: `to_money()` dispatches (`adapter.dispatch`) to `numeric(18, 2)` on DuckDB, `round(x, 2)` + `numeric` on BigQuery. Contract `data_type` switches on `target.type` inline; YAML anchors avoid repeating it ×16.
- Rejected: project macros in yml `data_type` (not in the yml render context); `vars` with Jinja in `dbt_project.yml` (value reached the contract unrendered).
- Seed `column_types` switch on `target.type` (`varchar` vs `string`).

## D16 · 2026-10-11 · CI on BigQuery
- PRs run a second build job on BigQuery (`--target ci`); the DuckDB job stays (free, tests full ingestion, builds docs for Pages).
- No ingest in the BigQuery CI job: `raw` is shared with prod, PRs must not overwrite it. CI reads `raw`, writes only `ci_pr_<N>_<run_id>_*`.
- `run_id` in dataset names: `cancel-in-progress` cancels older runs, and their `if: always()` cleanup must not drop a newer run's datasets.
- Cleanup = `dbt run-operation drop_ci_datasets` (`adapter.drop_schema`), refuses any target but `ci`.
- Auth = JSON key in secret `GCP_SA_KEY` via `google-github-actions/auth@v3`. Possible upgrade: Workload Identity Federation (no long-lived key).

## D17 · 2026-10-11 · CD and slim CI
- Slim CI and CD shipped in one PR: slim CI needs the manifest that only CD produces.
- CD (push to `main`, after lint + DuckDB build): dlt → BigQuery `raw` (replace, idempotent), `dbt build --target prod --exclude-resource-type unit_test`, docs generated from prod → Pages.
- Prod state = `manifest.json` as workflow artifact `prod-manifest` (90 days). PR job takes it from the last successful run on `main` via `gh run download` (`actions: read`). No artifact → full build. Rejected: GCS bucket (more IAM, not needed at this scale).
- PR build: `state:modified+ --defer --state prod-state` — modified models + children in `ci_pr_*`, unmodified refs resolve to prod datasets.
- `cancel-in-progress` only for PRs: a prod deploy is never cancelled halfway.
- DuckDB job no longer builds docs; it stays as the free full-pipeline check (fresh ingest).

## D18 · 2026-10-11 · Monthly-chunk ingestion and source freshness
- The Kaggle dump is static; arrival is simulated by purchase month. Order tables (`orders` + `order_items`, `order_payments`, `order_reviews` of those orders) load with `merge` on their primary keys; reference tables (customers, products, sellers, geolocation) are full snapshots, `replace`.
- Cursor = last loaded month in dlt source state (`orders_loaded_until`), committed atomically with the load. `--until YYYY-MM` caps a run; no flag loads the rest; `--full-refresh` = dlt `refresh="drop_sources"` (tables + state). Rejected: `dlt.sources.incremental` on the purchase timestamp: child tables have no timestamp and must follow their orders.
- Chunk selection runs lazily inside the resource generators, after dlt applies `refresh`; computing it in the source body would read the pre-refresh state.
- Every row gets `_loaded_at` (UTC instant of the run; TIMESTAMPTZ on DuckDB, TIMESTAMP on BigQuery): incremental cursor for dbt (phase 4) and `loaded_at_field` for freshness. Metadata column, so the "raw = all strings" rule doesn't apply.
- Freshness on all sources, `warn_after: 1 day`, no `error_after`: after the last month is loaded, order tables honestly stop changing; an error would fail every later deploy. Runs in the DuckDB CI job (fresh ingest, checks the plumbing) and in CD after ingest. Not in BigQuery PR CI (reads shared `raw`, nothing to learn).
- Migration of existing prod `raw` (replace, no `_loaded_at`) verified on a scratch BigQuery dataset: first run merges all rows without duplicates, the next is a no-op.
- `pyarrow-stubs` (dev): `pyarrow.compute` functions are generated at runtime, Pylance can't see them otherwise. Pyright: 0 errors.

## D19 · 2026-10-11 · Type checking with pyright
- `pyright` (dev dependency, pinned in uv.lock) with `[tool.pyright]` in `pyproject.toml`: Pylance reads the same section, so the editor and CI report the same problems. Mode `standard`, scope `ingestion/`.
- Pre-commit hook on Python changes (whole-project run, `pass_filenames: false`) and a CI job `pyright`.
- Gating without touching branch protection: required check `dbt build` now `needs: [sqlfluff-lint, pyright]`, so a type error skips it and blocks the merge.

## D20 · 2026-10-11 · Incremental fct_order_items
- `materialized: incremental`, `incremental_strategy: merge`, `unique_key: order_item_sk` — the one strategy both adapters support with update semantics (dbt-duckdb `merge` needs DuckDB ≥1.4).
- Cursor = `_loaded_at` from raw, carried through `stg_olist__orders` / `stg_olist__order_items`. Filter: item **or** its order loaded at/after `max(_loaded_at)` in `{{ this }}`, so an order reloaded with a new status reprocesses its items. `>=`: the last batch is re-merged (idempotent) rather than risk a gap. Customers are replaced every load, so not in the cursor. Output `_loaded_at = greatest(item, order)`.
- Rejected: `microbatch` (dbt ≥1.9). Batches by event time up to *now*; with 2016-2018 data every run would need explicit `--event-time-start/end`. A good fit later for Airflow backfills (phase 5).
- BigQuery: partition by `purchase_date` (month; daily would exceed partition limits for no gain at this size) + cluster by `seller_id, product_id`; config is `target.type`-conditional, DuckDB gets none.
- Contract stays enforced; `on_schema_change: append_new_columns`. `_loaded_at` type differs (TIMESTAMPTZ vs TIMESTAMP) → inline `target.type` switch.
- Unit test of the incremental branch: `overrides.macros.is_incremental: true` + fixture for `this`. The `this` fixture uses `format: sql`: a dict fixture makes dbt introspect the existing table, which fails on a fresh CI database and lacks newly added columns. Fixtures don't render macros → plain `cast(... as timestamp)`.
- Verified: DuckDB chunked run (2017-06 full → 2017-07 → rest → no-op) equals `--full-refresh` (112,650 rows, R$15,843,553.24); BigQuery full refresh + incremental run in scratch datasets, same totals.
- Prod migration: the existing prod table (plain table, no `_loaded_at`, not partitioned) can't be merged into → one-off full refresh. CD got a `workflow_dispatch` input `full_refresh` for this and future rebuilds.

## D21 · 2026-10-12 · Snapshot snap_sellers (SCD2)
- YAML-defined snapshot (dbt ≥1.9, `snapshots/snap_sellers.yml`, `relation: source(...)`): no SQL file, reads the raw source. Snapshot sources, not models, so history doesn't change when transformation logic does.
- `strategy: check` on `seller_zip_code_prefix`, `seller_city`, `seller_state`: the source has no `updated_at`. Not `check_cols: all` — `_loaded_at` / `_dlt_id` change on every load and would version every row each run.
- `hard_deletes: new_record`: a seller missing from a load gets a closing version with `dbt_is_deleted = 'True'`; reappearing opens a new one.
- Schema `snapshots` (`ci_pr_*_snapshots` in CI); `drop_ci_datasets` got the `_snapshots` suffix (would have leaked CI datasets).
- Tests: `dbt_scd_id` unique/not_null, (`seller_id`, `dbt_valid_from`) unique, `seller_id` unique where `dbt_valid_to is null` (one open version).
- The dump never changes sellers → prod has one version per seller. Mechanics shown by a local demo in the README (change + delete + restore → 3 versions each). `dim_sellers` stays current-state; nothing reads the snapshot yet.
- Verified: DuckDB demo; BigQuery scratch dataset (build + re-snapshot = no new versions, cleanup drops `_snapshots`).

## D22 · 2026-10-12 · Airflow 3 + Cosmos (local, Docker Compose)
- Airflow 3.3.2 (LocalExecutor, Postgres, SimpleAuthManager with all-admins: local only) + astronomer-cosmos 1.15.1. Compose lives in `airflow/`, repo mounted at `/opt/project`.
- dbt + dlt in a separate venv `/opt/project-venv` built from `uv.lock` (`uv sync --frozen --no-dev`): same versions as dev/CI, no clash with Airflow's pins. Cosmos runs dbt from it: `ExecutionMode.LOCAL` + `InvocationMode.SUBPROCESS`, `LoadMode.DBT_LS` (cached by Cosmos) — replaced by `DBT_MANIFEST` in D23.
- `TestBehavior.BUILD`: one `dbt build --select <node>` task per model/seed/snapshot (model + its tests, half the tasks of AFTER_EACH). Source nodes rendered as `dbt source freshness` tasks. (Superseded by D25: watcher mode, `AFTER_EACH`.) Rejected for now: `ExecutionMode.WATCHER` (one `dbt build` + sensor tasks) — faster, fits DuckDB's single writer, but newer; revisit if task overhead hurts.
- Two DAGs from one factory, not a `target` param (user asked for both warehouses): Cosmos fixes the profile/target at parse time, and dlt keeps a load cursor per destination, so each warehouse needs its own run history. `olist_elt_duckdb` → `data/airflow.duckdb` (dev file untouched); `olist_elt_bigquery` → `airflow_raw` + `airflow_*` datasets via target `airflow` and `DBT_RAW_SCHEMA` (prod stays with GitHub Actions CD), created only when a key is mounted.
- DuckDB: every writing task (dlt + dbt) in a 1-slot pool `duckdb`.
- `test_indirect_selection=cautious` + `should_detach_multiple_parents_tests=True`: with the default (eager), `dbt build --select stg_olist__orders` also picked relationships tests whose other parent wasn't built yet (first run: 6 failed tasks). Now a node task runs only its own tests; each multi-parent test is its own task after all parents.
- Schedule `@monthly` 2016-09..2018-10, `catchup=False`, `max_active_runs=1`; run for interval starting M loads `--until M`. History via `airflow backfill create ... --max-active-runs 1`: Airflow 3 backfills have their own limit (default 10) and ignore the DAG's — found when 3 months ran at once and raced on the Kaggle cache.
- `dbt deps` once in `airflow-init`, `install_dbt_deps=False`. Cosmos symlinks `dbt_packages` but drops `package-lock.yml`; dbt then checks `packages.yml` (2 packages) against `dbt_packages/` (3: transitive dbt_date) and fails. Fix: declare `dbt_date` explicitly — `dim_date` uses it directly anyway.

## D23 · 2026-10-12 · Airflow in CI, BigQuery DAG cleanup
- CI job `airflow DAGs parse` (PRs only): builds the image (buildx, GHA cache), runs `airflow/check_dags.py` in it — `DagBag` over the DAG folder, fails on import errors, a missing DAG or a DAG without dbt tasks. No metadata DB, no warehouse; a dummy `{}` key renders the BigQuery DAG (parsing never authenticates). Cosmos cache off (it lives in Airflow Variables = DB).
- `LoadMode.DBT_MANIFEST` instead of `DBT_LS`. Found on the first BigQuery run: Airflow 3 re-parses the DAG file in every task, and Cosmos' `dbt ls` cache lives in Airflow Variables, which tasks can't write → every task ran two uncached `dbt ls` (one per DAG in the file). With 10 parallel tasks on 4 CPUs that took >3 min, past the import timeout → endless retries; the DuckDB DAG hid it (1-slot pool, ~15 s). Now `dbt parse --target-path target_airflow` in `airflow-init` (and CI) writes the manifest; parse is 0.1–3 s (cold ~20 s). Cost: rerun `airflow-init` after changing models (execution always uses the live project; only the graph comes from the manifest). Import-timeout overrides (tried first: 180 s) removed.
- `drop_ci_datasets` → `drop_isolated_datasets`, allowed on targets `ci` and `airflow`, also drops `<schema>_raw` and `<schema>_raw_staging` (dlt dataset of the Airflow DAG and its merge staging; the first real run left `airflow_raw_staging` behind). Still refuses dev/prod.
- Pyright/Pylance: `airflow/` is an execution environment with `reportMissingImports` and `reportUnusedExpression` off. Airflow/Cosmos are only in the image (separate on purpose, D22); the parse job is that code's check.
- Task logs in the UI: `[api] secret_key` shared across components (local value, like the JWT secret) + a shared `airflow-logs` volume. Unset, each component picked a random key and the api-server's signed request to the scheduler's log server (LocalExecutor) was rejected.
- Verified on BigQuery: backfill 2016-09..10 → same numbers as DuckDB (raw/`fct_orders` 4 + 324, `fct_order_items` 369 over 2 loads, 1 snapshot version per seller), prod untouched; then `drop_isolated_datasets --target airflow` left only prod datasets.

## D24 · 2026-10-12 · Deploy log warnings
- A warning in the log is either fixed, or kept on purpose and explained here. Kept: the 6 data-test WARNs (known source defects, `severity: warn`) and dbt's BigQuery notice "numeric type and unspecified precision/scale" (re-checked: BigQuery rejects `cast(... as numeric(18, 2))`, "Parameterized types are not allowed in CAST expressions"; `to_money()` rounds instead). Silencing that one would mean overriding a dbt core macro or switching to `bignumeric` — worse than the notice.
- Fixed: `warn_unenforced: false` on mart `primary_key` constraints (DuckDB enforces them, BigQuery doesn't; `unique` + `not_null` tests check both); dlt's "Cannot create BigQuery Storage client" filtered in `pipeline.py` (Storage API is for fast reads, we only write); agate's `table_owner` RuntimeWarning in `dbt docs generate` (dbt-bigquery catalog query) ignored via `PYTHONWARNINGS` in that CI step only; runners pinned to `ubuntu-24.04` (`ubuntu-latest` moves to Ubuntu 26 on 2026-10-19 — upgrade on purpose, not by surprise).

## D25 · 2026-10-12 · Cosmos watcher mode (Airflow speed-up)
- `ExecutionMode.WATCHER` for both DAGs (revisits D22). A producer task runs `dbt source freshness` + one `dbt build`; every node is a deferrable sensor reading the producer's per-node results (XCom from dbt's JSON log). Measured, empty warehouse, months 2016-09 / 2016-10: DuckDB 29.1 / 28.0 → 5.6 / 5.7 min; BigQuery 18.9 → 8.9 / 6.1 min (first run without a Cosmos partial-parse cache). Same data as before on both (raw/`fct_orders` 328, `fct_order_items` 369 over 2 loads, 1 snapshot version per seller), prod untouched.
- Why it was slow: LOCAL mode ran one dbt process per node, each paying Python/dbt start + project parse (~36 s per task); on DuckDB one at a time (1-slot pool), on BigQuery 10 at a time on 4 CPUs (each task minutes). Tasks also started from the host's `partial_parse.msgpack` (Windows, other profile) → full reparse. Watcher pays that once per run, and dbt threads give DuckDB parallelism inside its single writer process.
- DuckDB pool only on the writers: `dlt_ingest` + producer (`ExecutionConfig.setup_operator_args`). Sensors in the 1-slot pool would wait for the producer they watch. CI `check_dags.py` asserts the pool holds exactly these two (mutation-checked).
- Watcher constraints found by running it: no `TestBehavior.BUILD` (→ `AFTER_EACH`, a run + test sensor per model, 53 tasks); detached multi-parent tests fail ("no information about the node": the producer reports tests per model) → `should_detach_multiple_parents_tests=False`; the default freshness callback skips nodes below stale (warn) sources → `no_freshness_skips` (freshness is warn-only, D18; a month without orders is honestly stale); unit tests excluded from the producer (`--exclude-resource-type unit_test`, CI's job).
- Re-runs: the producer skips itself on attempt 2+, and sensors then run their own node's dbt command — on DuckDB outside the pool → file-lock failures (found via a backfill `--reprocess-behavior`, which reuses run ids). Hence `retries=0` and "re-run = new DAG run, not Clear" (airflow/README.md). Data is safe either way (DuckDB lock; models idempotent). Verified: a new run for the already loaded 2016-10 → success, 53/53 tasks, data unchanged. Rejected: pool on the sensors (52 sensors serialized after the producer, ~7 min) to protect a path we don't use.
- BigQuery is still slower than DuckDB here: ~1 s of job overhead per node × 128 nodes on tiny data, slower dlt load jobs. Left for later: freshness + build are two dbt invocations in the producer (~40–60 s of start + parse each run); seeding a per-target partial parse.

## D26 · 2026-10-13 · Streamlit dashboard over the marts
- One app, two backends (`DASHBOARD_BACKEND`): DuckDB locally, BigQuery prod marts when deployed (Streamlit Community Cloud, PR 6.2). Pages send SQL that both engines accept (`extract`, `date '…'` literals, plain joins), the same cross-DB rule as the models; no ORM or per-engine query files. Verified: all 12 KPI tiles identical on DuckDB and BigQuery.
- Aggregation in SQL, not pandas: queries return chart-sized results (BigQuery bills scanned columns, not rows returned). `st.cache_data` (1 h) on every query. DuckDB opened read-only per query, so a running `dbt build` or dlt load can still take the write lock.
- Default period 2017-01 … 2018-08 (full months): the dump's 2016 months hold a handful of orders and Sep–Oct 2018 are partial, which plotted as a 100 % late rate and a fake revenue collapse. The full range is still selectable.
- dbt exposure `olist_dashboard` (type dashboard) on the 5 marts it reads → shown in docs lineage, selectable as `+exposure:olist_dashboard`.
- Streamlit + Altair in uv group `dashboard`, installed by default (`default-groups`), so CI type-checks (pyright) and smoke-tests it: `AppTest` renders every page after `dbt build` on DuckDB.
- Charts: Altair with a fixed two-color, CVD-checked palette (separate light/dark steps), tooltips on every mark, no dual axes (orders and revenue are two charts).

## D27 · 2026-10-13 · Dashboard deploy: Streamlit Community Cloud, read-only service account
- Free hosting that deploys from the GitHub repo (`main`, entrypoint `dashboard/app.py`). Rejected: Cloud Run (container, more setup, billing), a static export (loses the date filter).
- Own service account `dashboard-reader`, not the `dbt-runner` key: *BigQuery Job User* on the project (run queries) + *BigQuery Data Viewer* on dataset `marts` only. Dataset-level, not table-level: `dbt build --target prod` re-creates the tables, which would drop table-level grants. Verified: reads marts; `raw` and writes to `marts` → Forbidden.
- Key in the app's secrets as `[gcp_service_account]`; `DASHBOARD_BACKEND` as a top-level secret. Settings are read from env first, then `st.secrets` directly: Streamlit exports secrets to env only after `st.secrets` is first touched, i.e. after `warehouse.py` reads its settings at import.
- `dashboard/requirements.txt` (pinned to uv.lock versions): Community Cloud looks next to the entrypoint first, so it installs Streamlit + BigQuery client only, not dbt / dlt / DuckDB. Checked in a clean venv against BigQuery.
- Cost: chart-sized queries, 1 h cache, REST results (no Storage API client); the $1 budget alert stays the backstop.

## D28 · 2026-10-13 · Live feed: hourly replay of the dump, alerts, dashboard on Airflow marts
User question: the DAGs ran, but what happens when new data arrives? Olist never gets new data, so the feed is simulated. Three PRs (user choices in brackets).
- **Replay (PR 7.1, hourly):** DAGs `@hourly`, `start_date` 2026-10-13, no `end_date`, `catchup=False`. Each run loads the next purchase month *that has orders* (`pipeline.py --next`, cursor in dlt state). Not the next calendar month: 2016-11 has no orders, and an empty chunk never moves the cursor (stuck forever). ~25 runs replay 2016-09..2018-10, then each run loads 0 orders.
- Replaces the monthly schedule over 2016-09..2018-10 + backfill (D22): its `end_date` was in the past, so the scheduler never started a run by itself; the run's data interval no longer means a dump month. Manual `until` param stays for catch-up / recovery.
- `has_new_orders` (ShortCircuitOperator) skips the dbt group when the ingest's last stdout line (its XCom) is `orders_loaded=0`: no rebuild for no data, no BigQuery cost every hour once the dump is used up. Runs with `until` / `full_refresh` always build: re-running a month whose dbt failed loads 0 new orders but must rebuild. Mid-feed a failed dbt step heals on the next run (incremental cursor is still behind the rows).
- Rejected: Airflow backfill as the "feed" (with an hourly schedule the dates are meaningless); skipping the whole dlt load when nothing is new (would need the dlt state before `run`; the reference tables reload is cheap).
- **Alerts (PR 7.2):** Airflow 3 notifiers over SMTP, recipients in an Airflow Variable. [Mailpit in Compose by default, optional real SMTP via `airflow/.env`.]
- **Dashboard (PR 7.3):** reads `airflow_marts` (fed by the BigQuery DAG) via a setting; prod `marts` stays with CD. [Chosen over a GitHub Actions cron replay into prod (logic in two places) and Airflow writing prod (clashes with CD).] Caveat: local Airflow only runs while the PC is on; the stale-data alert makes that visible.

## D29 · 2026-10-13 · Email alerts: one email per run, Mailpit, Variables
- At most one email per run. Failures: DAG-level `on_failure_callback` (`SmtpNotifier`), not per task: a failed watcher producer fails ~50 sensors. Everything else: a `report` task after dbt with trigger rule `none_failed` (runs whether dbt built or was skipped; a failed dbt leaves it `upstream_failed`, so the run still fails and the failure email goes). `has_new_orders` uses `ignore_downstream_trigger_rules=False` so its skip does not swallow `report`.
- Failure email content (first version only said "run X failed: task_failure", user: not informative): a plain callback instead of a `SmtpNotifier` template. It takes the task Airflow names as the cause (callback `last_ti`) and reads its log file from the shared logs volume: exception + last output lines (e.g. `pipeline.py: error: ...`). For any dbt task it reads the producer's log instead and sends dbt's end-of-run block (`Completed with N error` … `Done. PASS=… ERROR=n`): the named task is often a symptom (a broken `stg_olist__sellers` was reported as `stg_olist__order_items.test`, which holds the relationships test to sellers), dbt's summary names the real node and message.
- OK email carries dbt's totals and warn-level tests (from the producer's log of the run): severity-warn data-quality tests never fail anything, so this is the only place they surface. Subject gets `, N dbt warnings`.
- `report` kinds: `OK` (orders loaded, or a manual `until` / `full_refresh` rebuild), `STALE` (no new orders for `olist_stale_after_hours`, default 24 = the source's `warn_after`), nothing otherwise. STALE repeats statelessly: the first hourly run past the threshold, then when `(age − threshold) mod 24 < 1` (~once a day). Rejected: a STALE email every hour (spam once the dump is used up); remembering "already alerted" in a Variable (state to keep right).
- Status data from `pipeline.py` itself: its last stdout line is JSON (`orders_loaded`, `orders_total`, `last_order_at`, `last_loaded_at` via `pipeline.sql_client()`), so one code path serves DuckDB and BigQuery, and the DuckDB read happens inside the task that already holds the file. `last_loaded_at` of raw orders only moves when orders arrive = the same signal as `dbt source freshness`. Rejected: dlt's `pipeline.dataset()` (dlt 1.31 + installed sqlglot: `qualify() got an unexpected keyword argument 'infer_schema'`); a separate status task (second DuckDB connection, pool slot).
- Recipients / threshold: Airflow Variables (`olist_alert_emails`, `olist_stale_after_hours`), editable in the UI. `airflow-init` creates them with defaults only when missing (visible in the UI, re-init keeps edits); code defaults cover a deleted Variable.
- Transport: connection `smtp_default` from env `AIRFLOW_CONN_SMTP_DEFAULT`; default Mailpit (`axllent/mailpit`, UI :8025, nothing leaves the machine), real SMTP via `SMTP_CONN` in `airflow/.env` (Gmail app password example). Extras are truthy strings, so flags are omitted, never set to `false`.
- Found by running: recipients fell back to the default although the Variable was set. Each container generated its own Fernet key (`AIRFLOW__CORE__FERNET_KEY` unset), so a Variable encrypted by the scheduler CLI was undecryptable in the dag-processor (callbacks) and api-server (task Variable reads): `Can't decrypt _val`, read as missing. Fixed with a shared local-only key in compose (`FERNET_KEY` override), same pattern as the shared `secret_key` (D23 / PR 5.2). `airflow tasks test` hid it (runs in the scheduler container, direct DB).

## D30 · 2026-10-13 · Failure emails from GitHub Actions
- User question: Airflow emails are covered, but what if a PR check, dbt or BigQuery fails in CI/CD? GitHub's own notification only reaches whoever triggered the run, with no detail.
- Job `notify-failure` (last in ci-cd.yml, `needs` every job, `if: failure() && vars.ALERT_EMAILS != ''`) runs `.github/scripts/notify_failure.py`: lists the run's failed jobs via `gh api` (`actions: read`), the failed step and a log excerpt — dbt's end-of-run block when present (failed nodes + messages, warnings, totals), else the lines up to the first `##[error]`. Checked locally on the old failed prod deploy (run 38210890178): excerpt shows `Name _loaded_at not found inside this_run` in `fct_order_items`.
- Stdlib `smtplib` + `gh`, no third-party mail action (supply chain; the secret stays in our code). SMTP from secret `SMTP_CONN` in the same Airflow-style URI as `airflow/.env`, recipients from repo variable `ALERT_EMAILS` (not a secret: editable in Settings → Variables). PR title passed via env, never inlined into the shell (user input). Fork PRs get no secrets → the script prints "not set" and exits 0.
- BigQuery problems are covered by whichever runner hit them (CD / CI job → this email; Airflow BigQuery DAG → D29); cost by the $1 GCP budget alert (Phase 3 setup).
