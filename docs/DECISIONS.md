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
