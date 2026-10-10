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
