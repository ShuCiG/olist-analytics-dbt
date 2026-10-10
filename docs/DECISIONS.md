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
