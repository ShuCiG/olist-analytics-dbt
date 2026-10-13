# olist-analytics-dbt

Portfolio project: ELT on Olist e-commerce data with dbt. Owner is strong in SQL/DWH, new to dbt — skip SQL basics, explain dbt-specific choices.

## Session protocol
1. Read `docs/PROGRESS.md` first (current phase, next steps). Read `docs/DECISIONS.md` only when a choice is in question.
2. Work in phases; do not start next phase's items without asking.
3. Before ending a session: update `docs/PROGRESS.md` (checklist + session log line), add any new decision to `docs/DECISIONS.md`.

## Git workflow (mandatory)
- Never commit or push to `main` (branch protection enforces it).
- One branch per task: `feat/…`, `fix/…`, `ci/…`, `docs/…`, `chore/…`. Commit + push to the branch.
- Open PR with `gh pr create` (fill `.github/pull_request_template.md`). **User merges — Claude never merges.**
- After user merges: `git switch main && git pull && git branch -D <branch> && git fetch -p` (`-D`: squash merge makes `-d` refuse).
- CI (`.github/workflows/ci-cd.yml`) on every PR: sqlfluff + pyright + Airflow DAG parse, `dbt build` on DuckDB (fresh dlt ingest), slim CI on BigQuery. On merge to main: dlt → BigQuery, `dbt build --target prod`, dbt docs → GitHub Pages. Failures → `notify-failure` job emails `vars.ALERT_EMAILS` (secret `SMTP_CONN`, script `.github/scripts/notify_failure.py`).

## Layout
- `ingestion/` — dlt pipeline: Olist CSV → DuckDB schema / BigQuery dataset `raw`, monthly chunks
- `airflow/` — Airflow 3 + Cosmos (Docker Compose), DAGs `olist_elt_duckdb` / `olist_elt_bigquery`, hourly replay of the dump (`--next` month per run)
- `transform/` — dbt project (staging → intermediate → marts)
- `dashboard/` — Streamlit app over marts (`DASHBOARD_BACKEND` = duckdb | bigquery), dbt exposure `olist_dashboard`
- `data/` — raw CSV + `olist.duckdb` (gitignored)
- `docs/` — PROGRESS.md, DECISIONS.md

## Commands
- Env: `uv sync` (Python 3.12 pinned; system Python 3.14 is unsupported by dbt)
- dbt: `cd transform && uv run dbt <cmd>` (profiles.yml lives in transform/; DuckDB path via env `DBT_DUCKDB_PATH`)
- Lint: `uv run sqlfluff lint transform/models transform/tests` (dbt templater, in-memory `lint` target, ~40s); `uv run sqlfluff fix` breaks Jinja-heavy files — review diff
- Types: `uv run pyright` (config `[tool.pyright]` in pyproject.toml, shared with Pylance); must stay at 0 errors
- Airflow: `docker compose -f airflow/docker-compose.yml up -d --build` (UI :8080); rerun `airflow-init` after model changes (manifest); see airflow/README.md
- Dashboard: `uv run streamlit run dashboard/app.py`; smoke test `uv run pytest dashboard` (needs built marts)
- Hooks: `uv run pre-commit install` once; runs whitespace/yaml/secrets checks, blocks commits to main, sqlfluff on changed SQL, pyright on Python changes

## Conventions
- Models: `stg_<source>__<entity>`, `int_<entity>_<verb>`, `fct_`, `dim_`
- Every model has a `.yml` entry with description; PKs get `unique` + `not_null`
- Cross-DB SQL only (DuckDB dev, BigQuery prod later): use `dbt.*` / `dbt_utils` macros, no engine-specific functions
- Code, docs, commits in English
