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
- CI (`.github/workflows/ci-cd.yml`) on every PR: dlt ingest → `dbt build`. On merge to main: same + dbt docs → GitHub Pages.

## Layout
- `ingestion/` — dlt pipeline: Olist CSV → DuckDB schema `raw`
- `transform/` — dbt project (staging → intermediate → marts)
- `data/` — raw CSV + `olist.duckdb` (gitignored)
- `docs/` — PROGRESS.md, DECISIONS.md

## Commands
- Env: `uv sync` (Python 3.12 pinned; system Python 3.14 is unsupported by dbt)
- dbt: `cd transform && uv run dbt <cmd>` (profiles.yml lives in transform/; DuckDB path via env `DBT_DUCKDB_PATH`)
- Lint: `uv run sqlfluff lint transform/models`

## Conventions
- Models: `stg_<source>__<entity>`, `int_<entity>_<verb>`, `fct_`, `dim_`
- Every model has a `.yml` entry with description; PKs get `unique` + `not_null`
- Cross-DB SQL only (DuckDB dev, BigQuery prod later): use `dbt.*` / `dbt_utils` macros, no engine-specific functions
- Code, docs, commits in English
