## What

Closes #

## Why

## Checks
- [ ] `dbt build` passes locally
- [ ] New/changed models have descriptions and tests in `.yml`
- [ ] `docs/DECISIONS.md` updated if a design decision was made
- [ ] New raw source table: loaded into prod `raw` once with the owner's OK (`uv run python ingestion/pipeline.py --destination bigquery`); pre-push checks it
- [ ] Touches `transform/` or `ingestion/`: `uv run pytest fault_injection` run (CI does, ~14 min; pre-push runs the control scenario only)
