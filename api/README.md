# Job-control API

A thin FastAPI service over what the repo already runs: start a pipeline run, a dbt build or the T-SQL / Spark parity
check, follow its state, read the result. No business logic of its own. The point is the request handling:
signed requests, idempotency keys, a job state machine, `problem+json` errors and a schema that CI checks for
breaking changes. Decisions: D134.

| Call | What it does |
|---|---|
| `POST /v1/jobs` | start `{"kind": "pipeline", "until": "2017-03"}` or `{"kind": "dbt_build", "select": "marts"}`; header `Idempotency-Key`; 202 + the job |
| `POST /v1/parity/run` | start the parity check (`{"skip_load": false}`); 503 until `OLIST_CSV_DIR` is set |
| `GET /v1/jobs/{id}` | `queued` / `running` / `succeeded` / `failed`, attempts, start and end time |
| `GET /v1/jobs/{id}/result` | exit code, summary, output tail, artifact path; 409 while the job is not finished |
| `GET /healthz`, `GET /readyz` | liveness; readiness (the job store answers). Unsigned |

Jobs run on the local DuckDB file only: the API cannot start anything against BigQuery, Databricks or Snowflake.

## Run

```bash
export API_HMAC_SECRET=$(python -c "import secrets; print(secrets.token_urlsafe(48))")   # see api/.env.example
export OLIST_CSV_DIR=<folder with the Olist CSVs>                                       # only for the parity endpoint
uv run uvicorn api.app:create_app --factory          # http://127.0.0.1:8000/docs
```

Job state lives in `data/api_jobs.sqlite3` (`API_DB_PATH`). A restart marks jobs that were running as failed
(`interrupted by a service restart`) and runs the queued ones again.

## Signing a request

Headers `X-Timestamp` (unix seconds), `X-Nonce` (16-64 characters of `A-Za-z0-9_-`, never reused) and `X-Signature`:
hex HMAC-SHA256 of `timestamp \n nonce \n METHOD \n path?query \n body` with the shared secret. A timestamp more
than 5 minutes off the server clock is refused, and so is a nonce seen before.

```python
import json, time, httpx
from api.signing import signed_headers

body = json.dumps({"kind": "dbt_build", "select": "marts"}).encode()
headers = signed_headers(secret, "POST", "/v1/jobs", body, now=time.time())
headers |= {"Content-Type": "application/json", "Idempotency-Key": "nightly-2026-11-25"}
httpx.post("http://127.0.0.1:8000/v1/jobs", content=body, headers=headers)
```

Retrying the same call (new nonce, same key and body) returns the same job (`Idempotent-Replayed: true`); the same
key with another body is a 409.

## Schema

`api/openapi.json` is generated (`uv run python -m api.openapi`) and checked in. Two checks: a pytest fails when the file
does not match the app, and on a PR `api/check_openapi.py origin/main` fails a breaking change (removed endpoint or
property, new required input, changed type, a response enum that grew) unless `info.version` gets a new major.
