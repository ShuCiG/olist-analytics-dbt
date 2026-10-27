"""Wait for the Databricks SQL warehouse before the weekly run, and tell "Databricks
can't start compute" apart from "our code broke" (D78).

Free Edition's serverless warehouse sometimes refuses to start for a while
(`BAD_REQUEST: Cannot create the resource, please try again later.`): the first
scheduled run failed on that, and a later start went through. So the job asks
first: a `SELECT 1` through the SQL Statement API, which starts a stopped warehouse
and needs only CAN_USE (the service principal's grant). Starting, or "can't create"
→ wait and ask again, up to DEADLINE; still not up → the run is skipped as
unavailable (output `available=false`, a warning, no failure email). Any other
answer (auth, permissions, a wrong warehouse id) fails the step.

Env:  DATABRICKS_HOST, DATABRICKS_HTTP_PATH (/sql/1.0/warehouses/<id>), and either
      DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (OAuth M2M, CI) or
      DATABRICKS_TOKEN (a personal token, local runs); GITHUB_OUTPUT if set
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

READY, RETRY, ERROR = "ready", "retry", "error"
DEADLINE = 20 * 60  # seconds of waiting in total
# Every attempt waits up to 50 s for the statement itself (API maximum)
WAIT_TIMEOUT = "50s"
UNAVAILABLE = "Cannot create the resource"
# Gateway / throttling answers: Databricks' side, worth another attempt
RETRY_HTTP = {429, 502, 503, 504}


def delays() -> list[int]:
    """Pauses between attempts: every 30 s for 3 min, then every 2 min."""
    return [30] * 6 + [120] * ((DEADLINE - 180) // 120)


def classify(http_status: int, body: dict[str, Any]) -> tuple[str, str]:
    """(outcome, detail) of one probe answer."""
    status = body.get("status") or {}
    message = body.get("message") or (status.get("error") or {}).get("message") or ""
    if UNAVAILABLE in message:
        return RETRY, message
    if http_status in RETRY_HTTP:
        return RETRY, f"HTTP {http_status} {message}".strip()
    if http_status != 200:
        return ERROR, f"HTTP {http_status} {body.get('error_code', '')}: {message}"
    state = status.get("state")
    if state == "SUCCEEDED":
        return READY, state
    # Still running after WAIT_TIMEOUT: the warehouse is starting (on_wait_timeout=CANCEL)
    if state in {"PENDING", "RUNNING", "CANCELED"}:
        return RETRY, f"statement {state} (warehouse starting)"
    return ERROR, f"statement {state}: {message}"


def _host() -> str:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    return host if host.startswith("https://") else f"https://{host}"


def _request(url: str, data: bytes, headers: dict[str, str]) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except json.JSONDecodeError:
            return e.code, {}


def _token() -> str:
    if token := os.environ.get("DATABRICKS_TOKEN"):
        return token
    pair = f"{os.environ['DATABRICKS_CLIENT_ID']}:{os.environ['DATABRICKS_CLIENT_SECRET']}"
    code, body = _request(
        f"{_host()}/oidc/v1/token",
        urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode(),
        {"Authorization": f"Basic {base64.b64encode(pair.encode()).decode()}",
         "Content-Type": "application/x-www-form-urlencoded"},
    )
    if code != 200 or "access_token" not in body:
        sys.exit(f"::error::OAuth token request failed: HTTP {code} {body.get('error_description', '')}")
    return body["access_token"]


def probe(token: str, warehouse_id: str) -> tuple[str, str]:
    code, body = _request(
        f"{_host()}/api/2.0/sql/statements/",
        json.dumps({"warehouse_id": warehouse_id, "statement": "SELECT 1",
                    "wait_timeout": WAIT_TIMEOUT, "on_wait_timeout": "CANCEL"}).encode(),
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    return classify(code, body)


def _output(available: bool) -> None:
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"available={str(available).lower()}\n")


def main() -> None:
    warehouse_id = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").rsplit("/", 1)[-1]
    token = _token()
    started = time.monotonic()
    for pause in [*delays(), None]:
        outcome, detail = probe(token, warehouse_id)
        elapsed = int(time.monotonic() - started)
        print(f"[{elapsed:>4}s] {outcome}: {detail}", flush=True)
        if outcome == READY:
            _output(True)
            return
        if outcome == ERROR:
            _output(False)
            sys.exit(f"::error::SQL warehouse {warehouse_id}: {detail}")
        if pause is None or elapsed + pause > DEADLINE:
            break
        time.sleep(pause)
    _output(False)
    note = (f"Databricks unavailable: SQL warehouse {warehouse_id} did not start within "
            f"{DEADLINE // 60} min ({detail}). Run skipped; the next weekly run covers it.")
    print(f"::warning::{note}")
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as f:
            f.write(note + "\n")


if __name__ == "__main__":
    main()
