"""Minimal Trino REST client (stdlib only): enough to run a statement as a given user."""

import json
import os
import urllib.error
import urllib.request
from typing import Any

URL = os.environ.get("TRINO_URL", "http://127.0.0.1:8090")


class TrinoError(Exception):
    """The statement failed; the message is Trino's own (an access denial reads 'Access Denied: ...')."""


def _call(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise TrinoError(f"HTTP {error.code}: {error.read().decode()[:300]}") from error


def execute(sql: str, user: str) -> list[list[Any]]:
    """Run one statement as `user` (X-Trino-User; the file rules key on it) and return all rows."""
    headers = {"X-Trino-User": user, "X-Trino-Catalog": "delta", "X-Trino-Schema": "olist"}
    page = _call(urllib.request.Request(f"{URL}/v1/statement", data=sql.encode(), headers=headers, method="POST"))
    rows: list[list[Any]] = []
    while True:
        if "error" in page:
            raise TrinoError(page["error"]["message"])
        rows.extend(page.get("data", []))
        if "nextUri" not in page:
            return rows
        page = _call(urllib.request.Request(page["nextUri"], headers=headers))
