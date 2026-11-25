"""The checked-in schema `api/openapi.json`: generated from the app, compared in CI.

    uv run python -m api.openapi          # rewrite api/openapi.json
    uv run python -m api.openapi --check  # fail if the file is out of date
"""

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from api.app import create_app
from api.problems import MEDIA_TYPE
from api.settings import REPO, Settings

SCHEMA_PATH = REPO / "api" / "openapi.json"


def build_schema() -> dict[str, Any]:
    """The OpenAPI document; needs no secret and touches no real job store."""
    with tempfile.TemporaryDirectory() as tmp:
        app = create_app(Settings(hmac_secret=b"x" * 32, db_path=Path(tmp) / "schema.sqlite3"))
        schema: dict[str, Any] = app.openapi()
    # FastAPI documents error models as application/json; the service answers problem+json
    for methods in schema["paths"].values():
        for operation in methods.values():
            for status, response in operation["responses"].items():
                content = response.get("content", {})
                if not status.startswith("2") and "application/json" in content:
                    content[MEDIA_TYPE] = content.pop("application/json")
    return schema


def render(schema: dict[str, Any]) -> str:
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def main() -> int:
    text = render(build_schema())
    if "--check" in sys.argv:
        if not SCHEMA_PATH.exists() or SCHEMA_PATH.read_text(encoding="utf-8") != text:
            print("api/openapi.json is out of date: run `uv run python -m api.openapi`")
            return 1
        return 0
    SCHEMA_PATH.write_text(text, encoding="utf-8", newline="\n")
    print(f"wrote {SCHEMA_PATH.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
