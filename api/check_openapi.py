"""Breaking changes in the API schema are versioned: run on a PR against main (CI job `python tests`).

Compares `api/openapi.json` with the one on the base branch. A breaking change needs a new
major `info.version`; any other change just has to keep the file in step with the code
(`python -m api.openapi --check`, also a pytest). Same rule as the data contracts
(data-product/check_compatibility.py): clients pin a major version.

Breaking, for a client written against the old schema:
- a path or method is gone, or a documented response status is gone;
- a new required parameter or required request property; a request property that is gone,
  changed type, or lost an allowed enum value;
- a response property that is gone or changed type; a response enum that gained a value.

    uv run python api/check_openapi.py [base-ref]   # default origin/main
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
SCHEMA = "api/openapi.json"
METHODS = ("get", "put", "post", "delete", "patch", "head", "options")

Json = dict[str, Any]


def resolve(doc: Json, schema: Json) -> Json:
    while "$ref" in schema:
        node: Any = doc
        for part in schema["$ref"].removeprefix("#/").split("/"):
            node = node[part]
        schema = node
    return schema


def _types(schema: Json) -> set[str]:
    if "type" in schema:
        return {schema["type"]}
    return {t for variant in schema.get("anyOf", []) if (t := variant.get("type")) is not None}


def compare_schema(old_doc: Json, new_doc: Json, old: Json, new: Json, where: str, direction: str) -> list[str]:
    """direction: `request` (the client sends it) or `response` (the client reads it)."""
    old, new = resolve(old_doc, old), resolve(new_doc, new)
    problems: list[str] = []
    if _types(old) and _types(new) and _types(old) != _types(new):
        problems.append(f"{where}: type {sorted(_types(old))} -> {sorted(_types(new))}")
    old_enum, new_enum = set(map(str, old.get("enum", []))), set(map(str, new.get("enum", [])))
    if old_enum and direction == "request" and old_enum - new_enum:
        problems.append(f"{where}: enum value(s) removed: {sorted(old_enum - new_enum)}")
    if old_enum and direction == "response" and new_enum - old_enum:
        problems.append(f"{where}: enum value(s) added: {sorted(new_enum - old_enum)}")
    old_props, new_props = old.get("properties", {}), new.get("properties", {})
    for name, prop in old_props.items():
        if name not in new_props:
            problems.append(f"{where}.{name}: property removed")
        else:
            problems += compare_schema(old_doc, new_doc, prop, new_props[name], f"{where}.{name}", direction)
    if direction == "request":
        for name in set(new.get("required", [])) - set(old.get("required", [])):
            problems.append(f"{where}.{name}: now required")
    if "items" in old and "items" in new:
        problems += compare_schema(old_doc, new_doc, old["items"], new["items"], f"{where}[]", direction)
    return problems


def _content_schemas(content: Json) -> Json:
    return {media: body.get("schema", {}) for media, body in content.items()}


def breaking_changes(old: Json, new: Json) -> list[str]:
    problems: list[str] = []
    for path, old_methods in old["paths"].items():
        for method in METHODS:
            if method not in old_methods:
                continue
            where = f"{method.upper()} {path}"
            new_op = new["paths"].get(path, {}).get(method)
            if new_op is None:
                problems.append(f"{where}: removed")
                continue
            old_op = old_methods[method]
            old_params = {(p["in"], p["name"]): p for p in old_op.get("parameters", [])}
            for key, param in {(p["in"], p["name"]): p for p in new_op.get("parameters", [])}.items():
                if param.get("required") and not old_params.get(key, {}).get("required"):
                    problems.append(f"{where}: parameter {key[1]} ({key[0]}) is now required")
            for media, schema in _content_schemas(old_op.get("requestBody", {}).get("content", {})).items():
                new_schema = _content_schemas(new_op.get("requestBody", {}).get("content", {})).get(media)
                if new_schema is None:
                    problems.append(f"{where}: request body {media} removed")
                else:
                    problems += compare_schema(old, new, schema, new_schema, f"{where} request", "request")
            if new_op.get("requestBody", {}).get("required") and not old_op.get("requestBody", {}).get("required"):
                problems.append(f"{where}: request body is now required")
            for status, response in old_op.get("responses", {}).items():
                new_response = new_op.get("responses", {}).get(status)
                if new_response is None:
                    problems.append(f"{where}: response {status} removed")
                    continue
                new_content = _content_schemas(new_response.get("content", {}))
                for media, schema in _content_schemas(response.get("content", {})).items():
                    if media not in new_content:
                        problems.append(f"{where}: response {status} {media} removed")
                    else:
                        problems += compare_schema(
                            old, new, schema, new_content[media], f"{where} {status}", "response"
                        )
    return problems


def major(version: str) -> int:
    return int(version.split(".")[0])


def verdict(old: Json, new: Json) -> list[str]:
    """What is wrong with the change; empty if it is versioned correctly."""
    problems = breaking_changes(old, new)
    if problems and major(new["info"]["version"]) <= major(old["info"]["version"]):
        return [
            f"breaking change without a new major version ({old['info']['version']} -> {new['info']['version']}):",
            *[f"  - {p}" for p in problems],
        ]
    return []


def main() -> int:
    base_ref = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    shown = subprocess.run(
        ["git", "show", f"{base_ref}:{SCHEMA}"], cwd=REPO, capture_output=True, text=True, encoding="utf-8", check=False
    )
    if shown.returncode != 0:
        print(f"{SCHEMA} is not on {base_ref}: nothing to compare (first version of the API)")
        return 0
    wrong = verdict(json.loads(shown.stdout), json.loads((REPO / SCHEMA).read_text(encoding="utf-8")))
    print("\n".join(wrong) if wrong else f"{SCHEMA}: no breaking change against {base_ref}")
    return 1 if wrong else 0


if __name__ == "__main__":
    sys.exit(main())
