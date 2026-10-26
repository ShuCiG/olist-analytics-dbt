"""Data diff of the marts a CI run built against their prod tables, as the Markdown
of a PR comment (D73).

Slim CI builds `state:modified+` into per-run datasets and defers the rest to prod.
This compares every mart that build made with the same model's prod table (the last
prod deploy, built from the same `raw`), row by row on the primary key, with
dbt-labs/audit_helper (macro `data_diff`, transform/macros/data_diff.sql):
rows identical / modified / added / removed, which columns differ, schema changes.
Marts only: what consumers read, and every changed upstream model shows up in the
marts below it. The bytes it bills carry the run's job label like every dbt query,
so the cost comment (D72) counts them.

Reads transform/target/{manifest,run_results}.json of the build (run it right after
`dbt build`, before another dbt command overwrites them) and the prod manifest.

Usage: python .github/scripts/data_diff.py <prod manifest> <out.md> [run url]
Env:   what `dbt --target ci` needs (DBT_CI_SCHEMA, Application Default Credentials)
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

# First line of the comment: the comment job finds and updates it on the next push
MARKER = "<!-- data-diff -->"
TRANSFORM = Path(__file__).resolve().parents[2] / "transform"
CHANGES = ("modified", "added", "removed", "nonunique_pk")


def primary_key(manifest: dict[str, Any], unique_id: str) -> list[str]:
    """The model's `primary_key` constraint, else the column of its `unique` test."""
    node = manifest["nodes"][unique_id]
    for constraint in node.get("constraints", []):
        if constraint["type"] == "primary_key" and constraint.get("columns"):
            return list(constraint["columns"])
    columns = [
        name
        for name, column in node.get("columns", {}).items()
        if any(c["type"] == "primary_key" for c in column.get("constraints", []))
    ]
    if columns:
        return columns
    for test in manifest["nodes"].values():
        meta = test.get("test_metadata") or {}
        if test["resource_type"] != "test" or test.get("attached_node") != unique_id:
            continue
        if meta.get("name") == "unique" and meta["kwargs"].get("column_name"):
            return [meta["kwargs"]["column_name"]]
        if meta.get("name") == "unique_combination_of_columns":
            return list(meta["kwargs"]["combination_of_columns"])
    return []


def relation(node: dict[str, Any]) -> dict[str, str]:
    return {"database": node["database"], "schema": node["schema"], "identifier": node["alias"]}


def plan(ci_manifest: dict[str, Any], run_results: dict[str, Any], prod_manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """The marts the build made, each with its key and both relations (prod: None when new)."""
    models = []
    for result in run_results["results"]:
        unique_id = result["unique_id"]
        node = ci_manifest["nodes"].get(unique_id)
        if result["status"] != "success" or not node or node["resource_type"] != "model":
            continue
        if node["package_name"] != ci_manifest["metadata"]["project_name"] or node["fqn"][1] != "marts":
            continue
        prod = prod_manifest["nodes"].get(unique_id)
        models.append({
            "name": node["name"] + (f"_v{node['version']}" if node.get("version") else ""),
            "pk": primary_key(ci_manifest, unique_id),
            "ci": relation(node),
            "prod": relation(prod) if prod else None,
        })
    return sorted(models, key=lambda m: m["name"])


def run_diff(models: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """`dbt run-operation data_diff`: with -q its JSON line is all it prints."""
    args = json.dumps({"models": models})
    proc = subprocess.run(
        ["dbt", "-q", "run-operation", "data_diff", "--target", "ci", "--args", args],
        cwd=TRANSFORM, capture_output=True, text=True, encoding="utf-8",
    )
    lines = [line for line in proc.stdout.splitlines() if line.startswith("[")]
    if proc.returncode != 0 or not lines:
        raise RuntimeError(f"dbt run-operation data_diff failed:\n{proc.stdout}\n{proc.stderr}")
    return json.loads(lines[-1])


def differs(result: dict[str, Any]) -> bool:
    if result["status"] != "compared":
        return True
    rows = result.get("rows", {})
    return any(rows.get(s, 0) for s in CHANGES) or any(result.get(k) for k in ("only_in_prod", "only_in_ci", "retyped"))


def column_changes(result: dict[str, Any]) -> str:
    parts = [f"`{c}`" for c in result.get("differing_columns", [])]
    parts += [f"new `{c}`" for c in result.get("only_in_ci", [])]
    parts += [f"dropped `{c}`" for c in result.get("only_in_prod", [])]
    parts += [f"`{r['column']}` {r['prod']} → {r['ci']}" for r in result.get("retyped", [])]
    return ", ".join(parts)


STATUS_NOTE = {
    "new": "new model, not in prod",
    "not_built": "no table in CI",
    "no_pk": "no primary key (constraint or unique test): not compared",
    "pk_not_shared": "primary key not in both tables: rows not compared",
}


def row(result: dict[str, Any]) -> str:
    name = f"`{result['name']}`"
    note = STATUS_NOTE.get(result["status"])
    if note:
        return f"| {name} | {note} | | | | {column_changes(result)} |"
    rows = result["rows"]
    prod = sum(rows.get(s, 0) for s in ("identical", "modified", "removed"))
    ci = sum(rows.get(s, 0) for s in ("identical", "modified", "added"))
    count = f"{prod:,} → {ci:,}"
    if rows.get("nonunique_pk"):
        count += f" ({rows['nonunique_pk']:,} with a duplicate key)"
    cells = [f"{rows.get(s, 0):,}" if rows.get(s) else "" for s in ("modified", "added", "removed")]
    return f"| {name} | {count} | {' | '.join(cells)} | {column_changes(result)} |"


def render(results: list[dict[str, Any]], run_url: str = "", error: str = "") -> str:
    run = f" ([CI run]({run_url}))" if run_url else ""
    lines = [MARKER, "### Data diff: this PR's marts vs prod", ""]
    if error:
        lines.append(f"The data diff failed{run}: see step `Data diff vs prod` of the run.")
        return "\n".join(lines) + "\n"
    if not results:
        lines.append(f"This PR's CI build changed no mart: nothing to compare{run}.")
        return "\n".join(lines) + "\n"
    changed = [r for r in results if differs(r)]
    if changed:
        lines.append(f"**{len(changed)} of {len(results)} marts differ from prod**{run}.")
    else:
        lines.append(f"**No differences**: the {len(results)} marts this PR builds match prod row for row{run}.")
    lines += ["", "| Mart | Rows prod → CI | Modified | Added | Removed | Columns |", "|---|--:|--:|--:|--:|---|"]
    lines += [row(r) for r in sorted(results, key=lambda r: (not differs(r), r["name"]))]
    samples = [
        f"- `{r['name']}` {status}: " + ", ".join(f"`{k}`" for k in keys)
        for r in results
        for status, keys in sorted(r.get("samples", {}).items())
    ]
    if samples:
        lines += ["", "<details><summary>Sample keys</summary>", "", *samples, "", "</details>"]
    lines += [
        "",
        "<sub>Prod: the tables of the last prod deploy, built from the same raw data as CI. "
        "Rows matched on the primary key, shared columns compared (dbt-labs/audit_helper). "
        "Changes merged to main after this branch was cut show up here too, reversed: rebase to drop them. "
        "Updated on every push.</sub>",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    if len(argv) not in (3, 4):
        print(__doc__, file=sys.stderr)
        return 2
    prod_manifest_path, out = argv[1:3]
    run_url = argv[3] if len(argv) == 4 else ""
    target = TRANSFORM / "target"
    ci_manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    run_results = json.loads((target / "run_results.json").read_text(encoding="utf-8"))
    prod_manifest = json.loads(Path(prod_manifest_path).read_text(encoding="utf-8"))
    status = 0
    try:
        # A failed step before the build leaves another command's results
        if run_results.get("args", {}).get("which") != "build":
            raise RuntimeError("target/run_results.json is not from `dbt build`")
        models = plan(ci_manifest, run_results, prod_manifest)
        keyed = [m for m in models if m["pk"]]
        results = run_diff(keyed) if keyed else []
        results += [{"name": m["name"], "status": "no_pk"} for m in models if not m["pk"]]
        body = render(results, run_url)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        body = render([], run_url, error=str(e))
        status = 1
    Path(out).write_text(body, encoding="utf-8")
    print(body)
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv))
