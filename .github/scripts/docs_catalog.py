"""Docs catalog of a slim prod deploy: the last deploy's catalog with the relations
this deploy can have changed read again (D77).

On BigQuery every catalog query bills 50 MiB per dataset (minimums of the
INFORMATION_SCHEMA views and `__TABLES__` it reads), whatever the number of tables.
A slim deploy can only change the relations of `state:modified+`, so the deploy step
reads the catalog of those alone (`dbt docs generate --select`), then runs
`dbt docs generate --empty-catalog` for the full manifest (compiled code of every
node) and the docs page, and this script puts the catalog together:

- entries of the selected relations come from the new partial catalog (a selected
  relation missing there no longer exists and is dropped);
- every other node or source of the manifest keeps the last deploy's entry (its
  columns can't have changed; its table stats are those of that deploy);
- nodes and sources gone from the manifest are dropped.

It then writes transform/target/catalog.json and static_index.html (the page with
manifest and catalog inlined, as `dbt docs generate --static` does).

Usage: python .github/scripts/docs_catalog.py <baseline catalog> <partial catalog> <selected ids>
       (selected ids: one unique_id per line, the selection of the partial run)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

TARGET = Path(__file__).resolve().parents[2] / "transform" / "target"
# Placeholders of dbt's docs page, replaced the same way by `docs generate --static`
MANIFEST_PLACEHOLDER = '"MANIFEST.JSON INLINE DATA"'
CATALOG_PLACEHOLDER = '"CATALOG.JSON INLINE DATA"'


def merge(
    fresh: dict[str, Any], baseline: dict[str, Any], partial: dict[str, Any],
    manifest: dict[str, Any], selected: set[str],
) -> dict[str, Any]:
    """`fresh` = the empty catalog of this run (its metadata is kept)."""
    current = set(manifest["nodes"]) | set(manifest["sources"])
    merged = dict(fresh)
    for key in ("nodes", "sources"):
        entries = {
            uid: entry for uid, entry in baseline.get(key, {}).items()
            if uid in current and uid not in selected
        }
        entries.update(partial.get(key, {}))
        merged[key] = dict(sorted(entries.items()))
    merged["errors"] = partial.get("errors")
    return merged


def static_page(index_html: str, manifest_text: str, catalog_text: str) -> str:
    for placeholder in (MANIFEST_PLACEHOLDER, CATALOG_PLACEHOLDER):
        if placeholder not in index_html:
            raise ValueError(f"dbt's docs page has no {placeholder}: its static format changed")
    return index_html.replace(MANIFEST_PLACEHOLDER, manifest_text).replace(CATALOG_PLACEHOLDER, catalog_text)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main(baseline_path: str, partial_path: str, selected_path: str) -> None:
    manifest_text = (TARGET / "manifest.json").read_text(encoding="utf-8")
    selected = {line.strip() for line in Path(selected_path).read_text(encoding="utf-8").splitlines() if line.strip()}
    partial = _read(Path(partial_path))
    catalog = merge(_read(TARGET / "catalog.json"), _read(Path(baseline_path)), partial, json.loads(manifest_text), selected)
    catalog_text = json.dumps(catalog)
    (TARGET / "catalog.json").write_text(catalog_text, encoding="utf-8")
    page = static_page((TARGET / "index.html").read_text(encoding="utf-8"), manifest_text, catalog_text)
    (TARGET / "static_index.html").write_text(page, encoding="utf-8")
    read = len(partial.get("nodes", {})) + len(partial.get("sources", {}))
    total = len(catalog["nodes"]) + len(catalog["sources"])
    print(f"Docs catalog: {read} relations read, {total - read} kept from the last deploy")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(*sys.argv[1:])
