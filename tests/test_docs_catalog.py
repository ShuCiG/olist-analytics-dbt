"""Docs catalog of a slim prod deploy (.github/scripts/docs_catalog.py, D77): which
entries come from the partial catalog, which from the last deploy's."""

import importlib.util
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("docs_catalog", ROOT / ".github" / "scripts" / "docs_catalog.py")
assert _spec and _spec.loader
docs_catalog = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(docs_catalog)


def _entry(*columns: str) -> dict[str, Any]:
    return {"columns": {c: {"type": "STRING", "index": i, "name": c} for i, c in enumerate(columns, 1)}}


MANIFEST = {
    "nodes": {"model.olist.kept": {}, "model.olist.changed": {}, "model.olist.dropped_table": {}, "model.olist.new": {}},
    "sources": {"source.olist.raw.orders": {}},
}
BASELINE = {
    "nodes": {
        "model.olist.kept": _entry("a"),
        "model.olist.changed": _entry("old"),
        "model.olist.dropped_table": _entry("x"),
        "model.olist.removed_from_project": _entry("y"),
    },
    "sources": {"source.olist.raw.orders": _entry("order_id")},
}
PARTIAL = {"nodes": {"model.olist.changed": _entry("new"), "model.olist.new": _entry("b")}, "sources": {}, "errors": None}
SELECTED = {"model.olist.changed", "model.olist.new", "model.olist.dropped_table"}
FRESH = {"metadata": {"generated_at": "now"}, "nodes": {}, "sources": {}, "errors": None}


def test_merge_takes_selected_from_partial_and_the_rest_from_baseline() -> None:
    merged = docs_catalog.merge(FRESH, BASELINE, PARTIAL, MANIFEST, SELECTED)
    assert merged["nodes"] == {
        "model.olist.changed": _entry("new"),
        "model.olist.kept": _entry("a"),
        "model.olist.new": _entry("b"),
    }
    assert merged["sources"] == BASELINE["sources"]
    assert merged["metadata"] == FRESH["metadata"]


def test_merge_drops_selected_relations_missing_from_partial() -> None:
    # Selected but not in the partial catalog: the relation no longer exists
    merged = docs_catalog.merge(FRESH, BASELINE, PARTIAL, MANIFEST, SELECTED)
    assert "model.olist.dropped_table" not in merged["nodes"]


def test_merge_drops_nodes_gone_from_the_manifest() -> None:
    merged = docs_catalog.merge(FRESH, BASELINE, PARTIAL, MANIFEST, SELECTED)
    assert "model.olist.removed_from_project" not in merged["nodes"]


def test_merge_with_nothing_selected_keeps_the_baseline() -> None:
    empty = {"nodes": {}, "sources": {}}
    merged = docs_catalog.merge(FRESH, BASELINE, empty, MANIFEST, set())
    assert set(merged["nodes"]) == {"model.olist.kept", "model.olist.changed", "model.olist.dropped_table"}


def test_static_page_inlines_manifest_and_catalog() -> None:
    page = docs_catalog.static_page('<script>m = "MANIFEST.JSON INLINE DATA"; c = "CATALOG.JSON INLINE DATA"</script>', '{"m": 1}', '{"c": 2}')
    assert page == '<script>m = {"m": 1}; c = {"c": 2}</script>'


def test_static_page_fails_on_a_changed_docs_page() -> None:
    with pytest.raises(ValueError, match="static format changed"):
        docs_catalog.static_page("<html></html>", "{}", "{}")
