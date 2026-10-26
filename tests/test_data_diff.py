"""PR data diff comment (.github/scripts/data_diff.py, D73): models it picks, Markdown it renders."""

import importlib.util
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("data_diff", ROOT / ".github" / "scripts" / "data_diff.py")
assert _spec and _spec.loader
data_diff = importlib.util.module_from_spec(_spec)
sys.modules["data_diff"] = data_diff
_spec.loader.exec_module(data_diff)


def _model(name: str, fqn: list[str], schema: str, **extra: Any) -> dict[str, Any]:
    return {
        "resource_type": "model", "package_name": "olist", "name": name, "fqn": ["olist", *fqn],
        "database": "proj", "schema": schema, "alias": extra.pop("alias", name), **extra,
    }


def _pk_column(name: str) -> dict[str, Any]:
    return {name: {"constraints": [{"type": "not_null"}, {"type": "primary_key"}]}}


def _unique_test(model: str, column: str) -> dict[str, Any]:
    return {"resource_type": "test", "attached_node": model, "test_metadata": {"name": "unique", "kwargs": {"column_name": column}}}


CI = {
    "metadata": {"project_name": "olist"},
    "nodes": {
        "model.olist.dim_sellers": _model("dim_sellers", ["marts", "dim_sellers"], "ci_pr_1_marts", columns=_pk_column("seller_id")),
        "model.olist.dim_date": _model("dim_date", ["marts", "dim_date"], "ci_pr_1_marts"),
        "test.olist.unique_dim_date": _unique_test("model.olist.dim_date", "date_day"),
        "model.olist.fct_orders.v2": _model("fct_orders", ["marts", "fct_orders", "v2"], "ci_pr_1_marts", version="2",
                                            constraints=[{"type": "primary_key", "columns": ["order_id"]}]),
        "model.olist.dim_new": _model("dim_new", ["marts", "dim_new"], "ci_pr_1_marts"),
        "model.olist.stg_olist__orders": _model("stg_olist__orders", ["staging", "olist", "stg_olist__orders"], "ci_pr_1_staging"),
        "model.elementary.dbt_models": {**_model("dbt_models", ["marts", "x"], "ci_pr_1_elementary"), "package_name": "elementary"},
        "model.olist.dim_failed": _model("dim_failed", ["marts", "dim_failed"], "ci_pr_1_marts"),
    },
}
PROD = {"nodes": {
    uid: {**node, "schema": "marts"} for uid, node in CI["nodes"].items() if uid != "model.olist.dim_new"
}}
RUN_RESULTS = {"results": [
    {"unique_id": uid, "status": "error" if uid.endswith("dim_failed") else "success"}
    for uid in [*CI["nodes"], "model.olist.gone"]
]}


def test_plan_picks_built_project_marts_with_keys_and_both_relations() -> None:
    models = {m["name"]: m for m in data_diff.plan(CI, RUN_RESULTS, PROD)}
    assert list(models) == ["dim_date", "dim_new", "dim_sellers", "fct_orders_v2"]
    assert models["dim_sellers"]["pk"] == ["seller_id"]  # column constraint
    assert models["fct_orders_v2"]["pk"] == ["order_id"]  # model constraint
    assert models["dim_date"]["pk"] == ["date_day"]  # unique test
    assert models["dim_new"]["pk"] == [] and models["dim_new"]["prod"] is None
    assert models["fct_orders_v2"]["ci"] == {"database": "proj", "schema": "ci_pr_1_marts", "identifier": "fct_orders"}
    assert models["fct_orders_v2"]["prod"] == {"database": "proj", "schema": "marts", "identifier": "fct_orders"}


IDENTICAL = {"name": "dim_date", "status": "compared", "rows": {"identical": 1460}, "samples": {},
             "only_in_prod": [], "only_in_ci": [], "retyped": [], "differing_columns": []}
CHANGED = {
    "name": "dim_sellers", "status": "compared",
    "rows": {"identical": 2907, "modified": 186, "added": 1, "removed": 2},
    "samples": {"added": ["zzz"], "modified": ["a1", "b2"]},
    "only_in_prod": ["old"], "only_in_ci": ["extra"], "retyped": [{"column": "n", "prod": "int64", "ci": "numeric"}],
    "differing_columns": ["city"],
}


def test_identical_marts() -> None:
    body = data_diff.render([IDENTICAL], "https://ci/run")
    assert body.startswith(data_diff.MARKER + "\n")
    assert "**No differences**: the 1 marts this PR builds match prod row for row ([CI run](https://ci/run))." in body
    assert "| `dim_date` | 1,460 → 1,460 |  |  |  |  |" in body
    assert "Sample keys" not in body


def test_changed_marts_first_with_columns_and_samples() -> None:
    new = {"name": "dim_new", "status": "new"}
    body = data_diff.render([IDENTICAL, CHANGED, new])
    assert "**2 of 3 marts differ from prod**." in body
    rows = [line for line in body.splitlines() if line.startswith("| `")]
    assert rows[0] == "| `dim_new` | new model, not in prod | | | |  |"
    assert rows[1] == ("| `dim_sellers` | 3,095 → 3,094 | 186 | 1 | 2 | "
                       "`city`, new `extra`, dropped `old`, `n` int64 → numeric |")
    assert rows[2].startswith("| `dim_date` |")
    assert "- `dim_sellers` added: `zzz`" in body and "- `dim_sellers` modified: `a1`, `b2`" in body


def test_schema_change_alone_differs() -> None:
    result = {**IDENTICAL, "only_in_ci": ["extra"]}
    assert data_diff.differs(result)
    assert not data_diff.differs(IDENTICAL)


def test_duplicate_keys_counted() -> None:
    result = {**IDENTICAL, "rows": {"identical": 10, "nonunique_pk": 4}}
    assert "10 → 10 (4 with a duplicate key)" in data_diff.render([result])
    assert data_diff.differs(result)


def test_no_marts_and_failure() -> None:
    assert "changed no mart: nothing to compare." in data_diff.render([])
    failed = data_diff.render([], "https://ci/run", error="boom")
    assert failed.startswith(data_diff.MARKER) and "The data diff failed ([CI run](https://ci/run))" in failed
