"""PR cost comment (.github/scripts/ci_cost.py, D72): the Markdown it renders."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("ci_cost", ROOT / ".github" / "scripts" / "ci_cost.py")
assert _spec and _spec.loader
ci_cost = importlib.util.module_from_spec(_spec)
sys.modules["ci_cost"] = ci_cost  # dataclasses resolve annotations through it
_spec.loader.exec_module(ci_cost)

MIB = ci_cost.MIB
NodeCost = ci_cost.NodeCost


def test_total_ranking_and_prod_column() -> None:
    ci = [
        NodeCost(None, None, 14, 220 * MIB),
        NodeCost("model", "dim_sellers", 3, 46 * MIB),
        NodeCost("test", "unique_dim_sellers_seller_id", 1, 10 * MIB),
    ]
    prod = {("model", "dim_sellers"): NodeCost("model", "dim_sellers", 2, 30 * MIB)}
    body = ci_cost.render(ci, prod, "https://ci/run")
    assert body.startswith(ci_cost.MARKER + "\n")
    assert "**276 MiB billed** in 18 jobs ([CI run](https://ci/run))." in body
    assert "The 1 of 2 nodes that prod built before cost 30 MiB in their last prod build." in body
    rows = [line for line in body.splitlines() if line.startswith("| ") and "---" not in line][1:]
    assert rows[0].startswith("| run-level")
    assert rows[1] == "| `dim_sellers` | model | 3 | 46 MiB | 30 MiB |"
    assert rows[2] == "| `unique_dim_sellers_seller_id` | test | 1 | 10 MiB | – |"


def test_top_nodes_then_the_rest_in_one_row() -> None:
    ci = [NodeCost("model", f"m{i:02}", 1, (100 - i) * MIB) for i in range(ci_cost.TOP + 3)]
    body = ci_cost.render(ci, {})
    assert "| `m14` |" in body and "| `m15` |" not in body
    assert f"| 3 more | | 3 | {85 + 84 + 83} MiB | |" in body
    assert "nodes that prod built" not in body


def test_unfinished_jobs_and_gib() -> None:
    body = ci_cost.render([NodeCost("model", "fct_orders_v2", 2, 1536 * MIB, unfinished=1)], {})
    assert "**1.50 GiB billed** in 2 jobs." in body
    assert "1 jobs were still running" in body


def test_no_jobs() -> None:
    body = ci_cost.render([], {})
    assert body.startswith(ci_cost.MARKER)
    assert "No dbt jobs found for this run." in body


def test_run_level_split_by_origin() -> None:
    ci = [
        NodeCost(None, None, 12, 180 * MIB, origin="elementary"),
        NodeCost(None, None, 5, 40 * MIB, origin="build"),
        NodeCost(None, None, 3, 30 * MIB, origin="run-operation"),
        NodeCost(None, None, 1, 10 * MIB, origin=""),
    ]
    rows = [line for line in ci_cost.render(ci, {}).splitlines() if line.startswith("| run-level")]
    assert rows == [
        "| run-level: Elementary uploads | run | 12 | 180 MiB | – |",
        "| run-level: dbt build (hooks, introspection) | run | 5 | 40 MiB | – |",
        "| run-level: dbt run-operation | run | 3 | 30 MiB | – |",
        "| run-level: dbt (hooks, introspection) | run | 1 | 10 MiB | – |",
    ]
