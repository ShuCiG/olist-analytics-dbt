"""Status page (D125): totals as `dbt build` prints them, findings with their decision,
contract and freshness summaries, the rendered page, and the wiring in the workflow."""

import importlib.util
import json
import re
from pathlib import Path
from typing import Any

import yaml

from alerting import status

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = yaml.safe_load((ROOT / ".github" / "workflows" / "ci-cd.yml").read_text(encoding="utf-8"))


def load(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RUN_RESULTS: dict[str, Any] = {
    "metadata": {"generated_at": "2026-11-22T08:22:31.238605Z", "dbt_version": "1.12.7"},
    "elapsed_time": 87.6,
    "results": [
        {"unique_id": "model.olist.a", "status": "success"},
        {"unique_id": "test.olist.t1", "status": "pass", "failures": 0},
        {"unique_id": "test.olist.t2", "status": "warn", "failures": 1359},
        {"unique_id": "test.olist.t3", "status": "warn", "failures": 3},
        {"unique_id": "test.olist.t4", "status": "fail", "failures": 2},
        {"unique_id": "model.olist.b", "status": "error"},
        {"unique_id": "model.olist.c", "status": "skipped"},
        {"unique_id": "model.olist.d", "status": "no-op"},
    ],
}
MANIFEST: dict[str, Any] = {
    "nodes": {
        "test.olist.t2": {
            "name": "is_not_before_x",
            "attached_node": "model.olist.fct_orders",
            "description": "Orders shipped\n  before payment.",
            "config": {"meta": {"alert_unit": "orders", "decision": "D14"}},
        },
        "test.olist.t3": {"name": "other", "description": "", "config": {"meta": {}}},
        "test.olist.t4": {"name": "broken", "description": "Broken.", "config": {}},
    }
}


def test_totals_use_the_buckets_of_the_dbt_end_of_run_line() -> None:
    assert status.build_totals(RUN_RESULTS) == {"pass": 2, "warn": 2, "error": 2, "skip": 1, "no-op": 1, "total": 8}


def test_findings_list_warnings_and_errors_with_the_decision_that_explains_them() -> None:
    findings = status.build_findings(RUN_RESULTS, MANIFEST)
    # errors first (most failures first), then warnings the same way
    assert [f["name"] for f in findings] == ["broken", "model.olist.b", "is_not_before_x", "other"]
    by_name = {f["name"]: f for f in findings}
    warn = by_name["is_not_before_x"]
    assert warn["description"] == "Orders shipped before payment."
    assert (warn["model"], warn["failures"], warn["unit"], warn["decision"]) == ("fct_orders", 1359, "orders", "D14")
    assert by_name["other"]["decision"] == ""
    assert by_name["broken"]["outcome"] == "error"


def test_contract_summary_counts_failed_checks() -> None:
    result = {
        "dataContractId": "olist-marts",
        "dataContractVersion": "1.7.0",
        "result": "failed",
        "checks": [
            {"result": "passed", "name": "a"},
            {"result": "failed", "model": "fct_order_items", "name": "Freshness < 1d"},
            {"result": "skipped", "name": "c"},
        ],
    }
    summary = status.contract(result)
    assert (summary["passed"], summary["failed"], summary["total"]) == (1, 1, 3)
    assert summary["failed_checks"] == ["fct_order_items Freshness < 1d"]


def test_freshness_summary_keeps_only_the_sources_that_are_not_fresh() -> None:
    sources = {
        "results": [
            {"unique_id": "source.olist.olist.orders", "status": "pass", "max_loaded_at_time_ago_in_s": 5},
            {
                "unique_id": "source.olist.olist.geolocation",
                "status": "warn",
                "max_loaded_at": "2026-10-11T20:31:00+00:00",
                "max_loaded_at_time_ago_in_s": 7200,
            },
        ]
    }
    fresh = status.freshness(sources)
    assert fresh["totals"] == {"pass": 1, "warn": 1}
    assert fresh["findings"][0]["source"] == "olist.geolocation" and fresh["findings"][0]["age_hours"] == 2.0
    assert status.freshness(None) == {"totals": {}, "findings": []}


def test_overall_state() -> None:
    ok = {"pass": 5, "warn": 0, "error": 0}
    assert status.overall(ok, [], {}) == "healthy"
    assert status.overall({**ok, "warn": 2}, [], {}) == "warnings"
    assert status.overall({**ok, "error": 1}, [], {}) == "failing"
    assert status.overall(ok, [{"failed": 1}], {}) == "failing"
    assert status.overall(ok, [], {"error": 1}) == "failing"


def test_page_renders_findings_and_flags_unexplained_warnings() -> None:
    report = status.build_status(RUN_RESULTS, MANIFEST, [], None, [], {"sha": "abcdef1234", "run_url": "https://x/run/1"})
    page = load("data-product/status_site.py", "status_site").render(report)
    assert "Orders shipped before payment." in page and "D14" in page
    assert "unexplained" in page, "a warning without a decision is visible"
    assert "abcdef1" in page and "https://x/run/1" in page
    assert "Checks failing" in page


def test_every_warn_severity_test_names_the_decision_that_keeps_it() -> None:
    """`meta.decision` is what the status page shows next to a known warning."""
    decisions = set(re.findall(r"^## (D\d+) ", (ROOT / "docs" / "DECISIONS.md").read_text(encoding="utf-8"), re.M))
    missing: list[str] = []

    def walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            config = node.get("config")
            if isinstance(config, dict) and config.get("severity") == "warn":
                decision = (config.get("meta") or {}).get("decision")
                if decision not in decisions:
                    missing.append(f"{path}: decision {decision!r}")
            for key, value in node.items():
                walk(value, f"{path}/{node.get('name', key)}")
        elif isinstance(node, list):
            for item in node:
                walk(item, path)

    for file in [*(ROOT / "transform" / "models").rglob("*.yml"), *(ROOT / "transform" / "tests").glob("*.yml")]:
        walk(yaml.safe_load(file.read_text(encoding="utf-8")), file.name)
    assert not missing, missing


def test_workflow_hands_the_build_results_to_the_deploy() -> None:
    build = WORKFLOW["jobs"]["dbt-build"]["steps"]
    assert any(s.get("with", {}).get("name") == "status-inputs" for s in build)
    runs = "\n".join(s.get("run", "") for s in build)
    for name in ("raw", "review-labels", "brasilapi-holidays", "marts"):
        assert f"status-inputs/contract-{name}.json" in runs
    deploy = "\n".join(s.get("run", "") for s in WORKFLOW["jobs"]["deploy-prod"]["steps"])
    assert "--name status-inputs" in deploy and "status_report.py" in deploy and "--out _site/status" in deploy
    # the SLO report is written before the page reads it
    assert deploy.index("slo_report.py") < deploy.index("status_report.py")
