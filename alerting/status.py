"""Data status summary (D125): totals and known warnings of a dbt build, source
freshness, data contract checks and the SLO states, in one structure for the public
page /status/ (.github/scripts/status_report.py renders it).

Pure functions over the JSON files the tools write (`run_results.json`,
`manifest.json`, `sources.json`, `datacontract ci --output-format json`), no I/O and
no dependencies: the numbers are the tools' own, not recomputed from anything else.
"""

from __future__ import annotations

from typing import Any

# dbt's end-of-run line `PASS=.. WARN=.. ERROR=.. SKIP=.. NO-OP=..`: the same buckets
OUTCOME = {
    "success": "pass",
    "pass": "pass",
    "warn": "warn",
    "error": "error",
    "fail": "error",
    "skipped": "skip",
    "no-op": "no-op",
}
OUTCOMES = ("pass", "warn", "error", "skip", "no-op")


def build_totals(run_results: dict[str, Any]) -> dict[str, int]:
    """Counts per outcome, as `dbt build` prints them (unit tests included), plus `total`."""
    totals: dict[str, int] = dict.fromkeys(OUTCOMES, 0)
    for result in run_results["results"]:
        totals[OUTCOME.get(result["status"], "error")] += 1
    totals["total"] = len(run_results["results"])
    return totals


def build_findings(run_results: dict[str, Any], manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Warned and failed nodes, failures counted, with the description the data team
    wrote for the business reader and the decision (`meta.decision`) that explains why
    the warning is kept. A finding without a decision shows up as unexplained."""
    nodes: dict[str, dict[str, Any]] = manifest.get("nodes", {})
    findings: list[dict[str, Any]] = []
    for result in run_results["results"]:
        outcome = OUTCOME.get(result["status"], "error")
        if outcome not in ("warn", "error"):
            continue
        node: dict[str, Any] = nodes.get(result["unique_id"], {})
        meta: dict[str, Any] = (node.get("config") or dict[str, Any]()).get("meta") or dict[str, Any]()
        findings.append(
            {
                "outcome": outcome,
                "name": node.get("name") or result["unique_id"],
                "model": (node.get("attached_node") or "").rsplit(".", 1)[-1],
                "description": " ".join((node.get("description") or "").split()),
                "failures": result.get("failures"),
                "unit": meta.get("alert_unit", ""),
                "decision": meta.get("decision", ""),
            }
        )
    findings.sort(key=lambda f: (f["outcome"] != "error", -(f["failures"] or 0), f["name"]))
    return findings


def freshness(sources: dict[str, Any] | None) -> dict[str, Any]:
    """Source freshness of `dbt source freshness`: counts per outcome and the non-passing ones."""
    if not sources:
        return {"totals": {}, "findings": []}
    totals: dict[str, int] = {}
    findings: list[dict[str, Any]] = []
    for result in sources["results"]:
        outcome = OUTCOME.get(result["status"], "error")
        totals[outcome] = totals.get(outcome, 0) + 1
        if outcome != "pass":
            findings.append(
                {
                    "outcome": outcome,
                    "source": result["unique_id"].split(".", 2)[-1],
                    "max_loaded_at": result.get("max_loaded_at"),
                    "age_hours": round(result.get("max_loaded_at_time_ago_in_s", 0) / 3600, 1),
                }
            )
    return {"totals": totals, "findings": findings}


def contract(result: dict[str, Any]) -> dict[str, Any]:
    """One data contract run (`datacontract ci --output-format json`)."""
    checks = result.get("checks", [])
    failed = [c for c in checks if c.get("result") not in ("passed", "skipped", None)]
    return {
        "id": result.get("dataContractId", "?"),
        "version": result.get("dataContractVersion", "?"),
        "result": result.get("result", "unknown"),
        "passed": sum(c.get("result") == "passed" for c in checks),
        "failed": len(failed),
        "total": len(checks),
        "failed_checks": [f"{c.get('model') or ''} {c.get('name') or c.get('key')}".strip() for c in failed],
    }


def overall(totals: dict[str, int], contracts: list[dict[str, Any]], freshness_totals: dict[str, int]) -> str:
    """`failing` when a check errors, `warnings` when only known warnings are left, else `healthy`."""
    broken = totals["error"] or any(c["failed"] for c in contracts) or freshness_totals.get("error")
    if broken:
        return "failing"
    if totals["warn"] or freshness_totals.get("warn"):
        return "warnings"
    return "healthy"


def build_status(
    run_results: dict[str, Any],
    manifest: dict[str, Any],
    contracts: list[dict[str, Any]],
    sources: dict[str, Any] | None,
    slos: list[dict[str, Any]],
    meta: dict[str, Any],
) -> dict[str, Any]:
    """The page's data. `contracts`: summaries from `contract()`; `slos`: the `slos` list of
    the SLO report; `meta`: commit, run URL and anything else the caller knows."""
    totals = build_totals(run_results)
    fresh = freshness(sources)
    return {
        "overall": overall(totals, contracts, fresh["totals"]),
        "built_at": run_results["metadata"]["generated_at"],
        "dbt_version": run_results["metadata"].get("dbt_version", ""),
        "elapsed_seconds": round(run_results.get("elapsed_time", 0)),
        "totals": totals,
        "findings": build_findings(run_results, manifest),
        "freshness": fresh,
        "contracts": contracts,
        "slos": slos,
        **meta,
    }
