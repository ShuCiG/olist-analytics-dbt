"""dbt exposures as jobs in the OpenLineage graph (#197): the dashboard reads the marts,
so Marquez shows it downstream of them, as dbt docs do.

An exposure's inputs are the outputs of the model events whose dbt node it depends on
(the `dbt_node` job facet carries the node's unique_id). Names thus come from the dbt
integration, per warehouse, with no list kept by hand; a model the build sent no event
for (failed, skipped) is left out.

No Airflow or OpenLineage imports: tested in the project venv (tests/test_lineage.py).
"""

from __future__ import annotations

from typing import Any


def exposures(manifest: dict[str, Any], type_: str = "dashboard") -> list[dict[str, Any]]:
    """The manifest's exposures of a type, as dbt writes them."""
    return sorted(
        (e for e in manifest["exposures"].values() if e["type"] == type_),
        key=lambda e: e["name"],
    )


def exposure_inputs(exposure: dict[str, Any], events: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Datasets the exposure reads ({namespace, name}): the outputs of the serialized
    model events whose dbt node is in its depends_on."""
    nodes = set(exposure["depends_on"]["nodes"])
    datasets = {
        (output["namespace"], output["name"])
        for event in events
        if event["job"].get("facets", {}).get("dbt_node", {}).get("unique_id") in nodes
        for output in event.get("outputs", [])
    }
    return [{"namespace": namespace, "name": name} for namespace, name in sorted(datasets)]
