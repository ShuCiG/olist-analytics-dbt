"""Names MetricFlow writes unquoted into SQL must not be reserved words (#282).

MetricFlow selects a foreign entity under its bare name (`order_id AS order`) and a metric
under its own name (`SUM(...) AS item_revenue`), without quoting. A primary entity is read
through its table alias (`orders_src.order_id`), so the primary `order` of fct_orders works;
only grouping by that entity itself would alias it (no pipeline query does). DuckDB accepts most
reserved words there, BigQuery does not, and CI queries the semantic layer on DuckDB only
(`mf validate-configs`), so a reserved name passes every check and fails in the BigQuery
DAG. This reads the semantic YAML directly: no manifest, no warehouse.
"""

from pathlib import Path
from typing import Any

import pytest
import yaml

MODELS = Path(__file__).resolve().parents[1] / "transform" / "models" / "marts" / "_marts__models.yml"

# BigQuery GoogleSQL reserved keywords
# (https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#reserved_keywords)
BIGQUERY_RESERVED = set(
    """
    all and any array as asc assert_rows_modified at between by case cast collate contains create
    cross cube current default define desc distinct else end enum escape except exclude exists
    extract false fetch following for from full group grouping groups hash having if ignore in
    inner intersect interval into is join lateral left like limit lookup merge natural new no not
    null nulls of on or order outer over partition preceding proto qualify range recursive respect
    right rollup rows select set some struct tablesample then to treat true unbounded union unnest
    using when where window with within
    """.split()
)


def unquoted_names(models: list[dict[str, Any]]) -> dict[str, str]:
    """Foreign entity and metric names of the inline semantic models (D99), each with where it is defined."""
    names: dict[str, str] = {}
    for model in models:
        if not (model.get("semantic_model") or {}).get("enabled"):
            continue
        for column in model.get("columns", []):
            entity = column.get("entity")
            if entity and entity.get("type") == "foreign":
                names[f"{model['name']}.{column['name']} entity"] = entity["name"]
        for metric in model.get("metrics", []):
            names[f"{model['name']} metric {metric['name']}"] = metric["name"]
    return names


def test_semantic_names_are_not_reserved_words() -> None:
    names = unquoted_names(yaml.safe_load(MODELS.read_text(encoding="utf-8"))["models"])
    assert len(names) > 20  # the YAML was read, not an empty list
    reserved = {where: name for where, name in names.items() if name.lower() in BIGQUERY_RESERVED}
    assert not reserved, f"reserved words MetricFlow would write unquoted: {reserved}"


@pytest.mark.parametrize(("name", "bad"), [("order", True), ("ORDER", True), ("order_item", False), ("seller", False)])
def test_the_check_catches_a_reserved_entity(name: str, bad: bool) -> None:
    models = [{"name": "m", "semantic_model": {"enabled": True}, "columns": [{"name": "c", "entity": {"name": name, "type": "foreign"}}]}]
    flagged = [n for n in unquoted_names(models).values() if n.lower() in BIGQUERY_RESERVED]
    assert bool(flagged) == bad


def person_entities(models: list[dict[str, Any]]) -> set[str]:
    """Entities on a column tagged person_id (D94), in any inline semantic model."""
    found = set()
    for model in models:
        if not (model.get("semantic_model") or {}).get("enabled"):
            continue
        for column in model.get("columns", []):
            if column.get("entity") and any("person_id" in tag for tag in column.get("policy_tags", [])):
                found.add(column["entity"]["name"])
    return found


def test_mcp_refuses_every_person_entity() -> None:
    # A new entity on a person id must be refused by the MCP server too (#281)
    from metrics_mcp.query import PERSON_ENTITIES

    found = person_entities(yaml.safe_load(MODELS.read_text(encoding="utf-8"))["models"])
    assert found, "no person entity found: the check would pass vacuously"
    assert found <= PERSON_ENTITIES, f"add to metrics_mcp.query.PERSON_ENTITIES: {sorted(found - PERSON_ENTITIES)}"
