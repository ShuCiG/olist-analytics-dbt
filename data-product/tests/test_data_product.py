"""The data product descriptor, its contracts and the dbt project agree.

`datacontract test` checks the contracts against the data; this checks the files
against each other and against the dbt project:
- the descriptor is valid ODPS, and every port points to an existing contract version,
- the product owner is the owner of the dbt groups,
- the output contract describes the public dbt marts column for column (names,
  types, primary keys), so a model change that breaks the promise fails CI,
- the input contract covers exactly the dbt sources,
- every consumer agreement (consumers/*.yaml) points to an existing port, a compatible
  contract version, a dbt exposure and metrics, and only uses promised columns,
- the versioning rule of check_compatibility.py, and the product page builds.

Needs the dbt packages (`dbt deps`, for `dbt parse`), not built data:
uv run pytest data-product
"""

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import yaml

PRODUCT_DIR = Path(__file__).resolve().parents[1]
REPO = PRODUCT_DIR.parent
TRANSFORM = REPO / "transform"
DESCRIPTOR = PRODUCT_DIR / "olist-commerce.odps.yaml"
ODPS_SCHEMA = PRODUCT_DIR / "odps-json-schema-v1.1.0.json"

# dbt column data types (DuckDB and BigQuery spellings) → ODCS logical types
LOGICAL_TYPES = {
    "string": "string", "varchar": "string",
    "bigint": "integer", "integer": "integer", "int64": "integer",
    "numeric": "number", "double": "number", "float64": "number",
    "boolean": "boolean", "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}


def load(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def product() -> dict[str, Any]:
    return load(DESCRIPTOR)


@pytest.fixture(scope="module")
def contracts() -> dict[str, dict[str, Any]]:
    """Contract files by id."""
    found = [load(p) for p in sorted((PRODUCT_DIR / "contracts").glob("*.odcs.yaml"))]
    by_id = {c["id"]: c for c in found}
    assert len(by_id) == len(found), "duplicate contract ids"
    return by_id


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    dbt = shutil.which("dbt", path=str(Path(sys.executable).parent))
    assert dbt, f"dbt not found next to {sys.executable}"
    # Default target (dev, DuckDB): only the project's structure is read, no data
    env = {k: v for k, v in os.environ.items() if k != "DBT_TARGET"}
    done = subprocess.run(
        [dbt, "parse", "--quiet"], cwd=TRANSFORM, env=env, capture_output=True, text=True, encoding="utf-8"
    )
    assert done.returncode == 0, done.stdout[-3000:] + done.stderr[-3000:]
    with (TRANSFORM / "target" / "manifest.json").open(encoding="utf-8") as f:
        return json.load(f)


def output_contract(product: dict[str, Any], contracts: dict[str, dict[str, Any]], port: str) -> dict[str, Any]:
    (spec,) = [p for p in product["outputPorts"] if p["name"] == port]
    return contracts[spec["contractId"]]


def test_descriptor_is_valid_odps(product: dict[str, Any]) -> None:
    with ODPS_SCHEMA.open(encoding="utf-8") as f:
        schema = json.load(f)
    jsonschema.Draft201909Validator(schema).validate(product)
    assert product["apiVersion"] == "v1.1.0"


def test_ports_reference_existing_contract_versions(
    product: dict[str, Any], contracts: dict[str, dict[str, Any]]
) -> None:
    refs = [(p["contractId"], p["version"]) for p in product["inputPorts"] + product["outputPorts"] if "contractId" in p]
    refs += [(c["id"], c["version"]) for p in product["outputPorts"] for c in p.get("inputContracts", [])]
    for contract_id, version in refs:
        assert contract_id in contracts, f"no contract file with id {contract_id}"
        assert contracts[contract_id]["version"] == version, f"{contract_id}: port pins {version}"
    # Every contract belongs to this product and is used by a port
    used = {cid for cid, _ in refs}
    for contract_id, contract in contracts.items():
        assert contract["dataProduct"] == product["id"], contract_id
        assert contract_id in used, f"{contract_id} is not bound to any port"


def test_product_owner_owns_the_dbt_groups(
    product: dict[str, Any], contracts: dict[str, dict[str, Any]], manifest: dict[str, Any]
) -> None:
    def owners(doc: dict[str, Any]) -> set[str]:
        return {m["name"] for m in doc["team"]["members"] if m.get("role") == "Product Owner"}

    (owner,) = owners(product)
    for contract_id, contract in contracts.items():
        assert owners(contract) == {owner}, contract_id
    groups = {g["name"]: g["owner"]["name"] for g in manifest["groups"].values()}
    (declared,) = [p["value"] for p in product["customProperties"] if p["property"] == "dbtGroups"]
    assert set(declared) == set(groups)
    assert set(groups.values()) == {owner}


def mart_model(manifest: dict[str, Any], relation: str) -> dict[str, Any]:
    """The dbt model built as `relation`: for a versioned model, its latest version."""
    nodes = [
        n for n in manifest["nodes"].values()
        if n["resource_type"] == "model" and n["alias"] == relation and n["package_name"] == "olist"
    ]
    assert len(nodes) == 1, f"no single dbt model builds {relation}"
    return nodes[0]


def test_output_contract_matches_dbt_marts(
    product: dict[str, Any], contracts: dict[str, dict[str, Any]], manifest: dict[str, Any]
) -> None:
    contract = output_contract(product, contracts, "marts")
    for obj in contract["schema"]:
        node = mart_model(manifest, obj["physicalName"])
        where = f"{obj['name']} ({node['unique_id']})"
        # The promise is backed by dbt: public, with an enforced model contract
        assert node["access"] == "public", where
        assert node["config"]["contract"]["enforced"], where
        if node.get("version") is not None:
            assert node["version"] == node["latest_version"], f"{where}: contract must follow the latest version"

        promised = {p["name"]: p for p in obj["properties"]}
        built = node["columns"]
        assert list(promised) == list(built), (
            f"{where}: columns differ from the contract.\n"
            f"  only in dbt: {sorted(set(built) - set(promised))}\n"
            f"  only in contract: {sorted(set(promised) - set(built))}\n"
            "  (same set, other order: keep the dbt column order)"
        )
        for name, prop in promised.items():
            data_type = built[name]["data_type"].split("(")[0].strip().lower()
            if data_type.startswith("timestamp"):  # with / without time zone
                data_type = "timestamp"
            assert LOGICAL_TYPES.get(data_type) == prop["logicalType"], (
                f"{where}.{name}: dbt type {built[name]['data_type']} vs contract {prop['logicalType']}"
            )
        dbt_keys = {
            name for name, col in built.items() if any(c["type"] == "primary_key" for c in col["constraints"])
        }
        assert {n for n, p in promised.items() if p.get("primaryKey")} == dbt_keys, f"{where}: primary key"


def test_metrics_port_points_to_the_semantic_layer(product: dict[str, Any], manifest: dict[str, Any]) -> None:
    (port,) = [p for p in product["outputPorts"] if p["name"] == "metrics"]
    (definition,) = port["authoritativeDefinitions"]
    path = definition["url"].split("/blob/main/", 1)[1]
    assert (REPO / path).is_file(), path
    assert manifest["metrics"], "the dbt project defines no metrics"


def test_input_contract_covers_the_dbt_sources(
    product: dict[str, Any], contracts: dict[str, dict[str, Any]], manifest: dict[str, Any]
) -> None:
    (port,) = product["inputPorts"]
    contract = contracts[port["contractId"]]
    sources = {s["name"] for s in manifest["sources"].values() if s["source_name"] == "olist"}
    assert {obj["physicalName"] for obj in contract["schema"]} == sources


@pytest.fixture(scope="module")
def agreements() -> list[dict[str, Any]]:
    found = [load(p) for p in sorted((PRODUCT_DIR / "consumers").glob("*.yaml"))]
    assert found, "no consumer agreements"
    return found


def test_consumer_agreements_match_the_product(
    product: dict[str, Any],
    contracts: dict[str, dict[str, Any]],
    agreements: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    ports = {p["name"]: p for p in product["outputPorts"]}
    for agreement in agreements:
        who, provider = agreement["id"], agreement["provider"]
        assert agreement["kind"] == "DataUsageAgreement", who
        assert provider["dataProduct"] == product["id"], who
        port = ports[provider["outputPort"]]
        assert port["contractId"] == provider["contractId"], who
        contract = contracts[provider["contractId"]]

        # Compatible = same major version, not newer than the contract
        pinned, current = (tuple(map(int, v.split("."))) for v in (provider["contractVersion"], contract["version"]))
        assert pinned[0] == current[0] and pinned <= current, (
            f"{who} pins {provider['contractVersion']}, contract is {contract['version']}: "
            "update the consumer for the breaking change, then renew the agreement"
        )
        assert provider["server"] in {s["server"] for s in contract["servers"]}, who
        assert provider["role"] in {r["role"] for r in contract.get("roles", [])}, who

        promised = {obj["physicalName"]: {p["name"] for p in obj["properties"]} for obj in contract["schema"]}
        for table, columns in agreement["uses"].items():
            missing = set(columns) - promised.get(table, set())
            assert not missing, f"{who} uses {table}.{sorted(missing)}, which {contract['id']} no longer promises"

        # The dbt exposure (lineage in the dbt docs) lists the same tables
        (exposure,) = [e for e in manifest["exposures"].values() if e["name"] == agreement["consumer"]["dbtExposure"]]
        exposed = {manifest["nodes"][uid]["alias"] for uid in exposure["depends_on"]["nodes"]}
        assert exposed == set(agreement["uses"]), f"{who}: exposure depends on {sorted(exposed)}"

        metrics = agreement.get("metrics")
        if metrics:
            assert ports[metrics["outputPort"]]["type"] == "semantic-layer", who
            defined = {m["name"] for m in manifest["metrics"].values()}
            assert set(metrics["names"]) <= defined, f"{who}: unknown metrics {set(metrics['names']) - defined}"


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, PRODUCT_DIR / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("old", "new", "changed", "breaking", "ok"),
    [
        ("1.0.0", "1.0.0", False, False, True),   # untouched
        ("1.0.0", "1.0.1", True, False, True),    # description, new rule
        ("1.0.0", "1.1.0", True, False, True),    # added column
        ("1.0.0", "1.0.0", True, False, False),   # changed, version kept
        ("1.0.0", "1.1.0", True, True, False),    # breaking as a minor
        ("1.4.2", "2.0.0", True, True, True),     # breaking as a major
        ("2.0.0", "1.9.0", True, False, False),   # version went down
    ],
)
def test_contract_versioning_rule(old: str, new: str, changed: bool, breaking: bool, ok: bool) -> None:
    check = load_script("check_compatibility")
    assert (check.verdict(old, new, changed, breaking) is None) == ok


def test_product_page_builds(tmp_path: Path, product: dict[str, Any], agreements: list[dict[str, Any]]) -> None:
    out = tmp_path / "data-product"
    load_script("build_site").build(out)
    page = (out / "index.html").read_text(encoding="utf-8")
    assert product["name"] in page
    for agreement in agreements:
        assert agreement["name"] in page
    # Relative links (contract pages) point to files that were built; ../ is the dbt docs
    for href in re.findall(r'href="([^"]+)"', page):
        if not href.startswith(("http", "../")):
            assert (out / href).is_file(), href
