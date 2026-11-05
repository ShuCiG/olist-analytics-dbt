"""Column impact of a dbt change, as the Markdown of a PR comment (D101): which columns
the PR changes and who reads them, from the manifests of the PR and of prod. No
warehouse query.

1. Compiled SQL of both sides is rewritten to one naming: a relation's database and
   schema depend on the target (ci_pr_1_marts vs marts), so each table becomes
   `model.olist.fct_orders`, found through the manifest. The cursor of an incremental
   model is dropped: CI builds it in full, prod's compiled SQL has a filter on the last
   deploy's timestamp.
2. A column changed if it is added, removed, retyped in the YAML, or computed
   differently (lineage/columns.py). A filter, join or grouping that differs changes
   every column of the model.
3. Column lineage is followed downstream, from both sides: the PR's for new columns,
   prod's for removed ones. A model with no column path falls back to the model level.
4. Each column reached is looked up in the semantic layer, the data product's contracts
   and usage agreements, and the exposures.

The PR manifest holds compiled SQL for what `dbt build --select state:modified+`
selected; the rest was deferred to prod, so prod's SQL is what it reads.

Run from the repository root, after `dbt build`, before any other dbt command:
    python -m lineage.impact <prod manifest> <PR manifest> <out.md> [run url]
"""

from __future__ import annotations

import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot
import yaml
from sqlglot import exp
from sqlglot.errors import SqlglotError

from lineage.columns import ColumnTrace, ModelTrace, trace_models

Manifest = dict[str, Any]
Column = tuple[str, str]  # node unique_id, column name; WHOLE_MODEL = every column

# First line of the comment: job pr-comments finds and updates it on the next push
MARKER = "<!-- column-impact -->"
# Second line when nothing changed: updates an earlier comment, never opens a new one
UPDATE_ONLY = "<!-- update-only -->"
WHOLE_MODEL = "*"
ROOT = Path(__file__).resolve().parents[1]
# GitHub refuses a comment over 65,536 characters
MAX_BODY = 60_000
# Names shown per table cell; the details below the table list all downstream columns
MAX_NAMES = 8
TRACED_TYPES = ("model", "snapshot", "seed")
DIMENSION_REF = re.compile(r"Dimension\('(\w+)__(\w+)'\)")


@dataclass
class Consumers:
    """What reads the marts, from the data product's YAML (data-product/)."""

    contracts: list[dict[str, Any]] = field(default_factory=list)
    agreements: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Change:
    """One changed column of a model and everything downstream of it."""

    node: str
    column: str
    kind: str
    downstream: list[Column] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)
    semantic_fields: list[str] = field(default_factory=list)
    contract_columns: list[str] = field(default_factory=list)
    agreement_columns: list[str] = field(default_factory=list)
    exposures: list[str] = field(default_factory=list)


def _project_nodes(manifest: Manifest) -> dict[str, dict[str, Any]]:
    project = manifest["metadata"]["project_name"]
    return {
        uid: node
        for uid, node in manifest["nodes"].items()
        if node["resource_type"] in TRACED_TYPES and node["package_name"] == project
    }


def _relation_key(relation: str | None) -> str:
    return (relation or "").replace("`", "").replace('"', "").lower()


def _canonical_names(manifest: Manifest) -> dict[str, str]:
    """unique_id -> `<type>.<package or source>.<table>`: the same on every target."""
    names = {
        uid: f"{node['resource_type']}.{node['package_name']}.{node['alias']}".lower()
        for uid, node in manifest["nodes"].items()
        if node["resource_type"] in TRACED_TYPES
    }
    for uid, source in manifest.get("sources", {}).items():
        # An identifier can hold dots (INFORMATION_SCHEMA.JOBS_BY_PROJECT): three parts here
        identifier = source["identifier"].replace(".", "_")
        names[uid] = f"source.{source['source_name']}.{identifier}".lower()
    return names


def _relations(manifest: Manifest, names: dict[str, str]) -> dict[str, str]:
    """Relation as written in compiled SQL -> canonical name."""
    relations: dict[str, str] = {}
    for section in ("nodes", "sources"):
        for uid, node in manifest.get(section, {}).items():
            if uid in names and node.get("relation_name"):
                relations[_relation_key(node["relation_name"])] = names[uid]
    return relations


def _drop_cursor(tree: exp.Expr) -> None:
    """Remove an incremental model's cursor: `where col >= timestamp '...'` conjuncts,
    or a comparison with a subquery (`col > (select max(col) from this)`). Only one side
    has them, depending on whether the relation exists at compile time."""
    for where in list(tree.find_all(exp.Where)):
        kept = [c for c in where.this.flatten() if not _is_cursor(c)] if isinstance(where.this, exp.And) else (
            [] if _is_cursor(where.this) else [where.this]
        )
        if not kept:
            where.pop()
        else:
            condition = kept[0]
            for other in kept[1:]:
                condition = exp.and_(condition, other, copy=False)
            where.set("this", condition)


def _is_cursor(condition: exp.Expr) -> bool:
    if not isinstance(condition, (exp.GT, exp.GTE)) or not isinstance(condition.left, exp.Column):
        return False
    right = condition.right
    if isinstance(right, exp.Subquery):
        return True
    return isinstance(right, exp.Cast) and isinstance(right.this, exp.Literal)


def canonical_sql(node: dict[str, Any], relations: dict[str, str], dialect: str) -> str | None:
    """The node's compiled SQL, without comments, on canonical table names. None if it
    has none or sqlglot can't read it."""
    compiled = node.get("compiled_code")
    if not compiled:
        return None
    try:
        tree = sqlglot.parse_one(compiled, read=dialect)
    except SqlglotError:
        return None
    for table in tree.find_all(exp.Table):
        canonical = relations.get(".".join(part.name for part in table.parts).lower())
        if canonical:
            database, schema, name = canonical.split(".")
            table.set("catalog", exp.to_identifier(database))
            table.set("db", exp.to_identifier(schema))
            table.set("this", exp.to_identifier(name))
    if node.get("config", {}).get("materialized") == "incremental":
        _drop_cursor(tree)
    return tree.sql(dialect=dialect, comments=False)


def _shape(sql: str, dialect: str) -> str:
    """The query without the select lists of its CTEs and top-level selects: what decides
    which rows there are (from, join, where, group by), not what each column holds."""
    tree = sqlglot.parse_one(sql, read=dialect)
    for select in tree.find_all(exp.Select):
        if select.parent is None or isinstance(select.parent, (exp.CTE, exp.SetOperation)):
            select.set("expressions", [])
    return tree.sql(dialect=dialect, comments=False)


def _declared_types(node: dict[str, Any]) -> dict[str, str]:
    return {
        name.lower(): column["data_type"].lower()
        for name, column in node.get("columns", {}).items()
        if column.get("data_type")
    }


class _Side:
    """One manifest with its SQL traced on canonical names."""

    def __init__(self, manifest: Manifest, fallback: _Side | None = None) -> None:
        self.manifest = manifest
        self.nodes = _project_nodes(manifest)
        self.names = _canonical_names(manifest)
        self.dialect = manifest["metadata"]["adapter_type"]
        # `--defer` resolves refs in memory and leaves no trace in the manifest: a
        # selected node's SQL names prod's relations of its unselected parents
        self.relations = {**(fallback.relations if fallback else {}), **_relations(manifest, self.names)}
        self.sql = {
            uid: sql for uid, node in self.nodes.items() if (sql := canonical_sql(node, self.relations, self.dialect))
        }
        # What the PR's unselected nodes read: prod's SQL, unchanged
        merged = dict(self.sql)
        if fallback:
            for uid in self.nodes:
                if uid not in merged and uid in fallback.sql:
                    merged[uid] = fallback.sql[uid]
        by_name = {self.names[uid]: sql for uid, sql in merged.items()}
        traces = trace_models(by_name, self.dialect)
        self.uid_by_name = {name: uid for uid, name in self.names.items()}
        self.traces: dict[str, ModelTrace] = {
            self.uid_by_name[name]: trace for name, trace in traces.items() if name in self.uid_by_name
        }

    def columns(self, uid: str) -> dict[str, ColumnTrace]:
        trace = self.traces.get(uid)
        return {c.lower(): t for c, t in trace.columns.items()} if trace else {}

    def traced(self, uid: str) -> bool:
        trace = self.traces.get(uid)
        return bool(trace and trace.complete and trace.columns)


def _direct_changes(prod: _Side, pr: _Side) -> list[Change]:
    changes: list[Change] = []
    for uid, node in pr.nodes.items():
        if uid not in prod.nodes:
            changes.append(Change(uid, WHOLE_MODEL, "new model"))
            continue
        changes += _model_changes(uid, node, prod, pr)
    disabled = pr.manifest.get("disabled", {})
    for uid in prod.nodes.keys() - pr.nodes.keys():
        # Disabled on this target (the job costs mart: prod only) is not removed
        if uid not in disabled:
            changes.append(Change(uid, WHOLE_MODEL, "model removed"))
    return changes


def _model_changes(uid: str, node: dict[str, Any], prod: _Side, pr: _Side) -> list[Change]:
    before, after = prod.sql.get(uid), pr.sql.get(uid)
    prod_node = prod.nodes[uid]
    if after is None and node.get("checksum") != prod_node.get("checksum"):
        return [Change(uid, WHOLE_MODEL, "changed, no compiled SQL to compare")]
    types_before, types_after = _declared_types(prod_node), _declared_types(node)
    retyped = {
        column: f"retyped {types_before[column]} → {types_after[column]}"
        for column in types_before.keys() & types_after.keys()
        if types_before[column] != types_after[column]
    }
    changes = [Change(uid, column, kind) for column, kind in sorted(retyped.items())]
    if after is None or before is None or before == after:
        return changes
    if not (prod.traced(uid) and pr.traced(uid)):
        return [Change(uid, WHOLE_MODEL, "SQL changed, columns not traced")]
    if _shape(before, prod.dialect) != _shape(after, pr.dialect):
        return [Change(uid, WHOLE_MODEL, "filter, join or grouping changed")]
    old, new = prod.columns(uid), pr.columns(uid)
    for column in sorted(old.keys() | new.keys()):
        if column not in old:
            changes.append(Change(uid, column, "added"))
        elif column not in new:
            changes.append(Change(uid, column, "removed"))
        elif old[column].expression != new[column].expression and column not in retyped:
            changes.append(Change(uid, column, "expression changed"))
    return changes


class _Graph:
    """Who reads what, on the union of both sides: a removed column has its readers in
    prod only, a new one in the PR only."""

    def __init__(self, prod: _Side, pr: _Side) -> None:
        self.edges: dict[Column, set[Column]] = {}
        self.columns_of: dict[str, set[str]] = {}
        self.children: dict[str, set[str]] = {}
        self.reads: dict[str, set[str]] = {}
        self.untraced: set[str] = set()
        self.nodes = prod.nodes.keys() | pr.nodes.keys()
        for side in (prod, pr):
            for child, node in side.nodes.items():
                for parent in node.get("depends_on", {}).get("nodes", []):
                    self.children.setdefault(parent, set()).add(child)
                if side is pr or child not in pr.nodes:
                    if not side.traced(child):
                        self.untraced.add(child)
                for column, trace in side.columns(child).items():
                    for table, source_column in trace.inputs:
                        parent = side.uid_by_name.get(table.lower())
                        if parent:
                            source = (parent, source_column.lower())
                            self.edges.setdefault(source, set()).add((child, column))
                            self.columns_of.setdefault(parent, set()).add(source_column.lower())
                            self.reads.setdefault(child, set()).add(parent)

    def readers(self, uid: str, column: str) -> set[Column]:
        names = self.columns_of.get(uid, set()) if column == WHOLE_MODEL else {column}
        found = {reader for name in names for reader in self.edges.get((uid, name), set())}
        for child in self.children.get(uid, set()):
            # No column path to follow: the model level, as dbt's own lineage
            if child in self.nodes and (child in self.untraced or uid not in self.reads.get(child, set())):
                found.add((child, WHOLE_MODEL))
        return found

    def downstream(self, uid: str, column: str) -> set[Column]:
        seen: set[Column] = set()
        stack = [(uid, column)]
        while stack:
            for reader in self.readers(*stack.pop()):
                if reader not in seen:
                    seen.add(reader)
                    stack.append(reader)
        return seen


def _expression_columns(expression: str | None) -> set[str]:
    if not expression:
        return set()
    try:
        return {c.name.lower() for c in sqlglot.parse_one(expression).find_all(exp.Column)}
    except SqlglotError:
        return set()


class _SemanticLayer:
    """Columns behind the metrics, semantic dimensions and entities of both manifests."""

    def __init__(self, prod: Manifest, pr: Manifest) -> None:
        self.metrics: dict[Column, set[str]] = {}
        self.fields: dict[Column, set[str]] = {}
        for manifest in (prod, pr):
            self._load(manifest)

    def _load(self, manifest: Manifest) -> None:
        models: dict[str, str] = {}
        by_entity: dict[str, list[str]] = {}
        dimension_columns: dict[tuple[str, str], str] = {}
        direct: dict[str, set[Column]] = {}
        semantic_models = manifest.get("semantic_models", {}).values()
        for model in semantic_models:
            uid = model["depends_on"]["nodes"][0]
            models[model["name"]] = uid
            for entity in model.get("entities", []):
                by_entity.setdefault(entity["name"], []).append(model["name"])
                for column in _expression_columns(entity.get("expr") or entity["name"]):
                    self.fields.setdefault((uid, column), set()).add(f"entity `{entity['name']}`")
            for dimension in model.get("dimensions", []):
                for column in _expression_columns(dimension.get("expr") or dimension["name"]):
                    dimension_columns[(model["name"], dimension["name"])] = column
                    self.fields.setdefault((uid, column), set()).add(f"dimension `{dimension['name']}`")
        time_dimensions = {m["name"]: (m.get("defaults") or {}).get("agg_time_dimension") for m in semantic_models}
        for metric in manifest.get("metrics", {}).values():
            params = metric["type_params"]
            aggregation = params.get("metric_aggregation_params") or {}
            model_name = aggregation.get("semantic_model")
            columns = direct.setdefault(metric["name"], set())
            if model_name in models:
                uid = models[model_name]
                for column in _expression_columns(params.get("expr")):
                    columns.add((uid, column))
                time_dimension: str | None = aggregation.get("agg_time_dimension") or time_dimensions.get(model_name)
                if time_dimension:
                    column = dimension_columns.get((model_name, time_dimension))
                    columns.add((uid, column or time_dimension.lower()))
            for where in (metric.get("filter") or {}).get("where_filters", []):
                for entity, dimension in DIMENSION_REF.findall(where["where_sql_template"]):
                    for candidate in by_entity.get(entity, []):
                        if (candidate, dimension) in dimension_columns:
                            columns.add((models[candidate], dimension_columns[(candidate, dimension)]))
        # Ratio and derived metrics read other metrics
        readers: dict[str, set[str]] = {}
        for metric in manifest.get("metrics", {}).values():
            for parent in metric["depends_on"]["nodes"]:
                if parent.startswith("metric."):
                    readers.setdefault(parent.split(".")[-1], set()).add(metric["name"])
        for metric, columns in direct.items():
            affected = {metric}
            stack = [metric]
            while stack:
                for reader in readers.get(stack.pop(), set()) - affected:
                    affected.add(reader)
                    stack.append(reader)
            for column in columns:
                self.metrics.setdefault(column, set()).update(affected)

    def lookup(self, table: dict[Column, set[str]], uid: str, column: str) -> set[str]:
        if column == WHOLE_MODEL:
            return {name for (model, _), names in table.items() if model == uid for name in names}
        return table.get((uid, column), set())


def load_consumers(root: Path = ROOT) -> Consumers:
    product = root / "data-product"
    return Consumers(
        contracts=[yaml.safe_load(p.read_text(encoding="utf-8")) for p in sorted((product / "contracts").glob("*.odcs.yaml"))],
        agreements=[yaml.safe_load(p.read_text(encoding="utf-8")) for p in sorted((product / "consumers").glob("*.yaml"))],
    )


def _contract_columns(contract: dict[str, Any], table: str, column: str) -> set[str]:
    columns: set[str] = set()
    for element in contract.get("schema", []):
        if element.get("physicalName", element["name"]).lower() != table:
            continue
        names = {p["name"].lower() for p in element.get("properties", [])}
        columns |= names if column == WHOLE_MODEL else names & {column}
    return columns


def _agreement_columns(agreement: dict[str, Any], table: str, column: str) -> set[str]:
    names = {c.lower() for c in (agreement.get("uses") or {}).get(table, [])}
    return names if column == WHOLE_MODEL else names & {column}


def analyze(prod_manifest: Manifest, pr_manifest: Manifest, consumers: Consumers) -> list[Change]:
    """The PR's changed columns, each with what it reaches downstream."""
    prod = _Side(prod_manifest)
    pr = _Side(pr_manifest, fallback=prod)
    changes = _direct_changes(prod, pr)
    if not changes:
        return []
    graph = _Graph(prod, pr)
    semantic = _SemanticLayer(prod_manifest, pr_manifest)
    exposures = [*prod_manifest.get("exposures", {}).values(), *pr_manifest.get("exposures", {}).values()]
    all_nodes = {**prod.nodes, **pr.nodes}
    for change in changes:
        reached = {(change.node, change.column)} | graph.downstream(change.node, change.column)
        change.downstream = sorted(reached - {(change.node, change.column)})
        metrics: set[str] = set()
        fields: set[str] = set()
        contract_columns: set[str] = set()
        agreement_columns: set[str] = set()
        reached_exposures: set[str] = set()
        for uid, column in reached:
            metrics |= semantic.lookup(semantic.metrics, uid, column)
            fields |= {f"{f} of `{_display(all_nodes, uid)}`" for f in semantic.lookup(semantic.fields, uid, column)}
            table = all_nodes[uid]["alias"].lower() if uid in all_nodes else ""
            for contract in consumers.contracts:
                contract_columns |= {
                    f"{contract['id']}: {table}.{c}" for c in _contract_columns(contract, table, column)
                }
            for agreement in consumers.agreements:
                agreement_columns |= {
                    f"{agreement['id']}: {table}.{c}" for c in _agreement_columns(agreement, table, column)
                }
            reached_exposures |= {e["name"] for e in exposures if uid in e["depends_on"]["nodes"]}
        for agreement in consumers.agreements:
            names = set((agreement.get("metrics") or {}).get("names", []))
            agreement_columns |= {f"{agreement['id']}: metric {m}" for m in names & metrics}
        change.metrics = sorted(metrics)
        change.semantic_fields = sorted(fields)
        change.contract_columns = sorted(contract_columns)
        change.agreement_columns = sorted(agreement_columns)
        change.exposures = sorted(reached_exposures)
    return sorted(changes, key=lambda c: (_display(all_nodes, c.node), c.column))


def _display(nodes: dict[str, dict[str, Any]], uid: str) -> str:
    node = nodes.get(uid)
    if not node:
        return uid.split(".")[-1]
    return node["name"] + (f"_v{node['version']}" if node.get("version") else "")


def _names(values: list[str]) -> str:
    shown = [f"`{v}`" for v in values[:MAX_NAMES]]
    more = len(values) - MAX_NAMES
    return ", ".join(shown) + (f" +{more} more" if more > 0 else "")


def _by_owner(values: list[str]) -> str:
    """`owner: name` entries as `owner: a, b` per contract or agreement."""
    owners: dict[str, list[str]] = {}
    for value in values:
        owner, name = value.split(": ", 1)
        owners.setdefault(owner, []).append(name)
    return "; ".join(f"{owner}: {_names(names)}" for owner, names in owners.items())


def _count(number: int, noun: str) -> str:
    return f"{number} {noun}{'' if number == 1 else 's'}"


def _column_label(nodes: dict[str, dict[str, Any]], uid: str, column: str) -> str:
    return f"{_display(nodes, uid)}.{column}"


LIMITS = (
    "<sub>From the manifests of this PR and the last prod deploy, no warehouse query. Lineage follows values: a column "
    "used only in a child's filter or join is not followed, and `.*` marks the model-level fallback (every column, where "
    "the SQL changed its rows or lineage can't be traced). Types come from the YAML, not the warehouse. Changes merged to "
    "main after this branch was cut show up here too, reversed: rebase to drop them. Updated on every push.</sub>"
)


def render(changes: list[Change], nodes: dict[str, dict[str, Any]], run_url: str = "", error: str = "") -> str:
    run = f" ([CI run]({run_url}))" if run_url else ""
    lines = [MARKER]
    if error:
        return "\n".join([*lines, "### Column impact", "", f"The analysis failed{run}: see step `Column impact` of the run."]) + "\n"
    if not changes:
        return "\n".join([*lines, UPDATE_ONLY, "### Column impact", "", f"This PR changes no model column{run}."]) + "\n"
    models = {c.node for c in changes}
    metrics = {m for c in changes for m in c.metrics}
    contract = {x for c in changes for x in c.contract_columns}
    agreement = {x for c in changes for x in c.agreement_columns}
    exposures = {x for c in changes for x in c.exposures}
    lines += [
        "### Column impact",
        "",
        f"**{_count(len(changes), 'changed column')} in {_count(len(models), 'model')}**: reaches "
        f"{_count(len(metrics), 'metric')}, {_count(len(contract), 'contract column')}, "
        f"{_count(len(agreement), 'usage agreement entry').replace('entrys', 'entries')} and "
        f"{_count(len(exposures), 'exposure')}{run}.",
    ]
    fallback = sorted({c.kind + ": `" + _display(nodes, c.node) + "`" for c in changes if c.column == WHOLE_MODEL})
    if fallback:
        lines += ["", "Model level, not column level: " + "; ".join(fallback) + "."]
    lines += [
        "",
        "| Changed column | Change | Downstream columns | Metrics | Contract | Usage agreements | Exposures |",
        "|---|---|--:|---|---|---|---|",
    ]
    for c in changes:
        lines.append(
            f"| `{_column_label(nodes, c.node, c.column)}` | {c.kind} | {len(c.downstream)} | {_names(c.metrics)} | "
            f"{_by_owner(c.contract_columns)} | {_by_owner(c.agreement_columns)} | {_names(c.exposures)} |"
        )
    details = ["", "<details><summary>Downstream columns and semantic fields</summary>", ""]
    for c in changes:
        reached = [_column_label(nodes, uid, column) for uid, column in c.downstream]
        details.append(f"- `{_column_label(nodes, c.node, c.column)}` → " + (", ".join(f"`{r}`" for r in reached) or "no reader"))
        if c.semantic_fields:
            details.append(f"  - semantic layer: {', '.join(c.semantic_fields)}")
    details += ["", "</details>"]
    footer = ["", LIMITS]
    body = "\n".join([*lines, *details, *footer]) + "\n"
    if len(body) <= MAX_BODY:
        return body
    # Too long for a comment: the table alone, then cut by row
    note = ["", "Downstream columns left out: too long for a comment."]
    body = "\n".join([*lines, *note, *footer]) + "\n"
    while len(body) > MAX_BODY and len(lines) > 8:
        lines.pop()
        body = "\n".join([*lines, "", "Table cut: too many changed columns for a comment.", *footer]) + "\n"
    return body


def main(argv: list[str]) -> int:
    if len(argv) not in (4, 5):
        print(__doc__, file=sys.stderr)
        return 2
    prod_path, pr_path, out = argv[1:4]
    run_url = argv[4] if len(argv) == 5 else ""
    # sqlglot warns per WEEK(SUNDAY) it reads, dozens of times per model
    logging.getLogger("sqlglot").setLevel(logging.ERROR)
    status = 0
    try:
        prod = json.loads(Path(prod_path).read_text(encoding="utf-8"))
        pr = json.loads(Path(pr_path).read_text(encoding="utf-8"))
        run_results = json.loads((Path(pr_path).parent / "run_results.json").read_text(encoding="utf-8"))
        # A failure before the build leaves another command's manifest, with no model compiled
        if run_results.get("args", {}).get("which") not in ("build", "compile"):
            raise RuntimeError("the PR manifest is not from `dbt build`")
        body = render(analyze(prod, pr, load_consumers()), {**_project_nodes(prod), **_project_nodes(pr)}, run_url)
    except (RuntimeError, OSError, ValueError, KeyError, SqlglotError) as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        body = render([], {}, run_url, error=str(e))
        status = 1
    Path(out).write_text(body, encoding="utf-8")
    print(body)
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv))
