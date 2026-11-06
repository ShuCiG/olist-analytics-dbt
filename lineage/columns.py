"""Column-level lineage of dbt models from their compiled SQL (D87), for the
OpenLineage events the Airflow DAGs send to Marquez (airflow/dags/olist_lineage.py).

OpenLineage's own SQL parser (what the dbt integration uses) knows no table schemas,
so it stops at every `select *`: the import CTEs of a dbt model (`with orders as
(select * from stg_orders)`) and its last line (`select * from final`). It traced 2
of 25 models. sqlglot expands a star over a CTE from the CTE's own select list, and
over a model from that model's output columns once they are known: models are
traced parents first, each adding its columns to the schema the next ones see. Raw
tables have no schema here; a star over one keeps the column name (`select *` passes
columns through unchanged).

No Airflow or OpenLineage imports: tested in the project venv (tests/test_lineage.py).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node, lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, build_scope

# output column -> its input columns, as (table, column); table = database.schema.name,
# the dataset name OpenLineage's dbt integration gives each relation
ColumnLineage = dict[str, list[tuple[str, str]]]


@dataclass(frozen=True)
class ColumnTrace:
    inputs: list[tuple[str, str]]
    # Every hop of the column through the query: equal strings = same computation
    expression: str
    # A branch ends on a column reference, not a table: no inputs means "not placed"
    # rather than a constant or count(*)
    reads_columns: bool


@dataclass(frozen=True)
class ModelTrace:
    columns: dict[str, ColumnTrace]
    # False: a star over unknown columns, a column that reads something unplaced, or a
    # query whose filters and joins can't be read
    complete: bool
    # Input columns that decide which rows there are (where, join, group by, having,
    # qualify), in any CTE: no value of them has to reach an output column
    row_inputs: list[tuple[str, str]] = field(default_factory=list)


def _table_name(table: exp.Table) -> str:
    return ".".join(part.name for part in table.parts)


def _leaves(node: Node, column: str) -> Iterator[tuple[str, str]]:
    """Input columns of a lineage node: the tables at the ends of its branches."""
    name = node.name.split(".")[-1]
    if name == "*":
        name = column
    if isinstance(node.source, exp.Table) and not node.downstream:
        yield _table_name(node.source), name
    for child in node.downstream:
        yield from _leaves(child, name)


def _schema(tables: set[str], known: dict[str, list[str]]) -> dict:
    """sqlglot's nested schema (database -> schema -> table -> columns) of the given
    tables whose columns are known."""
    schema: dict = {}
    for table in tables & known.keys():
        database, schema_name, name = table.split(".")
        schema.setdefault(database, {}).setdefault(schema_name, {})[name] = {
            column: "unknown" for column in known[table]
        }
    return schema


def _guessed(sql: str, tables: set[str], known: dict[str, list[str]], dialect: str) -> dict:
    """`known`, plus every column name the SQL uses for each table of unknown columns.

    sqlglot leaves an unqualified column unplaced when its CTE reads `select *` from
    such a table and the query joins elsewhere (dim_sellers_history: `seller_id` from
    the snapshot). A superset of the real columns is enough to place it; it is only
    used for columns the first pass left without inputs (see column_lineage).
    """
    used = sorted({c.name for c in sqlglot.parse_one(sql, read=dialect).find_all(exp.Column)})
    unknown = {t for t in tables - known.keys() if t.count(".") == 2}
    return _schema(tables, {**known, **{t: used for t in unknown}})


def _column_trace(node: Node, column: str) -> ColumnTrace:
    hops = list(node.walk())
    return ColumnTrace(
        inputs=sorted(set(_leaves(node, column))),
        expression="\n".join(sorted({f"{hop.name} = {hop.expression.sql(comments=False)}" for hop in hops})),
        reads_columns=any(hop.expression.find(exp.Column) for hop in hops if not hop.downstream),
    )


def _trace(sql: str, schema: dict, dialect: str) -> dict[str, ColumnTrace]:
    nodes = lineage(None, sql, schema, dialect=dialect)
    assert isinstance(nodes, dict)  # all columns when `column` is None
    return {column: _column_trace(node, column) for column, node in nodes.items()}


def _source_columns(scope: Scope, table: str, name: str, seen: set[tuple[int, str]]) -> Iterator[tuple[str, str]]:
    """Input columns behind a column of a scope's source. A source that isn't there
    belongs to an outer query (a correlated subquery)."""
    current: Scope | None = scope
    while current is not None and table not in current.sources:
        current = current.parent
    source = current.sources[table] if current is not None else None
    if isinstance(source, exp.Table):
        yield _table_name(source), name
    elif isinstance(source, Scope):
        yield from _output_columns(source, name, seen)


def _output_columns(scope: Scope, name: str, seen: set[tuple[int, str]]) -> Iterator[tuple[str, str]]:
    """Input columns behind an output column of a CTE, derived table or union."""
    key = (id(scope), name.lower())
    if key in seen:
        return
    seen.add(key)
    if scope.set_operation_scopes:
        for branch in scope.set_operation_scopes:
            yield from _output_columns(branch, name, seen)
        return
    if not isinstance(scope.expression, exp.Select):
        return
    selects = scope.expression.selects
    for select in selects:
        if select.alias_or_name.lower() == name.lower():
            for column in select.find_all(exp.Column):
                yield from _column_inputs(column, scope, seen)
            return
    # A star over a table of unknown columns passes the column through unchanged
    if any(select.is_star for select in selects):
        for table in scope.selected_sources:
            yield from _source_columns(scope, table, name, seen)


def _column_inputs(column: exp.Column, scope: Scope, seen: set[tuple[int, str]]) -> Iterator[tuple[str, str]]:
    # Unqualified: sqlglot couldn't place it, so any source may hold it. A superset is
    # safe: only a column that exists and changed starts a path.
    for table in [column.table] if column.table else list(scope.selected_sources):
        yield from _source_columns(scope, table, column.name, seen)


def _row_columns(select: exp.Select) -> Iterator[exp.Column]:
    for clause in ("where", "group", "having", "qualify"):
        if node := select.args.get(clause):
            yield from node.find_all(exp.Column)
    for join in select.args.get("joins") or []:
        # Not the whole join: a derived table's own columns aren't the join's condition
        if on := join.args.get("on"):
            yield from on.find_all(exp.Column)


def _row_inputs(sql: str, schema: dict, dialect: str) -> list[tuple[str, str]]:
    tree = qualify(
        sqlglot.parse_one(sql, read=dialect), dialect=dialect, schema=schema, validate_qualify_columns=False, identify=False
    )
    root = build_scope(tree)
    if root is None:
        return []
    scopes = {id(scope.expression): scope for scope in root.traverse()}
    inputs: set[tuple[str, str]] = set()
    for select in tree.find_all(exp.Select):
        for column in _row_columns(select):
            # A column of a subquery in the condition is placed by that subquery
            scope = scopes.get(id(column.find_ancestor(exp.Select)))
            if scope is not None:
                inputs |= set(_column_inputs(column, scope, set()))
    return sorted(inputs)


def column_lineage(models: dict[str, str], dialect: str) -> dict[str, ColumnLineage]:
    """Lineage of every model that parses, see trace_models: model name -> output
    column -> input columns."""
    return {
        name: {column: traced.inputs for column, traced in model.columns.items()}
        for name, model in trace_models(models, dialect).items()
    }


def trace_models(models: dict[str, str], dialect: str) -> dict[str, ModelTrace]:
    """Trace every model that parses: model name (database.schema.name) -> compiled
    SQL in, model name -> its output columns out.

    A model sqlglot can't read (a Python model's code) is left out. With a schema it
    can't fit (a column it can't place) the model is traced again without one.
    """
    tables: dict[str, set[str]] = {}
    for name, sql in models.items():
        try:
            tables[name] = {
                _table_name(t) for t in sqlglot.parse_one(sql, read=dialect).find_all(exp.Table)
            }
        except SqlglotError:
            continue
    columns: dict[str, list[str]] = {}
    result: dict[str, ModelTrace] = {}
    done: set[str] = set()

    def visit(name: str) -> None:
        if name in done:
            return
        done.add(name)
        for parent in tables[name]:
            if parent in tables:
                visit(parent)
        schema = _schema(tables[name], columns)
        try:
            traced = _trace(models[name], schema, dialect)
        except SqlglotError:
            if not schema:
                return
            try:
                traced = _trace(models[name], {}, dialect)
            except SqlglotError:
                return
        if any(not trace.inputs for trace in traced.values()):
            try:
                guessed = _trace(models[name], _guessed(models[name], tables[name], columns, dialect), dialect)
            except SqlglotError:
                guessed = {}
            for column, trace in traced.items():
                if not trace.inputs and column in guessed:
                    traced[column] = guessed[column]
        # A star over a table of unknown columns (the snapshot's `select *` from raw)
        # stays a `*` column: no output column to name, and the model's columns stay
        # unknown to its children
        star = "*" in traced
        if star:
            del traced["*"]
        else:
            columns[name] = list(traced)
        placed = all(trace.inputs or not trace.reads_columns for trace in traced.values())
        row_inputs: list[tuple[str, str]] | None = None
        for attempt in (schema, {}):
            try:
                row_inputs = _row_inputs(models[name], attempt, dialect)
                break
            except SqlglotError:
                continue
        result[name] = ModelTrace(traced, not star and placed and row_inputs is not None, row_inputs or [])

    for name in tables:
        visit(name)
    return result
