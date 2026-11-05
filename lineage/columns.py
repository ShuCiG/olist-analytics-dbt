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
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node, lineage

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
    # False: a star over unknown columns, or a column that reads something unplaced
    complete: bool


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
        result[name] = ModelTrace(traced, complete=not star and placed)

    for name in tables:
        visit(name)
    return result
