"""lineage/impact.py (D101): which columns a PR changes and who reads them, on fake
manifests shaped as dbt writes them on BigQuery: prod with every model compiled, the PR
with compiled SQL only for state:modified+ and refs of the rest pointing at prod."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from lineage.impact import MARKER, MAX_BODY, UPDATE_ONLY, WHOLE_MODEL, Change, Consumers, analyze, main, render

PROD_STG = "`p`.`staging`.`stg_orders`"
PROD_FCT = "`p`.`marts`.`fct_orders`"
CI_FCT = "`p`.`ci_pr_1_marts`.`fct_orders`"

STG_SQL = """
-- Staging
with source as (select * from `p`.`raw`.`orders`)
select order_id, status, amount, amount * 2 as doubled from source
"""
FCT_SQL = f"""
with orders as (select * from {PROD_STG})
select order_id, status, amount as order_amount, doubled from orders
"""
REPORT_SQL = f"""
with orders as (select * from {PROD_FCT})
select status, sum(order_amount) as revenue from orders group by status
"""
ITEMS_SQL = f"""
with orders as (select * from {PROD_STG}),
final as (
    select order_id, amount from orders
    where amount > 0 and order_id is not null and _loaded_at >= timestamp '2026-10-11 20:56:01+00:00'
)
select * from final
"""
ITEMS_CI_SQL = f"""
with orders as (select * from {PROD_STG}),
final as (select order_id, amount from orders where amount > 0 and order_id is not null)
select * from final
"""


def _node(name: str, sql: str | None, parents: list[str], schema: str = "marts", **extra: Any) -> dict[str, Any]:
    return {
        "resource_type": "model", "package_name": "olist", "name": name, "alias": name, "schema": schema,
        "relation_name": f"`p`.`{schema}`.`{name}`", "compiled_code": sql, "checksum": {"checksum": name},
        "depends_on": {"nodes": parents}, "columns": {}, "config": {"materialized": "table"}, **extra,
    }


STG = "model.olist.stg_orders"
FCT = "model.olist.fct_orders"
REPORT = "model.olist.fct_report"
ITEMS = "model.olist.fct_items"


def prod_manifest() -> dict[str, Any]:
    return {
        "metadata": {"project_name": "olist", "adapter_type": "bigquery"},
        "nodes": {
            STG: _node("stg_orders", STG_SQL, ["source.olist.raw.orders"], "staging"),
            FCT: _node("fct_orders", FCT_SQL, [STG], columns={
                "order_amount": {"data_type": "numeric"}, "status": {"data_type": "string"},
            }),
            REPORT: _node("fct_report", REPORT_SQL, [FCT]),
            ITEMS: _node("fct_items", ITEMS_SQL, [STG], config={"materialized": "incremental"}),
        },
        "sources": {
            "source.olist.raw.orders": {
                "source_name": "raw", "identifier": "orders", "relation_name": "`p`.`raw`.`orders`",
            },
        },
        "semantic_models": {
            "semantic_model.olist.orders": {
                "name": "orders", "depends_on": {"nodes": [FCT]}, "defaults": {"agg_time_dimension": None},
                "entities": [{"name": "order", "expr": "order_id"}],
                "dimensions": [{"name": "status", "expr": None}],
            },
        },
        "metrics": {
            "metric.olist.revenue": {
                "name": "revenue", "depends_on": {"nodes": ["semantic_model.olist.orders"]},
                "type_params": {
                    "expr": "order_amount",
                    "metric_aggregation_params": {"semantic_model": "orders", "agg": "sum"},
                },
                "filter": {"where_filters": [{"where_sql_template": "{{ Dimension('order__status') }} != 'canceled'"}]},
            },
            "metric.olist.orders": {
                "name": "orders", "depends_on": {"nodes": ["semantic_model.olist.orders"]},
                "type_params": {
                    "expr": "order_id",
                    "metric_aggregation_params": {"semantic_model": "orders", "agg": "count"},
                },
                "filter": None,
            },
            "metric.olist.aov": {
                "name": "aov", "depends_on": {"nodes": ["metric.olist.revenue", "metric.olist.orders"]},
                "type_params": {},
                "filter": None,
            },
        },
        "exposures": {
            "exposure.olist.dash": {"name": "dash", "depends_on": {"nodes": [REPORT]}},
            "exposure.olist.other": {"name": "other", "depends_on": {"nodes": [ITEMS]}},
        },
    }


def pr_manifest(**sql: str) -> dict[str, Any]:
    """The PR side: `sql` = model name -> its compiled SQL (selected nodes); the rest,
    unselected, has none and carries the PR's own relation, not prod's."""
    manifest = deepcopy(prod_manifest())
    for node in manifest["nodes"].values():
        node["relation_name"] = node["relation_name"].replace("`marts`", "`ci_pr_1_marts`").replace("`staging`", "`ci_pr_1_staging`")
        node["compiled_code"] = sql.get(node["name"])
    return manifest


CONSUMERS = Consumers(
    contracts=[{"id": "marts", "schema": [{"name": "fct_orders", "physicalName": "fct_orders", "properties": [
        {"name": "order_id"}, {"name": "order_amount"}, {"name": "status"},
    ]}]}],
    agreements=[{
        "id": "dashboard",
        "uses": {"fct_orders": ["order_amount"], "fct_report": ["revenue"]},
        "metrics": {"names": ["aov"]},
    }],
)


def _by_column(changes: list[Change]) -> dict[tuple[str, str], Change]:
    return {(c.node, c.column): c for c in changes}


def test_nothing_selected_means_no_change() -> None:
    assert analyze(prod_manifest(), pr_manifest(), CONSUMERS) == []


def test_recompiled_on_ci_schemas_is_not_a_change() -> None:
    # A parent's schema differs (selected, ci_*) and comments differ: same SQL
    report = REPORT_SQL.replace(PROD_FCT, CI_FCT) + "\n-- a comment"
    assert analyze(prod_manifest(), pr_manifest(fct_report=report), CONSUMERS) == []


def test_cursor_of_an_incremental_model_is_not_a_change() -> None:
    # Prod compiled the model on its last deploy's timestamp, CI builds it in full
    assert analyze(prod_manifest(), pr_manifest(fct_items=ITEMS_CI_SQL), CONSUMERS) == []


def test_mart_column_names_metrics_contract_agreement_exposure() -> None:
    fct = FCT_SQL.replace("amount as order_amount", "amount + 1 as order_amount")
    report = REPORT_SQL.replace(PROD_FCT, CI_FCT)

    changes = analyze(prod_manifest(), pr_manifest(fct_orders=fct, fct_report=report), CONSUMERS)

    assert [(c.node, c.column, c.kind) for c in changes] == [(FCT, "order_amount", "expression changed")]
    change = changes[0]
    assert change.downstream == [(REPORT, "revenue")]
    # A ratio metric reads the metric that reads the column; `orders` does not
    assert change.metrics == ["aov", "revenue"]
    assert change.contract_columns == ["marts: fct_orders.order_amount"]
    assert change.agreement_columns == ["dashboard: fct_orders.order_amount", "dashboard: fct_report.revenue", "dashboard: metric aov"]
    # Exposures of the changed model and of the models downstream, not the unrelated one
    assert change.exposures == ["dash"]


def test_staging_change_follows_lineage_to_the_marts() -> None:
    stg = STG_SQL.replace("amount * 2 as doubled", "amount * 3 as doubled")

    changes = analyze(prod_manifest(), pr_manifest(stg_orders=stg), CONSUMERS)

    assert [(c.node, c.column) for c in changes] == [(STG, "doubled")]
    assert changes[0].downstream == [(FCT, "doubled")]
    assert changes[0].metrics == []


def test_filter_on_a_metric_dimension_reaches_the_metric() -> None:
    fct = FCT_SQL.replace("select order_id, status,", "select order_id, upper(status) as status,")
    changes = analyze(prod_manifest(), pr_manifest(fct_orders=fct), CONSUMERS)
    assert _by_column(changes)[(FCT, "status")].metrics == ["aov", "revenue"]
    assert "dimension `status` of `fct_orders`" in _by_column(changes)[(FCT, "status")].semantic_fields


def test_added_and_removed_columns() -> None:
    # `doubled` removed: its reader is known from prod only. `amount_cents` new.
    fct = FCT_SQL.replace("doubled from orders", "amount * 100 as amount_cents from orders")

    by_column = _by_column(analyze(prod_manifest(), pr_manifest(fct_orders=fct), CONSUMERS))

    assert by_column[(FCT, "amount_cents")].kind == "added"
    assert by_column[(FCT, "doubled")].kind == "removed"


def test_removed_column_reaches_readers_known_from_prod_only() -> None:
    stg = STG_SQL.replace(", amount * 2 as doubled", "")
    fct = FCT_SQL.replace(", doubled from", " from")

    by_column = _by_column(analyze(prod_manifest(), pr_manifest(stg_orders=stg, fct_orders=fct), CONSUMERS))

    assert by_column[(STG, "doubled")].kind == "removed"
    assert by_column[(STG, "doubled")].downstream == [(FCT, "doubled")]


def test_retyped_in_the_yaml() -> None:
    pr = pr_manifest()
    pr["nodes"][FCT]["columns"]["status"]["data_type"] = "int64"
    changes = analyze(prod_manifest(), pr, CONSUMERS)
    assert [(c.column, c.kind) for c in changes] == [("status", "retyped string → int64")]
    assert changes[0].contract_columns == ["marts: fct_orders.status"]


def test_changed_filter_changes_every_column() -> None:
    fct = FCT_SQL.replace("from orders", "from orders where status != 'x'")

    changes = analyze(prod_manifest(), pr_manifest(fct_orders=fct), CONSUMERS)

    assert [(c.node, c.column, c.kind) for c in changes] == [(FCT, WHOLE_MODEL, "filter, join or grouping changed")]
    assert (REPORT, "revenue") in changes[0].downstream
    assert changes[0].metrics == ["aov", "orders", "revenue"]
    assert set(changes[0].contract_columns) == {"marts: fct_orders.order_id", "marts: fct_orders.order_amount", "marts: fct_orders.status"}


def test_untraceable_sql_falls_back_to_the_model_level() -> None:
    fct = "this is not sql ((("
    prod = prod_manifest()
    changes = analyze(prod, pr_manifest(fct_orders=fct), CONSUMERS)
    # Parse failure on the PR side with a changed checksum: nothing to compare
    assert changes == []  # same checksum: dbt didn't see it as changed

    pr = pr_manifest(fct_orders=fct)
    pr["nodes"][FCT]["checksum"] = {"checksum": "other"}
    changes = analyze(prod, pr, CONSUMERS)
    assert [(c.node, c.column) for c in changes] == [(FCT, WHOLE_MODEL)]
    assert (REPORT, "revenue") in changes[0].downstream


def test_child_with_no_column_path_is_reached_at_the_model_level() -> None:
    # A child that reads the parent through something lineage can't see (BQML's
    # ML.PREDICT(MODEL ...)): no column path, still downstream by depends_on
    prod = prod_manifest()
    prod["nodes"]["model.olist.scores"] = _node("scores", "select 1 as score", [FCT])
    pr = pr_manifest(fct_orders=FCT_SQL.replace("amount as order_amount", "amount + 1 as order_amount"))
    pr["nodes"]["model.olist.scores"] = _node("scores", "select 1 as score", [FCT])

    changes = analyze(prod, pr, CONSUMERS)

    assert ("model.olist.scores", WHOLE_MODEL) in changes[0].downstream


def _impact_of_status_with(name: str, sql: str) -> set[tuple[str, str]]:
    """What a changed stg_orders.status reaches, with one more child of stg_orders."""
    prod = prod_manifest()
    prod["nodes"][f"model.olist.{name}"] = _node(name, sql, [STG])
    stg = STG_SQL.replace("select order_id, status,", "select order_id, upper(status) as status,")
    pr = pr_manifest(stg_orders=stg)
    pr["nodes"][f"model.olist.{name}"] = _node(name, sql, [STG], "ci_pr_1_marts", compiled_code=None)
    changes = _by_column(analyze(prod, pr, CONSUMERS))
    return set(changes[(STG, "status")].downstream)


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"with o as (select * from {PROD_STG}) select order_id from o where status = 'x'", id="where"),
        pytest.param(f"select order_id, count(*) as n from {PROD_STG} group by order_id having max(status) = 'x'", id="having"),
        pytest.param(f"with o as (select * from {PROD_STG}) select max(order_id) as m from o group by status", id="group by"),
        pytest.param(f"select status as s, max(order_id) as m from {PROD_STG} group by 1", id="positional group by"),
        pytest.param(f"select a.order_id from {PROD_STG} a join `p`.`raw`.`x` b on a.status = b.status", id="join"),
        pytest.param(f"select a.order_id from {PROD_STG} a join `p`.`raw`.`x` b using (status)", id="join using"),
        pytest.param(f"select order_id from {PROD_STG} where order_id in (select order_id from {PROD_STG} where status = 'x')", id="subquery"),
        pytest.param(f"select a.order_id from {PROD_STG} a where exists (select 1 from `p`.`raw`.`x` b where b.id = a.status)", id="correlated"),
        pytest.param(
            f"with o as (select order_id, status as st from {PROD_STG}), f as (select * from o) select order_id from f where st = 'x'",
            id="renamed in a CTE",
        ),
        pytest.param(
            f"with o as (select order_id, upper(status) as st from {PROD_STG}) select order_id from o where st = 'x'",
            id="computed in a CTE",
        ),
        pytest.param(
            f"with o as (select order_id, row_number() over (partition by status order by order_id) as rn from {PROD_STG}) "
            "select order_id from o where rn = 1",
            id="row_number",
        ),
        pytest.param(f"select order_id from {PROD_STG} qualify row_number() over (partition by status order by order_id) = 1", id="qualify"),
        pytest.param(
            f"with o as (select order_id, status from {PROD_STG} union all select order_id, status from {PROD_STG}) "
            "select order_id from o where status = 'x'",
            id="union",
        ),
        pytest.param(f"select d.order_id from (select order_id from {PROD_STG} where status = 'x') d", id="derived table"),
    ],
)
def test_column_used_only_for_rows_reaches_the_whole_child(sql: str) -> None:
    assert ("model.olist.child", WHOLE_MODEL) in _impact_of_status_with("child", sql)


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"select order_id from {PROD_STG} where amount > 0", id="filter on another column"),
        pytest.param(
            f"select a.order_id from {PROD_STG} a join (select id, status from `p`.`raw`.`x`) b on a.order_id = b.id and b.status = 'x'",
            id="another table's column of that name",
        ),
        pytest.param(
            f"with o as (select order_id, 'x' as status from {PROD_STG}) select order_id from o where status = 'x'",
            id="a constant of that name",
        ),
    ],
)
def test_other_columns_do_not_reach_the_child(sql: str) -> None:
    assert "model.olist.child" not in {uid for uid, _ in _impact_of_status_with("child", sql)}


def test_consumers_of_a_filtering_child_are_named() -> None:
    # fct_orders keeps no value of `status`: only its filter reads it
    fct = f"""
with orders as (select * from {PROD_STG})
select order_id, amount as order_amount, doubled from orders where status = 'x'
"""
    prod = prod_manifest()
    prod["nodes"][FCT]["compiled_code"] = fct
    stg = STG_SQL.replace("select order_id, status,", "select order_id, upper(status) as status,")

    change = _by_column(analyze(prod, pr_manifest(stg_orders=stg), CONSUMERS))[(STG, "status")]

    assert {(FCT, WHOLE_MODEL), (REPORT, "revenue")} <= set(change.downstream)
    assert change.metrics == ["aov", "orders", "revenue"]
    assert "marts: fct_orders.order_amount" in change.contract_columns
    assert change.exposures == ["dash"]


def test_new_removed_and_disabled_models() -> None:
    pr = pr_manifest()
    pr["nodes"]["model.olist.fct_new"] = _node("fct_new", f"select order_id from {PROD_STG}", [STG])
    del pr["nodes"][ITEMS]
    assert sorted((c.node, c.kind) for c in analyze(prod_manifest(), pr, CONSUMERS)) == [
        (ITEMS, "model removed"), ("model.olist.fct_new", "new model"),
    ]

    # Disabled on the CI target (prod-only models) is not removed
    pr["disabled"] = {ITEMS: [{}]}
    assert [c.kind for c in analyze(prod_manifest(), pr, CONSUMERS)] == ["new model"]


def test_changed_seed_is_model_level() -> None:
    prod = prod_manifest()
    seed = {"resource_type": "seed", "package_name": "olist", "name": "countries", "alias": "countries",
            "relation_name": "`p`.`seeds`.`countries`", "checksum": {"checksum": "a"}, "depends_on": {"nodes": []}}
    prod["nodes"]["seed.olist.countries"] = seed
    pr = pr_manifest()
    pr["nodes"]["seed.olist.countries"] = {**seed, "checksum": {"checksum": "b"}}
    assert [(c.column, c.kind) for c in analyze(prod, pr, CONSUMERS)] == [(WHOLE_MODEL, "changed, no compiled SQL to compare")]


NODES = prod_manifest()["nodes"]


def test_render_names_the_affected() -> None:
    fct = FCT_SQL.replace("amount as order_amount", "amount + 1 as order_amount")
    body = render(analyze(prod_manifest(), pr_manifest(fct_orders=fct), CONSUMERS), NODES, "https://ci/run")

    assert body.startswith(MARKER + "\n###")
    assert UPDATE_ONLY not in body
    assert "**1 changed column in 1 model**: reaches 2 metrics, 1 contract column, 3 usage agreement entries and 1 exposure ([CI run](https://ci/run))." in body
    assert "| `fct_orders.order_amount` | expression changed | 1 | `aov`, `revenue` | marts: `fct_orders.order_amount` |" in body
    assert "`fct_report.revenue`" in body
    assert "dimension" not in body.split("<details>")[0]


def test_render_says_when_it_fell_back_to_the_model_level() -> None:
    body = render([Change(FCT, WHOLE_MODEL, "SQL changed, columns not traced")], NODES)
    assert "Model level, not column level: SQL changed, columns not traced: `fct_orders`." in body
    assert "`fct_orders.*`" in body


def test_render_no_change_only_updates_an_earlier_comment() -> None:
    assert render([], NODES).splitlines()[:2] == [MARKER, UPDATE_ONLY]


def test_render_error_posts_a_comment() -> None:
    body = render([], {}, "https://ci/run", error="boom")
    assert body.splitlines()[0] == MARKER and UPDATE_ONLY not in body
    assert "failed" in body


def test_render_fits_a_comment() -> None:
    changes = [
        Change(FCT, f"column_with_a_rather_long_name_{i}", "added", downstream=[(REPORT, f"reader_{j}") for j in range(300)])
        for i in range(1000)
    ]
    body = render(changes, NODES)
    assert len(body) <= MAX_BODY
    assert "<details>" not in body
    assert "Table cut" in body


def _write(path: Path, data: dict[str, Any]) -> str:
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def test_main_writes_the_comment(tmp_path: Path) -> None:
    prod = _write(tmp_path / "prod.json", prod_manifest())
    fct = FCT_SQL.replace("amount as order_amount", "amount + 1 as order_amount")
    pr = _write(tmp_path / "manifest.json", pr_manifest(fct_orders=fct))
    _write(tmp_path / "run_results.json", {"args": {"which": "build"}})
    out = tmp_path / "out.md"

    assert main(["impact", prod, pr, str(out), "https://ci/run"]) == 0

    # The repository's own contract and agreement: fct_orders.order_amount is in both
    body = out.read_text(encoding="utf-8")
    assert "olist-marts: `fct_orders.order_amount`" in body
    assert "olist-dashboard: `fct_orders.order_amount`" in body


@pytest.mark.parametrize("which", ["run", None])
def test_main_refuses_another_commands_manifest(tmp_path: Path, which: str | None) -> None:
    # A failure before `dbt build` leaves a manifest with no model compiled, which
    # would read as "nothing changed"
    prod = _write(tmp_path / "prod.json", prod_manifest())
    pr = _write(tmp_path / "manifest.json", pr_manifest())
    _write(tmp_path / "run_results.json", {"args": {"which": which}})
    out = tmp_path / "out.md"

    assert main(["impact", prod, pr, str(out)]) == 1
    assert "failed" in out.read_text(encoding="utf-8")
