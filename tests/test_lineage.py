"""lineage/columns.py: column lineage of dbt models from compiled SQL, in the dbt style
(import CTEs and a final `select *`), on DuckDB and BigQuery naming."""

from lineage.columns import column_lineage
from lineage.exposures import exposure_inputs, exposures

STG_ORDERS = """
with source as (
    select * from "db"."raw"."orders"
),

renamed as (
    select
        order_id,
        customer_id,
        cast(order_purchase_timestamp as timestamp) as purchased_at
    from source
)

select * from renamed
"""

STG_ITEMS = """
with source as (
    select * from "db"."raw"."order_items"
)

select order_id, price, freight_value from source
"""

INT_ITEMS = """
with items as (
    select * from "db"."staging"."stg_items"
)

select
    order_id,
    count(*) as items_count,
    sum(price) + sum(freight_value) as order_amount
from items
group by order_id
"""

# Unqualified columns over a join: placed only with the parents' columns known
FCT_ORDERS = """
with orders as (
    select * from "db"."staging"."stg_orders"
),

items as (
    select * from "db"."intermediate"."int_items"
),

final as (
    select
        order_id,
        customer_id,
        cast(purchased_at as date) as purchase_date,
        coalesce(items_count, 0) as items_count,
        order_amount
    from orders
    left join items using (order_id)
)

select * from final
"""

MODELS = {
    "db.staging.stg_orders": STG_ORDERS,
    "db.staging.stg_items": STG_ITEMS,
    "db.intermediate.int_items": INT_ITEMS,
    "db.marts.fct_orders": FCT_ORDERS,
}


def test_traces_through_stars_and_ctes() -> None:
    traced = column_lineage(MODELS, "duckdb")

    assert traced["db.staging.stg_orders"] == {
        "order_id": [("db.raw.orders", "order_id")],
        "customer_id": [("db.raw.orders", "customer_id")],
        "purchased_at": [("db.raw.orders", "order_purchase_timestamp")],
    }
    assert traced["db.intermediate.int_items"]["order_amount"] == [
        ("db.staging.stg_items", "freight_value"),
        ("db.staging.stg_items", "price"),
    ]
    # count(*) reads no column
    assert traced["db.intermediate.int_items"]["items_count"] == []


def test_parents_columns_place_unqualified_columns() -> None:
    fct = column_lineage(MODELS, "duckdb")["db.marts.fct_orders"]

    assert fct == {
        # A `using` column is both sides' column
        "order_id": [("db.intermediate.int_items", "order_id"), ("db.staging.stg_orders", "order_id")],
        "customer_id": [("db.staging.stg_orders", "customer_id")],
        "purchase_date": [("db.staging.stg_orders", "purchased_at")],
        "items_count": [("db.intermediate.int_items", "items_count")],
        "order_amount": [("db.intermediate.int_items", "order_amount")],
    }


def test_star_over_unknown_table_in_a_join() -> None:
    # Snapshot over raw: its columns are unknown, and the model joins elsewhere
    sql = """
    with versions as (
        select * from "db"."snapshots"."snap_sellers"
    ),

    zip_codes as (
        select * from "db"."intermediate"."int_zip"
    ),

    numbered as (
        select seller_id, seller_zip_code_prefix as zip_code_prefix from versions
    )

    select numbered.seller_id, zip_codes.latitude
    from numbered
    left join zip_codes on numbered.zip_code_prefix = zip_codes.zip_code_prefix
    """
    models = {
        "db.snapshots.snap_sellers": 'select * from "db"."raw"."sellers"',
        "db.intermediate.int_zip": 'select zip_code_prefix, latitude from "db"."raw"."geo"',
        "db.marts.dim_sellers": sql,
    }

    traced = column_lineage(models, "duckdb")

    # A `select *` over a table of unknown columns names no output column
    assert traced["db.snapshots.snap_sellers"] == {}
    assert traced["db.marts.dim_sellers"] == {
        "seller_id": [("db.snapshots.snap_sellers", "seller_id")],
        "latitude": [("db.intermediate.int_zip", "latitude")],
    }


def test_bigquery_names() -> None:
    sql = """
    with source as (
        select * from `my-project`.`airflow_raw`.`orders`
    )

    select order_id, order_status as status from source
    """

    traced = column_lineage({"my-project.airflow_staging.stg_orders": sql}, "bigquery")

    assert traced == {
        "my-project.airflow_staging.stg_orders": {
            "order_id": [("my-project.airflow_raw.orders", "order_id")],
            "status": [("my-project.airflow_raw.orders", "order_status")],
        }
    }


def test_unparsable_model_is_left_out() -> None:
    python_model = "import pandas as pd\n\ndef model(dbt, session):\n    return dbt.ref('x')\n"

    traced = column_lineage({"db.marts.rfm": python_model, **MODELS}, "duckdb")

    assert "db.marts.rfm" not in traced
    assert "db.marts.fct_orders" in traced


def _model_event(unique_id: str, output: str) -> dict:
    return {
        "job": {"name": f"dag.{unique_id}.build.run", "facets": {"dbt_node": {"unique_id": unique_id}}},
        "outputs": [{"namespace": "bigquery", "name": output}],
    }


def test_exposure_reads_the_outputs_of_its_models() -> None:
    manifest = {
        "exposures": {
            "exposure.olist.ml_feed": {"name": "ml_feed", "type": "ml", "depends_on": {"nodes": []}},
            "exposure.olist.olist_dashboard": {
                "name": "olist_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["model.olist.fct_orders.v2", "model.olist.dim_customers"]},
            },
        }
    }
    events = [
        _model_event("model.olist.stg_orders", "p.airflow_staging.stg_orders"),
        # A versioned model: the event names the relation (alias), not the node
        _model_event("model.olist.fct_orders.v2", "p.airflow_marts.fct_orders"),
        _model_event("model.olist.fct_orders.v1", "p.airflow_marts.fct_orders_v1"),
        _model_event("model.olist.dim_customers", "p.airflow_marts.dim_customers"),
        {"job": {"name": "dag.test", "facets": {}}, "outputs": []},
    ]

    [dashboard] = exposures(manifest)

    assert exposure_inputs(dashboard, events) == [
        {"namespace": "bigquery", "name": "p.airflow_marts.dim_customers"},
        {"namespace": "bigquery", "name": "p.airflow_marts.fct_orders"},
    ]


def test_exposure_without_model_events_reads_nothing() -> None:
    dashboard = {"name": "d", "type": "dashboard", "depends_on": {"nodes": ["model.olist.dim_customers"]}}

    assert exposure_inputs(dashboard, [_model_event("model.olist.stg_orders", "x")]) == []
