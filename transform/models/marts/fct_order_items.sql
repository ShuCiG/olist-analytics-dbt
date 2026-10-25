-- Grain: one row per order line item.
-- Incremental: each run merges only items loaded since the last run, by `_loaded_at`.
-- An order and its items always load together (one chunk, one `_loaded_at`), so a
-- reloaded order (e.g. a new order_status) brings its items with it. A source that
-- reloaded an order alone would leave its items on the old status until a full
-- refresh: assert_order_items_loaded_with_their_order fails first (D70).
-- Customers are replaced on every load, so they are not part of the cursor;
-- `--full-refresh` rebuilds everything. Trade-off: a
-- changed customer (customer_unique_id, zip) reaches existing rows here only on a
-- full refresh, while fct_orders (a table) has it on the next build. The dump is
-- static, so customers never change after their orders load.
{{
    config(
        materialized='incremental',
        unique_key='order_item_sk',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        partition_by=(
            {'field': 'purchase_date', 'data_type': 'date', 'granularity': 'month'}
            if target.type == 'bigquery' else none
        ),
        cluster_by=(['seller_id', 'product_id'] if target.type == 'bigquery' else none),
        liquid_clustered_by=(['seller_id', 'product_id'] if target.type == 'databricks' else none),
    )
}}

with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

orders as (
    select * from {{ ref('int_orders_joined_to_customers') }}
),

final as (
    select
        order_items.order_item_sk,
        order_items.order_id,
        order_items.order_item_number,
        order_items.product_id,
        order_items.seller_id,
        orders.customer_unique_id,
        orders.order_status,
        cast(orders.purchased_at as date) as purchase_date,
        orders.purchased_at,
        order_items.shipping_limit_at,
        {{ to_money('order_items.price') }} as price,
        {{ to_money('order_items.freight_value') }} as freight_value,
        {{ to_money('order_items.price + order_items.freight_value') }} as item_amount,
        greatest(order_items._loaded_at, orders._loaded_at) as _loaded_at
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    {% if is_incremental() %}
        {% set cursor = incremental_cursor() %}
        -- A filter per table: on BigQuery each reads only its partitions since the
        -- cursor (D70); an `or` across the two would read both in full.
        -- >= not >: rows of the last run are re-merged (idempotent) rather than missed
        where
            order_items._loaded_at >= {{ cursor }}
            and orders._loaded_at >= {{ cursor }}
    {% endif %}
)

select * from final
