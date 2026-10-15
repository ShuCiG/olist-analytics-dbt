-- Grain: one row per order line item.
-- Incremental: each run merges only rows whose raw source was (re)loaded since the
-- last run, by `_loaded_at`. An item is reprocessed when the item or its order row
-- changes (e.g. a new order_status). Customers are replaced on every load, so they
-- are not part of the cursor; `--full-refresh` rebuilds everything.
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
        -- >= not >: rows of the last run are re-merged (idempotent) rather than missed
        where
            order_items._loaded_at >= (select max(this_run._loaded_at) from {{ this }} as this_run)
            or orders._loaded_at >= (select max(this_run._loaded_at) from {{ this }} as this_run)
    {% endif %}
)

select * from final
