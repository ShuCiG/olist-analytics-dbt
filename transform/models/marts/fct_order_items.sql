-- Grain: one row per order line item.
with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

final as (
    select
        order_items.order_item_sk,
        order_items.order_id,
        order_items.order_item_number,
        order_items.product_id,
        order_items.seller_id,
        customers.customer_unique_id,
        orders.order_status,
        cast(orders.purchased_at as date) as purchase_date,
        orders.purchased_at,
        order_items.shipping_limit_at,
        order_items.price,
        order_items.freight_value,
        order_items.price + order_items.freight_value as item_amount
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    inner join customers
        on orders.customer_id = customers.customer_id
)

select * from final
