-- Grain: one row per order, with the person behind it. Olist issues a new
-- customer_id per order; customer_unique_id groups a person's orders, and the
-- customer row holds the delivery location of that order. The one place where
-- orders meet customers: fct_orders, fct_order_items and dim_customers read it.
with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        customers.customer_unique_id,
        customers.zip_code_prefix,
        customers.city,
        customers.state,
        orders.order_status,
        orders.purchased_at,
        orders.approved_at,
        orders.delivered_to_carrier_at,
        orders.delivered_to_customer_at,
        orders.estimated_delivery_date,
        orders._loaded_at
    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
)

select * from joined
