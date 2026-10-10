with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

metrics as (
    select
        order_id,
        {{ dbt.datediff('purchased_at', 'approved_at', 'hour') }}
            as hours_to_approve,
        {{ dbt.datediff('purchased_at', 'delivered_to_customer_at', 'day') }}
            as days_to_deliver,
        {{ dbt.datediff('cast(delivered_to_customer_at as date)', 'estimated_delivery_date', 'day') }}
            as days_delivered_before_estimate,
        delivered_to_customer_at is not null as is_delivered,
        coalesce(cast(delivered_to_customer_at as date) > estimated_delivery_date, false) as is_delivered_late
    from orders
)

select * from metrics
