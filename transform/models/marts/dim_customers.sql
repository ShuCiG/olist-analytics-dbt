-- Grain: one row per person (customer_unique_id). Olist issues a new
-- customer_id per order, so location is taken from the person's latest order.
with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

payments as (
    select * from {{ ref('int_payments_pivoted_to_orders') }}
),

zip_codes as (
    select * from {{ ref('int_zip_codes_located') }}
),

customer_orders as (
    select
        customers.customer_unique_id,
        customers.zip_code_prefix,
        customers.city,
        customers.state,
        orders.order_id,
        orders.order_status,
        orders.purchased_at,
        row_number() over (
            partition by customers.customer_unique_id
            order by orders.purchased_at desc, orders.order_id asc
        ) as recency_rank
    from customers
    inner join orders
        on customers.customer_id = orders.customer_id
),

order_stats as (
    select
        customer_orders.customer_unique_id,
        min(customer_orders.purchased_at) as first_order_at,
        max(customer_orders.purchased_at) as last_order_at,
        count(*) as orders_count,
        sum(
            case
                when customer_orders.order_status not in ('canceled', 'unavailable')
                    then coalesce(payments.total_payment_value, 0)
                else 0
            end
        ) as lifetime_value
    from customer_orders
    left join payments
        on customer_orders.order_id = payments.order_id
    group by customer_orders.customer_unique_id
),

final as (
    select
        latest.customer_unique_id,
        latest.zip_code_prefix,
        latest.city,
        latest.state,
        zip_codes.latitude,
        zip_codes.longitude,
        order_stats.first_order_at,
        order_stats.last_order_at,
        order_stats.orders_count,
        order_stats.orders_count > 1 as is_repeat_customer,
        {{ to_money('order_stats.lifetime_value') }} as lifetime_value
    from customer_orders as latest
    inner join order_stats
        on latest.customer_unique_id = order_stats.customer_unique_id
    left join zip_codes
        on latest.zip_code_prefix = zip_codes.zip_code_prefix
    where latest.recency_rank = 1
)

select * from final
