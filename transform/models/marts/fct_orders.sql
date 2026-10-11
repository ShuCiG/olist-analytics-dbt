-- Grain: one row per order.
with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

items as (
    select * from {{ ref('int_order_items_aggregated_to_orders') }}
),

payments as (
    select * from {{ ref('int_payments_pivoted_to_orders') }}
),

delivery as (
    select * from {{ ref('int_orders_delivery_metrics') }}
),

reviews as (
    select * from {{ ref('int_order_reviews_deduplicated') }}
),

final as (
    select
        orders.order_id,
        customers.customer_unique_id,
        orders.order_status,
        cast(orders.purchased_at as date) as purchase_date,
        orders.purchased_at,
        orders.approved_at,
        orders.delivered_to_carrier_at,
        orders.delivered_to_customer_at,
        orders.estimated_delivery_date,

        coalesce(items.items_count, 0) as items_count,
        coalesce(items.distinct_sellers_count, 0) as distinct_sellers_count,
        {{ to_money('coalesce(items.items_amount, 0)') }} as items_amount,
        {{ to_money('coalesce(items.freight_amount, 0)') }} as freight_amount,
        {{ to_money('coalesce(items.order_amount, 0)') }} as order_amount,

        coalesce(payments.payments_count, 0) as payments_count,
        {{ to_money('coalesce(payments.total_payment_value, 0)') }} as payment_value,
        payments.max_installments,
        {{ to_money('coalesce(payments.credit_card_amount, 0)') }} as credit_card_amount,
        {{ to_money('coalesce(payments.boleto_amount, 0)') }} as boleto_amount,
        {{ to_money('coalesce(payments.voucher_amount, 0)') }} as voucher_amount,
        {{ to_money('coalesce(payments.debit_card_amount, 0)') }} as debit_card_amount,

        delivery.hours_to_approve,
        delivery.days_to_deliver,
        delivery.days_delivered_before_estimate,
        delivery.is_delivered,
        delivery.is_delivered_late,

        reviews.review_score,
        reviews.review_id is not null as has_review
    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
    left join items
        on orders.order_id = items.order_id
    left join payments
        on orders.order_id = payments.order_id
    left join delivery
        on orders.order_id = delivery.order_id
    left join reviews
        on orders.order_id = reviews.order_id
)

select * from final
