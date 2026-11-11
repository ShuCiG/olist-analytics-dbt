-- Grain: one row per order. Accumulating snapshot: one column per milestone of the
-- order's life, filled in as the order moves on, with the lags between them and the
-- furthest stage reached. The table is rebuilt, so each run shows every order's
-- current state.
with orders as (
    select * from {{ ref('int_orders_joined_to_customers') }}
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

-- Review columns come through stages too: joining the reviews here as well would
-- rejoin a model stages already reads (dbt_project_evaluator)
stages as (
    select * from {{ ref('int_orders_staged') }}
),

final as (
    select
        orders.order_id,
        orders.customer_unique_id,
        -- Pseudonym of the person for readers without access to the id (D94)
        {{ customer_key('orders.customer_unique_id') }} as customer_key,
        orders.order_status,
        stages.order_stage,
        stages.is_order_open,
        orders.is_synthetic,
        cast(orders.purchased_at as date) as purchase_date,
        orders.purchased_at,
        orders.approved_at,
        orders.delivered_to_carrier_at,
        orders.delivered_to_customer_at,
        orders.estimated_delivery_date,
        stages.review_created_at,
        stages.review_answered_at,

        coalesce(items.items_count, 0) as items_count,
        coalesce(items.distinct_sellers_count, 0) as distinct_sellers_count,
        {{ to_money('coalesce(items.items_amount, 0)') }} as items_amount,
        {{ to_money('coalesce(items.freight_amount, 0)') }} as freight_amount,
        {{ to_money('coalesce(items.order_amount, 0)') }} as order_amount,

        coalesce(payments.payments_count, 0) as payments_count,
        {{ to_money('coalesce(payments.total_payment_value, 0)') }} as paid_amount,
        payments.max_installments,
        {{ to_money('coalesce(payments.credit_card_amount, 0)') }} as credit_card_amount,
        {{ to_money('coalesce(payments.boleto_amount, 0)') }} as boleto_amount,
        {{ to_money('coalesce(payments.voucher_amount, 0)') }} as voucher_amount,
        {{ to_money('coalesce(payments.debit_card_amount, 0)') }} as debit_card_amount,

        delivery.hours_to_approve,
        delivery.days_approved_to_carrier,
        delivery.days_carrier_to_customer,
        stages.days_delivered_to_review,
        stages.hours_review_to_answer,
        delivery.days_to_deliver,
        delivery.days_delivered_before_estimate,
        delivery.is_delivered,
        delivery.is_delivered_late,

        stages.review_score,
        stages.has_review
    from orders
    left join items
        on orders.order_id = items.order_id
    left join payments
        on orders.order_id = payments.order_id
    left join delivery
        on orders.order_id = delivery.order_id
    left join stages
        on orders.order_id = stages.order_id
)

select * from final
