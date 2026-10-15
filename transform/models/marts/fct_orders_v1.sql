-- Version 1 of fct_orders, kept for consumers until its deprecation date: the
-- latest version with `paid_amount` under its v1 name `payment_value`. A plain
-- projection, so the two versions can't drift apart.
with orders as (
    select * from {{ ref('fct_orders', v=2) }}
),

final as (
    select
        order_id,
        customer_unique_id,
        order_status,
        purchase_date,
        purchased_at,
        approved_at,
        delivered_to_carrier_at,
        delivered_to_customer_at,
        estimated_delivery_date,
        items_count,
        distinct_sellers_count,
        items_amount,
        freight_amount,
        order_amount,
        payments_count,
        paid_amount as payment_value,
        max_installments,
        credit_card_amount,
        boleto_amount,
        voucher_amount,
        debit_card_amount,
        hours_to_approve,
        days_to_deliver,
        days_delivered_before_estimate,
        is_delivered,
        is_delivered_late,
        review_score,
        has_review
    from orders
)

select * from final
