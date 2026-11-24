-- Grain: one row per order. Order value and payments in USD at the rate of the purchase day (the
-- last published one on a weekend or bank holiday), next to the BRL amounts they come from (#351).
-- Generated months (D112) carry the real historic rate of their dates; `is_synthetic` tells whose
-- orders they are.
with orders as (
    select * from {{ ref('fct_orders') }}
),

rates as (
    select * from {{ ref('fct_fx_rates_daily') }}
),

final as (
    select
        orders.order_id,
        orders.is_synthetic,
        orders.purchase_date,
        rates.rate_date_used,
        rates.brl_per_usd,
        rates.rate_source,
        orders.order_amount,
        orders.paid_amount,
        {{ to_money('orders.order_amount / rates.brl_per_usd') }} as order_amount_usd,
        {{ to_money('orders.paid_amount / rates.brl_per_usd') }} as paid_amount_usd
    from orders
    left join rates
        on orders.purchase_date = rates.date_day
)

select * from final
