{{ config(severity='warn') }}
-- Known data quality issue: ~250 orders where paid amount differs from items + freight
-- by more than 1 BRL (installment interest, vouchers). Surfaced as a warning, not hidden.
select
    order_id,
    order_amount,
    paid_amount
from {{ ref('fct_orders') }}
where
    payments_count > 0
    and items_count > 0
    and abs(paid_amount - order_amount) > 1
