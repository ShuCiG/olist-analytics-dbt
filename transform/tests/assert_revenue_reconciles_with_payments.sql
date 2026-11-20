-- "Revenue" is the order amount (items + freight, D123); payments differ from it by
-- installment fees, vouchers and orders without items. The full dump differs by 0.02%;
-- more than 0.5% means payments or prices are missing or wrong.
with sold as (
    select
        sum(order_amount) as revenue,
        sum(paid_amount) as paid
    from {{ ref('fct_orders') }}
    where order_status not in ('canceled', 'unavailable')
)

select
    revenue,
    paid
from sold
where abs(paid - revenue) > 0.005 * revenue
