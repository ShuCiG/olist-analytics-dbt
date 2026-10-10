-- fct_order_items must reconcile exactly with fct_orders.order_amount.
with items as (
    select order_id, sum(item_amount) as items_total
    from {{ ref('fct_order_items') }}
    group by order_id
)

select
    orders.order_id,
    orders.order_amount,
    items.items_total
from {{ ref('fct_orders') }} as orders
inner join items
    on orders.order_id = items.order_id
where abs(orders.order_amount - items.items_total) > 0.01
