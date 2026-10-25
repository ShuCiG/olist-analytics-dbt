-- dlt loads an order and its items in one chunk with one `_loaded_at`
-- (ingestion/pipeline.py). fct_order_items filters each table on its own
-- `_loaded_at` (D70), which is only complete while this holds.
select
    order_items.order_id,
    order_items.order_item_id,
    order_items._loaded_at as item_loaded_at,
    orders._loaded_at as order_loaded_at
from {{ source('olist', 'order_items') }} as order_items
inner join {{ source('olist', 'orders') }} as orders
    on order_items.order_id = orders.order_id
where order_items._loaded_at != orders._loaded_at
