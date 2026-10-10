with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

aggregated as (
    select
        order_id,
        count(*) as items_count,
        count(distinct product_id) as distinct_products_count,
        count(distinct seller_id) as distinct_sellers_count,
        sum(price) as items_amount,
        sum(freight_value) as freight_amount,
        sum(price) + sum(freight_value) as order_amount
    from order_items
    group by order_id
)

select * from aggregated
