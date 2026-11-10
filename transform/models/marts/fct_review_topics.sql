-- Grain: one row per order whose review has a label (sentiment, topic, English
-- translation; D92). The order's category is the category of its most expensive
-- item, so a review counts once, under the product it was mostly about.
with reviews as (
    select * from {{ ref('int_order_reviews_labelled') }}
),

orders as (
    select * from {{ ref('int_orders_joined_to_customers') }}
),

delivery as (
    select * from {{ ref('int_orders_delivery_metrics') }}
),

items_ranked as (
    select
        items.order_id,
        products.category_name,
        row_number() over (
            partition by items.order_id
            order by items.price desc, items.order_item_number asc
        ) as item_rank
    from {{ ref('fct_order_items') }} as items
    inner join {{ ref('dim_products') }} as products
        on items.product_id = products.product_id
),

final as (
    select
        reviews.order_id,
        reviews.review_id,
        cast(orders.purchased_at as date) as purchase_date,
        orders.state as customer_state,
        items_ranked.category_name,
        reviews.review_score,
        reviews.sentiment,
        reviews.topic,
        reviews.translation_en,
        delivery.is_delivered,
        delivery.is_delivered_late,
        delivery.days_delivered_before_estimate,
        reviews.labeled_by
    from reviews
    inner join orders
        on reviews.order_id = orders.order_id
    inner join delivery
        on reviews.order_id = delivery.order_id
    left join items_ranked
        on
            reviews.order_id = items_ranked.order_id
            and items_ranked.item_rank = 1
)

select * from final
