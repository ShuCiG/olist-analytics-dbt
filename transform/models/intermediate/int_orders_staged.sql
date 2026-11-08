-- Review of each order (latest), its milestones and lags, and the furthest stage
-- each order reached.
-- The stage follows fulfilment first: a review counts only once the order is
-- delivered, so a review of an undelivered order (a complaint about a shipment
-- that never arrived, a canceled order) leaves the order at its fulfilment stage.
with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

reviews as (
    select * from {{ ref('int_order_reviews_deduplicated') }}
),

staged as (
    select
        orders.order_id,
        reviews.review_score,
        reviews.review_id is not null as has_review,
        reviews.review_created_at,
        reviews.review_answered_at,
        {{ dbt.datediff('orders.delivered_to_customer_at', 'reviews.review_created_at', 'day') }}
            as days_delivered_to_review,
        {{ dbt.datediff('reviews.review_created_at', 'reviews.review_answered_at', 'hour') }}
            as hours_review_to_answer,
        case
            when orders.delivered_to_customer_at is not null and reviews.review_answered_at is not null
                then 'reviewed'
            when orders.delivered_to_customer_at is not null then 'delivered'
            when orders.delivered_to_carrier_at is not null then 'shipped'
            when orders.approved_at is not null then 'approved'
            else 'purchased'
        end as order_stage,
        orders.order_status not in ('delivered', 'canceled', 'unavailable') as is_order_open
    from orders
    left join reviews
        on orders.order_id = reviews.order_id
)

select * from staged
