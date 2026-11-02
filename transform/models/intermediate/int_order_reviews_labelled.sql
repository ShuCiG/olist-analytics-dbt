-- Grain: one row per order whose review (the order's latest, as in fct_orders)
-- has a label. Orders without review text, or not labelled yet, are left out.
with reviews as (
    select * from {{ ref('int_order_reviews_deduplicated') }}
),

labels as (
    select * from {{ ref('stg_labels__review_labels') }}
),

joined as (
    select
        reviews.order_id,
        reviews.review_id,
        reviews.review_score,
        reviews.review_created_at,
        labels.sentiment,
        labels.topic,
        labels.translation_en,
        labels.labeled_by
    from reviews
    inner join labels
        on reviews.review_id = labels.review_id
)

select * from joined
