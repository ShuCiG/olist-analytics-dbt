-- Some orders have several reviews: keep the most recent one per order.
with reviews as (
    select * from {{ ref('stg_olist__order_reviews') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by review_created_at desc, review_answered_at desc, review_id
        ) as review_rank
    from reviews
)

select
    order_id,
    review_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_created_at,
    review_answered_at
from ranked
where review_rank = 1
