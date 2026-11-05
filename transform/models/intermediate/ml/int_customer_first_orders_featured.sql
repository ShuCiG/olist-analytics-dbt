-- Grain: one row per person (`customer_key`, never the id: D94), described by
-- their first order: the input of the repeat-purchase model (D96). Label
-- `ordered_again` = another order on a later day within 180 days of the first
-- (any status, as `orders_count`); null while fewer than 180 days of history
-- follow the first order. Sample: the last 90 days of labelled first orders are
-- the evaluation set (a split in time, as the model would be used), the earlier
-- ones train, the unlabelled ones are only scored. A table, not a view: BigQuery
-- ML bills training on what its input query scans (D96).
-- BigQuery SQL: the models reading it are BigQuery ML.
{{ config(materialized='table') }}

with window_bounds as (
    select * from {{ ref('int_order_history_bounded') }}
),

orders as (
    select
        *,
        row_number() over (partition by customer_key order by purchased_at, order_id) as order_rank
    from {{ ref('fct_orders') }}
),

first_orders as (
    select * from orders
    where order_rank = 1
),

-- Category of the first order = the category of its most expensive item
first_categories as (
    select
        items.order_id,
        array_agg(products.category_name order by items.item_amount desc, items.order_item_number limit 1)[
            offset(0)
        ] as category_name
    from {{ ref('fct_order_items') }} as items
    inner join first_orders on items.order_id = first_orders.order_id
    inner join {{ ref('dim_products') }} as products on items.product_id = products.product_id
    group by items.order_id
),

came_back as (
    select distinct first_orders.customer_key
    from first_orders
    inner join orders as later
        on
            first_orders.customer_key = later.customer_key
            and later.purchase_date > first_orders.purchase_date
            and later.purchase_date <= date_add(first_orders.purchase_date, interval 180 day)
),

final as (
    select
        first_orders.customer_key,
        first_orders.purchase_date as first_purchase_date,
        case
            when first_orders.purchase_date > window_bounds.labelled_until_date then 'unlabelled'
            when first_orders.purchase_date > date_sub(window_bounds.labelled_until_date, interval 90 day) then 'eval'
            else 'train'
        end as sample,
        case
            when first_orders.purchase_date <= window_bounds.labelled_until_date
                then came_back.customer_key is not null
        end as ordered_again,

        -- Features: what is known about the person after their first order
        first_orders.order_status as first_order_status,
        coalesce(first_categories.category_name, 'none') as first_category,
        customers.state as customer_state,
        format_date('%m', first_orders.purchase_date) as purchase_month,
        cast(first_orders.paid_amount as float64) as paid_amount,
        first_orders.items_count,
        first_orders.distinct_sellers_count,
        safe_divide(
            cast(first_orders.freight_amount as float64), cast(first_orders.order_amount as float64)
        ) as freight_share,
        case greatest(
            first_orders.credit_card_amount, first_orders.boleto_amount,
            first_orders.voucher_amount, first_orders.debit_card_amount
        )
            when 0 then 'none'
            when first_orders.credit_card_amount then 'credit_card'
            when first_orders.boleto_amount then 'boleto'
            when first_orders.voucher_amount then 'voucher'
            else 'debit_card'
        end as main_payment_type,
        first_orders.voucher_amount > 0 as used_voucher,
        first_orders.max_installments,
        case
            when not first_orders.is_delivered then 'not_delivered'
            when first_orders.is_delivered_late then 'late'
            else 'on_time'
        end as delivery_outcome,
        first_orders.days_to_deliver,
        first_orders.days_delivered_before_estimate,
        coalesce(cast(first_orders.review_score as string), 'none') as review_score
    from first_orders
    cross join window_bounds
    left join first_categories on first_orders.order_id = first_categories.order_id
    left join {{ ref('dim_customers') }} as customers on first_orders.customer_key = customers.customer_key
    left join came_back on first_orders.customer_key = came_back.customer_key
    where first_orders.purchase_date between window_bounds.history_start_date and window_bounds.history_end_date
)

select * from final
