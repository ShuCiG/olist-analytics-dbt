-- Grain: one row per ISO week of the history window, series and measure: the
-- input of the sales forecast (D96). Series = the top categories by revenue in
-- the window plus `all categories`; measures = orders (distinct orders with an
-- item in the series) and revenue (item price + freight). Orders not
-- canceled/unavailable. Only weeks wholly inside the window; weeks without sales
-- are zeros, so every series is gapless. The last 13 weeks are the backtest's
-- holdout. Weekly, not daily: BigQuery ML bills ARIMA_PLUS training by input
-- bytes times candidate models, and daily series cost ~7 times as much (D96).
-- BigQuery SQL: the models reading it are BigQuery ML.
{% set top_categories = var('forecast_top_categories', 7) %}
{% set holdout_weeks = 13 %}

with window_bounds as (
    select * from {{ ref('int_order_history_bounded') }}
),

weeks as (
    select distinct dates.iso_week_start_date as week_start_date
    from {{ ref('dim_date') }} as dates
    cross join window_bounds
    where
        dates.iso_week_start_date >= window_bounds.history_start_date
        and date_add(dates.iso_week_start_date, interval 6 day) <= window_bounds.history_end_date
),

items as (
    select
        items.order_id,
        date_trunc(items.purchase_date, isoweek) as week_start_date,
        items.item_amount,
        products.category_name
    from {{ ref('fct_order_items') }} as items
    inner join {{ ref('dim_products') }} as products on items.product_id = products.product_id
    where items.order_status not in ('canceled', 'unavailable')
),

items_in_window as (
    select items.*
    from items
    inner join weeks on items.week_start_date = weeks.week_start_date
),

top_categories as (
    select category_name
    from items_in_window
    group by category_name
    order by sum(item_amount) desc
    limit {{ top_categories }}
),

weekly as (
    select
        items_in_window.week_start_date,
        items_in_window.category_name,
        count(distinct items_in_window.order_id) as orders_count,
        sum(items_in_window.item_amount) as revenue
    from items_in_window
    inner join top_categories on items_in_window.category_name = top_categories.category_name
    group by 1, 2

    union all

    select
        week_start_date,
        'all categories' as category_name,
        count(distinct order_id) as orders_count,
        sum(item_amount) as revenue
    from items_in_window
    group by 1, 2
),

series as (
    select category_name from top_categories
    union all
    select 'all categories'
),

holdout_start as (
    select date_sub(max(week_start_date), interval {{ holdout_weeks - 1 }} week) as week_start_date
    from weeks
),

final as (
    select
        weeks.week_start_date,
        series.category_name,
        measure,
        cast(case measure
            when 'orders' then coalesce(weekly.orders_count, 0)
            else coalesce(weekly.revenue, 0)
        end as float64) as value,
        weeks.week_start_date >= holdout_start.week_start_date as is_backtest_holdout
    from weeks
    cross join series
    cross join holdout_start
    cross join unnest(['orders', 'revenue']) as measure
    left join weekly
        on
            weeks.week_start_date = weekly.week_start_date
            and series.category_name = weekly.category_name
)

select * from final
