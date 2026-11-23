-- Every state has weather for every day from the first purchase to the last delivery. A gap
-- means the feed was down (or stopped short of a longer order feed): the weather columns of
-- fct_order_weather are silently null for those days.
with order_days as (
    select
        min(cast(purchased_at as date)) as first_day,
        max(cast(delivered_to_customer_at as date)) as last_day
    from {{ ref('int_orders_joined_to_customers') }}
),

days as (
    select dim_date.date_day
    from {{ ref('dim_date') }} as dim_date
    inner join order_days
        on dim_date.date_day between order_days.first_day and order_days.last_day
),

states as (
    select distinct state
    from {{ ref('stg_olist__customers') }}
),

expected as (
    select
        states.state,
        days.date_day
    from states
    cross join days
)

select
    expected.state,
    expected.date_day
from expected
left join {{ ref('stg_openmeteo__weather_daily') }} as weather
    on expected.state = weather.state and expected.date_day = weather.weather_date
where weather.weather_date is null
