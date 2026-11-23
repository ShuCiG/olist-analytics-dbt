-- Grain: one row per order. The weather of the customer's state (its capital, #349) on the
-- day of the purchase and on the day of the delivery, next to the delivery outcome: "do late
-- deliveries come with rain". Weather is real for every order, the generated months of the
-- feed (D112) included; `is_synthetic` tells whose orders they are, so a weather-vs-delay
-- result is not read as real for them.
with orders as (
    select * from {{ ref('int_orders_joined_to_customers') }}
),

delivery as (
    select * from {{ ref('int_orders_delivery_metrics') }}
),

weather as (
    select * from {{ ref('stg_openmeteo__weather_daily') }}
),

measured as (
    select
        orders.order_id,
        orders.is_synthetic,
        orders.state as customer_state,
        cast(orders.purchased_at as date) as purchase_date,
        cast(orders.delivered_to_customer_at as date) as delivery_date,
        purchase_weather.precipitation_mm as purchase_precipitation_mm,
        purchase_weather.temperature_c as purchase_temperature_c,
        delivery_weather.precipitation_mm as delivery_precipitation_mm,
        delivery_weather.temperature_c as delivery_temperature_c,
        delivery.is_delivered,
        delivery.days_to_deliver,
        delivery.is_delivered_late
    from orders
    inner join delivery
        on orders.order_id = delivery.order_id
    left join weather as purchase_weather
        on
            orders.state = purchase_weather.state
            and cast(orders.purchased_at as date) = purchase_weather.weather_date
    left join weather as delivery_weather
        on
            orders.state = delivery_weather.state
            and cast(orders.delivered_to_customer_at as date) = delivery_weather.weather_date
),

final as (
    select
        *,
        case
            when delivery_precipitation_mm is null then 'unknown'
            when delivery_precipitation_mm < 0.1 then 'dry'
            when delivery_precipitation_mm < 5 then 'light'
            when delivery_precipitation_mm < 20 then 'moderate'
            else 'heavy'
        end as delivery_rain_band
    from measured
)

select * from final
