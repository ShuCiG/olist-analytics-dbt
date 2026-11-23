with raw_weather as (
    select * from {{ source('openmeteo', 'weather_daily') }}
),

renamed as (
    select
        state,
        cast(weather_date as date) as weather_date,
        {{ to_float('precipitation_mm') }} as precipitation_mm,
        {{ to_float('temperature_c') }} as temperature_c
    from raw_weather
)

select * from renamed
