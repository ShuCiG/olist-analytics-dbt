with raw_rates as (
    select * from {{ source('bcb', 'fx_rates_daily') }}
),

renamed as (
    select
        cast(rate_date as date) as rate_date,
        {{ to_float('brl_per_usd') }} as brl_per_usd,
        source as source_api
    from raw_rates
)

select * from renamed
