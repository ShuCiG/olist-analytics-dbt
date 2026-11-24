-- Grain: one row per calendar day from the first to the latest published rate. Banks publish on
-- business days only, so a weekend or a bank holiday carries the last published rate forward
-- (`is_carried_forward`); an order bought on such a day is converted at that rate (#351).
with rates as (
    select
        rate_date,
        brl_per_usd,
        source_api,
        lead(rate_date) over (order by rate_date) as next_rate_date
    from {{ ref('stg_bcb__fx_rates_daily') }}
),

latest as (
    select max(rate_date) as last_rate_date
    from rates
),

final as (
    select
        dim_date.date_day,
        rates.rate_date as rate_date_used,
        rates.brl_per_usd,
        rates.source_api as rate_source,
        dim_date.date_day <> rates.rate_date as is_carried_forward
    from {{ ref('dim_date') }} as dim_date
    inner join rates
        on
            dim_date.date_day >= rates.rate_date
            and (rates.next_rate_date is null or dim_date.date_day < rates.next_rate_date)
    inner join latest
        on dim_date.date_day <= latest.last_rate_date
)

select * from final
