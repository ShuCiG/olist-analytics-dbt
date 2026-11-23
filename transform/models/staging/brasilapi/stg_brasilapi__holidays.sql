with source as (
    select * from {{ source('brasilapi', 'holidays') }}
),

renamed as (
    select
        cast(holiday_date as date) as holiday_date,
        holiday_name,
        holiday_type,
        cast(holiday_year as {{ dbt.type_int() }}) as holiday_year,
        source as source_api
    from source
)

select * from renamed
