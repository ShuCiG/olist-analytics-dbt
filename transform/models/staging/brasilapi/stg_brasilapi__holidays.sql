-- The CTE is not called `source`: raw has a column of that name, and on BigQuery
-- `select source from source` returns the whole row (a struct), not the column.
with raw_holidays as (
    select * from {{ source('brasilapi', 'holidays') }}
),

renamed as (
    select
        cast(holiday_date as date) as holiday_date,
        holiday_name,
        holiday_type,
        cast(holiday_year as {{ dbt.type_int() }}) as holiday_year,
        source as source_api
    from raw_holidays
)

select * from renamed
