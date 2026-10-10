with source as (
    select * from {{ source('olist', 'geolocation') }}
),

renamed as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        cast(geolocation_lat as {{ dbt.type_float() }}) as latitude,
        cast(geolocation_lng as {{ dbt.type_float() }}) as longitude,
        geolocation_city as city,
        geolocation_state as state
    from source
)

select * from renamed
