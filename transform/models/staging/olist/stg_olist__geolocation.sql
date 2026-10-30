with source as (
    select * from {{ source('olist', 'geolocation') }}
),

renamed as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        {{ to_float("geolocation_lat") }} as latitude,
        {{ to_float("geolocation_lng") }} as longitude,
        geolocation_city as city,
        geolocation_state as state
    from source
)

select * from renamed
