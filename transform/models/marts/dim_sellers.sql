with sellers as (
    select * from {{ ref('stg_olist__sellers') }}
),

zip_codes as (
    select * from {{ ref('int_zip_codes_located') }}
),

final as (
    select
        sellers.seller_id,
        sellers.zip_code_prefix,
        sellers.city,
        sellers.state,
        zip_codes.latitude,
        zip_codes.longitude
    from sellers
    left join zip_codes
        on sellers.zip_code_prefix = zip_codes.zip_code_prefix
)

select * from final
