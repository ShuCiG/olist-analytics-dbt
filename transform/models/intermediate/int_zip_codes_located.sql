-- Geolocation has many points per zip prefix: collapse to one centroid per prefix.
with geolocation as (
    select * from {{ ref('stg_olist__geolocation') }}
),

located as (
    select
        zip_code_prefix,
        avg(latitude) as latitude,
        avg(longitude) as longitude,
        count(*) as points_count
    from geolocation
    group by zip_code_prefix
)

select * from located
