with source as (
    select * from {{ source('olist', 'products') }}
),

renamed as (
    select
        product_id,
        product_category_name as category_name_pt,
        -- source column names contain the typo "lenght"
        cast(product_name_lenght as {{ dbt.type_int() }}) as name_length,
        cast(product_description_lenght as {{ dbt.type_int() }}) as description_length,
        cast(product_photos_qty as {{ dbt.type_int() }}) as photos_qty,
        cast(product_weight_g as {{ dbt.type_int() }}) as weight_g,
        cast(product_length_cm as {{ dbt.type_int() }}) as length_cm,
        cast(product_height_cm as {{ dbt.type_int() }}) as height_cm,
        cast(product_width_cm as {{ dbt.type_int() }}) as width_cm
    from source
)

select * from renamed
