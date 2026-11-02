with source as (
    select * from {{ source('labels', 'review_labels') }}
),

renamed as (
    select
        review_id,
        sentiment,
        topic,
        translation_en,
        labeled_by,
        cast(guidelines_version as {{ dbt.type_int() }}) as guidelines_version
    from source
)

select * from renamed
