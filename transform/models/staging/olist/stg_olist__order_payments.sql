with source as (
    select * from {{ source('olist', 'order_payments') }}
),

renamed as (
    select
        order_id,
        cast(payment_sequential as {{ dbt.type_int() }}) as payment_sequential,
        payment_type,
        cast(payment_installments as {{ dbt.type_int() }}) as payment_installments,
        cast(payment_value as {{ dbt.type_numeric() }}) as payment_value
    from source
)

select * from renamed
