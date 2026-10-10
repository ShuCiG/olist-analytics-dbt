{%- set payment_types = ['credit_card', 'boleto', 'voucher', 'debit_card'] -%}

with payments as (
    select * from {{ ref('stg_olist__order_payments') }}
),

pivoted as (
    select
        order_id,
        count(*) as payments_count,
        max(payment_installments) as max_installments,
        sum(payment_value) as total_payment_value,
        {%- for payment_type in payment_types %}
        sum(case when payment_type = '{{ payment_type }}' then payment_value else 0 end)
            as {{ payment_type }}_amount{{ ',' if not loop.last }}
        {%- endfor %}
    from payments
    group by order_id
)

select * from pivoted
