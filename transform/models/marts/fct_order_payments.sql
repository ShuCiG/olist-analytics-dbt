-- Grain: one row per payment of an order (order_id, payment_sequential). An order can
-- be paid in several parts and by several methods (a card plus vouchers), so payment
-- type is a property of the payment, not of the order (D113). A table, not incremental:
-- payments are small next to the item and order facts. Order attributes come from
-- fct_orders, not int_orders_joined_to_customers: a third leaf mart on that one would
-- break dbt_project_evaluator's fan-out rule (fct_model_fanout).
with payments as (
    select * from {{ ref('stg_olist__order_payments') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['payments.order_id', 'payments.payment_sequential']) }}
            as payment_sk,
        payments.order_id,
        payments.payment_sequential,
        payments.payment_type,
        payments.payment_installments,
        orders.customer_unique_id,
        orders.order_status,
        orders.purchase_date,
        {{ to_money('payments.payment_value') }} as payment_value
    from payments
    inner join orders
        on payments.order_id = orders.order_id
)

select * from final
