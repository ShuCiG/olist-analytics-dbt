-- BigQuery ML: logistic regression, will a person order again within 180 days
-- of their first order (D96). Labelled people only; the split is in time
-- (`is_eval` = the last 90 days of labelled first orders), not random. About 3 %
-- come back, so the classes are weighted by their inverse frequency.
{{
    config(
        materialized='bqml_model',
        ml_options={
            'model_type': 'LOGISTIC_REG',
            'input_label_cols': ['ordered_again'],
            'auto_class_weights': true,
            'data_split_method': 'CUSTOM',
            'data_split_col': 'is_eval',
            'enable_global_explain': true,
        },
    )
}}

select
    sample = 'eval' as is_eval,
    ordered_again,
    first_order_status,
    first_category,
    customer_state,
    purchase_month,
    paid_amount,
    items_count,
    distinct_sellers_count,
    freight_share,
    main_payment_type,
    used_voucher,
    max_installments,
    delivery_outcome,
    days_to_deliver,
    days_delivered_before_estimate,
    review_score
from {{ ref('int_customer_first_orders_featured') }}
where sample in ('train', 'eval')
