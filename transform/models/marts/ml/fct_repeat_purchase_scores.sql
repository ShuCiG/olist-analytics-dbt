-- Grain: one row per person (`customer_key`) whose first order is in the history
-- window, scored by ml_repeat_purchase (D96): the probability of another order
-- within 180 days. `sample` says what the score is for: `train` (seen by the
-- model), `eval` (held out: compare with `ordered_again`), `unlabelled` (first
-- order too recent for a label: the people to act on).
with scored as (
    select
        customer_key,
        first_purchase_date,
        sample,
        ordered_again,
        (
            select probs.prob
            from unnest(predicted_ordered_again_probs) as probs
            where probs.label
        ) as repeat_probability
    from ml.predict(
        model {{ ref('ml_repeat_purchase') }},
        (select * from {{ ref('int_customer_first_orders_featured') }})
    )
),

final as (
    select
        customer_key,
        first_purchase_date,
        sample,
        ordered_again,
        repeat_probability,
        -- 1 = the tenth with the highest probability, within the sample
        ntile(10) over (partition by sample order by repeat_probability desc) as probability_decile
    from scored
)

select * from final
