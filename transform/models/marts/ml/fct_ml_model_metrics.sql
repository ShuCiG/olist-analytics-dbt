-- Grain: one row per BigQuery ML model, subject and metric (D96), long format so
-- one table holds what each model reports:
-- - ml_sales_forecast_backtest, per series (category_name, measure): BigQuery's
--   weekly error measures against the 13 held-out weeks (ML.EVALUATE) and
--   `holdout_total_ape`, the absolute % error of those 13 weeks summed up;
-- - ml_repeat_purchase: classifier metrics on the evaluation sample
--   (ML.EVALUATE), its size and repeat rate, `top_decile_lift` (repeat rate of
--   the top-scored tenth / the sample's rate), and per feature its global
--   attribution (ML.GLOBAL_EXPLAIN).
{% set forecast_metrics = [
    'mean_absolute_error',
    'root_mean_squared_error',
    'mean_absolute_percentage_error',
    'symmetric_mean_absolute_percentage_error',
] %}
{% set classifier_metrics = ['precision', 'recall', 'accuracy', 'f1_score', 'log_loss', 'roc_auc'] %}

with forecast_evaluation as (
    select *
    from ml.evaluate(
        model {{ ref('ml_sales_forecast_backtest') }},
        (
            select
                week_start_date,
                category_name,
                measure,
                value
            from {{ ref('int_category_sales_aggregated_weekly') }}
            where is_backtest_holdout
        ),
        struct(true as perform_aggregation, 13 as horizon)
    )
),

-- Error of the 13 held-out weeks summed up: what a quarter's plan would miss by
backtest_totals as (
    select
        category_name,
        measure,
        abs(sum(predicted_value) - sum(actual_value)) / nullif(sum(actual_value), 0) as holdout_total_ape
    from {{ ref('fct_sales_forecast') }}
    where prediction_type = 'backtest'
    group by 1, 2
),

classifier_evaluation as (
    select * from ml.evaluate(model {{ ref('ml_repeat_purchase') }})
),

eval_scores as (
    select * from {{ ref('fct_repeat_purchase_scores') }}
    where sample = 'eval'
),

metrics as (
    {% for metric in forecast_metrics %}
        select
            'ml_sales_forecast_backtest' as model_name,
            category_name,
            measure,
            cast(null as string) as feature_name,
            '{{ metric }}' as metric_name,
            {{ metric }} as metric_value
        from forecast_evaluation
        union all
    {% endfor %}

    select 'ml_sales_forecast_backtest', category_name, measure, null, 'holdout_total_ape', holdout_total_ape
    from backtest_totals

    {% for metric in classifier_metrics %}
        union all
        select 'ml_repeat_purchase', null, null, null, '{{ metric }}', {{ metric }}
        from classifier_evaluation
    {% endfor %}

    union all

    select 'ml_repeat_purchase', null, null, null, 'eval_rows', count(*)
    from eval_scores

    union all

    select 'ml_repeat_purchase', null, null, null, 'eval_repeat_rate', avg(if(ordered_again, 1, 0))
    from eval_scores

    union all

    select
        'ml_repeat_purchase',
        null,
        null,
        null,
        'top_decile_lift',
        avg(if(probability_decile = 1, if(ordered_again, 1, 0), null)) / avg(if(ordered_again, 1, 0))
    from eval_scores

    union all

    select 'ml_repeat_purchase', null, null, feature, 'attribution', attribution
    from ml.global_explain(model {{ ref('ml_repeat_purchase') }})
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['model_name', 'category_name', 'measure', 'feature_name', 'metric_name']) }}
            as metric_sk,
        model_name,
        category_name,
        measure,
        feature_name,
        metric_name,
        cast(metric_value as float64) as metric_value
    from metrics
)

select * from final
