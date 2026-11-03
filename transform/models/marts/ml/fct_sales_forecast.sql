-- Grain: one row per ISO week, series (category or `all categories`) and
-- measure (orders, revenue): actual values over the history window next to two
-- forecasts (D96): `backtest` = the last 13 weeks of the window predicted by a
-- model that didn't see them, `forecast` = the 13 weeks after the window, with
-- 90 % prediction intervals.
with actuals as (
    select
        week_start_date,
        category_name,
        measure,
        value
    from {{ ref('int_category_sales_aggregated_weekly') }}
),

predictions as (
    {% for prediction_type, model in [('backtest', 'ml_sales_forecast_backtest'), ('forecast', 'ml_sales_forecast')] %}
        select
            '{{ prediction_type }}' as prediction_type,
            cast(forecast_timestamp as date) as week_start_date,
            category_name,
            measure,
            forecast_value,
            prediction_interval_lower_bound,
            prediction_interval_upper_bound
        from ml.forecast(model {{ ref(model) }}, struct(13 as horizon, 0.9 as confidence_level))
        {{ 'union all' if not loop.last }}
    {% endfor %}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['week_start_date', 'category_name', 'measure']) }} as forecast_sk,
        week_start_date,
        category_name,
        measure,
        actuals.value as actual_value,
        predictions.prediction_type,
        predictions.forecast_value as predicted_value,
        predictions.prediction_interval_lower_bound as predicted_lower_bound,
        predictions.prediction_interval_upper_bound as predicted_upper_bound
    from actuals
    full outer join predictions using (week_start_date, category_name, measure)
)

select * from final
