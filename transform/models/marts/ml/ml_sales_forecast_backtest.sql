-- BigQuery ML: the same ARIMA_PLUS as ml_sales_forecast, trained without the
-- last 13 weeks of the history window, so its forecast of those weeks can be
-- compared with what actually happened (D96).
{{
    config(
        materialized='bqml_model',
        ml_options={
            'model_type': 'ARIMA_PLUS',
            'time_series_timestamp_col': 'week_start_date',
            'time_series_data_col': 'value',
            'time_series_id_col': ['category_name', 'measure'],
            'data_frequency': 'WEEKLY',
            'auto_arima_max_order': 3,
            'horizon': 13,
        },
    )
}}

select
    week_start_date,
    category_name,
    measure,
    value
from {{ ref('int_category_sales_aggregated_weekly') }}
where not is_backtest_holdout
