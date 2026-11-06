-- BigQuery ML: ARIMA_PLUS per series (category x measure) on the whole history
-- window, for the forecast of the 13 weeks after it (D96). Weekly: ~85 points
-- per series, no weekday seasonality or holidays to model. auto_arima_max_order
-- 3 (default 5) cuts the candidate models BigQuery fits and bills per series.
{{
    config(
        materialized='bqml_model',
        meta={
            'ml_options': {
                'model_type': 'ARIMA_PLUS',
                'time_series_timestamp_col': 'week_start_date',
                'time_series_data_col': 'value',
                'time_series_id_col': ['category_name', 'measure'],
                'data_frequency': 'WEEKLY',
                'auto_arima_max_order': 3,
                'horizon': 13,
            },
        },
    )
}}

select
    week_start_date,
    category_name,
    measure,
    value
from {{ ref('int_category_sales_aggregated_weekly') }}
