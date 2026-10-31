{#
    Overrides dbt_date.week_of_year (dispatch in dbt_project.yml) for dim_date.
    The package's per-adapter versions disagree: ceil(day_of_year / 7) on DuckDB,
    an ISO-like week on Snowflake and Databricks, EXTRACT(WEEK) on BigQuery.
    One definition on every engine, the one prod BigQuery always had: weeks
    start on Sunday, days before the year's first Sunday are week 0 (as
    strftime %U). day_of_week(isoweek=false) is 1 for Sunday on every engine.
#}
{% macro default__week_of_year(date) -%}
    cast(
        floor(
            ({{ dbt_date.day_of_year(date) }} + 7
            - {{ dbt_date.day_of_week(date, isoweek=false) }}) / 7
        ) as {{ dbt.type_int() }}
    )
{%- endmacro %}
