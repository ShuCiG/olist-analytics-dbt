{#
    Casts to the project's float type, an 8-byte double. Integer / integer is a
    double on DuckDB, BigQuery and Databricks, but NUMBER(p, 6) on Snowflake, so
    ratios cast an operand first. dbt.type_float() is a 4-byte FLOAT on DuckDB
    (float32: coordinates lose the 7th digit there).
    Keep in sync with the &float_type anchor in marts/_marts__models.yml.
#}
{% macro to_float(expression) -%}
    {{ return(adapter.dispatch('to_float', 'olist')(expression)) }}
{%- endmacro %}

{% macro default__to_float(expression) -%}
    cast({{ expression }} as double)
{%- endmacro %}

{% macro bigquery__to_float(expression) -%}
    cast({{ expression }} as float64)
{%- endmacro %}
