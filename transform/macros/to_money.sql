{#
    Casts an amount to the project's money type: BRL with 2 decimals.
    Explicit precision keeps mart contracts stable: without it DuckDB derives
    DECIMAL(28..38, 6) from sums and arithmetic, varying per column.
    BigQuery forbids parameterized types in CAST: plain NUMERIC (scale 9) + round.
    Keep in sync with the &money_type anchor in marts/_marts__models.yml.
#}
{% macro to_money(expression) -%}
    {{ return(adapter.dispatch('to_money', 'olist')(expression)) }}
{%- endmacro %}

{% macro default__to_money(expression) -%}
    cast({{ expression }} as numeric(18, 2))
{%- endmacro %}

{% macro bigquery__to_money(expression) -%}
    cast(round({{ expression }}, 2) as numeric)
{%- endmacro %}
