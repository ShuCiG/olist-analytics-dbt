{#
    Casts an amount to the project's money type: BRL with 2 decimals.
    Explicit precision keeps mart contracts stable: without it DuckDB derives
    DECIMAL(28..38, 6) from sums and arithmetic, varying per column.
#}
{% macro to_money(expression) -%}
    cast({{ expression }} as numeric(18, 2))
{%- endmacro %}
