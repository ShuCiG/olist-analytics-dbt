{#
    The cursor of an incremental model: the latest `column` already in {{ this }},
    to compare new rows against. BigQuery prunes partitions only on constant
    filters, not on a subquery (D69), so there the value is read first (one query,
    billed at BigQuery's 10 MiB minimum) and rendered as a literal. Other engines,
    parsing, and unit tests (their `this` is a fixture, not a table) get the
    subquery. An empty table gives null either way: no row passes `>=`.
#}
{% macro incremental_cursor(column='_loaded_at') -%}
    {{ return(adapter.dispatch('incremental_cursor', 'olist')(column)) }}
{%- endmacro %}

{% macro default__incremental_cursor(column) -%}
    (select max(this_run.{{ column }}) from {{ this }} as this_run)
{%- endmacro %}

{% macro bigquery__incremental_cursor(column) -%}
    {%- if not execute or model.resource_type == 'unit_test' -%}
        {{ default__incremental_cursor(column) }}
    {%- else -%}
        {%- set value = dbt_utils.get_single_value('select max(' ~ column ~ ') from ' ~ this) -%}
        {%- if value is none -%}
            cast(null as timestamp)
        {%- else -%}
            timestamp '{{ value }}'
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
