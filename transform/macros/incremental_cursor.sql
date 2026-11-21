{#
    The cursor of an incremental model: the latest `column` already in {{ this }},
    to compare new rows against. BigQuery prunes partitions only on constant
    filters, not on a subquery (D69), so there the value is read first (one query,
    billed at BigQuery's 10 MiB minimum) and rendered as a literal. Other engines,
    parsing, and unit tests (their `this` is a fixture, not a table) get the
    subquery. An empty table (exists, no rows) has no cursor: it falls back to the
    epoch, so the next run loads everything instead of silently nothing (`>= null`
    passes no row). The epoch, not 0001-01-01: `timestamp_sub(cursor, interval 1 day)`
    in fct_dbt_job_costs would overflow BigQuery's minimum timestamp.
#}
{% macro incremental_cursor(column='_loaded_at') -%}
    {{ return(adapter.dispatch('incremental_cursor', 'olist')(column)) }}
{%- endmacro %}

{% macro default__incremental_cursor(column) -%}
    coalesce(
        (select max(this_run.{{ column }}) from {{ this }} as this_run),
        cast('1970-01-01 00:00:00' as {{ dbt.type_timestamp() }})
    )
{%- endmacro %}

{% macro bigquery__incremental_cursor(column) -%}
    {%- if not execute or model.resource_type == 'unit_test' -%}
        {{ default__incremental_cursor(column) }}
    {%- else -%}
        {%- set value = dbt_utils.get_single_value('select max(' ~ column ~ ') from ' ~ this) -%}
        {%- if value is none -%}
            timestamp '1970-01-01 00:00:00'
        {%- else -%}
            timestamp '{{ value }}'
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
