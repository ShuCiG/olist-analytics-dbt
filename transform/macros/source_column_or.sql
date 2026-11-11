{#
    A source column that an existing table may not have yet: the column where the
    table has it (nulls replaced by `default`), else `default` alone. For a column the
    loader adds after the table was created: `_is_synthetic` (D112) reaches an
    existing raw orders table only with its next load of orders, and a static
    dump's prod raw gets none. Parsing and unit tests (no table) get the default.
#}
{% macro source_column_or(relation, column, default) -%}
    {%- set names = [] -%}
    {%- if execute and model.resource_type != 'unit_test' -%}
        {%- for existing in adapter.get_columns_in_relation(relation) -%}
            {%- do names.append(existing.name | lower) -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if column | lower in names -%}
        coalesce({{ column }}, {{ default }})
    {%- else -%}
        {{ default }}
    {%- endif -%}
{%- endmacro %}
