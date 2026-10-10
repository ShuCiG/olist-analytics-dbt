{#
    dev / prod: use the custom schema as-is (staging, marts, ...).
    Any other target (e.g. per-PR CI): prefix with target schema to isolate runs.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name in ['dev', 'prod'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
