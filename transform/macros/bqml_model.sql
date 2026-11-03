{#
    BigQuery ML model as a dbt node (D96): `create or replace model` from the
    model's SELECT, with the OPTIONS of config `ml_options`. Downstream models
    read it with `ML.FORECAST(MODEL {{ ref(...) }}, ...)` and friends, so training,
    prediction and evaluation sit in the DAG like any table. A BigQuery ML model
    is not a table: dbt can't test it, and the catalog of `docs generate` doesn't
    list it; the tests go on the tables that read it.
    persist_docs: the model's description goes on the BigQuery model (ALTER
    MODEL), as table comments do for tables (D79).
#}
{% materialization bqml_model, adapter='bigquery' -%}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set ml_options = config.get('ml_options', {}) -%}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main') -%}
        create or replace model {{ target_relation }}
        options(
            {%- for name, value in ml_options.items() %}
            {{ name }} = {{ bqml_option_value(value) }}{{ ',' if not loop.last }}
            {%- endfor %}
        )
        as (
            {{ sql }}
        )
    {%- endcall %}

    {%- if config.persist_relation_docs() and model.description %}
        {% call statement('describe_model') -%}
            alter model {{ target_relation }} set options(description = {{ bqml_option_value(model.description) }})
        {%- endcall %}
    {%- endif %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}

{# An OPTIONS value as BigQuery SQL: strings quoted, lists as arrays #}
{% macro bqml_option_value(value) -%}
    {%- if value is string -%}
        {{ value | tojson }}
    {%- elif value is sameas true or value is sameas false -%}
        {{ value | string | lower }}
    {%- elif value is iterable -%}
        [{% for item in value %}{{ bqml_option_value(item) }}{{ ', ' if not loop.last }}{% endfor %}]
    {%- else -%}
        {{ value }}
    {%- endif -%}
{%- endmacro %}
