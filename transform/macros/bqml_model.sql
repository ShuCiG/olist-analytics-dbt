{#
    BigQuery ML model as a dbt node (D96): `create or replace model` from the
    model's SELECT, with the OPTIONS of `meta.ml_options`. Downstream models
    read it with `ML.FORECAST(MODEL {{ ref(...) }}, ...)` and friends, so training,
    prediction and evaluation sit in the DAG like any table. A BigQuery ML model
    is not a table: dbt can't test it, and the catalog of `docs generate` doesn't
    list it; the tests go on the tables that read it.
    persist_docs: the model's description goes on the BigQuery model (ALTER
    MODEL), as table comments do for tables (D79).
    Training is billed on what the SELECT scans, at 50 times a query's price and
    quota: a view or ephemeral input fails here, before anything is trained (D96).
#}
{% materialization bqml_model, adapter='bigquery' -%}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set ml_options = config.meta_get('ml_options', {}) -%}

    {%- for node_id in model.depends_on.nodes -%}
        {%- set parent = graph.nodes.get(node_id) -%}
        {%- if parent and parent.config.materialized in ['view', 'ephemeral'] -%}
            {{ exceptions.raise_compiler_error(
                model.name ~ " trains on " ~ parent.name ~ ", a " ~ parent.config.materialized
                ~ ": BigQuery ML would bill its whole scan per training. Materialize it as a table (D96)."
            ) }}
        {%- endif -%}
    {%- endfor %}

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
