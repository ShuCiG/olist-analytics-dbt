{#
    Drops the per-PR datasets a CI run created: <target.schema>_<custom>
    plus <target.schema> itself. Called by CI after the build (always).
    Guarded: refuses to run on any target except `ci`.
    Usage: dbt run-operation drop_ci_datasets --target ci
#}
{% macro drop_ci_datasets() %}
    {% if target.name != 'ci' %}
        {{ exceptions.raise_compiler_error("drop_ci_datasets runs only on target 'ci', got '" ~ target.name ~ "'") }}
    {% endif %}
    {% set suffixes = ['', '_staging', '_intermediate', '_marts', '_seeds', '_snapshots'] %}
    {% for suffix in suffixes %}
        {% set relation = api.Relation.create(database=target.database, schema=target.schema ~ suffix) %}
        {% do log("Dropping " ~ relation.database ~ "." ~ relation.schema, info=true) %}
        {% do adapter.drop_schema(relation) %}
    {% endfor %}
{% endmacro %}
