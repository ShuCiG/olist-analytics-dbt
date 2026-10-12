{#
    Drops the datasets of an isolated BigQuery target: <target.schema>_<custom>,
    <target.schema> itself and <target.schema>_raw (dlt, Airflow DAG only).
    - ci: per-PR datasets, called by CI after the build (always)
    - airflow: the Airflow BigQuery DAG's datasets, run by hand to reset it
    Guarded: refuses any other target (never prod / dev).
    Usage: dbt run-operation drop_isolated_datasets --target ci|airflow
#}
{% macro drop_isolated_datasets() %}
    {% set allowed = ['ci', 'airflow'] %}
    {% if target.name not in allowed %}
        {{ exceptions.raise_compiler_error("drop_isolated_datasets runs only on targets " ~ allowed ~ ", got '" ~ target.name ~ "'") }}
    {% endif %}
    {% set suffixes = ['', '_raw', '_staging', '_intermediate', '_marts', '_seeds', '_snapshots'] %}
    {% for suffix in suffixes %}
        {% set relation = api.Relation.create(database=target.database, schema=target.schema ~ suffix) %}
        {% do log("Dropping " ~ relation.database ~ "." ~ relation.schema, info=true) %}
        {% do adapter.drop_schema(relation) %}
    {% endfor %}
{% endmacro %}
