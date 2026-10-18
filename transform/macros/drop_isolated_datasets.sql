{#
    Cleans up the datasets of an isolated BigQuery target: <target.schema>_<custom>,
    <target.schema> itself and <target.schema>_raw + _raw_staging (dlt dataset and
    its merge staging dataset; Airflow DAG only).
    - ci: per-PR datasets, called by CI after the build (always); the datasets are
      dropped.
    - airflow: the Airflow BigQuery DAG's datasets, run by hand to reset it; the
      datasets are emptied (every table and view dropped), not dropped: Terraform
      owns them and their access grants (infra/), so a reset leaves no drift.
    Guarded twice: refuses any other target (never prod / dev), and a dataset name
    outside the target's own namespace (ci: ci_*, airflow: airflow), so a prod name
    in DBT_CI_SCHEMA or profiles.yml can't turn the cleanup into a prod drop.
    Usage: dbt run-operation drop_isolated_datasets --target ci|airflow
#}
{% macro drop_isolated_datasets() %}
    {% set allowed = ['ci', 'airflow'] %}
    {% if target.name not in allowed %}
        {{ exceptions.raise_compiler_error("drop_isolated_datasets runs only on targets " ~ allowed ~ ", got '" ~ target.name ~ "'") }}
    {% endif %}
    {% set schema_ok = target.schema.startswith('ci_') if target.name == 'ci' else target.schema == 'airflow' %}
    {% if not schema_ok %}
        {{ exceptions.raise_compiler_error("drop_isolated_datasets: dataset '" ~ target.schema ~ "' is outside target '" ~ target.name ~ "' (ci: ci_*, airflow: airflow)") }}
    {% endif %}
    {% set suffixes = ['', '_raw', '_raw_staging', '_staging', '_intermediate', '_marts', '_seeds', '_snapshots', '_elementary'] %}
    {% for suffix in suffixes %}
        {% set relation = api.Relation.create(database=target.database, schema=target.schema ~ suffix) %}
        {% if target.name == 'airflow' %}
            {% set tables = adapter.list_relations_without_caching(relation) %}
            {% do log("Emptying " ~ relation.database ~ "." ~ relation.schema ~ " (" ~ tables | length ~ " relations)", info=true) %}
            {% for table in tables %}
                {% do adapter.drop_relation(table) %}
            {% endfor %}
        {% else %}
            {% do log("Dropping " ~ relation.database ~ "." ~ relation.schema, info=true) %}
            {% do adapter.drop_schema(relation) %}
        {% endif %}
    {% endfor %}
{% endmacro %}
