{#
    Data diff of models built by a CI run against their prod tables (D73), with
    dbt-labs/audit_helper. Called by .github/scripts/data_diff.py, which picks the
    models (the marts this run built), their primary keys and both relations from
    the CI and prod manifests:
        dbt -q run-operation data_diff --target ci --args '{models: [{name, pk,
            ci: {database, schema, identifier}, prod: {...} or none}]}'
    Per model:
    - columns only in prod / only in CI, and columns whose type changed (metadata, no query);
    - rows by status over the shared columns, matched on the primary key
      (compare_and_classify_relation_rows): identical, modified, added (only in
      CI), removed (only in prod), nonunique_pk; plus up to 3 sample keys a status;
    - only when rows were modified: which shared columns differ
      (compare_which_relation_columns_differ), one more query.
    Prints the results as one JSON line (with -q, the only output on stdout).
#}
{% macro data_diff(models) %}
    {% set results = [] %}
    {% for model in models %}
        {% set result = {'name': model.name, 'pk': model.pk} %}
        {% set ci = adapter.get_relation(database=model.ci.database, schema=model.ci.schema, identifier=model.ci.identifier) %}
        {% set prod = adapter.get_relation(database=model.prod.database, schema=model.prod.schema, identifier=model.prod.identifier) if model.prod else none %}
        {% if ci is none %}
            {% do result.update({'status': 'not_built'}) %}
        {% elif prod is none %}
            {% do result.update({'status': 'new'}) %}
        {% else %}
            {% do result.update(_data_diff_relations(prod, ci, model.pk)) %}
        {% endif %}
        {% do results.append(result) %}
    {% endfor %}
    {{ print(tojson(results)) }}
{% endmacro %}

{% macro _data_diff_relations(prod, ci, pk) %}
    {% set prod_types, ci_types = {}, {} %}
    {% for column in adapter.get_columns_in_relation(prod) %}
        {% do prod_types.update({column.name | lower: column.data_type | lower}) %}
    {% endfor %}
    {% for column in adapter.get_columns_in_relation(ci) %}
        {% do ci_types.update({column.name | lower: column.data_type | lower}) %}
    {% endfor %}
    {% set shared = prod_types.keys() | select('in', ci_types) | list %}
    {% set retyped = [] %}
    {% for column in shared if prod_types[column] != ci_types[column] %}
        {% do retyped.append({'column': column, 'prod': prod_types[column], 'ci': ci_types[column]}) %}
    {% endfor %}
    {% set schema_diff = {
        'only_in_prod': prod_types.keys() | reject('in', ci_types) | list,
        'only_in_ci': ci_types.keys() | reject('in', prod_types) | list,
        'retyped': retyped,
    } %}
    {# Rows can't be matched without the key on both sides #}
    {% if pk | reject('in', shared) | list %}
        {{ return(dict(schema_diff, status='pk_not_shared')) }}
    {% endif %}

    {# Status and sample keys: audit_helper counts each status over all rows before it
       keeps sample_limit rows a status #}
    {% set key_parts = [] %}
    {% for column in pk %}
        {% do key_parts.append("coalesce(cast(" ~ column ~ " as " ~ dbt.type_string() ~ "), 'null')") %}
        {% if not loop.last %}{% do key_parts.append("'|'") %}{% endif %}
    {% endfor %}
    {% set rows_sql %}
        select
            dbt_audit_row_status as row_status,
            dbt_audit_num_rows_in_status as rows_in_status,
            {{ dbt.concat(key_parts) }} as sample_key
        from (
            {{ audit_helper.compare_and_classify_relation_rows(prod, ci, primary_key_columns=pk, columns=shared, sample_limit=3) }}
        ) as classified
    {% endset %}
    {% set rows, samples = {}, {} %}
    {% for row in run_query(rows_sql).rows %}
        {% do rows.update({row['row_status']: row['rows_in_status'] | int}) %}
        {% if row['row_status'] != 'identical' %}
            {% set keys = samples.setdefault(row['row_status'], []) %}
            {% if row['sample_key'] not in keys %}{% do keys.append(row['sample_key']) %}{% endif %}
        {% endif %}
    {% endfor %}

    {% set differing = [] %}
    {% if rows.get('modified', 0) > 0 %}
        {% set columns_sql = audit_helper.compare_which_relation_columns_differ(prod, ci, primary_key_columns=pk, columns=shared) %}
        {% for row in run_query(columns_sql).rows if row['has_difference'] %}
            {% do differing.append(row['column_name']) %}
        {% endfor %}
    {% endif %}

    {{ return(dict(schema_diff, status='compared', rows=rows, samples=samples, differing_columns=differing)) }}
{% endmacro %}

{# audit_helper's default count(distinct a, b) over (...) is no DuckDB syntax: its
   dense_rank version (Postgres, Databricks) instead. Found through `dispatch` in
   dbt_project.yml. #}
{% macro duckdb___count_num_rows_in_status() %}
    {{ audit_helper._count_num_rows_in_status_without_distinct_window_func() }}
{% endmacro %}
