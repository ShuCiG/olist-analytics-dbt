{#
    Deletion requests (GDPR erasure, D105): remove a person from every layer, verified.

    A request is a row (customer_unique_id, requested_at) in the table named by env
    OLIST_DELETION_REQUESTS (`schema.table`, default `pii_keys.deletion_requests`; on
    BigQuery a dataset only the accounts that build and the owner can read; the
    owner's first request_deletion makes the table).

    A request is applied once per warehouse. Each raw schema has a log,
    `<raw>.deletion_log` (applied_at, requests_until, people, rows_deleted; no ids):
    a request newer than the log's last applied `requests_until` is pending there.
    The loader leaves the people of the applied requests out of every later load
    (ingestion/pipeline.py), so a deletion survives the next `replace` of customers
    and the next order month. A pending request's person still loads: a load that
    dropped their customer rows before apply ran would leave their orders with no
    way to find them.

    The pipelines do it themselves (#296): the prod deploy and both Airflow DAGs run
    apply_deletion_requests before every load (one small query when nothing is
    pending), and after an apply rebuild everything and run the check. By hand
    (docs/RUNNING.md, "Deletion requests"):
      dbt run-operation request_deletion --args "{customer_unique_id: <id>}"
      dbt run-operation apply_deletion_requests
      dbt build --select source:olist+ --exclude config.materialized:bqml_model
      dbt run-operation check_deletion_requests

    What goes: the person's customer rows, their orders and the orders' items,
    payments and reviews, in raw (and dlt's merge staging), and so in every relation
    built from raw. Tables and views follow on the rebuild; incremental models and
    snapshots keep old rows through any rebuild, so apply_deletion_requests deletes
    from them directly. BigQuery ML models keep their weights until the next
    retrain: aggregates, no rows.

    run-operation commits nothing by itself, so the macros that write commit at the
    end (deletion_commit).

    No id appears in the SQL of apply or check: they read the request table in a
    subquery, so the query history holds no id either (only request_deletion's insert).
#}

{% macro deletion_requests_relation() %}
    {% set name = env_var('OLIST_DELETION_REQUESTS', 'pii_keys.deletion_requests') %}
    {% if not modules.re.fullmatch('[A-Za-z0-9_]+\.[A-Za-z0-9_]+', name) %}
        {{ exceptions.raise_compiler_error("OLIST_DELETION_REQUESTS must be schema.table, got '" ~ name ~ "'") }}
    {% endif %}
    {% set schema, identifier = name.split('.') %}
    {{ return(api.Relation.create(database=target.database, schema=schema, identifier=identifier, type='table')) }}
{% endmacro %}


{% macro deletion_requests_exist(requests) %}
    {{ return(adapter.get_relation(database=requests.database, schema=requests.schema, identifier=requests.identifier) is not none) }}
{% endmacro %}


{# The log of applied requests of this target's raw schema (see the header) #}
{% macro deletion_log_relation() %}
    {% set customers = source('olist', 'customers') %}
    {{ return(api.Relation.create(database=customers.database, schema=customers.schema, identifier='deletion_log', type='table')) }}
{% endmacro %}


{% macro relation_exists(relation) %}
    {{ return(adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) is not none) }}
{% endmacro %}


{# `requested_at <= the last applied requests_until`: the requests this raw schema has applied #}
{% macro deletion_applied_filter(log) %}
    {% if not relation_exists(log) %}
        {{ return('1 = 0') }}
    {% endif %}
    {{ return('requested_at <= (select max(requests_until) from ' ~ log ~ ' where applied_at is not null)') }}
{% endmacro %}


{# People with a request this raw schema has not applied yet #}
{% macro deletion_requests_pending(requests, log) %}
    {% set sql = 'select count(distinct customer_unique_id) from ' ~ requests %}
    {% if relation_exists(log) %}
        {# coalesce on the comparison, not on the timestamp: no date literal across engines #}
        {% set sql = sql ~ ' where coalesce(requested_at > (select max(requests_until) from ' ~ log ~ ' where applied_at is not null), true)' %}
    {% endif %}
    {{ return(run_query(sql).columns[0].values()[0]) }}
{% endmacro %}


{#
    The person-linked values of the requests, as subqueries over raw (before it is
    cleaned). `where`: which requests (default: all).
#}
{% macro deletion_subqueries(requests, where=none) %}
    {% set customers = source('olist', 'customers') %}
    {% set orders = source('olist', 'orders') %}
    {% set people = 'select customer_unique_id from ' ~ requests ~ (' where ' ~ where if where else '') %}
    {% set customer_ids = 'select customer_id from ' ~ customers ~ ' where customer_unique_id in (' ~ people ~ ')' %}
    {{ return({
        'people': people,
        'keys': 'select ' ~ customer_key('customer_unique_id') ~ ' from ' ~ requests ~ (' where ' ~ where if where else ''),
        'customer_ids': customer_ids,
        'order_ids': 'select order_id from ' ~ orders ~ ' where customer_id in (' ~ customer_ids ~ ')',
    }) }}
{% endmacro %}


{#
    The relations of a schema, none if it doesn't exist. BigQuery lists them through
    its API (free; adapter method, as drop_isolated_datasets uses it); the others
    through dbt's list_relations_without_caching macro, which returns rows.
#}
{% macro schema_relations(database, schema) %}
    {% set schema_relation = api.Relation.create(database=database, schema=schema) %}
    {% if target.type == 'bigquery' %}
        {{ return(adapter.list_relations_without_caching(schema_relation)) }}
    {% endif %}
    {% if not adapter.check_schema_exists(database, schema) %}
        {{ return([]) }}
    {% endif %}
    {% set relations = [] %}
    {% for row in list_relations_without_caching(schema_relation) %}
        {% do relations.append(api.Relation.create(database=row['database'], schema=row['schema'], identifier=row['name'], type=row['type'])) %}
    {% endfor %}
    {{ return(relations) }}
{% endmacro %}


{% macro same_table(a, b) %}
    {{ return(a.schema | lower == b.schema | lower and a.identifier | lower == b.identifier | lower) }}
{% endmacro %}


{# Lower-case column names of a relation #}
{% macro column_names(relation) %}
    {{ return(adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list) }}
{% endmacro %}


{#
    run-operation commits nothing by itself: on DuckDB (and Snowflake) the deletes
    sit in the transaction dbt's metadata queries opened, rolled back on close.
    BigQuery and Databricks have no open transaction (each statement commits).
#}
{% macro deletion_commit() %}
    {{ return(adapter.dispatch('deletion_commit', 'olist')()) }}
{% endmacro %}

{% macro default__deletion_commit() %}
    {% do run_query('commit') %}
{% endmacro %}

{% macro bigquery__deletion_commit() %}
{% endmacro %}

{% macro databricks__deletion_commit() %}
{% endmacro %}


{# Rows a statement changed: the adapter's count (BigQuery), else the count DuckDB returns as a row #}
{% macro execute_counting(sql) %}
    {% set response, table = adapter.execute(sql, fetch=True) %}
    {% if response.rows_affected is not none and response.rows_affected >= 0 %}
        {{ return(response.rows_affected) }}
    {% elif table.rows | length %}
        {{ return(table.rows[0][0]) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}


{#
    Adds one request. The id is checked before it goes into SQL. The table is made
    on the first request. A repeated request adds nothing.
    Usage: dbt run-operation request_deletion --args "{customer_unique_id: <id>}"
#}
{% macro request_deletion(customer_unique_id) %}
    {% set id = customer_unique_id | string %}
    {% if not modules.re.fullmatch('[A-Za-z0-9_-]{1,64}', id) %}
        {{ exceptions.raise_compiler_error("request_deletion: customer_unique_id must be 1-64 letters, digits, '_' or '-'") }}
    {% endif %}
    {% set requests = deletion_requests_relation() %}
    {% if not deletion_requests_exist(requests) %}
        {% do adapter.create_schema(requests) %}
        {% do run_query('create table ' ~ requests ~ ' (customer_unique_id ' ~ dbt.type_string() ~ ' not null, requested_at ' ~ dbt.type_timestamp() ~ ' not null)') %}
    {% endif %}
    {# A one-row FROM: BigQuery allows no WHERE without one #}
    {% set added = execute_counting(
        'insert into ' ~ requests ~ ' (customer_unique_id, requested_at) select '
        ~ "'" ~ id ~ "', " ~ dbt.current_timestamp()
        ~ ' from (select 1 as one) as one_row where not exists (select 1 from ' ~ requests ~ " where customer_unique_id = '" ~ id ~ "')"
    ) %}
    {% do deletion_commit() %}
    {% do log("Deletion request " ~ ("added" if added else "already there") ~ " in " ~ requests, info=true) %}
    {% do log("Pending until applied: the next deploy / Airflow run applies it, or dbt run-operation apply_deletion_requests (docs/RUNNING.md)", info=true) %}
{% endmacro %}


{#
    Applies the pending requests to this target: deletes their people from the
    relations a rebuild doesn't clean: the incremental models and snapshots of this
    project, dlt's merge staging (its data tables emptied, dlt refills them on the
    next merge; dlt's own `_dlt_*` tables kept) and raw. Downstream first, raw last,
    since the downstream deletes find the person's orders through raw.

    Nothing pending: one query, nothing else. Otherwise a log row is opened with the
    newest request's time (`applied_at` null), every delete reads the requests up to
    it, so a request added meanwhile waits for the next apply as a whole, and the
    row is closed at the end. A failed apply leaves it open (BigQuery, Databricks:
    every statement commits) or rolls everything back (DuckDB, Snowflake: one
    transaction, committed at the end): either way the requests stay pending, and
    the next apply drops an open row and starts over. A raw schema with no
    tables yet has nothing to delete and still gets the row, so its first load
    leaves the people out.

    Not while a load runs on the same raw schema: the pipelines call it before their
    load, in the same run. Then rebuild everything built from raw and check.
    The last line, DELETIONS_APPLIED=<people>, is read by the callers.
    Usage: dbt run-operation apply_deletion_requests
#}
{% macro apply_deletion_requests() %}
    {% set requests = deletion_requests_relation() %}
    {% if not deletion_requests_exist(requests) %}
        {% do log("No deletion request table " ~ requests ~ ": nothing to delete", info=true) %}
        {% do log("DELETIONS_APPLIED=0", info=true) %}
        {{ return(none) }}
    {% endif %}
    {% set customers = source('olist', 'customers') %}
    {# A CI build reads prod's raw unless it loaded its own: never delete from that one #}
    {% if target.name == 'ci' and not customers.schema.startswith(target.schema ~ '_') %}
        {{ exceptions.raise_compiler_error("apply_deletion_requests on target ci deletes only from its own raw (" ~ target.schema ~ "_raw), not '" ~ customers.schema ~ "': set DBT_RAW_SCHEMA") }}
    {% endif %}
    {% set applied_log = deletion_log_relation() %}
    {% set people = deletion_requests_pending(requests, applied_log) %}
    {% if people == 0 %}
        {% do log("No pending deletion request in " ~ requests ~ " for " ~ customers.schema, info=true) %}
        {% do log("DELETIONS_APPLIED=0", info=true) %}
        {{ return(none) }}
    {% endif %}

    {% if not relation_exists(applied_log) %}
        {% do adapter.create_schema(applied_log) %}
        {% do run_query('create table ' ~ applied_log ~ ' (applied_at ' ~ dbt.type_timestamp() ~ ', requests_until ' ~ dbt.type_timestamp() ~ ' not null, people ' ~ dbt.type_int() ~ ' not null, rows_deleted ' ~ dbt.type_int() ~ ')') %}
    {% endif %}
    {% do run_query('delete from ' ~ applied_log ~ ' where applied_at is null') %}
    {% do run_query('insert into ' ~ applied_log ~ ' (applied_at, requests_until, people, rows_deleted) select cast(null as ' ~ dbt.type_timestamp() ~ '), max(requested_at), ' ~ people ~ ', cast(null as ' ~ dbt.type_int() ~ ') from ' ~ requests) %}

    {% set q = deletion_subqueries(requests, 'requested_at <= (select max(requests_until) from ' ~ applied_log ~ ')') %}
    {% set conditions = {
        'customer_unique_id': 'customer_unique_id in (' ~ q.people ~ ')',
        'customer_key': 'customer_key in (' ~ q['keys'] ~ ')',
        'customer_id': 'customer_id in (' ~ q.customer_ids ~ ')',
        'order_id': 'order_id in (' ~ q.order_ids ~ ')',
    } %}
    {% set total = namespace(rows=0) %}
    {% set raw_loaded = relation_exists(customers) and relation_exists(source('olist', 'orders')) %}

    {% if raw_loaded %}
        {% for node in graph.nodes.values()
            if node.resource_type in ['model', 'snapshot']
            and node.package_name == project_name
            and node.config.materialized in ['incremental', 'snapshot'] %}
            {% set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) %}
            {% if relation is not none %}
                {% set columns = column_names(relation) %}
                {% set where = conditions.items() | selectattr(0, 'in', columns) | map(attribute=1) | list %}
                {% if where %}
                    {% set deleted = execute_counting('delete from ' ~ relation ~ ' where ' ~ where | join(' or ')) %}
                    {% set total.rows = total.rows + (deleted or 0) %}
                    {% do log("Deleted " ~ deleted ~ " rows from " ~ relation ~ " (" ~ node.config.materialized ~ ")", info=true) %}
                {% endif %}
            {% endif %}
        {% endfor %}

        {% for relation in schema_relations(customers.database, customers.schema ~ '_staging')
            if not (relation.identifier | lower).startswith('_dlt') %}
            {# Statement, not adapter.truncate_relation: dbt-bigquery doesn't implement it #}
            {% do run_query('truncate table ' ~ relation) %}
            {% do log("Emptied " ~ relation ~ " (dlt merge staging)", info=true) %}
        {% endfor %}

        {% for table in ['order_items', 'order_payments', 'order_reviews'] if relation_exists(source('olist', table)) %}
            {% set deleted = execute_counting('delete from ' ~ source('olist', table) ~ ' where ' ~ conditions.order_id) %}
            {% set total.rows = total.rows + (deleted or 0) %}
            {% do log("Deleted " ~ deleted ~ " rows from " ~ source('olist', table), info=true) %}
        {% endfor %}
        {% set deleted = execute_counting('delete from ' ~ source('olist', 'orders') ~ ' where ' ~ conditions.customer_id) %}
        {% set total.rows = total.rows + (deleted or 0) %}
        {% do log("Deleted " ~ deleted ~ " rows from " ~ source('olist', 'orders'), info=true) %}
        {% set deleted = execute_counting('delete from ' ~ customers ~ ' where ' ~ conditions.customer_unique_id) %}
        {% set total.rows = total.rows + (deleted or 0) %}
        {% do log("Deleted " ~ deleted ~ " rows from " ~ customers, info=true) %}
    {% else %}
        {% do log("No raw tables in " ~ customers.schema ~ " yet: nothing to delete, the first load leaves the people out", info=true) %}
    {% endif %}

    {% do run_query('update ' ~ applied_log ~ ' set applied_at = ' ~ dbt.current_timestamp() ~ ', rows_deleted = ' ~ total.rows ~ ' where applied_at is null') %}
    {% do deletion_commit() %}
    {% do log("Deletion requests applied: " ~ people ~ " people, " ~ total.rows ~ " rows deleted (" ~ applied_log ~ ")", info=true) %}
    {% do log("Next: dbt build --select source:olist+ --exclude config.materialized:bqml_model, then dbt run-operation check_deletion_requests", info=true) %}
    {% do log("DELETIONS_APPLIED=" ~ people, info=true) %}
{% endmacro %}


{#
    Fails when any table of the project still holds a requested person: every
    schema dbt builds into (Elementary's included), raw and dlt's merge staging.
    Views store nothing and are skipped. Per table, by column name:
    - customer_unique_id in the requests, customer_key = the requests' pseudonym (D94)
    - customer_id / order_id missing from raw customers / orders: raw holds no
      orphans (checked on the full dump: 0), so after the deletion any such value
      is a deleted person's, wherever it is copied (incremental, stale table).
    `applied_only` (the pipelines): only the people of the requests this raw schema
    has applied, so a request added after the run's apply doesn't fail it.
    Usage: dbt run-operation check_deletion_requests [--args "{applied_only: true}"]
#}
{% macro check_deletion_requests(applied_only=false) %}
    {% set requests = deletion_requests_relation() %}
    {% if not deletion_requests_exist(requests) %}
        {% do log("No deletion request table " ~ requests ~ ": nothing to check", info=true) %}
        {{ return(none) }}
    {% endif %}
    {% set customers = source('olist', 'customers') %}
    {% set orders = source('olist', 'orders') %}
    {% set applied_log = deletion_log_relation() %}
    {% set q = deletion_subqueries(requests, deletion_applied_filter(applied_log) if applied_only else none) %}

    {% set schemas = [] %}
    {% for node in graph.nodes.values() if node.resource_type in ['model', 'seed', 'snapshot'] %}
        {% do schemas.append((node.database, node.schema)) %}
    {% endfor %}
    {% do schemas.extend([(customers.database, customers.schema), (customers.database, customers.schema ~ '_staging')]) %}

    {% set found = [] %}
    {% set checked = [] %}
    {% for database, schema in schemas | unique | sort %}
        {% for relation in schema_relations(database, schema)
            if relation.type == 'table' and not same_table(relation, requests) and not same_table(relation, applied_log) %}
            {% set columns = column_names(relation) %}
            {% set where = [] %}
            {% if 'customer_unique_id' in columns %}
                {% do where.append('customer_unique_id in (' ~ q.people ~ ')') %}
            {% endif %}
            {% if 'customer_key' in columns %}
                {% do where.append('customer_key in (' ~ q['keys'] ~ ')') %}
            {% endif %}
            {% if 'customer_id' in columns and not same_table(relation, customers) %}
                {% do where.append('customer_id not in (select customer_id from ' ~ customers ~ ')') %}
            {% endif %}
            {% if 'order_id' in columns and not same_table(relation, orders) %}
                {% do where.append('order_id not in (select order_id from ' ~ orders ~ ')') %}
            {% endif %}
            {% if where %}
                {% set rows = run_query('select count(*) from ' ~ relation ~ ' where ' ~ where | join(' or ')).columns[0].values()[0] %}
                {% do checked.append(relation) %}
                {% if rows > 0 %}
                    {% do found.append(relation ~ ': ' ~ rows ~ ' rows') %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {% if found %}
        {{ exceptions.raise_compiler_error("Deletion requests not applied, the people are still in:\n  " ~ found | join('\n  ')) }}
    {% endif %}
    {% do log("Deletion requests checked: none of the people in " ~ checked | length ~ " tables", info=true) %}
{% endmacro %}
