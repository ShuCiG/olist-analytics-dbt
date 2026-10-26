{#
    The comment dbt puts on every query (`query-comment` in dbt_project.yml). On
    BigQuery `job-label: true` turns its JSON keys into job labels, which the cost
    mart reads back from INFORMATION_SCHEMA (fct_dbt_job_costs, D71). dbt's default
    comment carries `node_id` (`model.olist.fct_orders.v2`), which the label rules
    (lowercase, [a-z0-9_-], 63 characters) turn into `model_olist_fct_orders_v2`:
    type, package and name run together. Separate keys keep them apart. Run-level
    queries (hooks, introspection) have no node: only `dbt_target`.
    dbt-bigquery adds `dbt_invocation_id` to every job by itself.
#}
{% macro query_comment(node) -%}
    {%- set labels = {'app': 'dbt', 'dbt_target': target.name} -%}
    {%- if node is not none -%}
        {#- Only models have versions; other nodes give an empty value, not none -#}
        {%- do labels.update({
            'dbt_resource_type': node.resource_type,
            'dbt_package': node.package_name,
            'dbt_node': node.name ~ ('_v' ~ node.version if node.version else ''),
        }) -%}
    {%- endif -%}
    {{- return(tojson(labels)) -}}
{%- endmacro %}
