{#
    The comment dbt puts on every query (`query-comment` in dbt_project.yml). On
    BigQuery `job-label: true` turns its JSON keys into job labels, which the cost
    mart reads back from INFORMATION_SCHEMA (fct_dbt_job_costs, D71). dbt's default
    comment carries `node_id` (`model.olist.fct_orders`), which the label rules
    (lowercase, [a-z0-9_-], 63 characters) turn into `model_olist_fct_orders`:
    type, package and name run together. Separate keys keep them apart. Run-level
    queries (hooks, introspection) have no node: only `dbt_target`.
    `dbt_command` (build, run-operation, generate for `docs generate`, ...) tells
    apart what a run-level query belongs to (#147).
    dbt-bigquery adds `dbt_invocation_id` to every job by itself.
    In GitHub Actions `github_run` (run id, attempt) ties the jobs of every dbt
    command in one workflow run together: the PR cost comment sums them (D72).
    `CI_COST_SCOPE` appends a suffix, so a job with its own cost report (the
    BigQuery deletion job, D105) stays out of that sum.
#}
{% macro query_comment(node) -%}
    {%- set labels = {'app': 'dbt', 'dbt_target': target.name, 'dbt_command': flags.WHICH} -%}
    {%- if env_var('GITHUB_RUN_ID', '') -%}
        {%- do labels.update({
            'github_run': env_var('GITHUB_RUN_ID') ~ '-' ~ env_var('GITHUB_RUN_ATTEMPT', '1')
                ~ ('-' ~ env_var('CI_COST_SCOPE') if env_var('CI_COST_SCOPE', '') else ''),
        }) -%}
    {%- endif -%}
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
