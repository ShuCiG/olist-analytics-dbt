-- Grain: one row per BigQuery job that dbt ran (every target: prod, airflow, ci).
-- dbt labels its jobs from the query comment (macros/query_comment.sql); jobs
-- without `dbt_target` (dlt, the dashboard, edr's own dbt project, dbt jobs from
-- before the labels) are left out.
-- Top-level jobs only: a multi-statement script (e.g. an incremental merge) is a
-- parent job whose bytes and slot time already sum up its child jobs.
-- Run-level jobs (no node) get an origin: `elementary` when the job or one of its
-- statements touches a table of Elementary (its on-run-end uploads), else `dbt`
-- (introspection, hooks, the catalog of `docs generate`, run-operations).
-- Incremental: the cursor is the newest job already here, minus a day, so jobs
-- that were still running at the last build (no bytes yet) are picked up; the
-- merge on job_id replaces them. JOBS_BY_PROJECT keeps 180 days and older rows
-- live only here, so `--full-refresh` (a manual prod rebuild) leaves the table as
-- it is (full_refresh=false); dropping the table is the way to rebuild it.
{{
    config(
        materialized='incremental',
        full_refresh=false,
        unique_key='job_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        partition_by={'field': 'created_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['dbt_target', 'dbt_node'],
    )
}}

with labelled as (
    select * from {{ ref('stg_bigquery__jobs') }}
    where
        dbt_target is not null
    {% if is_incremental() %}
        and created_at >= timestamp_sub({{ incremental_cursor('created_at') }}, interval 1 day)
    {% endif %}
),

jobs as (
    select
        *,
        timestamp_diff(ended_at, started_at, millisecond) as duration_ms
    from labelled
    where parent_job_id is null and state = 'DONE'
),

-- Statements of a script job carry the script's labels
statements as (
    select
        parent_job_id as job_id,
        logical_or(touches_elementary) as touches_elementary
    from labelled
    where parent_job_id is not null
    group by parent_job_id
),

final as (
    select
        jobs.job_id,
        jobs.dbt_invocation_id,
        jobs.dbt_target,
        coalesce(jobs.dbt_resource_type, 'run') as dbt_resource_type,
        jobs.dbt_package,
        jobs.dbt_node,
        jobs.dbt_command,
        case
            when jobs.dbt_resource_type is not null then null
            when jobs.touches_elementary or coalesce(statements.touches_elementary, false) then 'elementary'
            else 'dbt'
        end as run_origin,
        jobs.principal,
        jobs.statement_type,
        jobs.created_at,
        date(jobs.created_at) as created_date,
        jobs.duration_ms,
        jobs.is_cache_hit,
        jobs.is_failed,
        coalesce(jobs.bytes_processed, 0) as bytes_processed,
        coalesce(jobs.bytes_billed, 0) as bytes_billed,
        coalesce(jobs.slot_ms, 0) as slot_ms
    from jobs
    left join statements on jobs.job_id = statements.job_id
)

select * from final
