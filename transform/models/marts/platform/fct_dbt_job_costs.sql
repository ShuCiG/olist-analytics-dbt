-- Grain: one row per BigQuery job that dbt ran (every target: prod, airflow, ci).
-- dbt labels its jobs from the query comment (macros/query_comment.sql); jobs
-- without `dbt_target` (dlt, the dashboard, edr's own dbt project, dbt jobs from
-- before the labels) are left out.
-- Top-level jobs only: a multi-statement script (e.g. an incremental merge) is a
-- parent job whose bytes and slot time already sum up its child jobs.
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

with jobs as (
    select * from {{ ref('stg_bigquery__jobs') }}
    where
        dbt_target is not null
        and parent_job_id is null
        and state = 'DONE'
    {% if is_incremental() %}
        and created_at >= timestamp_sub({{ incremental_cursor('created_at') }}, interval 1 day)
    {% endif %}
),

final as (
    select
        job_id,
        dbt_invocation_id,
        dbt_target,
        coalesce(dbt_resource_type, 'run') as dbt_resource_type,
        dbt_package,
        dbt_node,
        principal,
        statement_type,
        created_at,
        date(created_at) as created_date,
        timestamp_diff(ended_at, started_at, millisecond) as duration_ms,
        is_cache_hit,
        is_failed,
        coalesce(bytes_processed, 0) as bytes_processed,
        coalesce(bytes_billed, 0) as bytes_billed,
        coalesce(slot_ms, 0) as slot_ms
    from jobs
)

select * from final
