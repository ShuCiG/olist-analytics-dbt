{#- BigQuery only (INFORMATION_SCHEMA, label arrays), enabled with the cost mart -#}
with source as (
    select * from {{ source('bigquery', 'jobs_by_project') }}
),

renamed as (
    select
        job_id,
        parent_job_id,
        creation_time as created_at,
        start_time as started_at,
        end_time as ended_at,
        user_email as principal,
        job_type,
        statement_type,
        state,
        error_result is not null as is_failed,
        coalesce(cache_hit, false) as is_cache_hit,
        total_bytes_processed as bytes_processed,
        total_bytes_billed as bytes_billed,
        total_slot_ms as slot_ms,
        -- Job labels (array of key/value pairs) as columns
        (
            select l.value from unnest(labels) as l
            where l.key = 'dbt_invocation_id'
        ) as dbt_invocation_id,
        (
            select l.value from unnest(labels) as l
            where l.key = 'dbt_target'
        ) as dbt_target,
        (
            select l.value from unnest(labels) as l
            where l.key = 'dbt_resource_type'
        ) as dbt_resource_type,
        (
            select l.value from unnest(labels) as l
            where l.key = 'dbt_package'
        ) as dbt_package,
        (
            select l.value from unnest(labels) as l
            where l.key = 'dbt_node'
        ) as dbt_node
    from source
)

select * from renamed
