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
        -- Reads or writes a table of Elementary (datasets `elementary`,
        -- `airflow_elementary`, `ci_..._elementary`). Metadata views don't count: the
        -- catalog of `docs generate` reads `__TABLES__` and INFORMATION_SCHEMA of every
        -- dataset. Elementary marks its queries in the query text, but reading `query`
        -- bills ~70 MiB per day of jobs; these columns are small (#147)
        exists(
            select 1 from unnest(array_concat(referenced_tables, [destination_table])) as t
            where
                ends_with(t.dataset_id, 'elementary')
                and not starts_with(t.table_id, '__')
                and not starts_with(t.table_id, 'INFORMATION_SCHEMA')
        ) as touches_elementary,
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
            where l.key = 'dbt_command'
        ) as dbt_command,
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
