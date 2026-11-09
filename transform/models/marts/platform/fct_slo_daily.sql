-- Grain: one row per day and SLO (freshness, quality) with the good and the total
-- events of that day, from the run history Elementary keeps (D38). Service levels,
-- error budget and burn rate are computed from these counts by
-- .github/scripts/slo_report.py (targets: data-product/slo.yaml, D111).
-- The deploy availability SLO has no row here: a failed deploy leaves no trace in
-- Elementary, the report counts the workflow runs on main instead.
-- Plain `timestamp` (UTC) throughout: dbt's type_timestamp() is a DATETIME on BigQuery,
-- which cannot be compared with a TIMESTAMP, and Elementary's strings carry `+00:00`.
-- Timestamps come from `generated_at` (UTC, when dbt wrote the result), never from
-- `dbt_invocations.run_started_at`: that column runs on the as-of clock (D38).
-- Rebuilt in full on every deploy (the history is small); enabled on every target
-- although the platform folder is off by default: it reads only Elementary's tables.
{{ config(enabled=true) }}

with invocations as (
    select
        invocation_id,
        command,
        cast(generated_at as timestamp) as finished_at
    from {{ ref('elementary', 'dbt_invocations') }}
),

-- Quality: a run that tested is bad when any error-severity test failed or errored
-- (warn-severity tests never fail a run). Runs without test results (run, compile,
-- the job-cost build) are no events.
tested_runs as (
    select
        invocation_id,
        max(
            case
                when lower(severity) = 'error' and lower(status) in ('fail', 'error') then 1
                else 0
            end
        ) as has_failed_error_test
    from {{ ref('elementary', 'elementary_test_results') }}
    group by invocation_id
),

quality as (
    select
        cast(invocations.finished_at as date) as slo_date,
        'quality' as slo,
        sum(case when tested_runs.has_failed_error_test = 0 then 1 else 0 end) as good_events,
        count(*) as total_events
    from invocations
    inner join tested_runs on invocations.invocation_id = tested_runs.invocation_id
    where invocations.command in ('build', 'test')
    group by cast(invocations.finished_at as date)
),

-- Freshness: an event is a load that brought new orders (the newest `_loaded_at` of
-- the orders source went up between two `dbt source freshness` readings). It is good
-- when `fct_orders` was rebuilt within 24 h of the reading. A load younger than 24 h
-- without a rebuild yet is no event: judged only when its time is up, counted back
-- from the newest result in the history (not the wall clock, so the model is
-- deterministic and testable).
readings as (
    select
        cast(snapshotted_at as timestamp) as read_at,
        cast(max_loaded_at as timestamp) as loaded_at,
        lag(cast(max_loaded_at as timestamp)) over (
            partition by unique_id order by cast(snapshotted_at as timestamp)
        ) as previous_loaded_at
    from {{ ref('elementary', 'dbt_source_freshness_results') }}
    where unique_id like 'source.%.orders'
),

loads as (
    select read_at
    from readings
    where loaded_at > previous_loaded_at
),

builds as (
    select cast(generated_at as timestamp) as built_at
    from {{ ref('elementary', 'dbt_run_results') }}
    where
        unique_id like 'model.%.fct_orders'
        and status = 'success'
),

history_end as (
    select max(built_at) as last_build_at from builds
),

rebuilds as (
    select
        loads.read_at,
        {{ dbt.datediff('loads.read_at', 'builds.built_at', 'minute') }}
            as minutes_to_rebuild
    from loads
    inner join builds on loads.read_at <= builds.built_at
),

judged_loads as (
    select
        loads.read_at,
        max(case when rebuilds.minutes_to_rebuild <= 1440 then 1 else 0 end) as was_rebuilt,
        {{ dbt.datediff('loads.read_at', 'history_end.last_build_at', 'minute') }}
            as minutes_in_history
    from loads
    left join rebuilds on loads.read_at = rebuilds.read_at
    cross join history_end
    group by loads.read_at, history_end.last_build_at
),

freshness as (
    select
        cast(read_at as date) as slo_date,
        'freshness' as slo,
        sum(was_rebuilt) as good_events,
        count(*) as total_events
    from judged_loads
    where was_rebuilt = 1 or minutes_in_history >= 1440
    group by cast(read_at as date)
),

combined as (
    select * from quality
    union all
    select * from freshness
)

select
    slo_date,
    slo,
    cast(good_events as {{ dbt.type_bigint() }}) as good_events,
    cast(total_events as {{ dbt.type_bigint() }}) as total_events
from combined
