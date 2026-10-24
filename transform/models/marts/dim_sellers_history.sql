-- Grain: one row per seller version (SCD2 from snapshot snap_sellers).
-- The snapshot stamps versions with its own run time, not with the time the seller
-- changed, and it started long after the dump's orders were placed. So the range is
-- widened at both ends for point-in-time joins on business timestamps: a seller's
-- first version is valid from 1900-01-01, the open version until 9999-12-31.
-- The snapshot time itself is kept as recorded_at.
with versions as (
    select * from {{ ref('snap_sellers') }}
),

zip_codes as (
    select * from {{ ref('int_zip_codes_located') }}
),

numbered as (
    select
        dbt_scd_id as seller_version_id,
        seller_id,
        -- Cast: the contract's integer; row_number() is bigint on some adapters, int on others
        cast(
            row_number() over (partition by seller_id order by dbt_valid_from) as {{ dbt.type_int() }}
        ) as version_number,
        seller_zip_code_prefix as zip_code_prefix,
        seller_city as city,
        seller_state as state,
        dbt_valid_from as recorded_at,
        dbt_valid_to,
        coalesce(dbt_is_deleted = 'True', false) as is_deleted
    from versions
),

final as (
    select
        numbered.seller_version_id,
        numbered.seller_id,
        numbered.version_number,
        numbered.zip_code_prefix,
        numbered.city,
        numbered.state,
        zip_codes.latitude,
        zip_codes.longitude,
        case
            when numbered.version_number = 1 then cast('1900-01-01' as {{ dbt.type_timestamp() }})
            else numbered.recorded_at
        end as valid_from,
        coalesce(numbered.dbt_valid_to, cast('9999-12-31' as {{ dbt.type_timestamp() }})) as valid_to,
        numbered.dbt_valid_to is null as is_current,
        numbered.is_deleted,
        numbered.recorded_at
    from numbered
    left join zip_codes
        on numbered.zip_code_prefix = zip_codes.zip_code_prefix
)

select * from final
