-- Every year from the first order year to next year has its holidays in raw. The
-- loader brings next year in the first run of a year; a missing year means the feed
-- was down then and is_holiday is silently false for all its days.
with expected as (
    select distinct extract(year from date_day) as holiday_year
    from {{ ref('dim_date') }}
    where extract(year from date_day) between 2016 and extract(year from current_date) + 1
),

loaded as (
    select
        holiday_year,
        count(*) as holidays
    from {{ ref('stg_brasilapi__holidays') }}
    group by holiday_year
)

select
    expected.holiday_year,
    coalesce(loaded.holidays, 0) as holidays
from expected
left join loaded on expected.holiday_year = loaded.holiday_year
where coalesce(loaded.holidays, 0) < 8
