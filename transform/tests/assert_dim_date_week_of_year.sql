-- week_of_year on dates checked by hand against BigQuery EXTRACT(WEEK):
-- Sunday-start weeks, days before the year's first Sunday are week 0
with expected as (
    -- Saturday before the first Sunday, then the first Sunday
    select
        '2016-01-02' as date_day,
        0 as week_of_year
    union all
    select
        '2016-01-03' as date_day,
        1 as week_of_year
    -- the year starts on a Sunday
    union all
    select
        '2017-01-01' as date_day,
        1 as week_of_year
    union all
    select
        '2017-01-08' as date_day,
        2 as week_of_year
    -- the year starts on a Monday
    union all
    select
        '2018-01-01' as date_day,
        0 as week_of_year
    union all
    select
        '2018-01-07' as date_day,
        1 as week_of_year
    -- last day of the dimension (Monday)
    union all
    select
        '2019-12-30' as date_day,
        52 as week_of_year
)

select
    expected.date_day,
    expected.week_of_year as expected_week_of_year,
    dim_date.week_of_year
from expected
left join {{ ref('dim_date') }} as dim_date
    on cast(expected.date_day as date) = dim_date.date_day
where
    dim_date.week_of_year is null
    or dim_date.week_of_year != expected.week_of_year
