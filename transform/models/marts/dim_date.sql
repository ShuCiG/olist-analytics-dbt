-- Far past the dump (2016-2018): the generated feed (D112) adds a month of orders a day.
-- The end date is exclusive: the last day is 2099-12-31.
-- Holidays (#348) exist for 2016 up to next year; later days are not holidays yet.
select
    calendar.*,
    holidays.holiday_date is not null as is_holiday,
    holidays.holiday_name
from (
    {{ dbt_date.get_date_dimension('2016-01-01', '2100-01-01') }}
) as calendar
left join {{ ref('stg_brasilapi__holidays') }} as holidays
    on calendar.date_day = holidays.holiday_date
