-- Far past the dump (2016-2018): the generated feed (D112) adds a month of orders a day.
-- The end date is exclusive: the last day is 2099-12-31.
{{ dbt_date.get_date_dimension('2016-01-01', '2100-01-01') }}
