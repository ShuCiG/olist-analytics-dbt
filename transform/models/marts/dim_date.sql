-- Far past the dump (2016-2018): the generated feed (D112) adds a month of orders a day
{{ dbt_date.get_date_dimension('2016-01-01', '2099-12-31') }}
