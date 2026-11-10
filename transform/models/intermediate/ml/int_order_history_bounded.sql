-- Grain: one row, the time window the BigQuery ML models learn from (D96).
-- The dump starts sparse (2016) and ends with a stub (Sep-Oct 2018: 20 orders
-- after ~6,500 a month), so neither end of the data is a usable series end. The
-- history runs from 2017-01-01 (as the dashboard's default period) to the last
-- complete month: the latest month with at least 10 % of the orders of the
-- busiest month. A first order has a known repeat label once 180 days of
-- history follow it.
with orders as (
    select purchase_date
    from {{ ref('fct_orders') }}
    where order_status not in ('canceled', 'unavailable')
),

months as (
    select
        date_trunc(purchase_date, month) as month_start,
        count(*) as orders_count
    from orders
    group by 1
),

complete_months as (
    select month_start
    from months
    where orders_count >= 0.1 * (select max(orders_count) from months)
),

final as (
    select
        date '2017-01-01' as history_start_date,
        last_day(max(month_start), month) as history_end_date,
        date_sub(last_day(max(month_start), month), interval 180 day) as labelled_until_date
    from complete_months
)

select * from final
