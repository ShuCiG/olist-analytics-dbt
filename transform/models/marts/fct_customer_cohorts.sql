-- Grain: one row per acquisition cohort (month of a person's first order) and
-- month since that first order, up to the latest month with orders. A spine of
-- calendar months keeps months nobody came back in as rows with zeros.
with customers as (
    select
        customer_unique_id,
        cast({{ dbt.date_trunc('month', 'first_order_at') }} as date) as cohort_month
    from {{ ref('dim_customers') }}
),

orders as (
    select
        customer_unique_id,
        order_status,
        paid_amount,
        cast({{ dbt.date_trunc('month', 'purchased_at') }} as date) as order_month
    from {{ ref('fct_orders') }}
),

last_month as (
    select max(order_month) as last_order_month
    from orders
),

cohort_sizes as (
    select
        cohort_month,
        count(*) as cohort_size
    from customers
    group by cohort_month
),

calendar_months as (
    select distinct cast(month_start_date as date) as calendar_month
    from {{ ref('dim_date') }}
),

spine as (
    select
        cohort_sizes.cohort_month,
        cohort_sizes.cohort_size,
        calendar_months.calendar_month as activity_month
    from cohort_sizes
    cross join last_month
    inner join calendar_months
        on
            cohort_sizes.cohort_month <= calendar_months.calendar_month
            and last_month.last_order_month >= calendar_months.calendar_month
),

-- Activity = any order (all statuses, as `orders_count` in dim_customers);
-- revenue = payments on orders not canceled/unavailable (as `lifetime_value`).
activity as (
    select
        customers.cohort_month,
        orders.order_month as activity_month,
        count(distinct orders.customer_unique_id) as active_customers,
        count(*) as orders_count,
        sum(
            case
                when orders.order_status not in ('canceled', 'unavailable')
                    then orders.paid_amount
                else 0
            end
        ) as revenue
    from orders
    inner join customers
        on orders.customer_unique_id = customers.customer_unique_id
    group by customers.cohort_month, orders.order_month
),

joined as (
    select
        spine.cohort_month,
        spine.activity_month,
        cast(
            {{ dbt.datediff('spine.cohort_month', 'spine.activity_month', 'month') }}
            as integer
        ) as months_since_first_order,
        spine.cohort_size,
        coalesce(activity.active_customers, 0) as active_customers,
        coalesce(activity.orders_count, 0) as orders_count,
        coalesce(activity.revenue, 0) as revenue
    from spine
    left join activity
        on
            spine.cohort_month = activity.cohort_month
            and spine.activity_month = activity.activity_month
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['cohort_month', 'months_since_first_order']) }}
            as cohort_month_sk,
        cohort_month,
        months_since_first_order,
        activity_month,
        cohort_size,
        active_customers,
        -- Integer / integer is a float on every engine here (dbt.type_float() would be
        -- a 4-byte FLOAT on DuckDB, not the contract's DOUBLE)
        active_customers / cohort_size as retention_rate,
        orders_count,
        {{ to_money('revenue') }} as revenue,
        {{ to_money('sum(revenue) over (
            partition by cohort_month
            order by months_since_first_order
            rows between unbounded preceding and current row
        )') }} as cumulative_revenue,
        {{ to_money('sum(revenue) over (
            partition by cohort_month
            order by months_since_first_order
            rows between unbounded preceding and current row
        ) / cohort_size') }} as cumulative_revenue_per_customer
    from joined
)

select * from final
