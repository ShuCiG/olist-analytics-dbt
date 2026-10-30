"""KPI tiles of the pages, one query each, for a purchase-date period.

Kept out of the page scripts so a test can run the same SQL: these numbers must
equal the dbt Semantic Layer metrics (transform/models/marts/_marts__semantic_models.yml),
checked by dashboard/tests/test_metrics.py against `mf query`.
"""

import datetime as dt

import pandas as pd

from warehouse import LOST_STATUSES, date_filter, query, table

Period = tuple[dt.date, dt.date]

_LOST = ", ".join(f"'{s}'" for s in LOST_STATUSES)


def sales(period: Period) -> pd.Series:
    """orders, customers, revenue, avg_order_value (metrics: orders, customers,
    revenue, average_order_value)."""
    kpi = query(f"""
        select
            count(*) as orders,
            count(distinct customer_unique_id) as customers,
            sum(case when order_status not in ({_LOST}) then order_amount end) as revenue,
            sum(case when order_status not in ({_LOST}) then 1 else 0 end) as sold_orders
        from {table('fct_orders')}
        where {date_filter(period)}
    """).iloc[0]
    kpi["avg_order_value"] = kpi.revenue / max(kpi.sold_orders, 1)
    return kpi


def delivery(period: Period) -> pd.Series:
    """Delivered orders only: orders, avg_days, late_rate, avg_score (metrics:
    delivered_orders, avg_days_to_deliver, late_delivery_rate, avg_review_score)."""
    return query(f"""
        select
            count(*) as orders,
            avg(days_to_deliver) as avg_days,
            avg(case when is_delivered_late then 1.0 else 0.0 end) as late_rate,
            avg(review_score) as avg_score
        from {table('fct_orders')}
        where is_delivered and {date_filter(period)}
    """).iloc[0]


def customers(period: Period) -> pd.Series:
    """People who ordered in the period: customers, repeat_rate (more than one
    order in the period), avg_spend (metrics: customers, revenue_per_customer;
    repeat_customer_rate over all the data)."""
    return query(f"""
        with per_customer as (
            select
                customer_unique_id,
                count(*) as orders,
                sum(case when order_status not in ({_LOST}) then order_amount else 0 end) as spend
            from {table('fct_orders')}
            where {date_filter(period)}
            group by 1
        )
        select
            count(*) as customers,
            avg(case when orders > 1 then 1.0 else 0.0 end) as repeat_rate,
            avg(spend) as avg_spend
        from per_customer
    """).iloc[0]


def cohort_filter(period: Period) -> str:
    """Cohorts whose first-order month overlaps the period (a cohort is a whole month)."""
    lo, hi = period
    return date_filter((lo.replace(day=1), hi), "cohort_month")


def cohorts(period: Period) -> pd.Series:
    """Cohorts acquired in the period: new_customers, retention_m1, retention_m3,
    ltv_3m (metrics: new_customers; cohort_retention_rate at months 1 and 3;
    ltv_3_months). A cohort too young for a month is left out of that month's rate."""
    return query(f"""
        select
            sum(case when months_since_first_order = 0 then cohort_size end) as new_customers,
            sum(case when months_since_first_order = 1 then active_customers end)
                / sum(case when months_since_first_order = 1 then cohort_size end) as retention_m1,
            sum(case when months_since_first_order = 3 then active_customers end)
                / sum(case when months_since_first_order = 3 then cohort_size end) as retention_m3,
            sum(case when months_since_first_order = 2 then cumulative_revenue end)
                / sum(case when months_since_first_order = 2 then cohort_size end) as ltv_3m
        from {table('fct_customer_cohorts')}
        where {cohort_filter(period)}
    """).iloc[0]
