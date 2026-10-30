import altair as alt
import streamlit as st

import charts
import kpis
from warehouse import period, query, table

st.title("Cohorts & retention")
st.caption(
    "A cohort = the people whose first order falls in a month; shown: cohorts acquired "
    "in the selected period. Coming back = any order in a later calendar month. "
    "A cohort too young for a month is left out of that month."
)

kpi = kpis.cohorts(period())

c1, c2, c3, c4 = st.columns(4)
c1.metric("New customers", f"{kpi.new_customers:,.0f}", border=True)
c2.metric("Back in month 1", f"{kpi.retention_m1:.2%}", border=True)
c3.metric("Back in month 3", f"{kpi.retention_m3:.2%}", border=True)
c4.metric("3-month value per customer", f"R$ {kpi.ltv_3m:,.0f}", border=True)

MAX_MONTHS = 12
cohorts = query(f"""
    select
        cohort_month,
        months_since_first_order,
        cohort_size,
        active_customers,
        retention_rate,
        cumulative_revenue,
        cumulative_revenue_per_customer
    from {table('fct_customer_cohorts')}
    where {kpis.cohort_filter(period())} and months_since_first_order <= {MAX_MONTHS}
    order by cohort_month, months_since_first_order
""")

# Over all the picked cohorts, each weighted by its size
curves = query(f"""
    select
        months_since_first_order,
        sum(active_customers) / sum(cohort_size) as retention_rate,
        sum(cumulative_revenue) / sum(cohort_size) as value_per_customer
    from {table('fct_customer_cohorts')}
    where {kpis.cohort_filter(period())} and months_since_first_order <= {MAX_MONTHS}
    group by months_since_first_order
    order by months_since_first_order
""")

st.subheader(f"Share who ordered again, by cohort and month (1–{MAX_MONTHS})")
# Month 0 is 100 % by definition: left out so the scale shows the returns
charts.heatmap(
    cohorts.query("months_since_first_order > 0"),
    "months_since_first_order", "cohort_month", "retention_rate", "Ordered again", ".2%",
    extra_tooltip=[
        alt.Tooltip("active_customers:Q", title="People", format=",.0f"),
        alt.Tooltip("cohort_size:Q", title="Cohort size", format=",.0f"),
    ],
)

left, right = st.columns(2)
with left:
    st.subheader("Retention curve")
    charts.curve(curves.query("months_since_first_order > 0"), "months_since_first_order",
                 "retention_rate", "Ordered again", ".2%")
with right:
    st.subheader("Value per acquired customer")
    charts.curve(curves, "months_since_first_order", "value_per_customer", "Revenue to date, R$", ",.0f")

with st.expander("Table"):
    st.dataframe(
        cohorts,
        hide_index=True,
        width="stretch",
        column_config={
            "cohort_month": st.column_config.DateColumn("Cohort", format="MMM YYYY"),
            "months_since_first_order": st.column_config.NumberColumn("Month", format="%d"),
            "cohort_size": st.column_config.NumberColumn("Cohort size", format="%d"),
            "active_customers": st.column_config.NumberColumn("Ordered that month", format="%d"),
            "retention_rate": st.column_config.NumberColumn("Share", format="percent"),
            "cumulative_revenue": st.column_config.NumberColumn("Revenue to date, R$", format="%.2f"),
            "cumulative_revenue_per_customer": st.column_config.NumberColumn(
                "Per customer, R$", format="%.2f"
            ),
        },
    )
