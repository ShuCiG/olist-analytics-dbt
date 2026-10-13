import altair as alt
import streamlit as st

import charts
from warehouse import monthly, period_filter, query, table

st.title("Delivery & reviews")
st.caption(
    "Delivered orders only. Late = delivered after the estimated date promised "
    "at purchase (`fct_orders.is_delivered_late`)."
)

delivered = f"is_delivered and {period_filter()}"

kpi = query(f"""
    select
        count(*) as orders,
        avg(days_to_deliver) as avg_days,
        avg(case when is_delivered_late then 1.0 else 0.0 end) as late_rate,
        avg(review_score) as avg_score
    from {table('fct_orders')}
    where {delivered}
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Delivered orders", f"{kpi.orders:,.0f}", border=True)
c2.metric("Avg days to deliver", f"{kpi.avg_days:.1f}", border=True)
c3.metric("Delivered late", f"{kpi.late_rate:.1%}", border=True)
c4.metric("Avg review score", f"{kpi.avg_score:.2f} / 5", border=True)

trend = monthly(query(f"""
    select
        extract(year from purchase_date) as yr,
        extract(month from purchase_date) as mo,
        avg(days_to_deliver) as avg_days,
        avg(case when is_delivered_late then 1.0 else 0.0 end) as late_rate
    from {table('fct_orders')}
    where {delivered}
    group by 1, 2
"""))

left, right = st.columns(2)
with left:
    st.subheader("Late deliveries by purchase month")
    charts.line(trend, "late_rate", "Delivered late", ".0%")
with right:
    st.subheader("Days to deliver by purchase month")
    charts.line(trend, "avg_days", "Avg days to deliver", ".1f")

scores = query(f"""
    select
        case when is_delivered_late then 'Late' else 'On time' end as delivery,
        review_score,
        count(*) as orders
    from {table('fct_orders')}
    where {delivered} and has_review
    group by 1, 2
""")
scores["share"] = scores["orders"] / scores.groupby("delivery")["orders"].transform("sum")
scores = scores.sort_values(["delivery", "review_score"], ascending=[False, True])

states = query(f"""
    select
        c.state,
        count(*) as orders,
        avg(case when o.is_delivered_late then 1.0 else 0.0 end) as late_rate,
        avg(o.days_to_deliver) as avg_days
    from {table('fct_orders')} as o
    inner join {table('dim_customers')} as c on o.customer_unique_id = c.customer_unique_id
    where o.is_delivered and {period_filter('o.purchase_date')}
    group by 1
    having count(*) >= 100
    order by late_rate desc
""")

left, right = st.columns(2)
with left:
    st.subheader("Review scores: late vs on time")
    charts.bar_grouped(scores, "review_score", "share", "delivery", "Share of reviewed orders", ".0%")
    late_low = scores.query("delivery == 'Late' and review_score <= 2")["share"].sum()
    on_time_low = scores.query("delivery == 'On time' and review_score <= 2")["share"].sum()
    st.caption(f"1–2 star reviews: **{late_low:.0%}** of late orders vs **{on_time_low:.0%}** of on-time ones.")
with right:
    st.subheader("Late rate by customer state")
    charts.bar_h(
        states, "late_rate", "state", "Delivered late", ".0%",
        extra_tooltip=[
            alt.Tooltip("orders:Q", title="Delivered orders", format=","),
            alt.Tooltip("avg_days:Q", title="Avg days to deliver", format=".1f"),
        ],
    )
    st.caption("States with at least 100 delivered orders in the period.")
