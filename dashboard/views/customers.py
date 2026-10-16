import altair as alt
import numpy as np
import streamlit as st

import charts
import kpis
from warehouse import LOST_STATUSES, period, period_filter, query, table

st.title("Customers & sellers")
lost = ", ".join(f"'{s}'" for s in LOST_STATUSES)
st.caption(
    "A customer is a person (`customer_unique_id`): Olist's `customer_id` changes "
    "with every order. Counts are within the selected period."
)

kpi = kpis.customers(period())

sellers_kpi = query(f"""
    select count(distinct seller_id) as sellers
    from {table('fct_order_items')}
    where {period_filter()}
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Customers", f"{kpi.customers:,.0f}", border=True)
c2.metric("Repeat customers", f"{kpi.repeat_rate:.1%}", border=True)
c3.metric("Avg spend per customer", f"R$ {kpi.avg_spend:,.0f}", border=True)
c4.metric("Active sellers", f"{sellers_kpi.sellers:,.0f}", border=True)

customer_states = query(f"""
    select
        c.state,
        count(distinct o.customer_unique_id) as customers,
        sum(case when o.order_status not in ({lost}) then o.order_amount end) as revenue
    from {table('fct_orders')} as o
    inner join {table('dim_customers')} as c on o.customer_unique_id = c.customer_unique_id
    where {period_filter('o.purchase_date')}
    group by 1
    order by customers desc
    limit 12
""")

sellers = query(f"""
    select
        s.seller_id,
        s.city,
        s.state,
        s.latitude,
        s.longitude,
        sum(i.item_amount) as revenue,
        count(distinct i.order_id) as orders
    from {table('fct_order_items')} as i
    inner join {table('dim_sellers')} as s on i.seller_id = s.seller_id
    where {period_filter('i.purchase_date')} and i.order_status not in ({lost})
    group by 1, 2, 3, 4, 5
""")

left, right = st.columns(2)
with left:
    st.subheader("Top 12 customer states")
    charts.bar_h(
        customer_states, "customers", "state", "Customers", ",.0f",
        extra_tooltip=[alt.Tooltip("revenue:Q", title="Revenue, R$", format=",.0f")],
    )
with right:
    st.subheader("Where sellers are")
    located = sellers.dropna(subset=["latitude", "longitude"]).copy()
    # Dot area ~ revenue (radius in metres)
    located["radius"] = 3000 + np.sqrt(located["revenue"]) * 60
    st.map(located, latitude="latitude", longitude="longitude", size="radius",
           color=charts.series_colors()[0] + "99", height=380)
    st.caption(f"{len(located):,} sellers with a known zip location; dot size ~ item revenue.")

st.subheader("Top 10 sellers by item revenue")
top = sellers.nlargest(10, "revenue")[["seller_id", "city", "state", "orders", "revenue"]]
st.dataframe(
    top,
    hide_index=True,
    width="stretch",
    column_config={
        "seller_id": st.column_config.TextColumn("Seller"),
        "city": "City",
        "state": "State",
        "orders": st.column_config.NumberColumn("Orders", format="%d"),
        "revenue": st.column_config.NumberColumn("Item revenue, R$", format="%.2f"),
    },
)
