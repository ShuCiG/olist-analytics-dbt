import altair as alt
import streamlit as st

import charts
from warehouse import LOST_STATUSES, monthly, period_filter, query, table

st.title("Sales overview")
lost = ", ".join(f"'{s}'" for s in LOST_STATUSES)
st.caption(
    "Revenue = items + freight of orders not canceled or unavailable "
    "(`fct_orders.order_amount`), in Brazilian reais."
)

kpi = query(f"""
    select
        count(*) as orders,
        count(distinct customer_unique_id) as customers,
        sum(case when order_status not in ({lost}) then order_amount end) as revenue,
        sum(case when order_status not in ({lost}) then 1 else 0 end) as sold_orders
    from {table('fct_orders')}
    where {period_filter()}
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders", f"{kpi.orders:,.0f}", border=True)
c2.metric("Customers", f"{kpi.customers:,.0f}", border=True)
c3.metric("Revenue", f"R$ {kpi.revenue / 1e6:,.2f}M", border=True)
c4.metric("Avg order value", f"R$ {kpi.revenue / max(kpi.sold_orders, 1):,.0f}", border=True)

trend = monthly(query(f"""
    select
        extract(year from purchase_date) as yr,
        extract(month from purchase_date) as mo,
        count(*) as orders,
        sum(case when order_status not in ({lost}) then order_amount end) as revenue
    from {table('fct_orders')}
    where {period_filter()}
    group by 1, 2
"""))

# Two measures, two charts: no dual axis
left, right = st.columns(2)
with left:
    st.subheader("Orders per month")
    charts.line(trend, "orders", "Orders", ",.0f")
with right:
    st.subheader("Revenue per month")
    charts.line(trend, "revenue", "Revenue, R$", ",.0f")

categories = query(f"""
    select
        coalesce(p.category_name, 'unknown') as category,
        sum(i.item_amount) as revenue,
        count(distinct i.order_id) as orders
    from {table('fct_order_items')} as i
    left join {table('dim_products')} as p on i.product_id = p.product_id
    where {period_filter('i.purchase_date')} and i.order_status not in ({lost})
    group by 1
    order by revenue desc
    limit 10
""")

payments = query(f"""
    select 'Credit card' as method, sum(credit_card_amount) as amount from {table('fct_orders')}
    where {period_filter()}
    union all
    select 'Boleto', sum(boleto_amount) from {table('fct_orders')} where {period_filter()}
    union all
    select 'Voucher', sum(voucher_amount) from {table('fct_orders')} where {period_filter()}
    union all
    select 'Debit card', sum(debit_card_amount) from {table('fct_orders')} where {period_filter()}
""")
payments["share"] = payments["amount"] / payments["amount"].sum()

left, right = st.columns([3, 2])
with left:
    st.subheader("Top 10 categories by item revenue")
    charts.bar_h(
        categories, "revenue", "category", "Item revenue, R$", ",.0f",
        extra_tooltip=[alt.Tooltip("orders:Q", title="Orders", format=",")],
    )
with right:
    st.subheader("Payment mix")
    charts.bar_h(
        payments, "share", "method", "Share of paid amount", ".0%",
        extra_tooltip=[alt.Tooltip("amount:Q", title="Amount, R$", format=",.0f")],
    )
    st.caption("Boleto: Brazilian bank slip, paid in cash or online banking.")
