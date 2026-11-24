import altair as alt
import charts
import kpis
import streamlit as st
from warehouse import LOST_STATUSES, monthly, period, period_filter, query, table

st.title("Sales overview")
lost = ", ".join(f"'{s}'" for s in LOST_STATUSES)
st.caption(
    "Revenue = items + freight of orders not canceled or unavailable "
    "(`fct_orders.order_amount`), in Brazilian reais."
)

kpi = kpis.sales(period())

c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders", f"{kpi.orders:,.0f}", border=True)
c2.metric("Customers", f"{kpi.customers:,.0f}", border=True)
c3.metric("Revenue", f"R$ {kpi.revenue / 1e6:,.2f}M", border=True)
c4.metric("Avg order value", f"R$ {kpi.avg_order_value:,.0f}", border=True)

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

st.subheader("Revenue in US dollars")
usd = monthly(query(f"""
    select
        extract(year from x.purchase_date) as yr,
        extract(month from x.purchase_date) as mo,
        sum(case when o.order_status not in ({lost}) then x.order_amount_usd end) as revenue_usd,
        avg(x.brl_per_usd) as brl_per_usd
    from {table('fct_order_fx')} as x
    inner join {table('fct_orders')} as o on x.order_id = o.order_id
    where {period_filter('x.purchase_date')}
    group by 1, 2
"""))
left, right = st.columns(2)
with left:
    charts.line(usd, "revenue_usd", "Revenue, US$", ",.0f")
with right:
    charts.line(usd, "brl_per_usd", "BRL per USD (average of the month's orders)", ".2f")
st.caption(
    "Each order is converted at the rate of its purchase day (Banco Central do Brasil PTAX, `fct_order_fx`); "
    "weekends take the last business day's rate. The rate is real for generated months too. "
    "In dollars, revenue follows the real depreciation as well as sales."
)
