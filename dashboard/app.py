"""Olist dashboard over the dbt marts. Run: uv run streamlit run dashboard/app.py"""

import streamlit as st

import warehouse

st.set_page_config(page_title="Olist analytics", page_icon=":bar_chart:", layout="wide")

pages = [
    st.Page("views/overview.py", title="Sales overview", icon=":material/payments:", default=True),
    st.Page("views/delivery.py", title="Delivery & reviews", icon=":material/local_shipping:"),
    st.Page("views/customers.py", title="Customers & sellers", icon=":material/storefront:"),
]
# The cost mart exists on BigQuery only (D71)
if warehouse.BACKEND == "bigquery":
    pages.append(st.Page("views/costs.py", title="dbt cost on BigQuery", icon=":material/savings:"))
page = st.navigation(pages)

lo, hi = warehouse.date_bounds()
default = warehouse.default_period(lo, hi)
loaded = warehouse.last_load()
with st.sidebar:
    picked = st.date_input(
        "Purchase date",
        value=default,
        min_value=lo,
        max_value=hi,
        format="YYYY-MM-DD",
        help=f"Data covers {lo} – {hi}. Default: full months only (2016 and Sep–Oct 2018 are sparse).",
    )
    # While the user is mid-pick the widget holds one date: keep the last full range
    if isinstance(picked, tuple) and len(picked) == 2:
        st.session_state["period"] = picked
    st.session_state.setdefault("period", default)
    st.caption(
        f"Data as of **{hi}** (latest order)"
        + (f", loaded {loaded} UTC" if loaded else "")
        + f". dbt marts on **{warehouse.source_label()}**, cached 1 h. "
        "Brazilian e-commerce orders by Olist, 2016–2018. "
        "[Code](https://github.com/ShuCiG/olist-analytics-dbt) · "
        "[dbt docs](https://shucig.github.io/olist-analytics-dbt/)"
    )

page.run()
