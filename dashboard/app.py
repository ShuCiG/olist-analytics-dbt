"""Olist dashboard over the dbt marts. Run: uv run streamlit run dashboard/app.py"""

import streamlit as st

import warehouse

st.set_page_config(page_title="Olist analytics", page_icon=":bar_chart:", layout="wide")

page = st.navigation(
    [
        st.Page("views/overview.py", title="Sales overview", icon=":material/payments:", default=True),
        st.Page("views/delivery.py", title="Delivery & reviews", icon=":material/local_shipping:"),
        st.Page("views/customers.py", title="Customers & sellers", icon=":material/storefront:"),
    ]
)

lo, hi = warehouse.date_bounds()
default = (max(lo, warehouse.DEFAULT_PERIOD[0]), min(hi, warehouse.DEFAULT_PERIOD[1]))
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
        f"Data: dbt marts on **{'BigQuery (prod)' if warehouse.BACKEND == 'bigquery' else 'DuckDB (local)'}**, "
        "cached 1 h. Brazilian e-commerce orders by Olist, 2016–2018. "
        "[Code](https://github.com/ShuCiG/olist-analytics-dbt) · "
        "[dbt docs](https://shucig.github.io/olist-analytics-dbt/)"
    )

page.run()
