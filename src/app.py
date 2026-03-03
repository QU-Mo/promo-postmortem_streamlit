import streamlit as st
from datetime import date
from google.cloud import bigquery


from raw_data import build_raw_data_sql, fetch_raw_data



st.set_page_config(page_title="Promo Post-Mortem", layout="wide")


def initialize_session_state() -> None:
    if "data" not in st.session_state:
        st.session_state["data"] = None
    if "sql" not in st.session_state:
        st.session_state["sql"] = None
    if "selected_dates" not in st.session_state:
        st.session_state["selected_dates"] = []

initialize_session_state()

st.title("Raw Data Query")
st.sidebar.header("Configuration")

# Sidebar configuration

st.sidebar.header("Configuration")
traffic_business_unit = st.sidebar.text_input("traffic_business_unit", value="PUC DE")
traffic_country = st.sidebar.text_input("traffic_country", value="DE")
order_company_name_short = st.sidebar.text_input("order_company_name_short", value="PUC")
order_channel = st.sidebar.text_input("order_channel", value="STATIONARY")
order_country = st.sidebar.text_input("order_country", value="DE")


picked_date = st.sidebar.date_input(
    "Calendar: pick one date",
    value=date.today(),
    key="raw_picker",
)

if st.sidebar.button("Add picked date"):
    if picked_date not in st.session_state["selected_dates"]:
        st.session_state["selected_dates"] = sorted(
            st.session_state["selected_dates"] + [picked_date]
        )

selected_dates = st.sidebar.multiselect(
    "IN dates used in SQL",
    options=st.session_state["selected_dates"],
    default=st.session_state["selected_dates"],
    key="raw_selected_dates",
)
st.session_state["selected_dates"] = selected_dates

if st.sidebar.button("Clear selected dates"):
    st.session_state["selected_dates"] = []
    st.rerun()



if st.sidebar.button("Run"):
    if not st.session_state["selected_dates"]:
        st.warning("Please select at least one date for IN filtering.")
        st.stop()
   
    bq_client = bigquery.Client()
    try:
        st.write(
            f"Running Raw Data BigQuery for {traffic_country}/{order_country} on {len(st.session_state['selected_dates'])} selected day(s)..."
        
        )
        df = fetch_raw_data(
            traffic_business_unit=traffic_business_unit,
            traffic_country=traffic_country,
            order_company_name_short=order_company_name_short,
            order_channel=order_channel,
            order_country=order_country,
            selected_dates=st.session_state["selected_dates"],
            bq_client=bq_client,
        )
        sql, _ = build_raw_data_sql(
            traffic_business_unit=traffic_business_unit,
            traffic_country=traffic_country,
            order_company_name_short=order_company_name_short,
            order_channel=order_channel,
            order_country=order_country,
            selected_dates=st.session_state["selected_dates"],
        )

        st.session_state["data"] = df
        st.session_state["sql"] = sql
    except Exception as e:
        st.session_state["data"] = None
        st.session_state["sql"] = None
        st.error(f"Error running query: {e}")



if st.session_state.get("data") is not None:
    st.subheader("Query Results")
    st.dataframe(st.session_state["data"], use_container_width=True)

if st.session_state.get("sql"):
    with st.expander("Generated SQL"):
        st.code(st.session_state["sql"], language="sql")