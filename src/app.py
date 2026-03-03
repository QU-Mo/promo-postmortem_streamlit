import streamlit as st
import pandas as pd
import json
from datetime import date, timedelta
from google.cloud import bigquery
# Import the SQL builder and fetch function
from promo import build_promo_sql, fetch_promo_data

# Set config FIRST before any Streamlit call
st.set_page_config(page_title="Python Project 2025", layout="wide")

# Initialize session state
def initialize_session_state():
    if "data" not in st.session_state:
        st.session_state["data"] = None

initialize_session_state()

# App Title
st.title("Python Project 2025")

# Sidebar configuration

st.sidebar.header("Configuration")
promotion_id = st.sidebar.text_input("promotion_id", value="34616")
channel = st.sidebar.text_input("channel", value="STATIONARY")
country = st.sidebar.text_input("country", value="DE")
company_name_short = st.sidebar.text_input("company_name_short", value="PUC")
date_range = st.sidebar.date_input("Select Date Range", [date(2024, 12, 1), date(2026, 1, 1)])



# Main application logic
if st.sidebar.button("Run"):
    st.write(f"Running BigQuery for {country} from {date_range[0]} to {date_range[1]}...")
    # Create BigQuery client
    bq_client = bigquery.Client()
    try:
        df = fetch_promo_data(
            promotion_id=promotion_id,
            channel=channel,
            country=country,
            company_name_short=company_name_short,
            start_date=date_range[0],
            end_date=date_range[1],
            bq_client=bq_client
        )
        st.session_state["data"] = df
        # Also show the SQL for transparency
        sql, _ = build_promo_sql(
            promotion_id=promotion_id,
            channel=channel,
            country=country,
            company_name_short=company_name_short,
            start_date=date_range[0],
            end_date=date_range[1]
        )
        st.session_state["sql"] = sql
    except Exception as e:
        st.session_state["data"] = None
        st.session_state["sql"] = None
        st.error(f"Error running query: {e}")

# Display results if available


if st.session_state.get("data") is not None:
    st.subheader("Query Results")
    st.dataframe(st.session_state["data"])