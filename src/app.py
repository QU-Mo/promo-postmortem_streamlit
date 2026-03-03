import streamlit as st
from datetime import date, timedelta
from google.cloud import bigquery


from raw_data import build_raw_data_sql, build_group_period_tables, fetch_raw_data



st.set_page_config(page_title="Promo Post-Mortem", layout="wide")


def initialize_session_state() -> None:
    if "data" not in st.session_state:
        st.session_state["data"] = None
    if "sql" not in st.session_state:
        st.session_state["sql"] = None
    if "group_tables" not in st.session_state:
        st.session_state["group_tables"] = {}

def normalize_date_range(selected_range) -> list[date]:
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start, end = selected_range
    else:
        start = selected_range
        end = selected_range

    if start is None or end is None:
        return []

    if start > end:
        start, end = end, start

    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]

def build_store_options(country: str) -> dict[str, list[str]]:
    if country == "AT":
        return {
            "control_group_1": ["0422", "0413"],
            "control_group_2": ["0422", "04137"],
            "testing_group_1": ["0422", "04137"],
            "testing_group_2": ["0422", "04137"],
        }
    return {
        "control_group_1": ["0049", "0053"],
        "control_group_2": ["0049", "0053"],
        "testing_group_1": ["0049", "0053"],
        "testing_group_2": ["0049", "0053"],
    }


initialize_session_state()

st.title("Promo Post Mortem")

st.sidebar.header("Configuration")

traffic_business_unit = st.sidebar.selectbox(
    "traffic_business_unit",
    options=["PUC DE", "PUC AT"],
)
traffic_country = st.sidebar.selectbox("traffic_country", options=["DE", "AT"])

order_company_name_short = st.sidebar.text_input("order_company_name_short", value="PUC")
order_channel = st.sidebar.text_input("order_channel", value="STATIONARY")
order_country = st.sidebar.selectbox("order_country", options=["DE", "AT"])

store_options = build_store_options(traffic_country)

control_group_1 = st.sidebar.multiselect(
    "control group1 (store code)",
    options=store_options["control_group_1"],
    default=store_options["control_group_1"],
)

control_group_2 = st.sidebar.multiselect(
    "control group 2 (store code)",
    options=store_options["control_group_2"],
    default=store_options["control_group_2"],
)
testing_group_1 = st.sidebar.multiselect(
    "testing group 1 (store code)",
    options=store_options["testing_group_1"],
    default=store_options["testing_group_1"],
)
testing_group_2 = st.sidebar.multiselect(
    "testing group 2(store code)",
    options=store_options["testing_group_2"],
    default=store_options["testing_group_2"],
)

baseline_range = st.sidebar.date_input(
    "baseline period",
    value=(date.today() - timedelta(days=7), date.today() - timedelta(days=1)),
    key="baseline_period",
)
promo_range = st.sidebar.date_input(
    "promo period",
    value=(date.today(), date.today()),
    key="promo_period",
)

baseline_dates = normalize_date_range(baseline_range)
promo_dates = normalize_date_range(promo_range)
selected_dates = sorted(set(baseline_dates + promo_dates))


if st.sidebar.button("Run"):
    if not selected_dates:
        st.warning("Please select at least one date in baseline or promo period.")
        st.stop()

    if not any([control_group_1, control_group_2, testing_group_1, testing_group_2]):
        st.warning("Please select at least one store code in any group.")
        st.stop()
   
    bq_client = bigquery.Client()
    try:
        st.write(
             f"Running Promo Post Mortem BigQuery for {traffic_country}/{order_country} on {len(selected_dates)} selected day(s)..."
        
        )
        df = fetch_raw_data(
            traffic_business_unit=traffic_business_unit,
            traffic_country=traffic_country,
            order_company_name_short=order_company_name_short,
            order_channel=order_channel,
            order_country=order_country,
            selected_dates=selected_dates,
            bq_client=bq_client,
        )
        sql, _ = build_raw_data_sql(
            traffic_business_unit=traffic_business_unit,
            traffic_country=traffic_country,
            order_company_name_short=order_company_name_short,
            order_channel=order_channel,
            order_country=order_country,
            selected_dates=selected_dates,
        )

        st.session_state["data"] = df
        st.session_state["sql"] = sql
        st.session_state["group_tables"] = build_group_period_tables(
            raw_df=df,
            control_group_1=control_group_1,
            control_group_2=control_group_2,
            testing_group_1=testing_group_1,
            testing_group_2=testing_group_2,
            baseline_dates=baseline_dates,
            promo_dates=promo_dates,
        )
    except Exception as e:
        st.session_state["data"] = None
        st.session_state["sql"] = None
        st.session_state["group_tables"] = {}
        st.error(f"Error running query: {e}")



if st.session_state.get("group_tables"):
    st.subheader("Grouped Results")
    group_store_map = {
        "Control Group 1": control_group_1,
        "Control Group 2": control_group_2,
        "Testing Group 1": testing_group_1,
        "Testing Group 2": testing_group_2,
    }

    for table_name, table_df in st.session_state["group_tables"].items():
        matching_group = next(
            (group_name for group_name in group_store_map if table_name.startswith(group_name)),
            None,
        )
        selected_codes = group_store_map.get(matching_group, [])
        st.markdown(f"**{table_name}**  ")
        st.caption(f"Selected store code(s): {', '.join(selected_codes) if selected_codes else 'None'}")
        st.dataframe(table_df, use_container_width=True)

if st.session_state.get("sql"):
    with st.expander("Generated SQL"):
        st.code(st.session_state["sql"], language="sql")