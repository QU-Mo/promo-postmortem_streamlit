import streamlit as st
from datetime import date, timedelta
from google.cloud import bigquery
import pandas as pd


from raw_data import (
    build_group_period_tables,
    fetch_raw_data,
    fetch_store_code_options,
)


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


def build_store_options(store_codes: list[str]) -> dict[str, list[str]]:
    if not store_codes:
        fallback_codes = ["0049", "0053"]
        return {
            "control_group_1": fallback_codes,
            "control_group_2": fallback_codes,
            "testing_group_1": fallback_codes,
            "testing_group_2": fallback_codes,
        }

    return {
        "control_group_1": store_codes,
        "control_group_2": store_codes,
        "testing_group_1": store_codes,
        "testing_group_2": store_codes,
    }


@st.cache_data(show_spinner=False)
def get_store_codes(order_company_name_short: str, order_channel: str, order_country: str) -> list[str]:
    bq_client = bigquery.Client()
    return fetch_store_code_options(
        order_company_name_short=order_company_name_short,
        order_channel=order_channel,
        order_country=order_country,
        bq_client=bq_client,
    )


def build_promo_impact_table(
    funnel_tables: dict[str, pd.DataFrame],
    selected_control_group: str,
    selected_testing_group: str,
) -> pd.DataFrame:
    control_df = funnel_tables.get(selected_control_group)
    testing_df = funnel_tables.get(selected_testing_group)
    if control_df is None or testing_df is None:
        return pd.DataFrame()

    control_pct = control_df[["KPI", "% Diff (Promo vs Baseline)"]].rename(
        columns={"% Diff (Promo vs Baseline)": f"{selected_control_group} %Diff"}
    )
    testing_pct = testing_df[["KPI", "% Diff (Promo vs Baseline)"]].rename(
        columns={"% Diff (Promo vs Baseline)": f"{selected_testing_group} %Diff"}
    )

    merged = testing_pct.merge(control_pct, on="KPI", how="inner")
    merged["Promo Impact"] = (
        merged[f"{selected_testing_group} %Diff"] - merged[f"{selected_control_group} %Diff"]
    )
    return merged


initialize_session_state()


RATE_KPIS = {
    "avg store absorption rate",
    "cal store conversion rate",
    "margin",
    "RP revenue share",
    "promo revenue share",
    "Promo Impact",
}


def _format_kpi_value(kpi: str, value: float, is_pct_diff: bool = False) -> str:
    if pd.isna(value):
        return "-"
    if is_pct_diff or kpi in RATE_KPIS:
        return f"{value:.2%}"
    return f"{value:,.0f}"


def format_funnel_table(table_df: pd.DataFrame) -> pd.DataFrame:
    formatted_df = table_df.copy()
    for col in ["Baseline Period", "Promo Period", "Abs Diff (Promo - Baseline)"]:
        formatted_df[col] = formatted_df.apply(
            lambda row: _format_kpi_value(row["KPI"], row[col]),
            axis=1,
        )

    formatted_df["% Diff (Promo vs Baseline)"] = formatted_df.apply(
        lambda row: _format_kpi_value(
            row["KPI"],
            row["% Diff (Promo vs Baseline)"],
            is_pct_diff=True,
        ),
        axis=1,
    )
    return formatted_df


def format_promo_impact_table(table_df: pd.DataFrame) -> pd.DataFrame:
    formatted_df = table_df.copy()
    percent_cols = [col for col in formatted_df.columns if col.endswith("%Diff") or col == "Promo Impact"]
    for col in percent_cols:
        formatted_df[col] = formatted_df.apply(
            lambda row: _format_kpi_value("Promo Impact", row[col], is_pct_diff=True),
            axis=1,
        )
    return formatted_df


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
vat = st.sidebar.number_input("VAT", min_value=0.0, value=1.0, step=0.01, format="%.2f")

try:
    store_codes = get_store_codes(order_company_name_short, order_channel, order_country)
except Exception as error:
    st.sidebar.warning(f"Could not load store codes from BigQuery: {error}")
    store_codes = []

store_options = build_store_options(store_codes)

control_group_1_select_all = st.sidebar.checkbox("Select all stores - control group 1", value=True)
if control_group_1_select_all:
    control_group_1 = store_options["control_group_1"]
else:
    control_group_1 = st.sidebar.multiselect(
        "control group1 (store code)",
        options=store_options["control_group_1"],
        default=[],
    )

control_group_2_select_all = st.sidebar.checkbox("Select all stores - control group 2", value=True)
if control_group_2_select_all:
    control_group_2 = store_options["control_group_2"]
else:
    control_group_2 = st.sidebar.multiselect(
        "control group 2 (store code)",
        options=store_options["control_group_2"],
        default=[],
    )

testing_group_1_select_all = st.sidebar.checkbox("Select all stores - testing group 1", value=True)
if testing_group_1_select_all:
    testing_group_1 = store_options["testing_group_1"]
else:
    testing_group_1 = st.sidebar.multiselect(
        "testing group 1 (store code)",
        options=store_options["testing_group_1"],
        default=[],
    )
testing_group_2_select_all = st.sidebar.checkbox("Select all stores - testing group 2", value=True)
if testing_group_2_select_all:
    testing_group_2 = store_options["testing_group_2"]
else:
    testing_group_2 = st.sidebar.multiselect(
        "testing group 2(store code)",
        options=store_options["testing_group_2"],
        default=[],
    )

control_group_1_note = st.sidebar.text_input("Control Group 1 description", value="")
control_group_2_note = st.sidebar.text_input("Control Group 2 description", value="")
testing_group_1_note = st.sidebar.text_input("Testing Group 1 description", value="")
testing_group_2_note = st.sidebar.text_input("Testing Group 2 description", value="")

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
        df = fetch_raw_data(
            traffic_business_unit=traffic_business_unit,
            traffic_country=traffic_country,
            order_company_name_short=order_company_name_short,
            order_channel=order_channel,
            order_country=order_country,
            selected_dates=selected_dates,
            bq_client=bq_client,
        )
        st.session_state["data"] = df
        st.session_state["sql"] = None
        st.session_state["group_tables"] = build_group_period_tables(
            raw_df=df,
            control_group_1=control_group_1,
            control_group_2=control_group_2,
            testing_group_1=testing_group_1,
            testing_group_2=testing_group_2,
            baseline_dates=baseline_dates,
            promo_dates=promo_dates,
            vat=vat,
        )
    except Exception as e:
        st.session_state["data"] = None
        st.session_state["sql"] = None
        st.session_state["group_tables"] = {}
        st.error(f"Error running query: {e}")

if st.session_state.get("group_tables"):
    st.subheader("Funnel Analysis")
    st.caption("excelude Sunday")
    group_store_map = {
        "Control Group 1": control_group_1,
        "Control Group 2": control_group_2,
        "Testing Group 1": testing_group_1,
        "Testing Group 2": testing_group_2,
    }
    group_description_map = {
        "Control Group 1": control_group_1_note,
        "Control Group 2": control_group_2_note,
        "Testing Group 1": testing_group_1_note,
        "Testing Group 2": testing_group_2_note,
    }

    def _group_label(group_name: str) -> str:
        desc = group_description_map.get(group_name, "").strip()
        return f"{group_name} ({desc})" if desc else group_name

    funnel_tables = st.session_state["group_tables"].get("funnel_tables", {})
    control_col, vs_col, testing_col = st.columns([3, 1, 3])
    with control_col:
        selected_control_group = st.selectbox(
            "Control",
            options=["Control Group 1", "Control Group 2"],
            index=0,
        )
    with vs_col:
        st.markdown("### VS")
    with testing_col:
        selected_testing_group = st.selectbox(
            "Testing",
            options=["Testing Group 1", "Testing Group 2"],
            index=0,
        )

    selected_groups = [selected_control_group, selected_testing_group]
    control_table_col, _, testing_table_col = st.columns([3, 1, 3])
    with control_table_col:
        control_df = funnel_tables.get(selected_control_group, pd.DataFrame())
        control_codes = group_store_map.get(selected_control_group, [])
        st.markdown(f"**{_group_label(selected_control_group)}**")
        st.caption(f"Selected store code(s): {', '.join(control_codes) if control_codes else 'None'}")
        st.dataframe(format_funnel_table(control_df), use_container_width=True)
    with testing_table_col:
        testing_df = funnel_tables.get(selected_testing_group, pd.DataFrame())
        testing_codes = group_store_map.get(selected_testing_group, [])
        st.markdown(f"**{_group_label(selected_testing_group)}**")
        st.caption(f"Selected store code(s): {', '.join(testing_codes) if testing_codes else 'None'}")
        st.dataframe(format_funnel_table(testing_df), use_container_width=True)

    promo_impact_df = build_promo_impact_table(
        funnel_tables=funnel_tables,
        selected_control_group=selected_control_group,
        selected_testing_group=selected_testing_group,
    )
    st.markdown("**Promo Impact**")
    st.caption(
        f"Promo Impact = {_group_label(selected_testing_group)} %Diff (Promo vs Baseline) - {_group_label(selected_control_group)} %Diff (Promo vs Baseline)"
    )
    st.dataframe(format_promo_impact_table(promo_impact_df), use_container_width=True)

    weekday_kpis = st.session_state["group_tables"].get("weekday_kpis", pd.DataFrame())
    chart_df = weekday_kpis[weekday_kpis["group"].isin(selected_groups)].copy()
    if not chart_df.empty:
        chart_df["group"] = chart_df["group"].map(_group_label)
        st.markdown("**Avg Store Absorption Rate by Weekday (Promo Period)**")
        st.line_chart(
            chart_df,
            x="weekday",
            y="avg_store_absorption_rate",
            color="group",
            use_container_width=True,
        )

        extra_weekday_kpis = [
            ("Cal Store Conversion Rate by Weekday (Promo Period)", "cal_store_conversion_rate"),
            ("Total Orders by Weekday (Promo Period)", "total_orders"),
            ("AOV by Weekday (Promo Period)", "AOV"),
            ("Total Quantity by Weekday (Promo Period)", "total_quantity"),
            ("Price per Item by Weekday (Promo Period)", "price_per_item"),
            ("Total Revenue by Weekday (Promo Period)", "total_revenue"),
            ("Total PC1 by Weekday (Promo Period)", "total_PC1"),
            ("Margin by Weekday (Promo Period)", "margin"),
            ("RP Revenue Share by Weekday (Promo Period)", "RP_revenue_share"),
            ("Promo Revenue Share by Weekday (Promo Period)", "promo_revenue_share"),
        ]

        for title, kpi_col in extra_weekday_kpis:
            st.markdown(f"**{title}**")
            st.line_chart(
                chart_df,
                x="weekday",
                y=kpi_col,
                color="group",
                use_container_width=True,
            )

if st.session_state.get("data") is not None:
    st.download_button(
        "Download build_raw_data_sql result table (CSV)",
        data=st.session_state["data"].to_csv(index=False),
        file_name="build_raw_data_sql_result.csv",
        mime="text/csv",
    )
