import streamlit as st
from datetime import date, timedelta
from google.cloud import bigquery
import pandas as pd
import altair as alt


from store_level_raw_data import (
    build_group_period_tables,
    fetch_raw_data,
    fetch_store_code_options,
)
from promo_article_section_level_raw_data import (
    build_selected_categories_existing_non_existing_pc1_waterfall_table,
    build_selected_categories_existing_non_existing_quantity_waterfall_table,
    build_selected_categories_existing_non_existing_waterfall_table,
    build_selected_categories_funnel_table,
    build_selected_categories_pc1_waterfall_table,
    build_selected_categories_promo_non_promo_pc1_waterfall_table,
    build_selected_categories_promo_non_promo_quantity_waterfall_table,
    build_selected_categories_promo_non_promo_waterfall_table,
    build_selected_categories_quantity_waterfall_table,
    build_selected_categories_waterfall_table,
    fetch_promo_article_section_level_raw_data,
)


st.set_page_config(page_title="Promo Post-Mortem", layout="wide")



ALL_ARTICLE_SEASONS = ["GANZJAHR", "SOMMER", "WINTER", "UNKNOWN"]
ALL_ARTICLE_SECTION_GROUP = [
    "D-ACCESSOIRES", "D-BAUKASTEN", "D-BLAZER", "D-BLUSE", "D-HOSE", "D-INDOOR-WESTE",
    "D-JACKE", "D-JUMPSUIT", "D-KLEID", "D-LEDER", "D-MANTEL", "D-MODESCHMUCK", "D-OUTDOOR-WESTE", "D-ROECKE",
    "D-STRICK", "D-SWEAT", "D-T-SHIRT", "D-TWINSET", "H-ACCESSOIRES", "H-ANZUG", "H-CITYHEMD", "H-FREIZEITHEMD",
    "H-HOSE", "H-INDOOR-WESTE", "H-JACKE", "H-KRAWATTE", "H-LEDER", "H-MANTEL", "H-OUTDOOR-WESTE", "H-POLO",
    "H-PSEUDO", "H-SAKKO", "H-STRICK", "H-SWEAT", "H-T-SHIRT", "K-ACCESSOIRES", "K-ANZUG", "K-BLUSE", "K-HEMD",
    "K-HOSE", "K-JACKE", "K-KLEID", "K-MANTEL", "K-ROCK", "K-SAKKO", "K-STRICK", "K-SWEAT", "K-T-SHIR", "K-WESTE",
    "SONSTIGES", "UNKNOWN",
]
ALL_ARTICLE_SECTIONS = [
    "ABENDKLEID", "ACCESSOIRE", "ANZUG", "ARMSCHMUCK", "BADEANZUG", "BADEMANTEL", "BADEWAESCHE", "BAUK-BLAZER",
    "BAUK-HOSE", "BAUK-KLEID", "BAUK-ROCK", "BAUK-SAKKO", "BAUK-WESTE", "BERMUDA", "BH", "BIKINI", "BK-SMOKING-HOSE",
    "BK-SMOKING-SAKKO", "BLAZER", "BLUSE", "BLUSE-1/1", "BLUSE-1/2", "BLUSE-3/4", "BLUSE-OHNE ARM", "BODY", "BOOTS",
    "BUERO", "CASUAL-HOSE", "CASUAL-SAKKO", "CITYH-1/1", "CITYH-1/1 EXTRAKURZ", "CITYH-1/1 EXTRALANG", "CITYH-1/2",
    "COCKT-STRICK", "COCKTAIL-ACCES", "COCKTAIL-BLUSE", "COCKTAIL-HOSE", "COCKTAIL-JACKE", "COCKTAIL-JUMPSUIT",
    "COCKTAIL-ROCK", "COCKTAIL-SHIRT", "COCKTAIL-TASCH", "COCKTAILKLEID", "COMMUNITY MASKE", "CORSAGE", "COSMETICS",
    "DECKE", "EINSTECKTUCH", "FINGERSCHMUCK", "FIVE POCKET", "FIVE-POCKET", "FREIZEITH-1/1", "FREIZEITH-1/2",
    "FUN", "GESELLSCHAFT", "GUERTEL", "HAARSCHMUCK", "HALSSCHMUCK", "HANDSCHUHE", "HANDTUCH", "HANDYHUELLE", "HEMD",
    "HOSE", "HOSE VERKUERZT", "HOSENTRAEGER", "HUT", "INDOOR-WESTE", "JACKE", "JACQUARDS", "JEANS", "JEANS-VERKUERZT",
    "JEGGINGS", "JERSEY UNTERTE", "JUMPSUIT", "KAPPE", "KISSEN", "KLEID", "KLEIDERKOMBI", "KLEINLEDER", "KLEINLEDERWAREN",
    "KOFFER", "KOPFBEDECKUNG", "KOPFHOERER", "KRAWATTE", "KRAWATTENKLAMMER", "KRAWATTENNADEL", "KUECHE", "KUMMERBUND",
    "LEDER", "LEDER-VEGAN", "LEGGINGS", "LIVING", "MANSCHETTENKNO", "MANTEL", "MANTEL-TW", "MODESCHMUCK", "MUETZE",
    "NACHTHEMD", "NSTIGES", "OFFENE SCHUHE", "OHRENWAERMER", "OHRSCHMUCK", "OUTDOOR-WESTE", "OVERSHIRT", "PANT",
    "PASCHMINA", "PFLEGEMITTEL", "POLO-1/1", "POLO-1/2", "POLO-3/4", "POLO-OHNE ARM", "PONCHO", "PSEUDO", "PYJAMA",
    "REGENSCHIRM", "ROCK", "SAKKO", "SCHAL", "SCHLEIFE", "SCHMUCK", "SCHUH", "SCHUHE", "SETS", "SHORTS", "SLIDES",
    "SLIP", "SNEAKER", "SONNENBRILLE", "SONST-ACC", "STIEFEL", "STRICK", "STRICKJACKE", "STRICKPULLOVER", "STRING",
    "STRUMPF", "STRUMPFHOSE", "SWEAT", "SWEATANZUG", "SWEATHOSE", "SWEATJACKE", "SWEATSHIRT", "Strick-1/2", "T-SH-O.ARM",
    "T-SH-O.ARM-HS", "T-SHIRT 1/1", "T-SHIRT 1/2", "T-SHIRT-1/1", "T-SHIRT-1/2", "T-SHIRT-3/4", "T-SHIRT-O ARM",
    "T-SHIRT-O. ARM", "TASCHE", "TECHNIK", "TRAVEL", "TUECHER", "TWINSET", "UHR", "WAESCHE", "WESTE", "UNKNOWN",
]

def initialize_session_state() -> None:
    if "data" not in st.session_state:
        st.session_state["data"] = None
    if "sql" not in st.session_state:
        st.session_state["sql"] = None
    if "group_tables" not in st.session_state:
        st.session_state["group_tables"] = {}
    if "category_group_tables" not in st.session_state:
        st.session_state["category_group_tables"] = {}


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

    control_metrics = control_df[["KPI", "% Diff (Promo vs Baseline)", "Abs Diff (Promo - Baseline)"]].rename(
        columns={
            "% Diff (Promo vs Baseline)": f"{selected_control_group} % Diff (Promo vs Baseline)",
            "Abs Diff (Promo - Baseline)": f"{selected_control_group} Abs Diff (Promo vs Baseline)",
        }
    )
    testing_metrics = testing_df[["KPI", "% Diff (Promo vs Baseline)", "Abs Diff (Promo - Baseline)"]].rename(
        columns={
            "% Diff (Promo vs Baseline)": f"{selected_testing_group} % Diff (Promo vs Baseline)",
            "Abs Diff (Promo - Baseline)": f"{selected_testing_group} Abs Diff (Promo vs Baseline)",
        }
    )

    merged = testing_metrics.merge(control_metrics, on="KPI", how="inner")
    merged["Promo Impact (Testing Group 1 %Diff - Control Group 1 %Diff )"] = (
        merged[f"{selected_testing_group} % Diff (Promo vs Baseline)"]
        - merged[f"{selected_control_group} % Diff (Promo vs Baseline)"]
    )
    merged["Promo Impact (Testing Group 1 Abs Diff - Control Group 1 Abs Diff )"] = (
        merged[f"{selected_testing_group} Abs Diff (Promo vs Baseline)"]
        - merged[f"{selected_control_group} Abs Diff (Promo vs Baseline)"]
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

RATE_KPI_KEYWORDS = ("margin", "rate", "share")


def _format_kpi_value(kpi: str, value: float, is_pct_diff: bool = False) -> str:
    if pd.isna(value):
        return "-"
    normalized_kpi = str(kpi).strip().lower()
    is_rate_kpi = normalized_kpi in RATE_KPIS or any(keyword in normalized_kpi for keyword in RATE_KPI_KEYWORDS)
    if is_pct_diff or is_rate_kpi:
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
    percent_cols = [col for col in formatted_df.columns if "% Diff (Promo vs Baseline)" in col or "%Diff" in col]
    abs_cols = [col for col in formatted_df.columns if "Abs Diff" in col]

    for col in percent_cols:
        formatted_df[col] = formatted_df.apply(
            lambda row: _format_kpi_value("Promo Impact", row[col], is_pct_diff=True),
            axis=1,
        )
    for col in abs_cols:
        formatted_df[col] = formatted_df.apply(
            lambda row: _format_kpi_value(row["KPI"], row[col]),
            axis=1,
        )
    return formatted_df


def build_weekday_chart(
    chart_df: pd.DataFrame,
    kpi_col: str,
    title: str,
) -> alt.Chart:
    chart_df = chart_df.copy()
    chart_df[kpi_col] = pd.to_numeric(chart_df[kpi_col], errors="coerce")

    format_pattern = ".2%"
    y_axis_format = ".0%"

    base = alt.Chart(chart_df).encode(
        x=alt.X("weekday:N", sort=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"], axis=alt.Axis(labelAngle=0, title=None)),
        y=alt.Y(f"{kpi_col}:Q", axis=alt.Axis(format=y_axis_format, title=None)),
        color=alt.Color("group:N", legend=alt.Legend(orient="bottom", direction="horizontal", title=None)),
    )

    bars = base.mark_bar().encode(xOffset="group:N")
    positive_labels = base.transform_filter(alt.datum[kpi_col] >= 0).mark_text(dy=-10, fontSize=13, fontWeight="bold").encode(
        text=alt.Text(f"{kpi_col}:Q", format=format_pattern),
        xOffset="group:N",
    )
    negative_labels = base.transform_filter(alt.datum[kpi_col] < 0).mark_text(dy=12, fontSize=13, fontWeight="bold").encode(
        text=alt.Text(f"{kpi_col}:Q", format=format_pattern),
        xOffset="group:N",
    )
    return (bars + positive_labels + negative_labels).properties(
        title=alt.TitleParams(text=title, anchor="middle"),
        height=360,
    ).configure_legend(
        orient="bottom",
        direction="horizontal",
        title=None,
    )


def build_selected_categories_waterfall_chart(waterfall_df: pd.DataFrame, title: str, y_axis_title: str) -> alt.Chart:
    if waterfall_df.empty:
        return alt.Chart(pd.DataFrame({"Step": [], "Value": []})).mark_bar()

    chart_df = waterfall_df.copy().reset_index(drop=True)
    chart_df["Value"] = pd.to_numeric(chart_df["Value"], errors="coerce").fillna(0.0)

    running_total = []
    starts = []
    current = 0.0
    for idx, row in chart_df.iterrows():
        if row["Type"] == "total":
            start = 0.0
            end = row["Value"]
            current = end
        else:
            start = current
            end = current + row["Value"]
            current = end
        starts.append(start)
        running_total.append(end)

    chart_df["start"] = starts
    chart_df["end"] = running_total
    chart_df["lower"] = chart_df[["start", "end"]].min(axis=1)
    chart_df["upper"] = chart_df[["start", "end"]].max(axis=1)
    chart_df["color_type"] = chart_df["Type"]
    chart_df.loc[(chart_df["Type"] == "delta") & (chart_df["Value"] >= 0), "color_type"] = "increase"
    chart_df.loc[(chart_df["Type"] == "delta") & (chart_df["Value"] < 0), "color_type"] = "decrease"

    bars = alt.Chart(chart_df).mark_bar().encode(
        x=alt.X("Step:N", sort=None, axis=alt.Axis(labelAngle=0, title=None)),
        y=alt.Y("lower:Q", title=y_axis_title),
        y2="upper:Q",
        color=alt.Color(
            "color_type:N",
            scale=alt.Scale(
                domain=["total", "increase", "decrease"],
                range=["#4C78A8", "#54A24B", "#E45756"],
            ),
            legend=None,
        ),
    )
    labels = alt.Chart(chart_df).mark_text(dy=-10, fontSize=13, fontWeight="bold").encode(
        x=alt.X("Step:N", sort=None),
        y=alt.Y("upper:Q"),
        text=alt.Text("Value:Q", format=",.2f"),
    )
    return (bars + labels).properties(title=alt.TitleParams(text=title, anchor="middle"), height=330)


def dataframe_height(df: pd.DataFrame, row_height: int = 35, header_height: int = 38, padding: int = 16) -> int:
    if df.empty:
        return 140
    return max(140, min(1200, header_height + len(df) * row_height + padding))


def format_date_list(dates: list[date]) -> str:
    if not dates:
        return "None"
    return f"{dates[0].isoformat()} to {dates[-1].isoformat()}"


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
vat = st.sidebar.number_input("VAT", min_value=0.0, value=1.19, step=0.01, format="%.2f")

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

article_section_group_select_all = st.sidebar.checkbox("Select all - article_section_group", value=True)
if article_section_group_select_all:
    article_section_groups = ALL_ARTICLE_SECTION_GROUP
else:
    article_section_groups = st.sidebar.multiselect(
        "article_section_group",
        options=ALL_ARTICLE_SECTION_GROUP,
        default=[],
    )

article_section_select_all = st.sidebar.checkbox("Select all - article_section", value=True)
if article_section_select_all:
    article_sections = ALL_ARTICLE_SECTIONS
else:
    article_sections = st.sidebar.multiselect(
        "article_section",
        options=ALL_ARTICLE_SECTIONS,
        default=[],
    )

article_season_select_all = st.sidebar.checkbox("Select all - article_season", value=False)
if article_season_select_all:
    article_seasons = ALL_ARTICLE_SEASONS
else:
    article_seasons = st.sidebar.multiselect(
        "article_season",
        options=ALL_ARTICLE_SEASONS,
        default=["GANZJAHR"],
    )

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

        category_df = fetch_promo_article_section_level_raw_data(
            order_company_name_short=order_company_name_short,
            order_channel=order_channel,
            order_country=order_country,
            selected_dates=selected_dates,
            article_section_groups=article_section_groups,
            article_sections=article_sections,
            article_seasons=article_seasons,
            bq_client=bq_client,
        )
        category_df["store_code"] = category_df["store_code"].astype(str).str.zfill(4)
        st.session_state["category_group_tables"] = {
            "Control Group 1": category_df[category_df["store_code"].isin([str(c).zfill(4) for c in control_group_1])],
            "Control Group 2": category_df[category_df["store_code"].isin([str(c).zfill(4) for c in control_group_2])],
            "Testing Group 1": category_df[category_df["store_code"].isin([str(c).zfill(4) for c in testing_group_1])],
            "Testing Group 2": category_df[category_df["store_code"].isin([str(c).zfill(4) for c in testing_group_2])],
        }
    except Exception as e:
        st.session_state["data"] = None
        st.session_state["sql"] = None
        st.session_state["group_tables"] = {}
        st.session_state["category_group_tables"] = {}
        st.error(f"Error running query: {e}")


    
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


selected_control_group = "Control Group 1"
selected_testing_group = "Testing Group 1"
if st.session_state.get("group_tables") or st.session_state.get("category_group_tables"):
    st.markdown(
        """
        <style>
            div[data-testid="stVerticalBlockBorderWrapper"]:has(#group-compare-anchor) {
                position: sticky;
                top: 0;
                z-index: 99;
                background: white;
                padding-top: 0.5rem;
                padding-bottom: 0.5rem;
                border-bottom: 1px solid rgba(49, 51, 63, 0.2);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<span id="group-compare-anchor"></span>', unsafe_allow_html=True)

    
    control_col, vs_col, testing_col = st.columns([5, 1, 5])
    with control_col:
        selected_control_group = st.selectbox(
            "Analysis Group A",
            options=["Control Group 1", "Control Group 2", "Testing Group 1", "Testing Group 2"],
            index=0,
        )
    
    with testing_col:
        selected_testing_group = st.selectbox(
            "Analysis Group B",
            options=["Control Group 1", "Control Group 2", "Testing Group 1", "Testing Group 2"],
            index=2,
        )

        info_control_col, _, info_testing_col = st.columns([3, 1, 3])
    with info_control_col:
        selected_control_codes = group_store_map.get(selected_control_group, [])
        st.caption(f"Selected store code(s): {', '.join(selected_control_codes) if selected_control_codes else 'None'}")
        st.caption(f"Baseline period: {format_date_list(baseline_dates)}")
        st.caption(f"Promo period: {format_date_list(promo_dates)}")
    with info_testing_col:
        selected_testing_codes = group_store_map.get(selected_testing_group, [])
        st.caption(f"Selected store code(s): {', '.join(selected_testing_codes) if selected_testing_codes else 'None'}")
        st.caption(f"Baseline period: {format_date_list(baseline_dates)}")
        st.caption(f"Promo period: {format_date_list(promo_dates)}")

if st.session_state.get("group_tables"):
    st.subheader("Store Level (All Categories) - Funnel Analysis (Exclude Sunday)")
    funnel_tables = st.session_state["group_tables"].get("funnel_tables", {})
    

    selected_groups = [selected_control_group, selected_testing_group]
    control_table_col, _, testing_table_col = st.columns([3, 1, 3])
    with control_table_col:
        control_df = funnel_tables.get(selected_control_group, pd.DataFrame())
        
        st.markdown(f"**{_group_label(selected_control_group)}**")
        st.dataframe(
            format_funnel_table(control_df),
            width='stretch',
            height=dataframe_height(control_df),
        )

    with testing_table_col:
        testing_df = funnel_tables.get(selected_testing_group, pd.DataFrame())
        
        st.markdown(f"**{_group_label(selected_testing_group)}**")
        st.dataframe(
            format_funnel_table(testing_df),
            width='stretch',
            height=dataframe_height(testing_df),
        )

    promo_impact_df = build_promo_impact_table(
        funnel_tables=funnel_tables,
        selected_control_group=selected_control_group,
        selected_testing_group=selected_testing_group,
    )
    st.markdown("**Promo Impact**")
    st.caption(
        "Promo Impact includes % Diff and Abs Diff comparisons between testing and control groups."
    )
    st.dataframe(
        format_promo_impact_table(promo_impact_df),
        width='stretch',
        height=dataframe_height(promo_impact_df),
    )

    st.markdown("### **show all Funnel KPI (promo vs baseline) charts.**")
    show_all_funnel_kpi_charts = st.toggle(
        "show_all_funnel_kpi_promo_vs_baseline",
        value=False,
        label_visibility="collapsed",
    )

    if show_all_funnel_kpi_charts:
        weekday_kpis = st.session_state["group_tables"].get("weekday_pct_diff_kpis", pd.DataFrame())
        chart_df = weekday_kpis[weekday_kpis["group"].isin(selected_groups)].copy()
        if not chart_df.empty:
            chart_df["group"] = chart_df["group"].map(_group_label)
            weekday_charts = [
                ("Avg Store Absorption Rate % Diff by Weekday", "avg_store_absorption_rate_pct_diff"),
                ("Cal Store Conversion Rate % Diff by Weekday", "cal_store_conversion_rate_pct_diff"),
                ("Total Orders % Diff by Weekday", "total_orders_pct_diff"),
                ("AOV % Diff by Weekday", "AOV_pct_diff"),
                ("Total Quantity % Diff by Weekday", "total_quantity_pct_diff"),
                ("Price per Item % Diff by Weekday", "price_per_item_pct_diff"),
                ("Total Revenue % Diff by Weekday", "total_revenue_pct_diff"),
                ("Total PC1 % Diff by Weekday", "total_PC1_pct_diff"),
                ("Margin % Diff by Weekday", "margin_pct_diff"),
                ("RP Revenue Share % Diff by Weekday", "RP_revenue_share_pct_diff"),
                ("Promo Revenue Share % Diff by Weekday", "promo_revenue_share_pct_diff"),
            ]

            for title, kpi_col in weekday_charts:
                st.altair_chart(
                    build_weekday_chart(chart_df=chart_df, kpi_col=kpi_col, title=title),
                    width='stretch',
                )


if st.session_state.get("category_group_tables"):
    st.subheader("Deep Dive： Store Level Selected Categories Analysis")
    category_control_table = build_selected_categories_funnel_table(
        group_df=st.session_state["category_group_tables"].get(selected_control_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        vat=vat,
    )
    category_testing_table = build_selected_categories_funnel_table(
        group_df=st.session_state["category_group_tables"].get(selected_testing_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        vat=vat,
    )

    category_funnel_tables = {
        selected_control_group: category_control_table,
        selected_testing_group: category_testing_table,
    }
    category_control_col, _, category_testing_col = st.columns([3, 1, 3])
    with category_control_col:
        st.markdown(f"**{_group_label(selected_control_group)}**")
        st.dataframe(format_funnel_table(category_control_table), width='stretch')
    with category_testing_col:
        st.markdown(f"**{_group_label(selected_testing_group)}**")
        st.dataframe(format_funnel_table(category_testing_table), width='stretch')

    category_promo_impact = build_promo_impact_table(
        funnel_tables=category_funnel_tables,
        selected_control_group=selected_control_group,
        selected_testing_group=selected_testing_group,
    )
    st.markdown("**Promo Impact**")
    st.dataframe(format_promo_impact_table(category_promo_impact), width='stretch')

    category_control_existing_split_waterfall = build_selected_categories_existing_non_existing_waterfall_table(
        group_df=st.session_state["category_group_tables"].get(selected_control_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
    )
    category_testing_existing_split_waterfall = build_selected_categories_existing_non_existing_waterfall_table(
        group_df=st.session_state["category_group_tables"].get(selected_testing_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
    )
    st.markdown("**Waterfall - Selected Categories Existing Insider vs New+Non Insider Revenue Bridge**")
    existing_split_control_col, _, existing_split_testing_col = st.columns([3, 1, 3])
    with existing_split_control_col:
        st.altair_chart(
            build_selected_categories_waterfall_chart(
                category_control_existing_split_waterfall,
                f"{_group_label(selected_control_group)}",
            ),
            width='stretch',
        )
    with existing_split_testing_col:
        st.altair_chart(
            build_selected_categories_waterfall_chart(
                category_testing_existing_split_waterfall,
                f"{_group_label(selected_testing_group)}",
            ),
            width='stretch',
        )

        category_control_promo_split_waterfall = build_selected_categories_promo_non_promo_waterfall_table(
        group_df=st.session_state["category_group_tables"].get(selected_control_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
    )
    category_testing_promo_split_waterfall = build_selected_categories_promo_non_promo_waterfall_table(
        group_df=st.session_state["category_group_tables"].get(selected_testing_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
    )
    st.markdown("**Waterfall - Selected Categories Promo vs Non Promo Revenue Bridge**")
    promo_split_control_col, _, promo_split_testing_col = st.columns([3, 1, 3])
    with promo_split_control_col:
        st.altair_chart(
            build_selected_categories_waterfall_chart(
                category_control_promo_split_waterfall,
                f"{_group_label(selected_control_group)}",
            ),
            width='stretch',
        )
    with promo_split_testing_col:
        st.altair_chart(
            build_selected_categories_waterfall_chart(
                category_testing_promo_split_waterfall,
                f"{_group_label(selected_testing_group)}",
            ),
            width='stretch',
        )

    category_control_waterfall = build_selected_categories_waterfall_table(
        group_df=st.session_state["category_group_tables"].get(selected_control_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
    )
    category_testing_waterfall = build_selected_categories_waterfall_table(
        group_df=st.session_state["category_group_tables"].get(selected_testing_group, pd.DataFrame()),
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
    )
    st.markdown("**Waterfall - Selected Categories RP vs BP Revenue Bridge**")
    category_waterfall_control_col, _, category_waterfall_testing_col = st.columns([3, 1, 3])
    with category_waterfall_control_col:
        st.altair_chart(
            build_selected_categories_waterfall_chart(
                category_control_waterfall,
                f"{_group_label(selected_control_group)}",
            ),
            width='stretch',
        )
    with category_waterfall_testing_col:
        st.altair_chart(
            build_selected_categories_waterfall_chart(
                category_testing_waterfall,
                f"{_group_label(selected_testing_group)}",
            ),
            width='stretch',
        )

if st.session_state.get("data") is not None:
    st.download_button(
        "Download build_raw_data_sql result table (CSV)",
        data=st.session_state["data"].to_csv(index=False),
        file_name="build_raw_data_sql_result.csv",
        mime="text/csv",
    )
