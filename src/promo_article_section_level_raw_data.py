from datetime import date
from google.cloud import bigquery
import pandas as pd


def build_promo_article_section_level_raw_data_sql(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
    baseline_dates: list[date],
    baseline_coefficient: float = 1.0,
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
    store_codes: list[str] | None = None,
    article_section_groups: list[str] | None = None,
    article_sections: list[str] | None = None,
    article_seasons: list[str] | None = None,
    insider_customer_types: list[str] | None = None,
    price_types: list[str] | None = None,
    promo_checks: list[str] | None = None,
) -> tuple[str, list]:
    """Returns SQL query and parameters for promo article-section-level raw data."""
    sql = f"""
    
      SELECT
      EXTRACT(DATE FROM ordered_at) AS ordered_date,
      country,
      company_name_short,
      channel,
      LPAD(CAST(tenant AS STRING), 4, '0') AS store_code,
      store_name,
      COALESCE(article_section_group, 'UNKNOWN') AS article_section_group,
      COALESCE(article_section, 'UNKNOWN') AS article_section,
      COALESCE(article_season, 'UNKNOWN') AS article_season,
      insider_customer_type,
      CASE WHEN article_price_red_eur IS NOT NULL THEN 'RP' ELSE 'BP' END AS price_type,
      CASE WHEN has_promotion THEN 'promo' ELSE 'non-promo' END AS promo_check,
      
     ROUND(COALESCE(SUM(revenue_after_cancellations_and_returns_eur_incl_forecast), 0), 2) AS total_revenue,
     ROUND(COALESCE(SUM(quantity_ordered_after_cancellations_and_returns_incl_forecast), 0), 2) AS total_quantity,
    ROUND(COALESCE(SUM(profit_contribution_1_eur_incl_forecast), 0), 2) AS total_PC1
    FROM `{order_table}` AS mco
    LEFT JOIN UNNEST(order_items) AS oi
    WHERE channel = @order_channel
      AND company_name_short = @order_company_name_short
      AND country = @order_country
      AND EXTRACT(DATE FROM ordered_at) IN UNNEST(@selected_dates)
      AND (@store_codes_is_empty OR LPAD(CAST(tenant AS STRING), 4, '0') IN UNNEST(@store_codes))
      AND (@article_section_groups_is_empty OR COALESCE(article_section_group, 'UNKNOWN') IN UNNEST(@article_section_groups))
      AND (@article_sections_is_empty OR COALESCE(article_section, 'UNKNOWN') IN UNNEST(@article_sections))
      AND (@article_seasons_is_empty OR COALESCE(article_season, 'UNKNOWN') IN UNNEST(@article_seasons))
      AND (@insider_customer_types_is_empty OR insider_customer_type IN UNNEST(@insider_customer_types))
      AND (
        @price_types_is_empty
        OR CASE WHEN article_price_red_eur IS NOT NULL THEN 'RP' ELSE 'BP' END IN UNNEST(@price_types)
      )
      AND (
        @promo_checks_is_empty
        OR CASE WHEN has_promotion THEN 'promo' ELSE 'non-promo' END IN UNNEST(@promo_checks)
      )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    """

    store_codes = store_codes or []
    article_section_groups = article_section_groups or []
    article_sections = article_sections or []
    article_seasons = article_seasons or []
    insider_customer_types = insider_customer_types or []
    price_types = price_types or []
    promo_checks = promo_checks or []
    normalized_dates = [str(d) for d in selected_dates]
    params = [
        bigquery.ScalarQueryParameter("order_company_name_short", "STRING", order_company_name_short),
        bigquery.ScalarQueryParameter("order_channel", "STRING", order_channel),
        bigquery.ScalarQueryParameter("order_country", "STRING", order_country),
        bigquery.ArrayQueryParameter("selected_dates", "DATE", normalized_dates),
        bigquery.ArrayQueryParameter("store_codes", "STRING", store_codes),
        bigquery.ArrayQueryParameter("article_section_groups", "STRING", article_section_groups),
        bigquery.ArrayQueryParameter("article_sections", "STRING", article_sections),
        bigquery.ArrayQueryParameter("article_seasons", "STRING", article_seasons),
        bigquery.ArrayQueryParameter("insider_customer_types", "STRING", insider_customer_types),
        bigquery.ArrayQueryParameter("price_types", "STRING", price_types),
        bigquery.ArrayQueryParameter("promo_checks", "STRING", promo_checks),
        bigquery.ScalarQueryParameter("store_codes_is_empty", "BOOL", len(store_codes) == 0),
        bigquery.ScalarQueryParameter("article_section_groups_is_empty", "BOOL", len(article_section_groups) == 0),
        bigquery.ScalarQueryParameter("article_sections_is_empty", "BOOL", len(article_sections) == 0),
        bigquery.ScalarQueryParameter("article_seasons_is_empty", "BOOL", len(article_seasons) == 0),
        bigquery.ScalarQueryParameter("insider_customer_types_is_empty", "BOOL", len(insider_customer_types) == 0),
        bigquery.ScalarQueryParameter("price_types_is_empty", "BOOL", len(price_types) == 0),
        bigquery.ScalarQueryParameter("promo_checks_is_empty", "BOOL", len(promo_checks) == 0),
    ]

    return sql, params


def apply_baseline_coefficient_to_promo_article_section_level_raw_data(
    raw_df: pd.DataFrame,
    baseline_dates: list[date],
    baseline_coefficient: float,
) -> pd.DataFrame:
    if raw_df.empty or baseline_coefficient == 1.0 or not baseline_dates:
        return raw_df

    adjusted_df = raw_df.copy()
    adjusted_df["ordered_date"] = pd.to_datetime(adjusted_df["ordered_date"]).dt.date
    baseline_date_set = set(baseline_dates)
    baseline_mask = adjusted_df["ordered_date"].isin(baseline_date_set)

    metric_cols = ["total_revenue", "total_quantity", "total_PC1"]
    for col in metric_cols:
        if col not in adjusted_df.columns:
            continue
        adjusted_df[col] = pd.to_numeric(adjusted_df[col], errors="coerce").astype(float)
        adjusted_df.loc[baseline_mask, col] = adjusted_df.loc[baseline_mask, col] * baseline_coefficient

    return adjusted_df


def fetch_promo_article_section_level_raw_data(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
    baseline_dates: list[date],
    baseline_coefficient: float,
    bq_client: bigquery.Client,
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
    store_codes: list[str] | None = None,
    article_section_groups: list[str] | None = None,
    article_sections: list[str] | None = None,
    article_seasons: list[str] | None = None,
    insider_customer_types: list[str] | None = None,
    price_types: list[str] | None = None,
    promo_checks: list[str] | None = None,
):
    sql, params = build_promo_article_section_level_raw_data_sql(
        order_company_name_short=order_company_name_short,
        order_channel=order_channel,
        order_country=order_country,
        selected_dates=selected_dates,
        baseline_dates=baseline_dates,
        baseline_coefficient=baseline_coefficient,
        order_table=order_table,
        store_codes=store_codes,
        article_section_groups=article_section_groups,
        article_sections=article_sections,
        article_seasons=article_seasons,
        insider_customer_types=insider_customer_types,
        price_types=price_types,
        promo_checks=promo_checks,
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query_job = bq_client.query(sql, job_config=job_config)
    raw_df = query_job.to_dataframe()
    return apply_baseline_coefficient_to_promo_article_section_level_raw_data(
        raw_df=raw_df,
        baseline_dates=baseline_dates,
        baseline_coefficient=baseline_coefficient,
    )



def build_selected_categories_funnel_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
    vat: float,
) -> pd.DataFrame:

    if group_df.empty:
        return pd.DataFrame()

    working_df = group_df.copy()
    working_df["ordered_date"] = pd.to_datetime(working_df["ordered_date"]).dt.date

    baseline_df = working_df[working_df["ordered_date"].isin(baseline_dates)]
    promo_df = working_df[working_df["ordered_date"].isin(promo_dates)]

    def _metrics(df: pd.DataFrame) -> dict[str, float]:
        revenue = pd.to_numeric(df["total_revenue"], errors="coerce")
        total_quantity = pd.to_numeric(df["total_quantity"], errors="coerce").sum(min_count=1)
        total_revenue = revenue.sum(min_count=1)
        total_pc1 = pd.to_numeric(df["total_PC1"], errors="coerce").sum(min_count=1)

        rp_revenue = revenue[df["price_type"] == "RP"].sum(min_count=1)
        promo_revenue = revenue[df["promo_check"] == "promo"].sum(min_count=1)
        existing_revenue = revenue[df["insider_customer_type"] == "EXISTING"].sum(min_count=1)

        quantity = pd.to_numeric(df["total_quantity"], errors="coerce")
        rp_quantity = quantity[df["price_type"] == "RP"].sum(min_count=1)
        promo_quantity = quantity[df["promo_check"] == "promo"].sum(min_count=1)
        existing_quantity = quantity[df["insider_customer_type"] == "EXISTING"].sum(min_count=1)

        pc1 = pd.to_numeric(df["total_PC1"], errors="coerce")
        rp_pc1 = pc1[df["price_type"] == "RP"].sum(min_count=1)
        promo_pc1 = pc1[df["promo_check"] == "promo"].sum(min_count=1)
        existing_pc1 = pc1[df["insider_customer_type"] == "EXISTING"].sum(min_count=1)

        return {
            "total quantity (selected categories)": total_quantity,
            "price per item (selected categories)": total_revenue / total_quantity if total_quantity else float("nan"),
            "total revenue (selected categories)": total_revenue,
            "total PC1 (selected categories)": total_pc1,
            "margin (selected categories)": round((total_pc1 / total_revenue) * vat, 4) if total_revenue else float("nan"),
            "RP revenue (selected categories)": rp_revenue,
            "promo revenue (selected categories)": promo_revenue,
            "existing revenue (selected categories)": existing_revenue,
            "RP quantity (selected categories)": rp_quantity,
            "promo quantity (selected categories)": promo_quantity,
            "existing quantity (selected categories)": existing_quantity,
            "RP PC1 (selected categories)": rp_pc1,
            "promo PC1 (selected categories)": promo_pc1,
            "existing PC1 (selected categories)": existing_pc1,
            "RP margin (selected categories)": round((rp_pc1 / rp_revenue) * vat, 4) if rp_revenue else float("nan"),
            "promo margin (selected categories)": round((promo_pc1 / promo_revenue) * vat, 4) if promo_revenue else float("nan"),
            "existing margin (selected categories)": round((existing_pc1 / existing_revenue) * vat, 4) if existing_revenue else float("nan"),
            "RP revenue share (selected categories)": rp_revenue / total_revenue if total_revenue else float("nan"),
            "promo revenue share (selected categories)": promo_revenue / total_revenue if total_revenue else float("nan"),
            "existing revenue share (selected categories)": existing_revenue / total_revenue if total_revenue else float("nan"),
        }

    baseline_metrics = _metrics(baseline_df)
    promo_metrics = _metrics(promo_df)

    rows = []
    for kpi in [
        "total quantity (selected categories)",
        "price per item (selected categories)",
        "total revenue (selected categories)",
        "total PC1 (selected categories)",
        "margin (selected categories)",
        "RP revenue (selected categories)",
        "promo revenue (selected categories)",
        "existing revenue (selected categories)",
        "RP quantity (selected categories)",
        "promo quantity (selected categories)",
        "existing quantity (selected categories)",
        "RP PC1 (selected categories)",
        "promo PC1 (selected categories)",
        "existing PC1 (selected categories)",
        "RP margin (selected categories)",
        "promo margin (selected categories)",
        "existing margin (selected categories)",
        "RP revenue share (selected categories)",
        "promo revenue share (selected categories)",
        "existing revenue share (selected categories)",
        
    ]:
        baseline_value = baseline_metrics.get(kpi, float("nan"))
        promo_value = promo_metrics.get(kpi, float("nan"))
        abs_diff = promo_value - baseline_value
        pct_diff = abs_diff / baseline_value if pd.notna(baseline_value) and baseline_value != 0 else float("nan")
        rows.append(
            {
                "KPI": kpi,
                "Baseline Period": baseline_value,
                "Promo Period": promo_value,
                "Abs Diff (Promo - Baseline)": abs_diff,
                "% Diff (Promo vs Baseline)": pct_diff,
            }
        )
    return pd.DataFrame(rows)

def build_selected_categories_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    if group_df.empty:
        return pd.DataFrame()

    working_df = group_df.copy()
    working_df["ordered_date"] = pd.to_datetime(working_df["ordered_date"]).dt.date
    working_df["total_revenue"] = pd.to_numeric(working_df["total_revenue"], errors="coerce")

    baseline_df = working_df[working_df["ordered_date"].isin(baseline_dates)]
    promo_df = working_df[working_df["ordered_date"].isin(promo_dates)]

    baseline_total_revenue = baseline_df["total_revenue"].sum(min_count=1)
    promo_total_revenue = promo_df["total_revenue"].sum(min_count=1)

    baseline_rp_revenue = baseline_df.loc[baseline_df["price_type"] == "RP", "total_revenue"].sum(min_count=1)
    promo_rp_revenue = promo_df.loc[promo_df["price_type"] == "RP", "total_revenue"].sum(min_count=1)

    baseline_bp_revenue = baseline_df.loc[baseline_df["price_type"] == "BP", "total_revenue"].sum(min_count=1)
    promo_bp_revenue = promo_df.loc[promo_df["price_type"] == "BP", "total_revenue"].sum(min_count=1)

    waterfall_rows = [
        {"Step": "Baseline revenue", "Value": baseline_total_revenue, "Type": "total"},
        {"Step": "RP revenue change", "Value": promo_rp_revenue - baseline_rp_revenue, "Type": "delta"},
        {"Step": "BP revenue change", "Value": promo_bp_revenue - baseline_bp_revenue, "Type": "delta"},
        {"Step": "Promo period revenue", "Value": promo_total_revenue, "Type": "total"},
    ]
    return pd.DataFrame(waterfall_rows)


def build_selected_categories_promo_non_promo_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    if group_df.empty:
        return pd.DataFrame()

    working_df = group_df.copy()
    working_df["ordered_date"] = pd.to_datetime(working_df["ordered_date"]).dt.date
    working_df["total_revenue"] = pd.to_numeric(working_df["total_revenue"], errors="coerce")

    baseline_df = working_df[working_df["ordered_date"].isin(baseline_dates)]
    promo_df = working_df[working_df["ordered_date"].isin(promo_dates)]

    baseline_total_revenue = baseline_df["total_revenue"].sum(min_count=1)
    promo_total_revenue = promo_df["total_revenue"].sum(min_count=1)

    baseline_promo_revenue = baseline_df.loc[baseline_df["promo_check"] == "promo", "total_revenue"].sum(min_count=1)
    promo_period_promo_revenue = promo_df.loc[promo_df["promo_check"] == "promo", "total_revenue"].sum(min_count=1)

    baseline_non_promo_revenue = baseline_df.loc[baseline_df["promo_check"] != "promo", "total_revenue"].sum(min_count=1)
    promo_period_non_promo_revenue = promo_df.loc[promo_df["promo_check"] != "promo", "total_revenue"].sum(min_count=1)

    waterfall_rows = [
        {"Step": "Baseline revenue", "Value": baseline_total_revenue, "Type": "total"},
        {"Step": "Promo revenue change", "Value": promo_period_promo_revenue - baseline_promo_revenue, "Type": "delta"},
        {"Step": "Non promo revenue change", "Value": promo_period_non_promo_revenue - baseline_non_promo_revenue, "Type": "delta"},
        {"Step": "Promo period revenue", "Value": promo_total_revenue, "Type": "total"},
    ]
    return pd.DataFrame(waterfall_rows)


def build_selected_categories_existing_non_existing_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    if group_df.empty:
        return pd.DataFrame()

    working_df = group_df.copy()
    working_df["ordered_date"] = pd.to_datetime(working_df["ordered_date"]).dt.date
    working_df["total_revenue"] = pd.to_numeric(working_df["total_revenue"], errors="coerce")

    baseline_df = working_df[working_df["ordered_date"].isin(baseline_dates)]
    promo_df = working_df[working_df["ordered_date"].isin(promo_dates)]

    baseline_total_revenue = baseline_df["total_revenue"].sum(min_count=1)
    promo_total_revenue = promo_df["total_revenue"].sum(min_count=1)

    baseline_existing_insider_revenue = baseline_df.loc[
        baseline_df["insider_customer_type"] == "EXISTING", "total_revenue"
    ].sum(min_count=1)
    promo_existing_insider_revenue = promo_df.loc[
        promo_df["insider_customer_type"] == "EXISTING", "total_revenue"
    ].sum(min_count=1)

    baseline_non_existing_revenue = baseline_df.loc[
        baseline_df["insider_customer_type"] != "EXISTING", "total_revenue"
    ].sum(min_count=1)
    promo_non_existing_revenue = promo_df.loc[
        promo_df["insider_customer_type"] != "EXISTING", "total_revenue"
    ].sum(min_count=1)

    waterfall_rows = [
        {"Step": "Baseline revenue", "Value": baseline_total_revenue, "Type": "total"},
        {
            "Step": "Existing insider revenue change",
            "Value": promo_existing_insider_revenue - baseline_existing_insider_revenue,
            "Type": "delta",
        },
        {
            "Step": "New + non insider revenue change",
            "Value": promo_non_existing_revenue - baseline_non_existing_revenue,
            "Type": "delta",
        },
        {"Step": "Promo period revenue", "Value": promo_total_revenue, "Type": "total"},
    ]
    return pd.DataFrame(waterfall_rows)


def _build_metric_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
    metric_col: str,
    step_labels: dict[str, str],
    split_col: str,
    split_positive_value: str,
    total_label: str,
) -> pd.DataFrame:
    if group_df.empty:
        return pd.DataFrame()

    working_df = group_df.copy()
    working_df["ordered_date"] = pd.to_datetime(working_df["ordered_date"]).dt.date
    working_df[metric_col] = pd.to_numeric(working_df[metric_col], errors="coerce")

    baseline_df = working_df[working_df["ordered_date"].isin(baseline_dates)]
    promo_df = working_df[working_df["ordered_date"].isin(promo_dates)]

    baseline_total = baseline_df[metric_col].sum(min_count=1)
    promo_total = promo_df[metric_col].sum(min_count=1)

    baseline_positive = baseline_df.loc[baseline_df[split_col] == split_positive_value, metric_col].sum(min_count=1)
    promo_positive = promo_df.loc[promo_df[split_col] == split_positive_value, metric_col].sum(min_count=1)

    baseline_negative = baseline_df.loc[baseline_df[split_col] != split_positive_value, metric_col].sum(min_count=1)
    promo_negative = promo_df.loc[promo_df[split_col] != split_positive_value, metric_col].sum(min_count=1)

    waterfall_rows = [
        {"Step": step_labels["baseline"], "Value": baseline_total, "Type": "total"},
        {"Step": step_labels["positive"], "Value": promo_positive - baseline_positive, "Type": "delta"},
        {"Step": step_labels["negative"], "Value": promo_negative - baseline_negative, "Type": "delta"},
        {"Step": step_labels["promo"], "Value": promo_total, "Type": "total"},
    ]
    return pd.DataFrame(waterfall_rows)


def build_selected_categories_quantity_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    return _build_metric_waterfall_table(
        group_df=group_df,
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        metric_col="total_quantity",
        step_labels={
            "baseline": "Baseline quantity",
            "positive": "RP quantity change",
            "negative": "BP quantity change",
            "promo": "Promo period quantity",
        },
        split_col="price_type",
        split_positive_value="RP",
        total_label="quantity",
    )


def build_selected_categories_promo_non_promo_quantity_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    return _build_metric_waterfall_table(
        group_df=group_df,
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        metric_col="total_quantity",
        step_labels={
            "baseline": "Baseline quantity",
            "positive": "Promo quantity change",
            "negative": "Non promo quantity change",
            "promo": "Promo period quantity",
        },
        split_col="promo_check",
        split_positive_value="promo",
        total_label="quantity",
    )


def build_selected_categories_existing_non_existing_quantity_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    return _build_metric_waterfall_table(
        group_df=group_df,
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        metric_col="total_quantity",
        step_labels={
            "baseline": "Baseline quantity",
            "positive": "Existing insider quantity change",
            "negative": "New + non insider quantity change",
            "promo": "Promo period quantity",
        },
        split_col="insider_customer_type",
        split_positive_value="EXISTING",
        total_label="quantity",
    )


def build_selected_categories_pc1_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    return _build_metric_waterfall_table(
        group_df=group_df,
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        metric_col="total_PC1",
        step_labels={
            "baseline": "Baseline PC1",
            "positive": "RP PC1 change",
            "negative": "BP PC1 change",
            "promo": "Promo period PC1",
        },
        split_col="price_type",
        split_positive_value="RP",
        total_label="PC1",
    )


def build_selected_categories_promo_non_promo_pc1_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    return _build_metric_waterfall_table(
        group_df=group_df,
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        metric_col="total_PC1",
        step_labels={
            "baseline": "Baseline PC1",
            "positive": "Promo PC1 change",
            "negative": "Non promo PC1 change",
            "promo": "Promo period PC1",
        },
        split_col="promo_check",
        split_positive_value="promo",
        total_label="PC1",
    )


def build_selected_categories_existing_non_existing_pc1_waterfall_table(
    group_df: pd.DataFrame,
    baseline_dates: list[date],
    promo_dates: list[date],
) -> pd.DataFrame:
    return _build_metric_waterfall_table(
        group_df=group_df,
        baseline_dates=baseline_dates,
        promo_dates=promo_dates,
        metric_col="total_PC1",
        step_labels={
            "baseline": "Baseline PC1",
            "positive": "Existing insider PC1 change",
            "negative": "New + non insider PC1 change",
            "promo": "Promo period PC1",
        },
        split_col="insider_customer_type",
        split_positive_value="EXISTING",
        total_label="PC1",
    )




def build_article_category_filter_options_sql(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
) -> tuple[str, list]:
    """Return SQL and params for category-filter options in sidebar controls."""
    sql = f"""
    SELECT DISTINCT
      COALESCE(article_section_group, 'UNKNOWN') AS article_section_group,
      COALESCE(article_section, 'UNKNOWN') AS article_section,
      COALESCE(article_season, 'UNKNOWN') AS article_season
    FROM `{order_table}` AS mco
    LEFT JOIN UNNEST(order_items) AS oi
    WHERE channel = @order_channel
      AND company_name_short = @order_company_name_short
      AND country = @order_country
      AND EXTRACT(DATE FROM ordered_at) IN UNNEST(@selected_dates)
    """
    params = [
        bigquery.ScalarQueryParameter("order_company_name_short", "STRING", order_company_name_short),
        bigquery.ScalarQueryParameter("order_channel", "STRING", order_channel),
        bigquery.ScalarQueryParameter("order_country", "STRING", order_country),
        bigquery.ArrayQueryParameter("selected_dates", "DATE", [str(d) for d in selected_dates]),
    ]
    return sql, params


def fetch_article_category_filter_options(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
    bq_client: bigquery.Client,
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
) -> dict[str, list[str]]:
    sql, params = build_article_category_filter_options_sql(
        order_company_name_short=order_company_name_short,
        order_channel=order_channel,
        order_country=order_country,
        selected_dates=selected_dates,
        order_table=order_table,
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = bq_client.query(sql, job_config=job_config).to_dataframe()
    if df.empty:
        return {
            "article_section_groups": [],
            "article_sections": [],
            "article_seasons": [],
        }
    return {
        "article_section_groups": sorted(df["article_section_group"].dropna().astype(str).unique().tolist()),
        "article_sections": sorted(df["article_section"].dropna().astype(str).unique().tolist()),
        "article_seasons": sorted(df["article_season"].dropna().astype(str).unique().tolist()),
    }