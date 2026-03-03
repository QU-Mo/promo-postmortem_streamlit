from datetime import date
import pandas as pd
from google.cloud import bigquery


def build_raw_data_sql(
    traffic_business_unit: str,
    traffic_country: str,
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
    traffic_table: str = "puc-p-dataf-retmkt-npii.reports.hystreet_instore_by_day_by_store",
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
) -> tuple[str, list]:
    """Returns SQL query string and parameters for the raw-data funnel table."""
    sql = f"""
    WITH traffic_date AS (
      SELECT
        DATE(visited_on) AS ordered_date,
        country,
        business_unit,
        LPAD(CAST(store_code AS STRING), 4, '0') AS store_code,
        store_name,
        COALESCE(SUM(pedestrian_footfall), 0) AS pedestrian_footfall,
        COALESCE(SUM(incoming_visitors), 0) / NULLIF(COALESCE(SUM(pedestrian_footfall), 0), 0) AS store_absorption_rate,
        COALESCE(SUM(orders), 0) / NULLIF(COALESCE(SUM(incoming_visitors), 0), 0) AS store_conversion_rate,
        COALESCE(SUM(incoming_visitors), 0) AS incoming_visitors,
        COALESCE(SUM(orders), 0) AS orders
      FROM `{traffic_table}`
      WHERE business_unit = @traffic_business_unit
        AND country = @traffic_country
        AND visited_on IN UNNEST(@selected_dates)
      GROUP BY 1, 2, 3, 4, 5
    ),
    store_level_mco_data AS (
      SELECT
        EXTRACT(DATE FROM ordered_at) AS ordered_date,
        country,
        company_name_short,
        channel,
        LPAD(CAST(tenant AS STRING), 4, '0') AS store_code,
        store_name,
        ROUND(COALESCE(SUM(revenue_after_cancellations_and_returns_eur_incl_forecast), 0), 2) AS total_revenue,
        ROUND(COALESCE(SUM(CASE WHEN article_price_red_eur IS NOT NULL THEN revenue_after_cancellations_and_returns_eur_incl_forecast END), 0), 2) AS total_RP_revenue,
        ROUND(COALESCE(SUM(CASE WHEN has_promotion THEN revenue_after_cancellations_and_returns_eur_incl_forecast END), 0), 2) AS total_promo_revenue,
        ROUND(COALESCE(SUM(quantity_ordered_after_cancellations_and_returns_incl_forecast), 0), 2) AS total_quantity,
        ROUND(COALESCE(SUM(CASE WHEN article_price_red_eur IS NOT NULL THEN quantity_ordered_after_cancellations_and_returns_incl_forecast END), 0), 2) AS total_RP_quantity,
        ROUND(COALESCE(SUM(CASE WHEN has_promotion THEN quantity_ordered_after_cancellations_and_returns_incl_forecast END), 0), 2) AS total_promo_quantity,
        ROUND(COALESCE(SUM(profit_contribution_1_eur_incl_forecast), 0), 2) AS total_PC1,
        ROUND(COALESCE(SUM(CASE WHEN article_price_red_eur IS NOT NULL THEN profit_contribution_1_eur_incl_forecast END), 0), 2) AS total_RP_PC1
      FROM `{order_table}` AS multichannel_orders
      LEFT JOIN UNNEST(multichannel_orders.order_items) AS multichannel_orders__order_items
      LEFT JOIN UNNEST(promotions) as multichannel_orders__order_items__promotions
      WHERE multichannel_orders.channel = @order_channel
        AND multichannel_orders.company_name_short = @order_company_name_short
        AND multichannel_orders.country = @order_country
        AND EXTRACT(DATE FROM ordered_at) IN UNNEST(@selected_dates)
      GROUP BY 1, 2, 3, 4, 5, 6
    ),
    stationary_funnel_combi AS (
      SELECT
        traffic_date.*,
        channel,
        total_revenue,
        total_RP_revenue,
        total_quantity,
        total_RP_quantity,
        total_PC1,
        total_RP_PC1
      FROM traffic_date
      LEFT JOIN store_level_mco_data
        ON traffic_date.ordered_date = store_level_mco_data.ordered_date
        AND traffic_date.country = store_level_mco_data.country
        AND SPLIT(traffic_date.business_unit, ' ')[OFFSET(0)] = store_level_mco_data.company_name_short
        AND traffic_date.store_code = store_level_mco_data.store_code
        AND traffic_date.store_name = store_level_mco_data.store_name
    )
    SELECT *
    FROM stationary_funnel_combi
    """

    normalized_dates = [str(d) for d in selected_dates]
    params = [
        bigquery.ScalarQueryParameter("traffic_business_unit", "STRING", traffic_business_unit),
        bigquery.ScalarQueryParameter("traffic_country", "STRING", traffic_country),
        bigquery.ScalarQueryParameter("order_company_name_short", "STRING", order_company_name_short),
        bigquery.ScalarQueryParameter("order_channel", "STRING", order_channel),
        bigquery.ScalarQueryParameter("order_country", "STRING", order_country),
        bigquery.ArrayQueryParameter("selected_dates", "DATE", normalized_dates),
    ]
    return sql, params


def fetch_raw_data(
    traffic_business_unit: str,
    traffic_country: str,
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
    bq_client: bigquery.Client,
    traffic_table: str = "puc-p-dataf-retmkt-npii.reports.hystreet_instore_by_day_by_store",
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
):
    sql, params = build_raw_data_sql(
        traffic_business_unit=traffic_business_unit,
        traffic_country=traffic_country,
        order_company_name_short=order_company_name_short,
        order_channel=order_channel,
        order_country=order_country,
        selected_dates=selected_dates,
        traffic_table=traffic_table,
        order_table=order_table,
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query_job = bq_client.query(sql, job_config=job_config)
    return query_job.to_dataframe()


def build_group_period_tables(
    raw_df: pd.DataFrame,
    control_group_1: list[str],
    control_group_2: list[str],
    testing_group_1: list[str],
    testing_group_2: list[str],
    baseline_dates: list[date],
    promo_dates: list[date],
) -> dict[str, pd.DataFrame]:
    """Split raw data into tables by group and period."""

    if raw_df.empty:
        return {}

    df = raw_df.copy()
    df["ordered_date"] = pd.to_datetime(df["ordered_date"]).dt.date
    df["store_code"] = df["store_code"].astype(str).str.zfill(4)

    def _subset(store_codes: list[str], period_dates: list[date]) -> pd.DataFrame:
        if not store_codes or not period_dates:
            return pd.DataFrame(columns=df.columns)
        normalized_store_codes = [str(code).zfill(4) for code in store_codes]
        return df[
            df["store_code"].isin(normalized_store_codes)
            & df["ordered_date"].isin(period_dates)
        ].sort_values(["ordered_date", "store_code"])

    return {
        "Control Group 1 - Baseline Period": _subset(control_group_1, baseline_dates),
        "Control Group 2 - Baseline Period": _subset(control_group_2, baseline_dates),
        "Control Group 1 - Promo Period": _subset(control_group_1, promo_dates),
        "Control Group 2 - Promo Period": _subset(control_group_2, promo_dates),
        "Testing Group 1 - Baseline Period": _subset(testing_group_1, baseline_dates),
        "Testing Group 2 - Baseline Period": _subset(testing_group_2, baseline_dates),
        "Testing Group 1 - Promo Period": _subset(testing_group_1, promo_dates),
        "Testing Group 2 - Promo Period": _subset(testing_group_2, promo_dates),
    }