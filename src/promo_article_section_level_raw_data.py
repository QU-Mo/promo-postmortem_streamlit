from datetime import date
from google.cloud import bigquery


def build_promo_article_section_level_raw_data_sql(
    traffic_business_unit: str,
    traffic_country: str,
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
    traffic_table: str = "puc-p-dataf-retmkt-npii.reports.hystreet_instore_by_day_by_store",
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
) -> tuple[str, list]:
    """Returns SQL query and parameters for promo article-section-level raw data."""
    sql = f"""
    WITH traffic_date AS (
      SELECT
        DATE(visited_on) AS ordered_date,
        country,
        business_unit,
        LPAD(CAST(store_code AS STRING), 4, '0') AS store_code,
        store_name,
        SUM(pedestrian_footfall) AS pedestrian_footfall,
        COALESCE(SUM(incoming_visitors), 0) / NULLIF(SUM(pedestrian_footfall), 0) AS store_absorption_rate,
        COALESCE(SUM(orders), 0) / NULLIF(COALESCE(SUM(incoming_visitors), 0), 0) AS store_conversion_rate,
        COALESCE(SUM(incoming_visitors), 0) AS incoming_visitors,
        COALESCE(SUM(orders), 0) AS orders
      FROM `{traffic_table}`
      WHERE business_unit = @traffic_business_unit
        AND country = @traffic_country
        AND visited_on IN UNNEST(@selected_dates)
      GROUP BY 1, 2, 3, 4, 5
    ),
    article_section_level_mco_data AS (
      SELECT
        EXTRACT(DATE FROM ordered_at) AS ordered_date,
        country,
        company_name_short,
        channel,
        LPAD(CAST(tenant AS STRING), 4, '0') AS store_code,
        store_name,
        COALESCE(oi.article_section, 'Unknown') AS article_section,
        ROUND(COALESCE(SUM(oi.revenue_after_cancellations_and_returns_eur_incl_forecast), 0), 2) AS total_revenue,
        ROUND(COALESCE(SUM(CASE WHEN oi.article_price_red_eur IS NOT NULL THEN oi.revenue_after_cancellations_and_returns_eur_incl_forecast END), 0), 2) AS total_RP_revenue,
        ROUND(COALESCE(SUM(CASE WHEN oi.has_promotion THEN oi.revenue_after_cancellations_and_returns_eur_incl_forecast END), 0), 2) AS total_promo_revenue,
        ROUND(COALESCE(SUM(oi.quantity_ordered_after_cancellations_and_returns_incl_forecast), 0), 2) AS total_quantity,
        ROUND(COALESCE(SUM(oi.profit_contribution_1_eur_incl_forecast), 0), 2) AS total_PC1
      FROM `{order_table}` AS mco
      LEFT JOIN UNNEST(mco.order_items) AS oi
      WHERE mco.channel = @order_channel
        AND mco.company_name_short = @order_company_name_short
        AND mco.country = @order_country
        AND EXTRACT(DATE FROM ordered_at) IN UNNEST(@selected_dates)
      GROUP BY 1, 2, 3, 4, 5, 6, 7
    )
    SELECT
      t.ordered_date,
      t.country,
      t.business_unit,
      t.store_code,
      t.store_name,
      t.pedestrian_footfall,
      t.store_absorption_rate,
      t.store_conversion_rate,
      t.incoming_visitors,
      t.orders,
      m.channel,
      m.article_section,
      m.total_revenue,
      m.total_RP_revenue,
      m.total_promo_revenue,
      m.total_quantity,
      m.total_PC1
    FROM traffic_date t
    LEFT JOIN article_section_level_mco_data m
      ON t.ordered_date = m.ordered_date
      AND t.country = m.country
      AND SPLIT(t.business_unit, ' ')[OFFSET(0)] = m.company_name_short
      AND t.store_code = m.store_code
      AND t.store_name = m.store_name
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


def fetch_promo_article_section_level_raw_data(
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
    sql, params = build_promo_article_section_level_raw_data_sql(
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
