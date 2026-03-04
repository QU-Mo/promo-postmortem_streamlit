from google.cloud import bigquery

def build_promo_sql(
    promotion_id: str,
    channel: str,
    country: str,
    company_name_short: str,
    start_date: str,
    end_date: str,
    target_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders"
) -> tuple[str, list]:
    """
    Returns the SQL query string and parameters for BigQuery.
    """
    sql = f"""
    WITH item_level_with_flags AS (
      SELECT
        FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(o.ordered_at, WEEK(MONDAY), 'Europe/Berlin'), 'Europe/Berlin') AS order_week,
        DATE(o.ordered_at, 'Europe/Berlin') AS order_date,
        FORMAT_TIMESTAMP('%A', o.ordered_at, 'Europe/Berlin') AS day_of_week,
        CASE 
          WHEN EXISTS (
            SELECT 1 
            FROM UNNEST(oi.promotions) AS promo 
            WHERE promo.promotion_id = @promotion_id
          ) THEN 'Yes' 
          ELSE 'No' 
        END AS item_has_specific_promotion_id,
        CASE WHEN oi.article_price_red_eur IS NOT NULL THEN 'red-price' ELSE 'black-price' END AS price,
        o.order_id,
        oi.revenue_after_cancellations_and_returns_eur_incl_forecast AS revenue,
        oi.profit_contribution_1_eur_incl_forecast AS pc1,
        oi.quantity_ordered_after_cancellations_and_returns_incl_forecast AS quantity,
        oi.promotion_discount_eur AS promotion_discount,
        IF(oi.article_price_red_eur IS NOT NULL AND oi.article_price_green_eur IS NULL, 
           oi.revenue_after_cancellations_and_returns_eur_incl_forecast, 
           NULL) AS revenue_red_price
      FROM `{target_table}` AS o
      LEFT JOIN UNNEST(o.order_items) AS oi
      WHERE 
        o.channel IN (@channel)
        AND o.company_name_short = @company_name_short
        AND o.country = @country
        AND o.ordered_at >= @start_date
        AND o.ordered_at <= @end_date
    )
    SELECT
      order_week,
      order_date,
      day_of_week,
      item_has_specific_promotion_id,
      price,
      COUNT(DISTINCT order_id) AS count_orders,
      SUM(revenue) AS revenue,
      SUM(pc1) AS pc1,
      SUM(quantity) AS items,
      SUM(promotion_discount) AS promotion_discount_eur,
      SUM(revenue_red_price) AS revenue_red_price,
      ROUND(SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_id)), 2) AS AOR,
      ROUND(SAFE_DIVIDE(SUM(pc1), SUM(revenue)/1.19), 2) AS PC1_margin
    FROM item_level_with_flags
    GROUP BY
      order_week, order_date, day_of_week, item_has_specific_promotion_id, price
    ORDER BY
      order_week DESC, order_date DESC, day_of_week, item_has_specific_promotion_id DESC
    """
    params = [
        bigquery.ScalarQueryParameter("promotion_id", "STRING", promotion_id),
        bigquery.ScalarQueryParameter("channel", "STRING", channel),
        bigquery.ScalarQueryParameter("company_name_short", "STRING", company_name_short),
        bigquery.ScalarQueryParameter("country", "STRING", country),
        bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", f"{start_date} 00:00:00"),
        bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", f"{end_date} 00:00:00"),
    ]
    return sql, params

def fetch_promo_data(
    promotion_id,
    channel,
    country,
    company_name_short,
    start_date,
    end_date,
    bq_client: bigquery.Client,
    target_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders"
):
    sql, params = build_promo_sql(
        promotion_id, channel, country, company_name_short, start_date, end_date, target_table
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query_job = bq_client.query(sql, job_config=job_config)
    return query_job.to_dataframe()