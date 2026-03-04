from datetime import date
from google.cloud import bigquery


def build_promo_article_section_level_raw_data_sql(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
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
      mco.country,
      mco.company_name_short,
      mco.channel,
      LPAD(CAST(mco.tenant AS STRING), 4, '0') AS store_code,
      mco.store_name,
      COALESCE(oi.article_section_group, 'UNKNOWN') AS article_section_group,
      COALESCE(oi.article_section, 'UNKNOWN') AS article_section,
      COALESCE(oi.article_season, 'UNKNOWN') AS article_season,
      oi.insider_customer_type,
      CASE WHEN oi.article_price_red_eur IS NOT NULL THEN 'RP' ELSE 'BP' END AS price_type,
      CASE WHEN oi.has_promotion THEN 'promo' ELSE 'non-promo' END AS promo_check,
      ROUND(COALESCE(SUM(oi.revenue_after_cancellations_and_returns_eur_incl_forecast), 0), 2) AS total_revenue,
      ROUND(COALESCE(SUM(oi.quantity_ordered_after_cancellations_and_returns_incl_forecast), 0), 2) AS total_quantity,
      ROUND(COALESCE(SUM(oi.profit_contribution_1_eur_incl_forecast), 0), 2) AS total_PC1
    FROM `{order_table}` AS mco
    LEFT JOIN UNNEST(mco.order_items) AS oi
    WHERE mco.channel = @order_channel
      AND mco.company_name_short = @order_company_name_short
      AND mco.country = @order_country
      AND EXTRACT(DATE FROM ordered_at) IN UNNEST(@selected_dates)
      AND (@store_codes_is_empty OR LPAD(CAST(mco.tenant AS STRING), 4, '0') IN UNNEST(@store_codes))
      AND (@article_section_groups_is_empty OR COALESCE(oi.article_section_group, 'UNKNOWN') IN UNNEST(@article_section_groups))
      AND (@article_sections_is_empty OR COALESCE(oi.article_section, 'UNKNOWN') IN UNNEST(@article_sections))
      AND (@article_seasons_is_empty OR COALESCE(oi.article_season, 'UNKNOWN') IN UNNEST(@article_seasons))
      AND (@insider_customer_types_is_empty OR oi.insider_customer_type IN UNNEST(@insider_customer_types))
      AND (
        @price_types_is_empty
        OR CASE WHEN oi.article_price_red_eur IS NOT NULL THEN 'RP' ELSE 'BP' END IN UNNEST(@price_types)
      )
      AND (
        @promo_checks_is_empty
        OR CASE WHEN oi.has_promotion THEN 'promo' ELSE 'non-promo' END IN UNNEST(@promo_checks)
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


def fetch_promo_article_section_level_raw_data(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    selected_dates: list[date],
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
    return query_job.to_dataframe()



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
      COALESCE(oi.article_section_group, 'UNKNOWN') AS article_section_group,
      COALESCE(oi.article_section, 'UNKNOWN') AS article_section,
      COALESCE(oi.article_season, 'UNKNOWN') AS article_season
    FROM `{order_table}` AS mco
    LEFT JOIN UNNEST(mco.order_items) AS oi
    WHERE mco.channel = @order_channel
      AND mco.company_name_short = @order_company_name_short
      AND mco.country = @order_country
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
