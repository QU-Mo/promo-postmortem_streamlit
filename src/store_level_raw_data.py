from datetime import date
import inspect
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
        total_promo_revenue,
        total_quantity,
        total_RP_quantity,
        total_promo_quantity,
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


FETCH_RAW_DATA_DEF = inspect.getsource(fetch_raw_data)


def build_store_code_options_sql(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
) -> tuple[str, list]:
    """Returns SQL query string and parameters for selectable store-code options."""
    sql = f"""
    SELECT
      DISTINCT LPAD(CAST(tenant AS STRING), 4, '0') AS store_code
    FROM `{order_table}` AS multichannel_orders
    LEFT JOIN UNNEST(multichannel_orders.order_items) AS multichannel_orders__order_items
    WHERE multichannel_orders.channel = @order_channel
      AND multichannel_orders.company_name_short = @order_company_name_short
      AND multichannel_orders.country = @order_country
    ORDER BY store_code
    """
    params = [
        bigquery.ScalarQueryParameter("order_company_name_short", "STRING", order_company_name_short),
        bigquery.ScalarQueryParameter("order_channel", "STRING", order_channel),
        bigquery.ScalarQueryParameter("order_country", "STRING", order_country),
    ]
    return sql, params


def fetch_store_code_options(
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    bq_client: bigquery.Client,
    order_table: str = "puc-p-dataf-retmkt-pii.datamarts.multichannel_orders",
) -> list[str]:
    sql, params = build_store_code_options_sql(
        order_company_name_short=order_company_name_short,
        order_channel=order_channel,
        order_country=order_country,
        order_table=order_table,
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query_job = bq_client.query(sql, job_config=job_config)
    store_code_df = query_job.to_dataframe()
    if store_code_df.empty:
        return []
    return store_code_df["store_code"].astype(str).str.zfill(4).tolist()


def build_group_period_tables(
    raw_df: pd.DataFrame,
    control_group_1: list[str],
    control_group_2: list[str],
    testing_group_1: list[str],
    testing_group_2: list[str],
    baseline_dates: list[date],
    promo_dates: list[date],
    vat: float,
) -> dict[str, pd.DataFrame]:
    """Build raw subset tables and funnel-analysis summary tables by group/period."""

    if raw_df.empty:
        return {}

    df = raw_df.copy()
    df["ordered_date"] = pd.to_datetime(df["ordered_date"]).dt.date
    df["store_code"] = df["store_code"].astype(str).str.zfill(4)
    df["weekday"] = pd.to_datetime(df["ordered_date"]).dt.day_name()

    group_config = {
        "Control Group 1": control_group_1,
        "Control Group 2": control_group_2,
        "Testing Group 1": testing_group_1,
        "Testing Group 2": testing_group_2,
    }
    period_config = {
        "Baseline Period": baseline_dates,
        "Promo Period": promo_dates,
    }

    def _subset(store_codes: list[str], period_dates: list[date]) -> pd.DataFrame:
        if not store_codes or not period_dates:
            return pd.DataFrame(columns=df.columns)
        normalized_store_codes = [str(code).zfill(4) for code in store_codes]
        return df[
            df["store_code"].isin(normalized_store_codes)
            & df["ordered_date"].isin(period_dates)
        ].sort_values(["ordered_date", "store_code"])

    numeric_sum_cols = [
        "pedestrian_footfall",
        "incoming_visitors",
        "orders",
        "total_revenue",
        "total_RP_revenue",
        "total_promo_revenue",
        "total_quantity",
        "total_RP_quantity",
        "total_promo_quantity",
        "total_PC1",
        "total_RP_PC1",
    ]
    avg_cols = ["store_absorption_rate", "store_conversion_rate"]

    for col in numeric_sum_cols + avg_cols:
        if col not in df.columns:
            df[col] = pd.NA

    def _safe_div(numerator: float, denominator: float) -> float:
        return float(numerator) / float(denominator) if denominator else 0.0

    def _period_metrics(subset_df: pd.DataFrame) -> dict[str, float]:
        if subset_df.empty:
            return {
                "pedestrian_footfall": 0.0,
                "footfall_trackable_stores_count": 0,
                "store_absorption_rate": 0.0,
                "incoming_visitors": 0.0,
                "cal_store_conversion_rate": 0.0,
                "total_orders": 0.0,
                "AOV": 0.0,
                "total_quantity": 0.0,
                "price_per_item": 0.0,
                "total_revenue": 0.0,
                "total_PC1": 0.0,
                "margin": 0.0,
                "RP_revenue_share": 0.0,
                "promo_revenue_share": 0.0,
            }

        filtered_df = subset_df[subset_df["weekday"] != "Sunday"].copy()
        if filtered_df.empty:
            return _period_metrics(pd.DataFrame(columns=subset_df.columns))

        weekday_frames = []
        for weekday, weekday_df in filtered_df.groupby("weekday", dropna=False):
            trackable_stores = weekday_df.loc[weekday_df["pedestrian_footfall"].notna(), "store_code"]
            trackable_count = int(trackable_stores.nunique())
            total_orders = weekday_df["orders"].fillna(0).sum()
            incoming_visitors = weekday_df["incoming_visitors"].fillna(0).sum()
            total_revenue = weekday_df["total_revenue"].fillna(0).sum()
            total_quantity = weekday_df["total_quantity"].fillna(0).sum()
            total_pc1 = weekday_df["total_PC1"].fillna(0).sum()
            total_rp_revenue = weekday_df["total_RP_revenue"].fillna(0).sum()
            total_promo_revenue = weekday_df["total_promo_revenue"].fillna(0).sum()

            weekday_frames.append(
                {
                    "weekday": weekday,
                    "pedestrian_footfall": weekday_df["pedestrian_footfall"].fillna(0).sum(),
                    "footfall_trackable_stores_count": trackable_count,
                    "store_absorption_rate": weekday_df["store_absorption_rate"].mean(),
                    "incoming_visitors": incoming_visitors,
                    "store_conversion_rate": weekday_df["store_conversion_rate"].mean(),
                    "total_orders": total_orders,
                    "total_revenue": total_revenue,
                    "total_RP_revenue": total_rp_revenue,
                    "total_promo_revenue": total_promo_revenue,
                    "total_quantity": total_quantity,
                    "total_PC1": total_pc1,
                    "cal_store_conversion_rate": _safe_div(total_orders, incoming_visitors),
                    "AOV": _safe_div(total_revenue, total_orders),
                    "margin": _safe_div(total_pc1, total_revenue) * vat,
                    "RP_revenue_share": _safe_div(total_rp_revenue, total_revenue),
                    "promo_revenue_share": _safe_div(total_promo_revenue, total_revenue),
                    "price_per_item": _safe_div(total_revenue, total_quantity),
                }
            )

        weekday_df = pd.DataFrame(weekday_frames)
        return {
            "pedestrian_footfall": weekday_df["pedestrian_footfall"].sum(),
            "footfall_trackable_stores_count": int(filtered_df.loc[filtered_df["pedestrian_footfall"].notna(), "store_code"].nunique()),
            "store_absorption_rate": weekday_df["store_absorption_rate"].mean(),
            "incoming_visitors": weekday_df["incoming_visitors"].sum(),
            "cal_store_conversion_rate": weekday_df["cal_store_conversion_rate"].mean(),
            "total_orders": weekday_df["total_orders"].sum(),
            "AOV": weekday_df["AOV"].mean(),
            "total_quantity": weekday_df["total_quantity"].sum(),
            "price_per_item": weekday_df["price_per_item"].mean(),
            "total_revenue": weekday_df["total_revenue"].sum(),
            "total_PC1": weekday_df["total_PC1"].sum(),
            "margin": weekday_df["margin"].mean(),
            "RP_revenue_share": weekday_df["RP_revenue_share"].mean(),
            "promo_revenue_share": weekday_df["promo_revenue_share"].mean(),
        }

    def _funnel_table(baseline_metrics: dict[str, float], promo_metrics: dict[str, float]) -> pd.DataFrame:
        def _pct_diff(baseline: float, promo: float) -> float:
            return _safe_div((promo - baseline), baseline) if baseline else 0.0

        row_defs = [
            (
                f"pedestrian footfall (footfall trackable stores #: {baseline_metrics['footfall_trackable_stores_count']} → {promo_metrics['footfall_trackable_stores_count']})",
                "pedestrian_footfall",
            ),
            ("avg store absorption rate", "store_absorption_rate"),
            ("incoming visitors", "incoming_visitors"),
            ("cal store conversion rate", "cal_store_conversion_rate"),
            ("total orders", "total_orders"),
            ("AOV", "AOV"),
            ("total quantity", "total_quantity"),
            ("price per item", "price_per_item"),
            ("total revenue", "total_revenue"),
            ("total PC1", "total_PC1"),
            ("margin", "margin"),
            ("RP revenue share", "RP_revenue_share"),
            ("promo revenue share", "promo_revenue_share"),
        ]

        rows = []
        for display_name, metric_key in row_defs:
            baseline_value = float(baseline_metrics.get(metric_key, 0.0) or 0.0)
            promo_value = float(promo_metrics.get(metric_key, 0.0) or 0.0)
            rows.append(
                {
                    "KPI": display_name,
                    "Baseline Period": baseline_value,
                    "Promo Period": promo_value,
                    "% Diff (Promo vs Baseline)": _pct_diff(baseline_value, promo_value),
                    "Abs Diff (Promo - Baseline)": promo_value - baseline_value,
                }
            )
        return pd.DataFrame(rows)

    def _weekday_kpi_table(group_name: str, subset_df: pd.DataFrame) -> pd.DataFrame:
        if subset_df.empty:
            return pd.DataFrame(
                columns=[
                    "weekday",
                    "group",
                    "avg_store_absorption_rate",
                    "cal_store_conversion_rate",
                    "total_orders",
                    "AOV",
                    "total_quantity",
                    "price_per_item",
                    "total_revenue",
                    "total_PC1",
                    "margin",
                    "RP_revenue_share",
                    "promo_revenue_share",
                ]
            )

        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        filtered_df = subset_df[subset_df["weekday"] != "Sunday"].copy()
        if filtered_df.empty:
            return pd.DataFrame(
                columns=[
                    "weekday",
                    "group",
                    "avg_store_absorption_rate",
                    "cal_store_conversion_rate",
                    "total_orders",
                    "AOV",
                    "total_quantity",
                    "price_per_item",
                    "total_revenue",
                    "total_PC1",
                    "margin",
                    "RP_revenue_share",
                    "promo_revenue_share",
                ]
            )

        weekday_frames = []
        for weekday, weekday_df in filtered_df.groupby("weekday", dropna=False):
            total_orders = weekday_df["orders"].fillna(0).sum()
            incoming_visitors = weekday_df["incoming_visitors"].fillna(0).sum()
            total_revenue = weekday_df["total_revenue"].fillna(0).sum()
            total_quantity = weekday_df["total_quantity"].fillna(0).sum()
            total_pc1 = weekday_df["total_PC1"].fillna(0).sum()
            total_rp_revenue = weekday_df["total_RP_revenue"].fillna(0).sum()
            total_promo_revenue = weekday_df["total_promo_revenue"].fillna(0).sum()

            weekday_frames.append(
                {
                    "weekday": weekday,
                    "avg_store_absorption_rate": weekday_df["store_absorption_rate"].mean(),
                    "cal_store_conversion_rate": _safe_div(total_orders, incoming_visitors),
                    "total_orders": total_orders,
                    "AOV": _safe_div(total_revenue, total_orders),
                    "total_quantity": total_quantity,
                    "price_per_item": _safe_div(total_revenue, total_quantity),
                    "total_revenue": total_revenue,
                    "total_PC1": total_pc1,
                    "margin": _safe_div(total_pc1, total_revenue) * vat,
                    "RP_revenue_share": _safe_div(total_rp_revenue, total_revenue),
                    "promo_revenue_share": _safe_div(total_promo_revenue, total_revenue),
                }
            )

        weekday_df = pd.DataFrame(weekday_frames)
        weekday_df["weekday"] = pd.Categorical(weekday_df["weekday"], categories=weekday_order, ordered=True)
        weekday_df = weekday_df.sort_values("weekday")
        weekday_df["group"] = group_name
        return weekday_df[
            [
                "weekday",
                "group",
                "avg_store_absorption_rate",
                "cal_store_conversion_rate",
                "total_orders",
                "AOV",
                "total_quantity",
                "price_per_item",
                "total_revenue",
                "total_PC1",
                "margin",
                "RP_revenue_share",
                "promo_revenue_share",
            ]
        ]

    subset_tables: dict[str, pd.DataFrame] = {}
    funnel_tables: dict[str, pd.DataFrame] = {}
    period_metrics: dict[str, dict[str, dict[str, float]]] = {}
    weekday_kpi_frames: list[pd.DataFrame] = []

    for group_name, stores in group_config.items():
        metrics_by_period: dict[str, dict[str, float]] = {}
        promo_subset = pd.DataFrame(columns=df.columns)
        for period_name, period_dates in period_config.items():
            table_name = f"{group_name} - {period_name}"
            period_subset = _subset(stores, period_dates)
            subset_tables[table_name] = period_subset
            metrics_by_period[period_name] = _period_metrics(period_subset)
            if period_name == "Promo Period":
                promo_subset = period_subset

        period_metrics[group_name] = metrics_by_period
        funnel_tables[group_name] = _funnel_table(
            baseline_metrics=metrics_by_period["Baseline Period"],
            promo_metrics=metrics_by_period["Promo Period"],
        )
        weekday_kpi_frames.append(_weekday_kpi_table(group_name, promo_subset))

    weekday_kpis = pd.concat(weekday_kpi_frames, ignore_index=True)

    return {
        "subset_tables": subset_tables,
        "funnel_tables": funnel_tables,
        "period_metrics": period_metrics,
        "weekday_kpis": weekday_kpis,
    }
