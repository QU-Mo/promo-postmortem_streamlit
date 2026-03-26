from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


DEFAULT_PRIMARY_KPIS = (
    "total revenue",
    "total PC1",
    "margin",
)
PROMO_VS_BASELINE_COL = "% Diff (Promo vs Baseline)"
PROMO_IMPACT_COL = "Promo Impact (Group A %Diff - Group B %Diff )"


def _safe_float(value: Any) -> float | None:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return None
    return float(numeric_value)


def _extract_metric_value(table_df: pd.DataFrame, kpi_name: str, column: str) -> float | None:
    if table_df.empty or "KPI" not in table_df.columns or column not in table_df.columns:
        return None
    selected = table_df[table_df["KPI"] == kpi_name]
    if selected.empty:
        return None
    return _safe_float(selected.iloc[0][column])


def compute_ab_winner(
    control_df: pd.DataFrame,
    testing_df: pd.DataFrame,
    primary_kpis: tuple[str, ...] = DEFAULT_PRIMARY_KPIS,
    min_uplift: float = 0.01,
    min_support_kpis: int = 2,
) -> dict[str, Any]:
    """Compute deterministic winner based on promo-vs-baseline uplift from selected KPIs."""
    comparisons: list[dict[str, Any]] = []

    for kpi in primary_kpis:
        testing_pct_diff = _extract_metric_value(testing_df, kpi, "% Diff (Promo vs Baseline)")
        control_pct_diff = _extract_metric_value(control_df, kpi, "% Diff (Promo vs Baseline)")
        uplift = None
        decision = "insufficient_data"

        if testing_pct_diff is not None and control_pct_diff is not None:
            uplift = testing_pct_diff - control_pct_diff
            if uplift >= min_uplift:
                decision = "testing"
            elif uplift <= -min_uplift:
                decision = "control"
            else:
                decision = "tie"

        comparisons.append(
            {
                "kpi": kpi,
                "testing_pct_diff": testing_pct_diff,
                "control_pct_diff": control_pct_diff,
                "uplift": uplift,
                "decision": decision,
            }
        )

    testing_votes = sum(1 for entry in comparisons if entry["decision"] == "testing")
    control_votes = sum(1 for entry in comparisons if entry["decision"] == "control")

    winner = "tie"
    confidence = "low"
    if testing_votes >= min_support_kpis and testing_votes > control_votes:
        winner = "testing"
        confidence = "high" if testing_votes == len(primary_kpis) else "medium"
    elif control_votes >= min_support_kpis and control_votes > testing_votes:
        winner = "control"
        confidence = "high" if control_votes == len(primary_kpis) else "medium"
    elif testing_votes != control_votes:
        winner = "testing" if testing_votes > control_votes else "control"
        confidence = "low"

    reason_codes = [entry["kpi"] for entry in comparisons if entry["decision"] == winner]

    return {
        "winner": winner,
        "confidence": confidence,
        "min_uplift": min_uplift,
        "min_support_kpis": min_support_kpis,
        "comparisons": comparisons,
        "reason_codes": reason_codes,
        "vote_count": {
            "testing": testing_votes,
            "control": control_votes,
        },
    }


def extract_kpi_drivers(
    table_df: pd.DataFrame,
    *,
    value_col: str,
    top_n: int = 5,
    min_abs_value: float = 0.0,
) -> dict[str, list[dict[str, float]]]:
    """Extract top KPI drivers from a KPI table column."""
    if table_df.empty or "KPI" not in table_df.columns or value_col not in table_df.columns:
        return {"top_positive": [], "top_negative": [], "top_absolute": []}

    working_df = table_df[["KPI", value_col]].copy()
    working_df[value_col] = pd.to_numeric(working_df[value_col], errors="coerce")
    working_df = working_df.dropna(subset=[value_col])
    working_df = working_df[working_df[value_col].abs() >= float(min_abs_value)]
    if working_df.empty:
        return {"top_positive": [], "top_negative": [], "top_absolute": []}

    top_positive_df = working_df.sort_values(value_col, ascending=False).head(top_n)
    top_negative_df = working_df.sort_values(value_col, ascending=True).head(top_n)
    top_absolute_df = working_df.assign(abs_value=working_df[value_col].abs()).sort_values(
        "abs_value",
        ascending=False,
    ).head(top_n)

    def _to_records(input_df: pd.DataFrame) -> list[dict[str, float]]:
        records = []
        for _, row in input_df.iterrows():
            records.append(
                {
                    "kpi": str(row["KPI"]),
                    "value": float(row[value_col]),
                }
            )
        return records

    return {
        "top_positive": _to_records(top_positive_df),
        "top_negative": _to_records(top_negative_df),
        "top_absolute": _to_records(top_absolute_df),
    }


def build_report_payload(
    *,
    traffic_business_unit: str,
    traffic_country: str,
    order_company_name_short: str,
    order_channel: str,
    order_country: str,
    baseline_dates: list,
    promo_dates: list,
    selected_control_group: str,
    selected_testing_group: str,
    group_store_map: dict[str, list[str]],
    group_description_map: dict[str, str],
    control_df: pd.DataFrame,
    testing_df: pd.DataFrame,
    promo_impact_df: pd.DataFrame,
) -> dict[str, Any]:
    winner_info = compute_ab_winner(control_df=control_df, testing_df=testing_df)
    control_drivers = extract_kpi_drivers(control_df, value_col=PROMO_VS_BASELINE_COL)
    testing_drivers = extract_kpi_drivers(testing_df, value_col=PROMO_VS_BASELINE_COL)
    promo_impact_drivers = extract_kpi_drivers(promo_impact_df, value_col=PROMO_IMPACT_COL)

    return {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "traffic_business_unit": traffic_business_unit,
            "traffic_country": traffic_country,
            "order_company_name_short": order_company_name_short,
            "order_channel": order_channel,
            "order_country": order_country,
            "baseline_dates": [str(item) for item in baseline_dates],
            "promo_dates": [str(item) for item in promo_dates],
            "control_group": {
                "name": selected_control_group,
                "description": group_description_map.get(selected_control_group, ""),
                "store_codes": group_store_map.get(selected_control_group, []),
            },
            "testing_group": {
                "name": selected_testing_group,
                "description": group_description_map.get(selected_testing_group, ""),
                "store_codes": group_store_map.get(selected_testing_group, []),
            },
        },
        "kpis": {
            "control_funnel": control_df.to_dict(orient="records"),
            "testing_funnel": testing_df.to_dict(orient="records"),
            "promo_impact": promo_impact_df.to_dict(orient="records"),
        },
        "winner_assessment": winner_info,
        "phase1_driver_analysis": {
            "control_group_drivers": control_drivers,
            "testing_group_drivers": testing_drivers,
            "promo_impact_drivers": promo_impact_drivers,
        },
    }


def build_phase1_summary_text(payload: dict[str, Any]) -> str:
    meta = payload.get("meta", {})
    control_group = meta.get("control_group", {}).get("name", "Group A")
    testing_group = meta.get("testing_group", {}).get("name", "Group B")

    control_df = pd.DataFrame(payload.get("kpis", {}).get("control_funnel", []))
    testing_df = pd.DataFrame(payload.get("kpis", {}).get("testing_funnel", []))

    def _get_value(df: pd.DataFrame, kpi: str, col: str) -> float | None:
        return _extract_metric_value(df, kpi, col)

    def _fmt_pct(v: float | None) -> str:
        if v is None:
            return "N/A"
        return f"{v:.2%}"

    def _fmt_abs(v: float | None) -> str:
        if v is None:
            return "N/A"
        return f"{v:,.0f}"

    def _signed_word(value: float | None, up_word: str = "increased", down_word: str = "decreased") -> str:
        if value is None:
            return "changed marginally"
        return up_word if value >= 0 else down_word

    def _build_group_lines(df: pd.DataFrame, label: str, group_name: str) -> list[str]:
        revenue_pct = _get_value(df, "total revenue", PROMO_VS_BASELINE_COL)
        absorb_pct = _get_value(df, "store absorption rate", PROMO_VS_BASELINE_COL)
        order_pct = _get_value(df, "total orders", PROMO_VS_BASELINE_COL)
        aov_pct = _get_value(df, "AOV", PROMO_VS_BASELINE_COL)
        qty_pct = _get_value(df, "total quantity", PROMO_VS_BASELINE_COL)
        ppi_pct = _get_value(df, "price per item", PROMO_VS_BASELINE_COL)
        existing_pct = _get_value(df, "existing revenue", PROMO_VS_BASELINE_COL)
        total_abs = _get_value(df, "total revenue", "Abs Diff (Promo - Baseline)")
        existing_abs = _get_value(df, "existing revenue", "Abs Diff (Promo - Baseline)")
        new_non_existing_abs = None
        if total_abs is not None and existing_abs is not None:
            new_non_existing_abs = total_abs - existing_abs

        funnel_order_sentence = (
            f"- Funnel view (Order level): {label} ({group_name}) total revenue "
            f"{_signed_word(revenue_pct)} by {_fmt_pct(revenue_pct)} vs Baseline Period. The primary driver is "
            f"store absorption rate, which {_signed_word(absorb_pct)} by {_fmt_pct(absorb_pct)}, lifting total orders "
            f"to {_fmt_pct(order_pct)}. AOV was {_fmt_pct(aov_pct)}, which "
            f"{'supported' if (aov_pct or 0) >= 0 else 'partly offset'} the revenue outcome."
        )

        funnel_item_sentence = (
            f"- Funnel view (Item level): {label} total revenue {_signed_word(revenue_pct)} by {_fmt_pct(revenue_pct)} "
            f"vs Baseline Period, with total quantity {_signed_word(qty_pct)} by {_fmt_pct(qty_pct)} and "
            f"price per item {_signed_word(ppi_pct)} by {_fmt_pct(ppi_pct)}. Together, these explain the item-level "
            f"revenue movement."
        )

        if existing_abs is None or new_non_existing_abs is None:
            component_sentence = (
                "- Component shift view: Existing-insider vs new/non-insider split is unavailable, so structural "
                "source attribution is not reliable for this group."
            )
        else:
            existing_word = "increased" if existing_abs >= 0 else "decreased"
            new_non_word = "increased" if new_non_existing_abs >= 0 else "decreased"
            component_sentence = (
                f"- Component shift view: Total revenue {_signed_word(revenue_pct)} by {_fmt_pct(revenue_pct)} vs Baseline Period. "
                f"Existing insider contribution (existing revenue) {existing_word} by {_fmt_pct(existing_pct)} "
                f"(Abs: {_fmt_abs(existing_abs)}). In contrast, new + non-insider revenue "
                f"(= total revenue - existing revenue) {new_non_word} by {_fmt_abs(new_non_existing_abs)} (Abs), "
                f"showing the mix-shift impact across customer components."
            )

        return [
            f"## {label} drivers (% Diff: Promo vs Baseline)",
            *[funnel_order_sentence, funnel_item_sentence, component_sentence],
            "",
        ]

    lines = [
        "# Campaign Post-Mortem Summary (Phase 1)",
        "",
        f"- Baseline Period: {', '.join(meta.get('baseline_dates', []))}",
        f"- Promo Period: {', '.join(meta.get('promo_dates', []))}",
        "",
    ]
    lines.extend(_build_group_lines(control_df, "Group A", control_group))
    lines.extend(_build_group_lines(testing_df, "Group B", testing_group))
    lines.append("---")
    lines.append("Note: This summary is generated from deterministic KPI rules and templates.")
    return "\n".join(lines)
