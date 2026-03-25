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
    winner_assessment = payload.get("winner_assessment", {})

    control_group = meta.get("control_group", {}).get("name", "Control")
    testing_group = meta.get("testing_group", {}).get("name", "Testing")
    winner = winner_assessment.get("winner", "tie")
    confidence = winner_assessment.get("confidence", "low")

    if winner == "testing":
        verdict = f"{testing_group} wins"
    elif winner == "control":
        verdict = f"{control_group} wins"
    else:
        verdict = "No clear winner (tie)"

    lines = [
        "# Campaign Post-Mortem Summary (Phase 1)",
        "",
        f"- Verdict: **{verdict}**",
        f"- Confidence: **{confidence}**",
        f"- Baseline Period: {', '.join(meta.get('baseline_dates', []))}",
        f"- Promo Period: {', '.join(meta.get('promo_dates', []))}",
        "",
        "## Deterministic winner checks",
    ]

    for item in winner_assessment.get("comparisons", []):
        uplift = item.get("uplift")
        uplift_text = "N/A" if uplift is None else f"{uplift:.2%}"
        lines.append(
            f"- {item.get('kpi')}: testing-control uplift = {uplift_text} ({item.get('decision')})"
        )

    driver_analysis = payload.get("phase1_driver_analysis", {})
    control_drivers = driver_analysis.get("control_group_drivers", {}).get("top_absolute", [])
    testing_drivers = driver_analysis.get("testing_group_drivers", {}).get("top_absolute", [])
    promo_impact_drivers = driver_analysis.get("promo_impact_drivers", {}).get("top_absolute", [])

    lines.append("")
    lines.append(f"## {control_group} drivers (% Diff: Promo vs Baseline)")
    if control_drivers:
        for item in control_drivers:
            lines.append(f"- {item.get('kpi')}: {float(item.get('value', 0.0)):.2%}")
    else:
        lines.append("- N/A")

    lines.append("")
    lines.append(f"## {testing_group} drivers (% Diff: Promo vs Baseline)")
    if testing_drivers:
        for item in testing_drivers:
            lines.append(f"- {item.get('kpi')}: {float(item.get('value', 0.0)):.2%}")
    else:
        lines.append("- N/A")

    lines.append("")
    lines.append("## Promo Impact drivers (%Diff gap: Group A - Group B)")
    if promo_impact_drivers:
        for item in promo_impact_drivers:
            lines.append(f"- {item.get('kpi')}: {float(item.get('value', 0.0)):.2%}")
    else:
        lines.append("- N/A")

    lines.append("")
    lines.append("---")
    lines.append("Note: This summary is generated from deterministic metrics; no autonomous agent reasoning is used.")

    return "\n".join(lines)