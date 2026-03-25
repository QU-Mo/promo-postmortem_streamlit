from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


DEFAULT_PRIMARY_KPIS = (
    "total revenue",
    "total PC1",
    "margin",
)


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

    promo_impact = payload.get("kpis", {}).get("promo_impact", [])
    impact_col = "Promo Impact (Group A %Diff - Group B %Diff )"
    top_impact_rows = []
    for row in promo_impact:
        impact_value = _safe_float(row.get(impact_col))
        if impact_value is None:
            continue
        top_impact_rows.append((row.get("KPI", "Unknown KPI"), impact_value))

    top_impact_rows.sort(key=lambda item: abs(item[1]), reverse=True)
    if top_impact_rows:
        lines.append("")
        lines.append("## Top KPI deltas (absolute)")
        for kpi, impact in top_impact_rows[:5]:
            lines.append(f"- {kpi}: {impact:.2%}")

    lines.append("")
    lines.append("---")
    lines.append("Note: This summary is generated from deterministic metrics; no autonomous agent reasoning is used.")

    return "\n".join(lines)