"""
Streamlit dashboard: column-level filtering and display helpers.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Columns excluded from interactive filters (identifiers / long text)
SKIP_FILTER_COLUMNS = frozenset({
    "name",
    "Name",
    "gate_fail_reasons",
    "Gate Fail Reasons",
    "missing_critical_fields",
    "Missing Critical Fields",
    "recommendation_reasons",
    "Recommendation Reasons",
    "risk_flags",
    "Risk Flags",
    "reason_codes",
    "Reason Codes",
    "metric_source_summary",
    "Metric Sources",
    "analysis_caveat",
    "Analysis Caveat",
    "user_filter_reasons",
    "staged_entry_plan",
    "action_note",
})

BOOLEAN_COLUMNS = frozenset({
    "gate_passed",
    "Gate Passed",
    "template_supported",
    "Template Supported",
    "user_filter_passed",
    "User Filter Passed",
})

CATEGORICAL_MAX_UNIQUE = 40


def leaderboard_to_display_df(lb: pd.DataFrame) -> pd.DataFrame:
    """Rename snake_case leaderboard columns to readable Title Case."""
    rename = {
        "ticker": "Ticker",
        "name": "Name",
        "sector": "Sector",
        "basic_industry": "Industry",
        "template": "Template",
        "peer_level": "Peer Level",
        "performance": "Performance",
        "valuation": "Valuation",
        "growth": "Growth",
        "profitability": "Profitability",
        "entry_point": "Entry Point",
        "contrarian": "Contrarian",
        "red_flags": "Red Flags",
        "opportunity_score": "Score",
        "investability_status": "Investability",
        "potential_score": "Potential",
        "valuation_gap_score": "Valuation Gap",
        "recommendation": "Recommendation",
        "confidence": "Confidence",
        "confidence_score": "Confidence Score",
        "research_status": "Research Status",
        "research_tier": "Research Tier",
        "data_quality_status": "Data Quality",
        "data_quality_score": "Data Quality Score",
        "expected_upside_pct": "Upside %",
        "expected_downside_pct": "Downside %",
        "risk_reward_ratio": "Risk/Reward",
        "risk_reward_score": "Risk Reward Score",
        "selection_score": "Selection",
        "gate_passed": "Gate Passed",
        "gate_fail_reasons": "Gate Fail Reasons",
        "peer_group_quality": "Peer Quality",
        "template_supported": "Template Supported",
        "market_mode": "Market Mode",
        "market_regime_source": "Market Source",
        "sector_regime_label": "Sector Regime",
        "value_trap_score": "Value Trap",
        "entry_signal": "Entry Signal",
        "user_profile": "Profile",
        "user_filter_passed": "Profile Passed",
        "missing_critical_fields": "Missing Critical Fields",
        "shortlist_rank_score": "Shortlist Score",
        "shortlist_caveat": "Shortlist Caveat",
    }
    out = lb.rename(columns={k: v for k, v in rename.items() if k in lb.columns})
    for col in out.columns:
        if col in BOOLEAN_COLUMNS or col == "Gate Passed":
            out[col] = out[col].map(
                lambda x: True if x is True or str(x).lower() in {"true", "1", "yes"} else (
                    False if x is False or str(x).lower() in {"false", "0", "no"} else pd.NA
                )
            )
    numeric_hints = [
        "Score", "Selection", "Performance", "Valuation", "Growth", "Profitability",
        "Entry Point", "Contrarian", "Red Flags", "Upside %", "Downside %",
        "Risk/Reward", "Potential", "Valuation Gap", "Data Quality Score",
        "Value Trap", "Shortlist Score", "Confidence Score",
    ]
    for col in numeric_hints:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _is_numeric_series(series: pd.Series) -> bool:
    if pd.api.types.is_numeric_dtype(series):
        return True
    coerced = pd.to_numeric(series, errors="coerce")
    return coerced.notna().sum() >= max(3, int(0.6 * series.notna().sum()))


def apply_column_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    """Apply per-column filters from session state."""
    if df.empty or not filters:
        return df
    mask = pd.Series(True, index=df.index)
    for col, spec in filters.items():
        if col not in df.columns or not spec:
            continue
        kind = spec.get("kind")
        if kind == "categorical":
            selected = spec.get("values") or []
            if selected:
                mask &= df[col].astype(str).isin([str(v) for v in selected])
        elif kind == "boolean":
            want = spec.get("value")
            if want is not None:
                mask &= df[col] == want
        elif kind == "numeric":
            lo = spec.get("min")
            hi = spec.get("max")
            numeric = pd.to_numeric(df[col], errors="coerce")
            if lo is not None:
                mask &= numeric.fillna(-1e18) >= lo
            if hi is not None:
                mask &= numeric.fillna(1e18) <= hi
        elif kind == "text":
            text = (spec.get("contains") or "").strip().lower()
            if text:
                mask &= df[col].astype(str).str.lower().str.contains(text, na=False)
    return df.loc[mask]


def detect_column_filter_kind(series: pd.Series, col_name: str) -> str:
    if col_name in SKIP_FILTER_COLUMNS:
        return "skip"
    if col_name in BOOLEAN_COLUMNS:
        return "boolean"
    if _is_numeric_series(series):
        return "numeric"
    nunique = series.dropna().astype(str).nunique()
    if nunique <= CATEGORICAL_MAX_UNIQUE:
        return "categorical"
    return "text"


def score_color(val):
    if val is None or pd.isna(val):
        return "background-color: #f0f0f0"
    if val >= 70:
        return "background-color: #c6efce; color: #006100"
    if val >= 50:
        return "background-color: #ffeb9c; color: #9c6500"
    return "background-color: #ffc7ce; color: #9c0006"


SCORE_STYLE_COLUMNS = frozenset({
    "Score", "Performance", "Valuation", "Growth", "Profitability",
    "Entry Point", "Contrarian", "Red Flags", "Potential", "Valuation Gap",
    "Selection", "Shortlist Score",
})
