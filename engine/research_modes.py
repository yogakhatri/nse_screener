"""
Research output modes and return personas for long-term shortlists.

Modes control how strict the engine is when surfacing 2–3 names per search.
Personas adjust ranking toward compounders, quality/value, or steady income.
"""
from __future__ import annotations

from typing import Any, Dict, FrozenSet, Tuple

VALID_RESEARCH_MODES = ("high_conviction", "research_shortlist", "thematic")
VALID_RETURN_PERSONAS = ("compounder", "quality_value", "steady_income")

# Primary shortlist size (user-facing "search" result).
TOP_PICK_COUNT: int = 3
# Secondary tier for manual follow-up.
NEXT_TIER_COUNT: int = 5

MODE_CONFIG: Dict[str, Dict[str, Any]] = {
    "high_conviction": {
        "label": "High conviction",
        "description": "Only names with full gates, actionable data, and high-confidence tier.",
        "allowed_tiers": frozenset({"High Confidence Research"}),
        "require_gate_passed": True,
        "allow_data_incomplete_primary": False,
        "min_confidence_rank": 1,  # Medium+
    },
    "research_shortlist": {
        "label": "Research shortlist",
        "description": "Strong fundamentals with explicit caveats when risk evidence is partial.",
        "allowed_tiers": frozenset({"High Confidence Research", "Qualified Watchlist"}),
        "require_gate_passed": False,
        "allow_data_incomplete_primary": False,
        "min_confidence_rank": 0,  # Low+
    },
    "thematic": {
        "label": "Thematic / policy watch",
        "description": "Sector beneficiaries from configured policy themes; verify filings manually.",
        "allowed_tiers": frozenset({"High Confidence Research", "Qualified Watchlist", "Data Incomplete"}),
        "require_gate_passed": False,
        "allow_data_incomplete_primary": True,
        "min_confidence_rank": 0,
        "require_policy_theme_match": True,
    },
}

PERSONA_CONFIG: Dict[str, Dict[str, Any]] = {
    "compounder": {
        "label": "Compounder",
        "description": "High ROCE, sustained growth, strong red-flag profile for 3–10 year holds.",
        "rank_boost": {"potential_score": 0.08, "profitability": 0.06, "growth": 0.06},
        "min_roce": 12.0,
        "min_rev_cagr_3y": 8.0,
        "min_red_flags_score": 60.0,
    },
    "quality_value": {
        "label": "Quality + value",
        "description": "Balanced quality, valuation gap, and risk/reward for 1–5 year holds.",
        "rank_boost": {"valuation_gap_score": 0.08, "selection_score": 0.06, "risk_reward_score": 0.04},
    },
    "steady_income": {
        "label": "Steady / income",
        "description": "Dividend, balance-sheet stability, lower volatility character (not guaranteed yield).",
        "rank_boost": {"red_flags": 0.10, "dividend_yield_score": 0.08, "profitability": 0.04},
        "min_dividend_yield": 0.8,
        "max_debt_to_equity": 1.5,
    },
}


def normalize_research_mode(value: str | None) -> str:
    mode = (value or "research_shortlist").strip().lower()
    if mode not in VALID_RESEARCH_MODES:
        raise ValueError(f"research_mode must be one of {VALID_RESEARCH_MODES}")
    return mode


def normalize_return_persona(value: str | None) -> str:
    persona = (value or "quality_value").strip().lower()
    if persona not in VALID_RETURN_PERSONAS:
        raise ValueError(f"return_persona must be one of {VALID_RETURN_PERSONAS}")
    return persona


def mode_config(mode: str) -> Dict[str, Any]:
    return MODE_CONFIG[normalize_research_mode(mode)]


def persona_config(persona: str) -> Dict[str, Any]:
    return PERSONA_CONFIG[normalize_return_persona(persona)]
