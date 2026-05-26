"""
Lightweight macro/regime context for each run (does not change scores).

Documents how the engine interpreted the market environment and what
analysts should verify manually for shocks (COVID-style events are not
named automatically — only price/breadth footprints).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


REGIME_GUIDANCE = {
    "bear": (
        "Bear regime: stricter buy gates and higher score thresholds are active. "
        "Prefer quality balance sheets; verify pledge, governance, and liquidity. "
        "Consider delayed entry until breadth improves."
    ),
    "bull": (
        "Bull regime: more names may pass watch thresholds. "
        "Still require full critical-risk evidence before any Buy Candidate label."
    ),
    "neutral": (
        "Neutral regime: stock selection depends on sector and individual gates. "
        "Cross-check macro headlines (rates, oil, geopolitics) manually."
    ),
}


SHOCK_CHECKLIST = [
    "Review RBI rate path and INR vs USD for FII-sensitive sectors.",
    "Check Union Budget / election / major policy headlines for sector beneficiaries.",
    "For IT/pharma exporters: US/EU demand and client commentary on calls.",
    "For banks/NBFCs: credit growth, GNPA trajectory, liquidity events.",
    "For commodities/industrials: input costs (oil, metals) and margin pass-through.",
    "Stress-test thesis: what happens if Nifty drawdown exceeds 15% from here?",
    "Confirm company-specific news (not just index move) before upgrading Watchlist to Buy.",
]


def build_macro_context(
    *,
    market_mode: str,
    market_regime_source: str,
    market_regime_confidence: str,
    run_date: str,
    policy_themes: tuple[str, ...] = (),
) -> Dict[str, Any]:
    """Return JSON-serializable macro notes for analysts (informational only)."""
    mode = (market_mode or "neutral").lower()
    return {
        "run_date": run_date,
        "market_mode": mode,
        "market_regime_source": market_regime_source,
        "market_regime_confidence": market_regime_confidence,
        "regime_guidance": REGIME_GUIDANCE.get(mode, REGIME_GUIDANCE["neutral"]),
        "policy_themes_active": list(policy_themes),
        "shock_event_modeling": (
            "No explicit COVID/war/election calendar in engine. "
            "Regime and drawdown metrics reflect outcomes in prices only."
        ),
        "manual_shock_checklist": list(SHOCK_CHECKLIST),
        "buy_candidate_reminder": (
            "Buy Candidate requires score merit + gate pass + actionable data. "
            "Empty buy_candidates.csv is normal when pledge/governance fields are missing."
        ),
    }
