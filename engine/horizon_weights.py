"""
Horizon-aware opportunity-score weights for long-term holding periods.
"""
from __future__ import annotations

from typing import Dict

from .config import BEAR_OPPORTUNITY_WEIGHTS, OPPORTUNITY_WEIGHTS

VALID_ENGINE_HORIZONS = ("6m", "1y", "3y", "5y", "10y")

# Per-horizon multipliers applied to base OPPORTUNITY_WEIGHTS keys, then renormalized.
HORIZON_CARD_MULTIPLIERS: Dict[str, Dict[str, float]] = {
    "6m": {
        "performance": 1.05,
        "valuation": 1.15,
        "entry_point": 1.25,
        "growth": 0.85,
        "profitability": 0.90,
        "contrarian": 1.0,
    },
    "1y": {
        "performance": 1.0,
        "valuation": 1.10,
        "entry_point": 1.10,
        "growth": 1.0,
        "profitability": 1.0,
        "contrarian": 1.0,
    },
    "3y": {
        "performance": 0.95,
        "valuation": 1.0,
        "entry_point": 0.95,
        "growth": 1.15,
        "profitability": 1.10,
        "contrarian": 1.0,
    },
    "5y": {
        "performance": 0.90,
        "valuation": 0.90,
        "entry_point": 0.85,
        "growth": 1.20,
        "profitability": 1.20,
        "contrarian": 0.95,
    },
    "10y": {
        "performance": 0.85,
        "valuation": 0.85,
        "entry_point": 0.80,
        "growth": 1.25,
        "profitability": 1.30,
        "contrarian": 0.90,
    },
}


def normalize_horizon(value: str | None) -> str:
    horizon = (value or "1y").strip().lower()
    if horizon not in VALID_ENGINE_HORIZONS:
        raise ValueError(f"investment_horizon must be one of {VALID_ENGINE_HORIZONS}")
    return horizon


def get_opportunity_weights(market_mode: str, investment_horizon: str | None = None) -> Dict[str, float]:
    """Return normalized opportunity card weights for market regime and horizon."""
    base = dict(
        BEAR_OPPORTUNITY_WEIGHTS if market_mode == "bear" else OPPORTUNITY_WEIGHTS
    )
    horizon = normalize_horizon(investment_horizon)
    multipliers = HORIZON_CARD_MULTIPLIERS.get(horizon, {})
    scaled = {k: base[k] * multipliers.get(k, 1.0) for k in base}
    total = sum(scaled.values())
    if total <= 0:
        return base
    return {k: round(v / total, 6) for k, v in scaled.items()}
