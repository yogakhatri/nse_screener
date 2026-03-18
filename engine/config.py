"""
NSE Stock Rating Engine — Configuration
=======================================

This file is intentionally verbose and heavily commented so teammates can
change values safely and understand impact before doing so.

How to use this file
--------------------
1) Change one logical block at a time (for example only risk gates).
2) Run a dry run and compare:
   - buy_candidates count
   - sector concentration
   - avoid/uninvestable count
3) Log weight/rule changes in your run notes.

Quick impact guide
------------------
- Increase a threshold -> stricter filter -> fewer stocks pass.
- Increase a weight    -> that component influences rank more.
- Tighten caps/gates   -> safer portfolio, lower turnover, fewer names.
- Relax caps/gates     -> more names, higher risk of false positives.
"""
from __future__ import annotations

from typing import FrozenSet

# ============================================================================
# Core Scoring and Data Coverage Controls
# ============================================================================

# Minimum peer population at each fallback level.
# Higher values improve percentile stability but can force broader peer buckets.
PEER_MIN_BASIC_INDUSTRY: int = 8
PEER_MIN_INDUSTRY: int = 5

# Minimum trading history (used by upstream loaders/adapters).
# Raise this to avoid newly listed volatility; lower it to include fresh IPOs.
MIN_TRADING_DAYS: int = 252

# Required weighted coverage on a card to mark it rankable.
# 0.65 is a practical balance for India small/mid-caps with patchy coverage.
# Moving to 0.75+ will materially reduce the rankable universe.
CARD_DATA_THRESHOLD: float = 0.65

# A stock must have at least this many rankable cards out of 6.
# Raising this value increases reliability but will remove more names.
MIN_RANKABLE_CARDS: int = 4

# Production publish gate.
# These controls operate at TEMPLATE level, not just whole-universe level.
# Goal: if one template (for example Banks) is missing most of its cards,
# we do not quietly publish outputs that make that template look supported.
#
# Interpretation:
# - Each active template must have all configured core cards present at least at
#   MIN_TEMPLATE_CARD_RANKABLE_PCT rankable coverage.
# - Each active template must also have red-flags coverage above the threshold,
#   because recommendations without risk coverage are not publishable.
# - If a template fails, production runs should stop unless the operator
#   explicitly uses debug/skip-quality-gate mode.
MIN_TEMPLATE_CARD_RANKABLE_PCT: float = 25.0
MIN_TEMPLATE_RED_FLAGS_RANKABLE_PCT: float = 25.0
MIN_TEMPLATE_AVG_CORE_RANKABLE_PCT: float = 55.0
QUALITY_GATE_REQUIRE_ALL_CORE_CARDS: bool = True

# Raw price-derived metric controls.
# When enabled, locally cached bhavcopy history becomes the preferred source for
# price/technical metrics. CSV values are used only when raw history is missing.
ENABLE_RAW_PRICE_METRICS: bool = True
PRICE_HISTORY_LOOKBACK_SESSIONS: int = 1300
RAW_PRICE_METRIC_FALLBACK_TO_CSV: bool = True

# Outlier clipping for peer distributions before percentile scoring.
# Narrower bounds (e.g. 0.05/0.95) reduce tail influence more aggressively.
WINSORIZE_LOWER: float = 0.03
WINSORIZE_UPPER: float = 0.97

# ============================================================================
# Market Regime and Down-Market Behavior
# ============================================================================

# Default mode used when not specified at runtime.
# Choices: "bull" | "neutral" | "bear" | "auto"
DEFAULT_MARKET_MODE: str = "auto"

# Auto mode signal thresholds (simple but robust with available fields).
# If median 6M return <= -8% OR median drawdown recovery <= 45 -> bear.
# If median 6M return >= +8% AND median drawdown recovery >= 60 -> bull.
AUTO_BEAR_RETURN_6M_THRESHOLD: float = -8.0
AUTO_BEAR_DRAWDOWN_RECOVERY_THRESHOLD: float = 45.0
AUTO_BULL_RETURN_6M_THRESHOLD: float = 8.0
AUTO_BULL_DRAWDOWN_RECOVERY_THRESHOLD: float = 60.0

# In bear markets, we require higher quality and stronger gate checks.
BEAR_MODE_QUALITY_BONUS: float = 1.10   # Multiplies quality contribution.
BEAR_MODE_RISK_PENALTY: float = 1.15    # Multiplies red-flag penalty.

# ============================================================================
# Template Routing (taxonomy)
# ============================================================================

# Update these sets if NSE basic industry labels in your source change.
# If labels drift and are not updated here, template routing quality degrades.
TEMPLATE_BANKS: FrozenSet[str] = frozenset([
    "Private Sector Bank",
    "Public Sector Bank",
    "Foreign Bank",
    "Bank - Private",
    "Bank - Public",
])

TEMPLATE_NBFC: FrozenSet[str] = frozenset([
    "Finance - NBFC",
    "Housing Finance",
    "Micro Finance",
    "NBFC",
    "HFC",
    "Asset Finance Company",
])

# ============================================================================
# Card Weights by Template
# ============================================================================
# NOTE:
# - Weights in each card must sum to 1.0.
# - In this tuned profile, quality and cash-flow durability have more influence,
#   especially useful during corrections.

CARD_WEIGHTS = {
    "A": {
        "performance": {
            "return_1y": 0.22,
            "return_6m": 0.13,
            "cagr_5y": 0.12,
            "peer_price_strength": 0.20,
            "drawdown_recovery": 0.18,
            "forward_view": 0.15,
        },
        "valuation": {
            "pe_percentile": 0.17,
            "pb_percentile": 0.10,
            "p_cfo_percentile": 0.20,
            "ev_ebitda_percentile": 0.14,
            "hist_val_band": 0.14,
            "fcf_yield": 0.13,
            "iv_gap": 0.12,
        },
        "growth": {
            "rev_cagr_3y": 0.18,
            "eps_cagr_3y": 0.18,
            "rev_growth_yoy": 0.14,
            "eps_growth_yoy": 0.14,
            "peer_growth_rank": 0.16,
            "growth_stability": 0.20,
        },
        "profitability": {
            "roce_3y_median": 0.20,
            "ebitda_margin": 0.17,
            "cfo_pat_ratio": 0.20,
            "margin_trend": 0.13,
            "roa": 0.10,
            "fcf_consistency": 0.20,
        },
        "entry_point": {
            "discount_to_iv": 0.29,
            "rsi_state": 0.10,
            "price_vs_200dma": 0.15,
            "price_vs_50dma": 0.10,
            "volume_delivery": 0.14,
            "rs_turn": 0.11,
            "volatility_compression": 0.11,
        },
        "red_flags": {
            "promoter_pledge": 0.18,
            "asm_gsm_risk": 0.22,
            "default_distress": 0.23,
            "accounting_quality": 0.15,
            "liquidity_manipulation": 0.12,
            "governance_event": 0.10,
        },
    },
    "B": {
        "performance": {
            "return_1y": 0.23,
            "return_6m": 0.14,
            "cagr_5y": 0.10,
            "peer_price_strength": 0.19,
            "drawdown_recovery": 0.17,
            "forward_view": 0.17,
        },
        "valuation": {
            "pb_percentile": 0.33,
            "roe_adj_pb": 0.21,
            "pe_percentile": 0.08,
            "hist_pb_band": 0.20,
            "fair_value_gap": 0.18,
        },
        "growth": {
            "advances_growth": 0.23,
            "deposit_growth": 0.19,
            "nii_growth": 0.20,
            "fee_income_growth": 0.11,
            "earnings_growth": 0.15,
            "growth_stability": 0.12,
        },
        "profitability": {
            "roa": 0.20,
            "roe": 0.18,
            "nim": 0.20,
            "cost_to_income": 0.11,
            "provision_coverage": 0.16,
            "credit_cost_discipline": 0.15,
        },
        "entry_point": {
            "discount_to_fair_pb": 0.30,
            "rsi_state": 0.10,
            "price_vs_200dma": 0.15,
            "price_vs_50dma": 0.10,
            "volume": 0.10,
            "rs_turn": 0.15,
            "drawdown_normalization": 0.10,
        },
        "red_flags": {
            "gnpa_nnpa_stress": 0.20,
            "pcr_weakness": 0.16,
            "capital_adequacy_stress": 0.16,
            "slippages_stress": 0.15,
            "governance_promoter": 0.10,
            "surveillance_default": 0.23,
        },
    },
    "C": {
        "performance": {
            "return_1y": 0.23,
            "return_6m": 0.14,
            "cagr_5y": 0.10,
            "peer_price_strength": 0.19,
            "drawdown_recovery": 0.17,
            "forward_view": 0.17,
        },
        "valuation": {
            "pb_percentile": 0.33,
            "roe_adj_pb": 0.21,
            "pe_percentile": 0.08,
            "hist_pb_band": 0.20,
            "fair_value_gap": 0.18,
        },
        "growth": {
            "aum_growth": 0.23,
            "advances_growth": 0.19,
            "nii_growth": 0.20,
            "fee_income_growth": 0.11,
            "earnings_growth": 0.15,
            "growth_stability": 0.12,
        },
        "profitability": {
            "roa": 0.20,
            "roe": 0.18,
            "nim": 0.20,
            "cost_to_income": 0.11,
            "provision_coverage": 0.16,
            "credit_cost_discipline": 0.15,
        },
        "entry_point": {
            "discount_to_fair_pb": 0.30,
            "rsi_state": 0.10,
            "price_vs_200dma": 0.15,
            "price_vs_50dma": 0.10,
            "volume": 0.10,
            "rs_turn": 0.15,
            "drawdown_normalization": 0.10,
        },
        "red_flags": {
            "gnpa_nnpa_stress": 0.20,
            "alm_mismatch": 0.16,
            "capital_adequacy_stress": 0.16,
            "slippages_stress": 0.15,
            "governance_promoter": 0.10,
            "surveillance_default": 0.23,
        },
    },
}

# ============================================================================
# Composite Score Weights
# ============================================================================

# Final opportunity score (red flags are applied as cap/penalty separately).
# In down markets, profitability + valuation are the best drawdown protectors,
# so they have slightly higher influence.
OPPORTUNITY_WEIGHTS = {
    "valuation": 0.24,
    "growth": 0.20,
    "profitability": 0.24,
    "entry_point": 0.16,
    "performance": 0.16,
}

# Potential-first lens (long-term compounding quality).
POTENTIAL_SCORE_WEIGHTS = {
    "growth": 0.34,
    "profitability": 0.32,
    "performance": 0.16,
    "red_flags": 0.18,
}

# Valuation and timing lens (how much upside from current levels).
VALUATION_GAP_SCORE_WEIGHTS = {
    "valuation": 0.72,
    "entry_point": 0.28,
}

# Sector regime composite.
# Higher value means a sector has stronger internals in current tape.
SECTOR_REGIME_WEIGHTS = {
    "performance": 0.30,
    "growth": 0.25,
    "profitability": 0.25,
    "red_flags": 0.20,
}

# Selection score used for final sorting (reward/risk aware).
SELECTION_SCORE_WEIGHTS = {
    "opportunity_score": 0.40,
    "potential_score": 0.30,
    "risk_reward_score": 0.30,
}

# ============================================================================
# Recommendation and Risk/Reward Settings
# ============================================================================

# Converts confidence score (0-100) to qualitative bucket.
CONFIDENCE_HIGH_THRESHOLD: float = 85.0
CONFIDENCE_MEDIUM_THRESHOLD: float = 65.0

# Buy/watch thresholds.
# In bear mode we intentionally require stronger potential + valuation.
BUY_POTENTIAL_THRESHOLD: float = 72.0
BUY_VALUATION_GAP_THRESHOLD: float = 66.0
WATCH_POTENTIAL_THRESHOLD: float = 56.0
WATCH_VALUATION_GAP_THRESHOLD: float = 46.0

# Expected downside floor/cap to avoid unstable extremes.
MIN_EXPECTED_DOWNSIDE_PCT: float = 5.0
MAX_EXPECTED_DOWNSIDE_PCT: float = 35.0

# Upside confidence penalties:
# lower valuation confidence / lower quality -> upside haircut.
UPSIDE_HAIRCUT_LOW_CONFIDENCE: float = 0.80
UPSIDE_HAIRCUT_MEDIUM_CONFIDENCE: float = 0.90

# ============================================================================
# Investability Gate (hard filters before "Buy Candidate")
# ============================================================================

# Any failure here downgrades recommendation even if scores look attractive.
GATE_MIN_RED_FLAGS_SCORE: float = 45.0
GATE_MIN_CONFIDENCE_FOR_BUY: str = "Medium"   # "Low" | "Medium" | "High"
GATE_MIN_LIQUIDITY_TURNOVER_CR: float = 1.0
GATE_MAX_PROMOTER_PLEDGE_PCT: float = 35.0
GATE_MAX_DEFAULT_DISTRESS_RISK: float = 60.0

# Bear-market overrides for stricter filtering.
BEAR_GATE_MIN_RED_FLAGS_SCORE: float = 55.0
BEAR_GATE_MAX_PROMOTER_PLEDGE_PCT: float = 25.0
BEAR_GATE_MAX_DEFAULT_DISTRESS_RISK: float = 45.0

# ============================================================================
# Entry Staging and Execution Risk
# ============================================================================

# Position staging percentages for 3-tranche entries.
# Sum should be 100.
ENTRY_STAGE_WEIGHTS = (40, 35, 25)

# Stop-loss guidance envelope. Used only for action-sheet suggestions.
MIN_STOP_LOSS_PCT: float = 8.0
MAX_STOP_LOSS_PCT: float = 18.0

# ============================================================================
# Portfolio Construction Controls
# ============================================================================

# Keep this concentrated enough for alpha but diversified for risk.
PORTFOLIO_MAX_HOLDINGS: int = 18

# Max capital per stock and per sector in suggested portfolio.
PORTFOLIO_MAX_SINGLE_STOCK_WEIGHT_PCT: float = 8.0
PORTFOLIO_MAX_SECTOR_WEIGHT_PCT: float = 28.0

# Minimum confidence to include in suggested portfolio.
PORTFOLIO_MIN_CONFIDENCE: str = "Medium"

# ============================================================================
# Monitoring and Recalibration
# ============================================================================

# Recommendation outcome tracking horizon in trading days.
OUTCOME_HORIZON_DAYS: int = 30

# If hit rate for Buy candidates drops below this level, flag recalibration.
BUY_HIT_RATE_ALERT_THRESHOLD: float = 0.50

# ============================================================================
# Labels and Status Maps
# ============================================================================

CARD_LABELS = {
    "performance": [
        (0, 20, "Very Low"),
        (20, 40, "Low"),
        (40, 60, "Neutral"),
        (60, 80, "Good"),
        (80, 100, "High"),
    ],
    "growth": [
        (0, 20, "Very Low"),
        (20, 40, "Low"),
        (40, 60, "Neutral"),
        (60, 80, "Good"),
        (80, 100, "High"),
    ],
    "profitability": [
        (0, 20, "Very Low"),
        (20, 40, "Low"),
        (40, 60, "Neutral"),
        (60, 80, "Good"),
        (80, 100, "High"),
    ],
    "valuation": [
        (0, 20, "Very Expensive"),
        (20, 40, "Expensive"),
        (40, 60, "Fair"),
        (60, 80, "Attractive"),
        (80, 100, "Very Attractive"),
    ],
    "entry_point": [
        (0, 20, "Poor"),
        (20, 40, "Weak"),
        (40, 60, "Neutral"),
        (60, 80, "Good"),
        (80, 100, "Strong"),
    ],
    "red_flags": [
        (0, 20, "Severe"),
        (20, 40, "High"),
        (40, 60, "Moderate"),
        (60, 80, "Low"),
        (80, 100, "None"),
    ],
}

# Red flag caps map red-flag card score to max allowed opportunity score.
RED_FLAG_CAPS = [
    (0, 20, None, "Uninvestable"),
    (20, 40, 40.0, "Avoid"),
    (40, 60, 60.0, "Watchlist"),
    (60, 80, 80.0, "Investable"),
    (80, 100, None, "Investable"),
]

INVESTABILITY = [
    (70, 100, "Investable"),
    (50, 70, "Watchlist"),
    (0, 50, "Avoid"),
]


# ============================================================================
# Runtime Helpers and Validation
# ============================================================================

SUPPORTED_TEMPLATE_CODES = ("A", "B", "C")
SUPPORTED_CARD_NAMES = (
    "performance",
    "valuation",
    "growth",
    "profitability",
    "entry_point",
    "red_flags",
)
TEMPLATE_DISPLAY_NAMES = {
    "A": "General",
    "B": "Bank",
    "C": "NBFC/HFC",
}
CONFIDENCE_LEVELS = ("Low", "Medium", "High")


def configured_template_codes() -> tuple[str, ...]:
    return tuple(CARD_WEIGHTS.keys())


def configured_core_cards() -> tuple[str, ...]:
    return tuple(OPPORTUNITY_WEIGHTS.keys())


def infer_template_code_from_basic_industry(basic_industry: str) -> str:
    if basic_industry in TEMPLATE_BANKS:
        return "B"
    if basic_industry in TEMPLATE_NBFC:
        return "C"
    return "A"


def template_display_name(template_code: str) -> str:
    return TEMPLATE_DISPLAY_NAMES.get(str(template_code), str(template_code))


def validate_runtime_config() -> None:
    def _assert_close(name: str, weights: dict[str, float], target: float = 1.0) -> None:
        total = round(sum(float(v) for v in weights.values()), 8)
        if abs(total - target) > 1e-6:
            raise ValueError(f"{name} must sum to {target}, found {total}")

    template_codes = configured_template_codes()
    unsupported_templates = [code for code in template_codes if code not in SUPPORTED_TEMPLATE_CODES]
    if unsupported_templates:
        raise ValueError(
            "Unsupported template codes in CARD_WEIGHTS: "
            f"{unsupported_templates}. Supported codes: {SUPPORTED_TEMPLATE_CODES}"
        )

    for template_code, cards in CARD_WEIGHTS.items():
        missing_cards = [card for card in SUPPORTED_CARD_NAMES if card not in cards]
        extra_cards = [card for card in cards if card not in SUPPORTED_CARD_NAMES]
        if missing_cards or extra_cards:
            raise ValueError(
                f"Template {template_code} cards must match {SUPPORTED_CARD_NAMES}. "
                f"Missing={missing_cards}, Extra={extra_cards}"
            )
        for card_name, weights in cards.items():
            if not weights:
                raise ValueError(f"Template {template_code} card {card_name} cannot be empty")
            _assert_close(f"CARD_WEIGHTS[{template_code}][{card_name}]", weights)

    composite_weight_sets = {
        "OPPORTUNITY_WEIGHTS": OPPORTUNITY_WEIGHTS,
        "POTENTIAL_SCORE_WEIGHTS": POTENTIAL_SCORE_WEIGHTS,
        "VALUATION_GAP_SCORE_WEIGHTS": VALUATION_GAP_SCORE_WEIGHTS,
        "SECTOR_REGIME_WEIGHTS": SECTOR_REGIME_WEIGHTS,
        "SELECTION_SCORE_WEIGHTS": SELECTION_SCORE_WEIGHTS,
    }
    for name, weights in composite_weight_sets.items():
        if not weights:
            raise ValueError(f"{name} cannot be empty")
        _assert_close(name, weights)

    missing_labels = [card for card in SUPPORTED_CARD_NAMES if card not in CARD_LABELS]
    if missing_labels:
        raise ValueError(f"CARD_LABELS missing cards: {missing_labels}")

    invalid_conf = [
        label for label in [GATE_MIN_CONFIDENCE_FOR_BUY, PORTFOLIO_MIN_CONFIDENCE]
        if label not in CONFIDENCE_LEVELS
    ]
    if invalid_conf:
        raise ValueError(
            f"Confidence thresholds must be one of {CONFIDENCE_LEVELS}, found {invalid_conf}"
        )

    if CONFIDENCE_HIGH_THRESHOLD <= CONFIDENCE_MEDIUM_THRESHOLD:
        raise ValueError(
            "CONFIDENCE_HIGH_THRESHOLD must be greater than CONFIDENCE_MEDIUM_THRESHOLD"
        )

    if len(ENTRY_STAGE_WEIGHTS) != 3 or sum(ENTRY_STAGE_WEIGHTS) != 100:
        raise ValueError("ENTRY_STAGE_WEIGHTS must contain exactly 3 values summing to 100")

    pct_thresholds = {
        "MIN_TEMPLATE_CARD_RANKABLE_PCT": MIN_TEMPLATE_CARD_RANKABLE_PCT,
        "MIN_TEMPLATE_RED_FLAGS_RANKABLE_PCT": MIN_TEMPLATE_RED_FLAGS_RANKABLE_PCT,
        "MIN_TEMPLATE_AVG_CORE_RANKABLE_PCT": MIN_TEMPLATE_AVG_CORE_RANKABLE_PCT,
    }
    for name, value in pct_thresholds.items():
        if not (0.0 <= float(value) <= 100.0):
            raise ValueError(f"{name} must be within 0-100, found {value}")

    if PRICE_HISTORY_LOOKBACK_SESSIONS < MIN_TRADING_DAYS:
        raise ValueError(
            "PRICE_HISTORY_LOOKBACK_SESSIONS must be >= MIN_TRADING_DAYS "
            f"({MIN_TRADING_DAYS}), found {PRICE_HISTORY_LOOKBACK_SESSIONS}"
        )
