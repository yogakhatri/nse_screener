"""
User research preferences for long-term stock shortlisting.

This layer never relaxes the engine's safety gates. It only filters or re-ranks
already scored stocks according to a user's documented research profile.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .models import RawStockData, StockRating


from .policy_themes import normalize_policy_themes
from .research_modes import normalize_research_mode, normalize_return_persona

VALID_HORIZONS = ("6m", "1y", "3y", "5y", "10y")
VALID_RISK_LEVELS = ("conservative", "balanced", "aggressive")
VALID_MARKET_CAP_PREFERENCES = ("all", "large", "mid", "small", "micro", "exclude_micro")
WEIGHT_KEYS = (
    "selection_score",
    "potential_score",
    "valuation_gap_score",
    "risk_reward_score",
    "profitability",
    "growth",
    "red_flags",
    "entry_point",
    "dividend_yield_score",
)

MARKET_CAP_RANGES_CR = {
    "large": (20_000.0, None),
    "mid": (5_000.0, 20_000.0),
    "small": (500.0, 5_000.0),
    "micro": (0.0, 500.0),
}

DEFAULT_HORIZON_WEIGHTS = {
    "6m": {
        "selection_score": 0.30,
        "valuation_gap_score": 0.20,
        "risk_reward_score": 0.20,
        "entry_point": 0.20,
        "red_flags": 0.10,
    },
    "1y": {
        "selection_score": 0.35,
        "potential_score": 0.25,
        "valuation_gap_score": 0.20,
        "risk_reward_score": 0.10,
        "red_flags": 0.10,
    },
    "3y": {
        "potential_score": 0.35,
        "profitability": 0.20,
        "growth": 0.20,
        "red_flags": 0.15,
        "selection_score": 0.10,
    },
    "5y": {
        "potential_score": 0.30,
        "profitability": 0.25,
        "growth": 0.20,
        "red_flags": 0.20,
        "valuation_gap_score": 0.05,
    },
    "10y": {
        "potential_score": 0.28,
        "profitability": 0.30,
        "growth": 0.18,
        "red_flags": 0.22,
        "valuation_gap_score": 0.02,
    },
}


@dataclass(frozen=True)
class ResearchPreferences:
    """Validated user preferences for filtering and ranking research outputs."""

    profile_name: str = "default"
    investment_horizon: str = "1y"
    risk_level: str = "balanced"
    sector_preference: tuple[str, ...] = ()
    market_cap_preference: str = "all"
    min_market_cap_cr: Optional[float] = None
    max_market_cap_cr: Optional[float] = None
    max_pe: Optional[float] = None
    max_pb: Optional[float] = None
    min_fcf_yield: Optional[float] = None
    min_iv_gap: Optional[float] = None
    min_expected_upside_pct: Optional[float] = None
    min_rev_growth_yoy: Optional[float] = None
    min_eps_growth_yoy: Optional[float] = None
    min_rev_cagr_3y: Optional[float] = None
    max_debt_to_equity: Optional[float] = None
    min_interest_coverage: Optional[float] = None
    min_roce: Optional[float] = None
    min_roe: Optional[float] = None
    min_cfo_pat_ratio: Optional[float] = None
    min_dividend_yield: Optional[float] = None
    custom_weights: Dict[str, float] = field(default_factory=dict)
    research_mode: str = "research_shortlist"
    return_persona: str = "quality_value"
    policy_themes: tuple[str, ...] = ()

    def validate(self) -> "ResearchPreferences":
        """Return self after checking user-controlled inputs are safe."""
        if self.investment_horizon not in VALID_HORIZONS:
            raise ValueError(f"investment_horizon must be one of {VALID_HORIZONS}")
        if self.risk_level not in VALID_RISK_LEVELS:
            raise ValueError(f"risk_level must be one of {VALID_RISK_LEVELS}")
        if self.market_cap_preference not in VALID_MARKET_CAP_PREFERENCES:
            raise ValueError(f"market_cap_preference must be one of {VALID_MARKET_CAP_PREFERENCES}")
        if self.min_market_cap_cr is not None and self.min_market_cap_cr < 0:
            raise ValueError("min_market_cap_cr cannot be negative")
        if self.max_market_cap_cr is not None and self.max_market_cap_cr < 0:
            raise ValueError("max_market_cap_cr cannot be negative")
        if (
            self.min_market_cap_cr is not None
            and self.max_market_cap_cr is not None
            and self.min_market_cap_cr > self.max_market_cap_cr
        ):
            raise ValueError("min_market_cap_cr cannot exceed max_market_cap_cr")

        numeric_fields = (
            "max_pe",
            "max_pb",
            "min_fcf_yield",
            "min_iv_gap",
            "min_expected_upside_pct",
            "min_rev_growth_yoy",
            "min_eps_growth_yoy",
            "min_rev_cagr_3y",
            "max_debt_to_equity",
            "min_interest_coverage",
            "min_roce",
            "min_roe",
            "min_cfo_pat_ratio",
            "min_dividend_yield",
        )
        for field_name in numeric_fields:
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, (int, float)) or not math.isfinite(float(value))):
                raise ValueError(f"{field_name} must be numeric")
        non_negative_fields = (
            "max_pe",
            "max_pb",
            "min_iv_gap",
            "min_expected_upside_pct",
            "max_debt_to_equity",
            "min_interest_coverage",
            "min_dividend_yield",
        )
        for field_name in non_negative_fields:
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} cannot be negative")

        unknown_weights = sorted(k for k in self.custom_weights if k not in WEIGHT_KEYS)
        if unknown_weights:
            raise ValueError(f"custom_weights contains unsupported keys: {unknown_weights}")
        if any(not math.isfinite(float(v)) for v in self.custom_weights.values()):
            raise ValueError("custom_weights must contain finite numeric values")
        if any(v < 0 for v in self.custom_weights.values()):
            raise ValueError("custom_weights cannot contain negative values")
        if self.custom_weights and sum(self.custom_weights.values()) <= 0:
            raise ValueError("custom_weights must have a positive total")
        normalize_research_mode(self.research_mode)
        normalize_return_persona(self.return_persona)
        normalize_policy_themes(self.policy_themes)
        return self

    @property
    def is_default(self) -> bool:
        """Return True when no explicit filters or custom weights are active."""
        return self == ResearchPreferences()


def _as_optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    return float(value)


def _split_csv(value: str | Iterable[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = value
    return tuple(sorted({str(part).strip() for part in parts if str(part).strip()}))


def _normalize_profile_dict(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = set(ResearchPreferences.__dataclass_fields__)
    unknown = sorted(k for k in payload if k not in allowed)
    if unknown:
        raise ValueError(f"Unknown profile fields: {unknown}")
    normalized = dict(payload)
    if "sector_preference" in normalized:
        normalized["sector_preference"] = _split_csv(normalized["sector_preference"])
    if "policy_themes" in normalized:
        normalized["policy_themes"] = normalize_policy_themes(normalized["policy_themes"])
    return normalized


def load_research_preferences(path: str | None = None, **overrides: Any) -> ResearchPreferences:
    """Load a profile JSON and apply explicit CLI overrides."""
    payload: dict[str, Any] = {}
    if path:
        profile_path = Path(path)
        try:
            payload = json.loads(profile_path.read_text())
        except FileNotFoundError as exc:
            raise ValueError(f"Profile config not found: {profile_path}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"Profile config is not valid JSON: {profile_path} ({exc})") from exc
        if not isinstance(payload, dict):
            raise ValueError("Profile config must be a JSON object")
        payload = _normalize_profile_dict(payload)

    for key, value in overrides.items():
        if value is None or value == "":
            continue
        if key == "sector_preference":
            payload[key] = _split_csv(value)
        elif key == "policy_themes":
            payload[key] = normalize_policy_themes(value)
        elif key == "custom_weights":
            payload[key] = parse_custom_weights(value)
        elif key in ResearchPreferences.__dataclass_fields__:
            payload[key] = value

    numeric_fields = {
        "min_market_cap_cr",
        "max_market_cap_cr",
        "max_pe",
        "max_pb",
        "min_fcf_yield",
        "min_iv_gap",
        "min_expected_upside_pct",
        "min_rev_growth_yoy",
        "min_eps_growth_yoy",
        "min_rev_cagr_3y",
        "max_debt_to_equity",
        "min_interest_coverage",
        "min_roce",
        "min_roe",
        "min_cfo_pat_ratio",
        "min_dividend_yield",
    }
    for key in numeric_fields:
        if key in payload:
            payload[key] = _as_optional_float(payload[key])

    return ResearchPreferences(**payload).validate()


def parse_custom_weights(value: str | dict[str, Any] | None) -> dict[str, float]:
    """Parse custom ranking weights from JSON text or a dict."""
    if value is None or value == "":
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"custom_weights must be JSON object text ({exc})") from exc
    else:
        parsed = value
    if not isinstance(parsed, dict):
        raise ValueError("custom_weights must be a JSON object")
    return {str(k): float(v) for k, v in parsed.items()}


def preferences_to_dict(preferences: ResearchPreferences) -> dict[str, Any]:
    """Serialize preferences to JSON-friendly metadata."""
    data = {
        key: getattr(preferences, key)
        for key in ResearchPreferences.__dataclass_fields__
    }
    data["sector_preference"] = list(preferences.sector_preference)
    data["policy_themes"] = list(preferences.policy_themes)
    return data


def _metric_value(rating: StockRating, stock: RawStockData, metric: str) -> Optional[float]:
    card_map = {
        "profitability": rating.profitability.score,
        "growth": rating.growth.score,
        "red_flags": rating.red_flags.score,
        "entry_point": rating.entry_point.score,
    }
    if metric in card_map:
        return card_map[metric]
    if metric == "selection_score":
        return rating.selection_score
    if metric == "potential_score":
        return rating.potential_score
    if metric == "valuation_gap_score":
        return rating.valuation_gap_score
    if metric == "risk_reward_score":
        return rating.risk_reward_score
    return stock.fundamentals.get(metric)


def _weighted_profile_score(
    rating: StockRating,
    stock: RawStockData,
    preferences: ResearchPreferences,
) -> Optional[float]:
    weights = preferences.custom_weights or DEFAULT_HORIZON_WEIGHTS[preferences.investment_horizon]
    total = 0.0
    weighted = 0.0
    for metric, weight in weights.items():
        value = _metric_value(rating, stock, metric)
        if value is None:
            continue
        weighted += float(value) * float(weight)
        total += float(weight)
    if total <= 0:
        return None
    score = weighted / total
    if preferences.risk_level == "conservative":
        score *= 0.92 + ((rating.red_flags.score or 50.0) / 100.0) * 0.08
    elif preferences.risk_level == "aggressive":
        score *= 1.03
    return round(min(max(score, 0.0), 100.0), 2)


def _market_cap_filter_reasons(
    market_cap_cr: Optional[float],
    preferences: ResearchPreferences,
) -> list[str]:
    reasons: list[str] = []
    if preferences.market_cap_preference == "exclude_micro":
        if market_cap_cr is None:
            reasons.append("market cap missing for exclude_micro preference")
        elif market_cap_cr < MARKET_CAP_RANGES_CR["micro"][1]:
            reasons.append("market cap is below micro-cap exclusion threshold")
    elif preferences.market_cap_preference in MARKET_CAP_RANGES_CR:
        low, high = MARKET_CAP_RANGES_CR[preferences.market_cap_preference]
        if market_cap_cr is None:
            reasons.append(f"market cap missing for {preferences.market_cap_preference} preference")
        elif market_cap_cr < low or (high is not None and market_cap_cr >= high):
            reasons.append(f"market cap not in {preferences.market_cap_preference} range")

    if preferences.min_market_cap_cr is not None:
        if market_cap_cr is None:
            reasons.append("market cap missing for min_market_cap_cr filter")
        elif market_cap_cr < preferences.min_market_cap_cr:
            reasons.append(f"market cap {market_cap_cr:.1f}Cr < {preferences.min_market_cap_cr:.1f}Cr")
    if preferences.max_market_cap_cr is not None:
        if market_cap_cr is None:
            reasons.append("market cap missing for max_market_cap_cr filter")
        elif market_cap_cr > preferences.max_market_cap_cr:
            reasons.append(f"market cap {market_cap_cr:.1f}Cr > {preferences.max_market_cap_cr:.1f}Cr")
    return reasons


def _threshold_reasons(stock: RawStockData, rating: StockRating, preferences: ResearchPreferences) -> list[str]:
    def metric_value(metric: str) -> Optional[float]:
        if metric == "iv_gap":
            if stock.fundamentals.get("iv_gap") is not None:
                return stock.fundamentals.get("iv_gap")
            return stock.fundamentals.get("fair_value_gap")
        return stock.fundamentals.get(metric)

    checks = [
        ("pe_percentile", preferences.max_pe, "<=", "P/E"),
        ("pb_percentile", preferences.max_pb, "<=", "P/B"),
        ("fcf_yield", preferences.min_fcf_yield, ">=", "FCF yield"),
        ("iv_gap", preferences.min_iv_gap, ">=", "IV gap"),
        ("rev_growth_yoy", preferences.min_rev_growth_yoy, ">=", "Revenue growth YoY"),
        ("eps_growth_yoy", preferences.min_eps_growth_yoy, ">=", "EPS growth YoY"),
        ("rev_cagr_3y", preferences.min_rev_cagr_3y, ">=", "Revenue CAGR 3Y"),
        ("debt_to_equity", preferences.max_debt_to_equity, "<=", "Debt/equity"),
        ("interest_coverage", preferences.min_interest_coverage, ">=", "Interest coverage"),
        ("roce_3y_median", preferences.min_roce, ">=", "ROCE"),
        ("roe", preferences.min_roe, ">=", "ROE"),
        ("cfo_pat_ratio", preferences.min_cfo_pat_ratio, ">=", "CFO/PAT"),
        ("dividend_yield", preferences.min_dividend_yield, ">=", "Dividend yield"),
    ]
    reasons: list[str] = []
    for metric, threshold, op, label in checks:
        if threshold is None:
            continue
        value = metric_value(metric)
        if value is None:
            reasons.append(f"{label} missing")
        elif metric in {"pe_percentile", "pb_percentile"} and value <= 0:
            reasons.append(f"{label} is non-positive and cannot be treated as cheap")
        elif op == "<=" and value > threshold:
            reasons.append(f"{label} {value:.2f} > {threshold:.2f}")
        elif op == ">=" and value < threshold:
            reasons.append(f"{label} {value:.2f} < {threshold:.2f}")

    if preferences.min_expected_upside_pct is not None:
        upside = rating.expected_upside_pct
        if upside is None:
            reasons.append("expected upside missing")
        elif upside < preferences.min_expected_upside_pct:
            reasons.append(f"expected upside {upside:.2f}% < {preferences.min_expected_upside_pct:.2f}%")
    return reasons


def apply_research_preferences(
    ratings: Dict[str, StockRating],
    stocks: Dict[str, RawStockData],
    preferences: ResearchPreferences,
) -> None:
    """Annotate ratings with user filter status and profile-specific ranking score."""
    from .policy_themes import sector_matches_themes

    preferred_sectors = {s.lower() for s in preferences.sector_preference}
    theme_sectors_only = bool(preferences.policy_themes)
    for ticker, rating in ratings.items():
        stock = stocks[ticker]
        reasons: list[str] = []
        if preferred_sectors and rating.classification.sector.lower() not in preferred_sectors:
            reasons.append(f"sector {rating.classification.sector} not in preference list")
        if theme_sectors_only:
            if not sector_matches_themes(rating.classification.sector, preferences.policy_themes):
                reasons.append("sector does not match active policy_themes")

        reasons.extend(_market_cap_filter_reasons(stock.fundamentals.get("market_cap_cr"), preferences))
        reasons.extend(_threshold_reasons(stock, rating, preferences))

        if preferences.risk_level == "conservative":
            if rating.red_flags.score is None or rating.red_flags.score < 65:
                reasons.append("conservative profile requires red_flags score >= 65")
            if rating.data_quality_status != "Actionable Data":
                reasons.append("conservative profile requires Actionable Data")
            if rating.value_trap_score is not None and rating.value_trap_score >= 45:
                reasons.append("conservative profile excludes elevated value-trap risk")
        elif preferences.risk_level == "aggressive":
            if rating.data_quality_status == "Weak Data":
                reasons.append("aggressive profile still excludes Weak Data")

        rating.user_profile_name = preferences.profile_name
        rating.user_filter_reasons = reasons
        rating.user_filter_passed = len(reasons) == 0
        rating.user_profile_score = _weighted_profile_score(rating, stock, preferences)
        rating.user_profile_notes = [
            f"horizon={preferences.investment_horizon}",
            f"risk_level={preferences.risk_level}",
            f"research_mode={preferences.research_mode}",
            f"return_persona={preferences.return_persona}",
        ]
        if preferences.policy_themes:
            rating.user_profile_notes.append(
                "policy_themes=" + ",".join(preferences.policy_themes)
            )


def filter_preference_rows(rows: list[dict]) -> list[dict]:
    """Return rows passing user filters, sorted by profile score then selection score."""
    filtered = [row for row in rows if row.get("user_filter_passed", True)]
    return sorted(
        filtered,
        key=lambda row: (
            row.get("user_profile_score") or row.get("selection_score") or 0.0,
            row.get("selection_score") or 0.0,
        ),
        reverse=True,
    )
