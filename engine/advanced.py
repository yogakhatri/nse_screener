"""
Advanced overlays for recommendation quality, downside control, and portfolio safety.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .config import (
    AUTO_BEAR_DRAWDOWN_RECOVERY_THRESHOLD,
    AUTO_BEAR_RETURN_6M_THRESHOLD,
    AUTO_BULL_DRAWDOWN_RECOVERY_THRESHOLD,
    AUTO_BULL_RETURN_6M_THRESHOLD,
    BANK_CRITICAL_RISK_FIELDS,
    BEAR_GATE_MAX_DEFAULT_DISTRESS_RISK,
    BEAR_GATE_MAX_PROMOTER_PLEDGE_PCT,
    BEAR_GATE_MIN_RED_FLAGS_SCORE,
    BEAR_MODE_QUALITY_BONUS,
    BEAR_MODE_RISK_PENALTY,
    BUY_HIT_RATE_ALERT_THRESHOLD,
    DAILY_LIST_MAX_BANK_NAMES,
    DAILY_LIST_MAX_FINANCIAL_NAMES,
    DAILY_LIST_MAX_NAMES,
    DAILY_LIST_MAX_NBFC_NAMES,
    DAILY_LIST_MAX_PER_SECTOR,
    DAILY_LIST_MIN_CONFIDENCE,
    BUY_POTENTIAL_THRESHOLD,
    BUY_VALUATION_GAP_THRESHOLD,
    CALIBRATION_MIN_HIT_RATE_PCT,
    CALIBRATION_MIN_MEAN_RETURN_PCT,
    CALIBRATION_MIN_SAMPLE_SIZE,
    CALIBRATION_PROFILE_PATH,
    CONFIDENCE_HIGH_THRESHOLD,
    CONFIDENCE_MEDIUM_THRESHOLD,
    DEFAULT_MARKET_MODE,
    ENTRY_STAGE_WEIGHTS,
    GENERAL_CRITICAL_RISK_FIELDS,
    GATE_MAX_DEFAULT_DISTRESS_RISK,
    GATE_MAX_PROMOTER_PLEDGE_PCT,
    GATE_MIN_CONFIDENCE_FOR_BUY,
    GATE_MIN_LIQUIDITY_TURNOVER_CR,
    GATE_MIN_RED_FLAGS_SCORE,
    INDEX_BEAR_BREADTH_THRESHOLD,
    INDEX_BEAR_RETURN_6M_THRESHOLD,
    INDEX_BULL_BREADTH_THRESHOLD,
    INDEX_BULL_RETURN_6M_THRESHOLD,
    INDEX_REGIME_LOOKBACK_SESSIONS,
    INDEX_REGIME_MIN_SESSIONS,
    MAX_EXPECTED_DOWNSIDE_PCT,
    MAX_MISSING_CRITICAL_FIELDS_ACTIONABLE,
    MAX_MISSING_CRITICAL_FIELDS_RESEARCH,
    MAX_STOP_LOSS_PCT,
    MIN_FINANCIAL_ASSET_QUALITY_FIELDS,
    MIN_MARKET_MODE_COVERAGE_PCT,
    MIN_MARKET_MODE_OBSERVATIONS,
    MIN_DATA_QUALITY_SCORE_ACTIONABLE,
    MIN_DATA_QUALITY_SCORE_RESEARCH,
    MIN_EXPECTED_DOWNSIDE_PCT,
    MISSING_CRITICAL_FIELD_PENALTY,
    NBFC_CRITICAL_RISK_FIELDS,
    MIN_STOP_LOSS_PCT,
    GATE_MIN_DATA_QUALITY_SCORE,
    OUTCOME_HORIZON_DAYS,
    PORTFOLIO_MAX_HOLDINGS,
    PORTFOLIO_MAX_SECTOR_WEIGHT_PCT,
    PORTFOLIO_MAX_SINGLE_STOCK_WEIGHT_PCT,
    PORTFOLIO_MIN_CONFIDENCE,
    POTENTIAL_SCORE_WEIGHTS,
    SECTOR_REGIME_WEIGHTS,
    SELECTION_SCORE_WEIGHTS,
    UPSIDE_HAIRCUT_LOW_CONFIDENCE,
    UPSIDE_HAIRCUT_MEDIUM_CONFIDENCE,
    VALUATION_GAP_SCORE_WEIGHTS,
    VALUE_TRAP_BLOCK_THRESHOLD,
    VALUE_TRAP_WARN_THRESHOLD,
    WATCH_POTENTIAL_THRESHOLD,
    WATCH_VALUATION_GAP_THRESHOLD,
)
from .models import RawStockData, StockRating


def _weighted_average(parts: Iterable[Tuple[float | None, float]]) -> float | None:
    total = 0.0
    weighted = 0.0
    for value, weight in parts:
        if value is None:
            continue
        weighted += value * weight
        total += weight
    if total == 0:
        return None
    return weighted / total


def _confidence_to_rank(label: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get((label or "").lower(), 0)


def _confidence_from_score(score: float | None) -> str:
    if score is None:
        return "Low"
    if score >= CONFIDENCE_HIGH_THRESHOLD:
        return "High"
    if score >= CONFIDENCE_MEDIUM_THRESHOLD:
        return "Medium"
    return "Low"


def _source_score(value: str, scores: dict[str, float], default: float) -> float:
    key = (value or "").strip().lower()
    return scores.get(key, default)


def _has_critical_value(stock: RawStockData, field: str) -> bool:
    """Return True only when a critical risk field has a real value or source evidence."""
    value = stock.fundamentals.get(field)
    provenance = stock.metric_provenance.get(field)
    if field in {"asm_stage", "gsm_stage"}:
        return provenance is not None or value not in {None, ""}
    if field == "governance_events":
        return provenance is not None or (isinstance(value, list) and len(value) > 0)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value)
    return True


def _critical_risk_fields(template_code: str) -> tuple[str, ...]:
    """Return the configured critical-risk field set for a template."""
    if template_code == "B":
        return BANK_CRITICAL_RISK_FIELDS
    if template_code == "C":
        return NBFC_CRITICAL_RISK_FIELDS
    return GENERAL_CRITICAL_RISK_FIELDS


def _missing_critical_fields(rating: StockRating, stock: RawStockData) -> list[str]:
    """List critical risk fields that are unavailable for this stock/template."""
    fields = _critical_risk_fields(rating.template.value)
    return [field for field in fields if not _has_critical_value(stock, field)]


def _financial_asset_quality_count(stock: RawStockData) -> int:
    """Count present bank/NBFC asset-quality fields used by financial valuation gates."""
    fields = ("gnpa_pct", "nnpa_pct", "pcr_pct", "car_pct", "nim", "credit_cost_discipline")
    return sum(1 for field in fields if _has_critical_value(stock, field))


def _load_index_regime_signal(run_date: Optional[date]) -> tuple[str, str, str] | None:
    """
    Infer broad market regime from locally cached NSE index files.

    Returns (mode, source, confidence) when enough index history is present; otherwise None.
    """
    if run_date is None:
        return None
    indices_dir = Path("data/raw/prices/indices")
    if not indices_dir.exists():
        return None

    dated_files: list[tuple[date, Path]] = []
    for path in indices_dir.glob("ind_close_all_*.csv"):
        token = path.stem.replace("ind_close_all_", "")
        try:
            file_date = datetime.strptime(token, "%d%b%Y").date()
        except ValueError:
            continue
        if file_date <= run_date:
            dated_files.append((file_date, path))
    if not dated_files:
        return None

    dated_files.sort(key=lambda item: item[0])
    selected = dated_files[-INDEX_REGIME_LOOKBACK_SESSIONS:]
    index_series: dict[str, list[float]] = defaultdict(list)
    for _, path in selected:
        try:
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                columns = reader.fieldnames or []
                index_col = next(
                    (c for c in columns if c.strip().lower().replace(" ", "") in {"indexname", "index"}),
                    None,
                )
                close_col = next(
                    (
                        c for c in columns
                        if c.strip().lower().replace(" ", "") in {
                            "closingindexvalue",
                            "closingvalue",
                            "close",
                            "closevalue",
                        }
                    ),
                    None,
                )
                if not index_col or not close_col:
                    continue
                for row in reader:
                    name = str(row.get(index_col, "")).strip().upper()
                    if not name:
                        continue
                    try:
                        close = float(str(row.get(close_col, "")).replace(",", ""))
                    except ValueError:
                        continue
                    index_series[name].append(close)
        except OSError:
            continue

    primary = index_series.get("NIFTY 500") or index_series.get("NIFTY 50")
    if not primary or len(primary) < INDEX_REGIME_MIN_SESSIONS:
        return None

    lookback = min(126, len(primary) - 1)
    if lookback <= 0 or primary[-lookback] == 0:
        return None
    primary_ret_6m = (primary[-1] / primary[-lookback] - 1.0) * 100.0

    breadth_returns = []
    for closes in index_series.values():
        if len(closes) <= lookback or closes[-lookback] == 0:
            continue
        breadth_returns.append((closes[-1] / closes[-lookback] - 1.0) * 100.0)
    breadth_pct = (
        sum(1 for value in breadth_returns if value > 0.0) / len(breadth_returns) * 100.0
        if breadth_returns
        else 50.0
    )

    confidence = "High" if len(primary) >= 252 and len(breadth_returns) >= 4 else "Medium"
    if primary_ret_6m <= INDEX_BEAR_RETURN_6M_THRESHOLD or breadth_pct <= INDEX_BEAR_BREADTH_THRESHOLD:
        return "bear", "nse_index_cache", confidence
    if primary_ret_6m >= INDEX_BULL_RETURN_6M_THRESHOLD and breadth_pct >= INDEX_BULL_BREADTH_THRESHOLD:
        return "bull", "nse_index_cache", confidence
    return "neutral", "nse_index_cache", confidence


def infer_market_mode_details(
    stocks: Dict[str, RawStockData],
    requested_mode: str | None = None,
    run_date: Optional[date] = None,
) -> tuple[str, str, str]:
    """
    Decide regime mode with source and confidence metadata.

    Priority: explicit override, local NSE index cache, then broad stock-level fallback.
    """
    mode = (requested_mode or DEFAULT_MARKET_MODE).lower().strip()
    if mode in {"bear", "bull", "neutral"}:
        return mode, "manual_override", "High"

    index_signal = _load_index_regime_signal(run_date)
    if index_signal is not None:
        return index_signal

    ret_6m = []
    dd_recovery = []
    for stock in stocks.values():
        r6 = stock.fundamentals.get("return_6m")
        dr = stock.fundamentals.get("drawdown_recovery")
        if r6 is not None:
            ret_6m.append(r6)
        if dr is not None:
            dd_recovery.append(dr)

    observations = max(len(ret_6m), len(dd_recovery))
    coverage_pct = observations / len(stocks) * 100.0 if stocks else 0.0
    if observations < MIN_MARKET_MODE_OBSERVATIONS or coverage_pct < MIN_MARKET_MODE_COVERAGE_PCT:
        return "neutral", "stock_fallback_low_coverage", "Low"

    median_r6 = sorted(ret_6m)[len(ret_6m) // 2] if ret_6m else 0.0
    median_dd = sorted(dd_recovery)[len(dd_recovery) // 2] if dd_recovery else 50.0

    if median_r6 <= AUTO_BEAR_RETURN_6M_THRESHOLD or median_dd <= AUTO_BEAR_DRAWDOWN_RECOVERY_THRESHOLD:
        return "bear", "stock_price_breadth", "Medium"
    if median_r6 >= AUTO_BULL_RETURN_6M_THRESHOLD and median_dd >= AUTO_BULL_DRAWDOWN_RECOVERY_THRESHOLD:
        return "bull", "stock_price_breadth", "Medium"
    return "neutral", "stock_price_breadth", "Medium"


def _data_quality(rating: StockRating, stock: RawStockData) -> tuple[float, str, list[str]]:
    """
    Estimate whether a row has enough source strength to support action.

    This is separate from stock quality: a cheap, high-growth company can still
    be non-actionable if its source coverage is weak or stale.
    """
    reasons: list[str] = []
    cls_score = _source_score(
        rating.classification_confidence,
        {"high": 100.0, "medium": 70.0, "low": 35.0},
        25.0,
    )
    fund_score = _source_score(
        rating.fundamentals_source,
        {
            "screener_public_page": 85.0,
            "screener_csv": 70.0,
            "fundamentals_csv": 70.0,
            "unit_test": 100.0,
        },
        35.0,
    )
    price_score = _source_score(
        rating.price_source,
        {
            "bhavcopy_local": 95.0,
            "csv_fallback": 65.0,
            "csv_only": 55.0,
            "unit_test": 100.0,
            "missing": 0.0,
        },
        30.0,
    )
    core_cards = [
        rating.performance,
        rating.valuation,
        rating.growth,
        rating.profitability,
        rating.entry_point,
        rating.red_flags,
    ]
    coverage_score = round(sum(card.data_coverage for card in core_cards) / len(core_cards) * 100.0, 2)
    valuation_score = rating.valuation_confidence_score or 0.0

    missing_critical = len(rating.missing_critical_fields)
    missing_penalty = min(35.0, missing_critical * MISSING_CRITICAL_FIELD_PENALTY)

    score = round(
        cls_score * 0.15
        + fund_score * 0.20
        + price_score * 0.20
        + coverage_score * 0.25
        + valuation_score * 0.20,
        2,
    )
    score = max(0.0, round(score - missing_penalty, 2))

    if cls_score < 60:
        reasons.append("classification confidence below Medium")
    if fund_score < 60:
        reasons.append("fundamentals source is weak/unknown")
    if price_score < 60:
        reasons.append("price source is missing or weak")
    if coverage_score < 60:
        reasons.append("core card coverage below 60%")
    if valuation_score < 60:
        reasons.append("valuation evidence below 60%")
    if rating.peer_group_quality == "Weak":
        reasons.append("peer group is below configured minimum")
    if missing_critical:
        reasons.append(
            f"missing critical risk fields ({missing_critical}): "
            + ", ".join(rating.missing_critical_fields[:8])
        )

    if missing_critical > MAX_MISSING_CRITICAL_FIELDS_RESEARCH:
        status = "Weak Data"
    elif score >= MIN_DATA_QUALITY_SCORE_ACTIONABLE and missing_critical <= MAX_MISSING_CRITICAL_FIELDS_ACTIONABLE:
        status = "Actionable Data"
    elif score >= MIN_DATA_QUALITY_SCORE_RESEARCH:
        status = "Research Only Data"
    else:
        status = "Weak Data"

    return score, status, reasons


def infer_market_mode(
    stocks: Dict[str, RawStockData],
    requested_mode: str | None = None,
    run_date: Optional[date] = None,
) -> str:
    """
    Decide regime mode. Kept as a compatibility wrapper over infer_market_mode_details().
    """
    mode, _, _ = infer_market_mode_details(stocks, requested_mode=requested_mode, run_date=run_date)
    return mode


def _sector_regime_maps(ratings: Dict[str, StockRating]) -> Tuple[Dict[str, float], Dict[str, str]]:
    by_sector: dict[str, list[StockRating]] = defaultdict(list)
    for rating in ratings.values():
        by_sector[rating.classification.sector].append(rating)

    scores: Dict[str, float] = {}
    labels: Dict[str, str] = {}
    for sector, items in by_sector.items():
        parts = []
        for key, weight in SECTOR_REGIME_WEIGHTS.items():
            card_score = []
            for it in items:
                value = getattr(it, key).score if hasattr(it, key) else None
                if value is not None:
                    card_score.append(value)
            mean_score = sum(card_score) / len(card_score) if card_score else None
            parts.append((mean_score, weight))
        score = _weighted_average(parts)
        sector_score = round(score if score is not None else 50.0, 2)
        scores[sector] = sector_score
        if sector_score >= 70:
            labels[sector] = "Tailwind"
        elif sector_score >= 55:
            labels[sector] = "Constructive"
        elif sector_score >= 40:
            labels[sector] = "Mixed"
        else:
            labels[sector] = "Headwind"
    return scores, labels


def _drawdown_resilience(rating: StockRating) -> float | None:
    perf_dd = rating.performance.sub_scores.get("drawdown_recovery")
    cfo_quality = rating.profitability.sub_scores.get("cfo_pat_ratio")
    rf_score = rating.red_flags.score
    val = _weighted_average([(perf_dd, 0.45), (cfo_quality, 0.25), (rf_score, 0.30)])
    if val is None:
        return None
    return round(val, 2)


def _valuation_confidence(rating: StockRating, stock: RawStockData) -> float:
    valuation_cov = rating.valuation.data_coverage * 100.0
    has_iv = stock.fundamentals.get("iv_gap") is not None or stock.fundamentals.get("fair_value_gap") is not None
    has_hist_band = (
        stock.fundamentals.get("hist_val_band") is not None
        or stock.fundamentals.get("hist_pb_band") is not None
    )
    evidence_bonus = (8.0 if has_iv else 0.0) + (7.0 if has_hist_band else 0.0)
    confidence = min(100.0, valuation_cov + evidence_bonus)
    return round(confidence, 2)


def _expected_upside_downside(
    rating: StockRating,
    stock: RawStockData,
    market_mode: str,
) -> Tuple[float | None, float | None, float | None, float | None]:
    raw_gap = stock.fundamentals.get("iv_gap")
    if raw_gap is None:
        raw_gap = stock.fundamentals.get("fair_value_gap")

    if raw_gap is None:
        # fallback from normalized valuation gap score
        if rating.valuation_gap_score is None:
            upside = None
        else:
            upside = max(0.0, (rating.valuation_gap_score - 50.0) * 0.9)
    else:
        upside = max(0.0, float(raw_gap))

    if upside is not None:
        if rating.recommendation_confidence.lower() == "low":
            upside *= UPSIDE_HAIRCUT_LOW_CONFIDENCE
        elif rating.recommendation_confidence.lower() == "medium":
            upside *= UPSIDE_HAIRCUT_MEDIUM_CONFIDENCE

        if market_mode == "bear":
            upside *= 0.88
        elif market_mode == "bull":
            upside *= 1.05

        # Sector tailwind/headwind adjustment.
        if rating.sector_regime_score is not None:
            adj = 0.85 + (rating.sector_regime_score / 100.0) * 0.30
            upside *= adj

    red_flag_risk = 100.0 - (rating.red_flags.score or 50.0)
    entry_risk = max(0.0, 55.0 - (rating.entry_point.score or 50.0))
    downside = 8.0 + red_flag_risk * 0.18 + entry_risk * 0.12
    if rating.drawdown_resilience_score is not None:
        downside -= (rating.drawdown_resilience_score - 50.0) * 0.08
    if market_mode == "bear":
        downside *= 1.18
    elif market_mode == "bull":
        downside *= 0.92

    downside = max(MIN_EXPECTED_DOWNSIDE_PCT, min(MAX_EXPECTED_DOWNSIDE_PCT, downside))
    ratio = (upside / downside) if (upside is not None and downside > 0) else None
    rr_score = None
    if ratio is not None:
        rr_score = max(0.0, min(100.0, ratio * 35.0))

    return (
        round(upside, 2) if upside is not None else None,
        round(downside, 2),
        round(ratio, 2) if ratio is not None else None,
        round(rr_score, 2) if rr_score is not None else None,
    )


def _load_calibration_profile(path: str = CALIBRATION_PROFILE_PATH) -> dict:
    """Load optional backtest-derived calibration profile."""
    profile_path = Path(path)
    if not profile_path.exists():
        return {
            "status": "Not Calibrated",
            "global_multiplier": 1.0,
            "notes": ["no model calibration profile found"],
        }
    try:
        data = json.loads(profile_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "Calibration Unusable",
            "global_multiplier": 1.0,
            "notes": [f"unable to read calibration profile: {exc}"],
        }
    sample_size = int(data.get("sample_size", 0) or 0)
    if sample_size < CALIBRATION_MIN_SAMPLE_SIZE:
        return {
            "status": "Calibration Sample Too Small",
            "global_multiplier": 1.0,
            "notes": [f"sample_size {sample_size} < {CALIBRATION_MIN_SAMPLE_SIZE}"],
        }
    multiplier = float(data.get("global_multiplier", 1.0) or 1.0)
    return {
        "status": str(data.get("status") or "Calibrated"),
        "global_multiplier": max(0.60, min(1.15, multiplier)),
        "sector_multipliers": data.get("sector_multipliers") or {},
        "notes": data.get("notes") or [],
    }


def _calibration_multiplier(rating: StockRating, profile: dict) -> tuple[float, str]:
    """Return calibration multiplier and status for one rating."""
    base = float(profile.get("global_multiplier", 1.0) or 1.0)
    sector_key = (rating.classification.sector or "").strip().lower()
    sector_multipliers = {
        str(k).strip().lower(): float(v)
        for k, v in (profile.get("sector_multipliers") or {}).items()
    }
    multiplier = base * sector_multipliers.get(sector_key, 1.0)
    return max(0.60, min(1.15, multiplier)), str(profile.get("status") or "Not Calibrated")


def _apply_upside_calibration(
    upside: float | None,
    downside: float | None,
    multiplier: float,
) -> tuple[float | None, float | None, float | None]:
    """Apply calibration multiplier to upside and recompute risk/reward fields."""
    if upside is None:
        return None, None, None
    calibrated_upside = round(max(0.0, upside * multiplier), 2)
    if downside is None or downside <= 0:
        return calibrated_upside, None, None
    ratio = round(calibrated_upside / downside, 2)
    rr_score = round(max(0.0, min(100.0, ratio * 35.0)), 2)
    return calibrated_upside, ratio, rr_score


def _value_trap_assessment(
    rating: StockRating,
    stock: RawStockData,
) -> tuple[float, list[str]]:
    """Detect cheap-looking stocks where fundamentals or risks make the discount suspect."""
    f = stock.fundamentals
    flags: list[tuple[str, float]] = []

    valuation_looks_cheap = (
        (rating.valuation.score is not None and rating.valuation.score >= 65.0)
        or (rating.valuation_gap_score is not None and rating.valuation_gap_score >= 60.0)
        or (rating.expected_upside_pct is not None and rating.expected_upside_pct >= 20.0)
    )
    if not valuation_looks_cheap:
        return 0.0, []

    if (f.get("rev_growth_yoy") is not None and f["rev_growth_yoy"] < 0) or (
        f.get("eps_growth_yoy") is not None and f["eps_growth_yoy"] < 0
    ):
        flags.append(("declining revenue or earnings", 18.0))
    if f.get("margin_trend") is not None and f["margin_trend"] < 0:
        flags.append(("margin trend is negative", 10.0))
    if f.get("cfo_pat_ratio") is not None and f["cfo_pat_ratio"] < 0.6:
        flags.append(("weak cash conversion", 14.0))
    if f.get("fcf_consistency") is not None and f["fcf_consistency"] < 40:
        flags.append(("poor free-cash-flow consistency", 10.0))
    if f.get("debt_to_equity") is not None and f["debt_to_equity"] > 2.0 and rating.template.value == "A":
        flags.append(("high leverage for non-financial stock", 12.0))
    if f.get("default_distress") is not None and f["default_distress"] > 45:
        flags.append(("elevated default/distress risk", 14.0))
    if f.get("pledge_pct") is not None and f["pledge_pct"] > 25:
        flags.append(("material promoter pledge", 12.0))
    if rating.red_flags.score is not None and rating.red_flags.score < 50:
        flags.append(("red-flag score below neutral", 18.0))
    if rating.sector_regime_score is not None and rating.sector_regime_score < 40:
        flags.append(("sector regime is a headwind", 8.0))
    if f.get("drawdown_recovery") is not None and f["drawdown_recovery"] < 25:
        flags.append(("near 52-week low without recovery", 8.0))

    score = min(100.0, sum(weight for _, weight in flags))
    return round(score, 2), [label for label, _ in flags]


def _investability_gate(
    rating: StockRating,
    stock: RawStockData,
    market_mode: str,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    required_conf_rank = _confidence_to_rank(GATE_MIN_CONFIDENCE_FOR_BUY)
    conf_rank = _confidence_to_rank(rating.recommendation_confidence)

    red_min = BEAR_GATE_MIN_RED_FLAGS_SCORE if market_mode == "bear" else GATE_MIN_RED_FLAGS_SCORE
    pledge_max = BEAR_GATE_MAX_PROMOTER_PLEDGE_PCT if market_mode == "bear" else GATE_MAX_PROMOTER_PLEDGE_PCT
    distress_max = (
        BEAR_GATE_MAX_DEFAULT_DISTRESS_RISK if market_mode == "bear" else GATE_MAX_DEFAULT_DISTRESS_RISK
    )

    red_score = rating.red_flags.score
    if red_score is None or red_score < red_min:
        reasons.append(f"Red flags too high (score<{red_min})")

    if conf_rank < required_conf_rank:
        reasons.append(f"Confidence below {GATE_MIN_CONFIDENCE_FOR_BUY}")

    turnover = stock.fundamentals.get("avg_daily_turnover_cr")
    if turnover is not None and turnover < GATE_MIN_LIQUIDITY_TURNOVER_CR:
        reasons.append(f"Low liquidity (<₹{GATE_MIN_LIQUIDITY_TURNOVER_CR} Cr/day)")

    pledge = stock.fundamentals.get("pledge_pct")
    if pledge is not None and pledge > pledge_max:
        reasons.append(f"High promoter pledge (>{pledge_max}%)")

    distress = stock.fundamentals.get("default_distress")
    if distress is not None and distress > distress_max:
        reasons.append(f"Default/distress risk too high (>{distress_max})")

    if rating.data_quality_score is not None and rating.data_quality_score < GATE_MIN_DATA_QUALITY_SCORE:
        reasons.append(f"Data quality below {GATE_MIN_DATA_QUALITY_SCORE}")

    if rating.peer_group_quality == "Weak":
        reasons.append("Peer group below configured minimum")

    if len(rating.missing_critical_fields) > MAX_MISSING_CRITICAL_FIELDS_ACTIONABLE:
        reasons.append(
            "Too many missing critical risk fields "
            f"({len(rating.missing_critical_fields)} > {MAX_MISSING_CRITICAL_FIELDS_ACTIONABLE})"
        )

    if rating.template.value in {"B", "C"}:
        aq_fields = _financial_asset_quality_count(stock)
        if aq_fields < MIN_FINANCIAL_ASSET_QUALITY_FIELDS:
            reasons.append(
                "Financial asset-quality evidence too thin "
                f"({aq_fields} < {MIN_FINANCIAL_ASSET_QUALITY_FIELDS})"
            )

    if rating.value_trap_score is not None and rating.value_trap_score >= VALUE_TRAP_BLOCK_THRESHOLD:
        reasons.append(f"Value-trap risk too high ({rating.value_trap_score} >= {VALUE_TRAP_BLOCK_THRESHOLD})")

    return len(reasons) == 0, reasons


def _staged_entry_plan(rating: StockRating) -> str:
    upside = rating.expected_upside_pct or 0.0
    downside = rating.expected_downside_pct or MIN_EXPECTED_DOWNSIDE_PCT
    stop_loss = max(MIN_STOP_LOSS_PCT, min(MAX_STOP_LOSS_PCT, downside * 0.8))
    s1, s2, s3 = ENTRY_STAGE_WEIGHTS

    if rating.entry_signal == "Accumulation Zone":
        trigger2 = "add on confirmation break above 20D high"
        trigger3 = "add after weekly close above 50DMA"
    elif rating.entry_signal == "Constructive Pullback":
        trigger2 = "add near support retest with volume"
        trigger3 = "add on RS turn positive for 2 weeks"
    else:
        trigger2 = "wait; add only after base breakout"
        trigger3 = "final add after trend confirmation"

    return (
        f"Stage1 {s1}% now; Stage2 {s2}% ({trigger2}); "
        f"Stage3 {s3}% ({trigger3}); "
        f"expected_upside={upside:.1f}%, expected_downside={downside:.1f}%, stop={stop_loss:.1f}%"
    )


def _research_status(rating: StockRating) -> tuple[str, str]:
    """
    Assign an internal research workflow state.

    This is intentionally stricter than the raw recommendation label because
    the internal tool should distinguish between "worth reviewing" and
    "actionable now".
    """
    if not rating.template_supported:
        return "Unsupported", "Template coverage is incomplete"

    cls_rank = _confidence_to_rank(rating.classification_confidence)
    rec_rank = _confidence_to_rank(rating.recommendation_confidence)

    if (
        rating.recommendation == "Buy Candidate"
        and rating.investability_gate_passed
        and rating.investability_status == "Investable"
        and rating.data_quality_status == "Actionable Data"
        and rec_rank >= _confidence_to_rank("Medium")
        and cls_rank >= _confidence_to_rank("Medium")
    ):
        return "Actionable", "Passes gate with adequate data confidence"

    if (
        rating.recommendation in {"Buy Candidate", "Watchlist"}
        and rating.data_quality_status in {"Actionable Data", "Research Only Data"}
        and rating.investability_status not in {
        "Unsupported Data",
        "Uninvestable",
        }
    ):
        return "Research Candidate", "Needs analyst review before action"

    return "Rejected", "Does not meet current research shortlist filters"


def _set_recommendation_explanation(rating: StockRating) -> None:
    """Populate machine-readable and analyst-readable recommendation reasons."""
    codes: list[str] = []
    reasons: list[str] = []
    risks: list[str] = []

    if rating.recommendation == "Buy Candidate":
        codes.append("BUY_THRESHOLD_PASS")
        reasons.append("Potential, valuation gap, investability gate, and data quality passed.")
    elif rating.recommendation == "Watchlist":
        codes.append("WATCHLIST_THRESHOLD_PASS")
        reasons.append("Interesting score profile, but not enough for immediate buy classification.")
    elif rating.recommendation == "Insufficient Data":
        codes.append("INSUFFICIENT_DATA")
        reasons.append("Critical data, source quality, or rankable coverage is insufficient.")
    elif rating.recommendation == "Unsupported":
        codes.append("UNSUPPORTED_TEMPLATE")
        reasons.append("Template coverage is not strong enough to publish a reliable score.")
    else:
        codes.append("REJECTED_BY_FILTERS")
        reasons.append("Did not pass current potential, valuation, quality, or risk filters.")

    if rating.potential_score is not None:
        reasons.append(f"Potential score={rating.potential_score:.1f}.")
    if rating.valuation_gap_score is not None:
        reasons.append(f"Valuation gap score={rating.valuation_gap_score:.1f}.")
    if rating.expected_upside_pct is not None and rating.risk_reward_ratio is not None:
        reasons.append(
            f"Expected upside={rating.expected_upside_pct:.1f}%, "
            f"risk/reward={rating.risk_reward_ratio:.2f}."
        )

    if rating.gate_fail_reasons:
        codes.append("GATE_FAIL")
        risks.extend(rating.gate_fail_reasons)
    if rating.missing_critical_fields:
        codes.append("UNKNOWN_CRITICAL_RISK")
        risks.append("Missing critical risk fields: " + ", ".join(rating.missing_critical_fields[:8]))
    if rating.value_trap_flags:
        severity = "VALUE_TRAP_BLOCK" if (rating.value_trap_score or 0.0) >= VALUE_TRAP_BLOCK_THRESHOLD else "VALUE_TRAP_WARN"
        codes.append(severity)
        risks.append("Value-trap flags: " + ", ".join(rating.value_trap_flags[:8]))
    if rating.peer_group_quality == "Weak":
        codes.append("WEAK_PEER_GROUP")
        risks.extend(rating.peer_group_reasons)
    if rating.data_quality_reasons:
        codes.append("DATA_QUALITY_LIMITATION")
        risks.extend(rating.data_quality_reasons[:6])

    deduped_codes = []
    for code in codes:
        if code not in deduped_codes:
            deduped_codes.append(code)
    deduped_risks = []
    for risk in risks:
        if risk and risk not in deduped_risks:
            deduped_risks.append(risk)

    rating.recommendation_reason_codes = deduped_codes
    rating.recommendation_reasons = reasons
    rating.recommendation_risk_flags = deduped_risks
    rating.analysis_caveat = (
        "Internal research output, not investment advice. Verify latest filings, prices, "
        "corporate actions, and risk events before acting."
    )


def apply_advanced_overlays(
    ratings: Dict[str, StockRating],
    stocks: Dict[str, RawStockData],
    market_mode: str,
    market_regime_source: str = "unknown",
    market_regime_confidence: str = "Low",
) -> None:
    """
    10-action upgrade layer:
    1) bear-mode behavior
    2) expected upside/downside
    3) sector regime
    4) drawdown resilience
    5) valuation confidence
    6) investability gate
    7) action notes
    8) staged entry plan
    9) selection score for tracking
    10) portfolio-ready values
    """
    sector_scores, sector_labels = _sector_regime_maps(ratings)
    calibration_profile = _load_calibration_profile()

    for ticker, rating in ratings.items():
        stock = stocks[ticker]
        rating.market_mode = market_mode
        rating.market_regime_source = market_regime_source
        rating.market_regime_confidence = market_regime_confidence
        rating.sector_regime_score = sector_scores.get(rating.classification.sector, 50.0)
        rating.sector_regime_label = sector_labels.get(rating.classification.sector, "Mixed")

        rating.drawdown_resilience_score = _drawdown_resilience(rating)
        rating.valuation_confidence_score = _valuation_confidence(rating, stock)
        rating.missing_critical_fields = _missing_critical_fields(rating, stock)
        rating.unknown_risk_flags = [
            f"missing:{field}" for field in rating.missing_critical_fields
        ]
        (
            rating.data_quality_score,
            rating.data_quality_status,
            rating.data_quality_reasons,
        ) = _data_quality(rating, stock)
        rating.recommendation_confidence_score = _weighted_average(
            [
                (rating.valuation_confidence_score, 0.30),
                (rating.potential_score, 0.30),
                (rating.red_flags.score, 0.25),
                (rating.data_quality_score, 0.15),
            ]
        )
        rating.recommendation_confidence = _confidence_from_score(rating.recommendation_confidence_score)
        rating.calibration_multiplier, rating.calibration_status = _calibration_multiplier(
            rating,
            calibration_profile,
        )

        upside, downside, rr_ratio, rr_score = _expected_upside_downside(rating, stock, market_mode)
        upside, calibrated_rr_ratio, calibrated_rr_score = _apply_upside_calibration(
            upside,
            downside,
            rating.calibration_multiplier,
        )
        if calibrated_rr_ratio is not None:
            rr_ratio = calibrated_rr_ratio
        if calibrated_rr_score is not None:
            rr_score = calibrated_rr_score
        rating.expected_upside_pct = upside
        rating.expected_downside_pct = downside
        rating.risk_reward_ratio = rr_ratio
        rating.risk_reward_score = rr_score
        rating.value_trap_score, rating.value_trap_flags = _value_trap_assessment(rating, stock)

        gate_passed, fail_reasons = _investability_gate(rating, stock, market_mode)
        rating.investability_gate_passed = gate_passed
        rating.gate_fail_reasons = fail_reasons

        # Recommendation upgrade with gate and bear-mode strictness.
        buy_potential = BUY_POTENTIAL_THRESHOLD
        buy_value = BUY_VALUATION_GAP_THRESHOLD
        watch_potential = WATCH_POTENTIAL_THRESHOLD
        watch_value = WATCH_VALUATION_GAP_THRESHOLD
        if market_mode == "bear":
            buy_potential += 4
            buy_value += 4
            watch_potential += 2
            watch_value += 2

        if (
            rating.investability_status == "Insufficient Data"
            or rating.data_quality_status == "Weak Data"
            or len(rating.missing_critical_fields) > MAX_MISSING_CRITICAL_FIELDS_RESEARCH
        ):
            rating.recommendation = "Insufficient Data"
        elif rating.value_trap_score is not None and rating.value_trap_score >= VALUE_TRAP_BLOCK_THRESHOLD:
            rating.recommendation = "Avoid"
        elif (
            rating.potential_score is not None
            and rating.valuation_gap_score is not None
            and rating.potential_score >= buy_potential
            and rating.valuation_gap_score >= buy_value
            and gate_passed
            and rating.investability_status == "Investable"
        ):
            rating.recommendation = "Buy Candidate"
        elif (
            rating.potential_score is not None
            and rating.valuation_gap_score is not None
            and rating.potential_score >= watch_potential
            and rating.valuation_gap_score >= watch_value
            and rating.investability_status not in {"Uninvestable", "Avoid"}
        ):
            rating.recommendation = "Watchlist"
        else:
            rating.recommendation = "Avoid"

        if not gate_passed and rating.recommendation == "Buy Candidate":
            rating.recommendation = "Watchlist"

        if market_mode == "bear":
            q_boost = BEAR_MODE_QUALITY_BONUS
            risk_penalty = BEAR_MODE_RISK_PENALTY
        else:
            q_boost = 1.0
            risk_penalty = 1.0

        rating.selection_score = round(
            (
                (rating.opportunity_score or 0.0) * SELECTION_SCORE_WEIGHTS["opportunity_score"]
                + (rating.potential_score or 0.0) * SELECTION_SCORE_WEIGHTS["potential_score"] * q_boost
                + (rating.risk_reward_score or 0.0) * SELECTION_SCORE_WEIGHTS["risk_reward_score"]
            ) * (rating.red_flags.score or 50.0) / 100.0 / risk_penalty * rating.calibration_multiplier,
            2,
        )

        rating.staged_entry_plan = _staged_entry_plan(rating)
        gate_note = "Gate passed" if gate_passed else f"Gate failed: {', '.join(fail_reasons)}"
        rating.action_note = (
            f"{rating.sector_regime_label} sector regime; "
            f"RR={rating.risk_reward_ratio or 0:.2f}; {gate_note}"
        )
        _set_recommendation_explanation(rating)
        rating.research_status, rating.research_status_reason = _research_status(rating)


def action_sheet_rows(ratings: Dict[str, StockRating]) -> List[dict]:
    rows: List[dict] = []
    for rating in ratings.values():
        rows.append(
            {
                "ticker": rating.ticker,
                "name": rating.name,
                "sector": rating.classification.sector,
                "template": rating.template.value,
                "template_supported": rating.template_supported,
                "template_support_status": rating.template_support_status,
                "template_support_reason": "; ".join(rating.template_support_reasons),
                "classification_source": rating.classification_source,
                "classification_confidence": rating.classification_confidence,
                "fundamentals_source": rating.fundamentals_source,
                "price_source": rating.price_source,
                "research_status": rating.research_status,
                "research_status_reason": rating.research_status_reason,
                "data_quality_score": rating.data_quality_score,
                "data_quality_status": rating.data_quality_status,
                "data_quality_reasons": "; ".join(rating.data_quality_reasons),
                "investability_status": rating.investability_status,
                "recommendation": rating.recommendation,
                "confidence": rating.recommendation_confidence,
                "confidence_score": rating.recommendation_confidence_score,
                "reason_codes": "; ".join(rating.recommendation_reason_codes),
                "recommendation_reasons": " ".join(rating.recommendation_reasons),
                "risk_flags": "; ".join(rating.recommendation_risk_flags),
                "market_mode": rating.market_mode,
                "market_regime_source": rating.market_regime_source,
                "market_regime_confidence": rating.market_regime_confidence,
                "sector_regime": rating.sector_regime_label,
                "selection_score": rating.selection_score,
                "potential_score": rating.potential_score,
                "valuation_gap_score": rating.valuation_gap_score,
                "expected_upside_pct": rating.expected_upside_pct,
                "expected_downside_pct": rating.expected_downside_pct,
                "risk_reward_ratio": rating.risk_reward_ratio,
                "entry_signal": rating.entry_signal,
                "staged_entry_plan": rating.staged_entry_plan,
                "gate_passed": rating.investability_gate_passed,
                "gate_fail_reasons": "; ".join(rating.gate_fail_reasons),
                "peer_group_quality": rating.peer_group_quality,
                "peer_group_reasons": "; ".join(rating.peer_group_reasons),
                "missing_critical_fields": "; ".join(rating.missing_critical_fields),
                "unknown_risk_flags": "; ".join(rating.unknown_risk_flags),
                "value_trap_score": rating.value_trap_score,
                "value_trap_flags": "; ".join(rating.value_trap_flags),
                "calibration_status": rating.calibration_status,
                "calibration_multiplier": rating.calibration_multiplier,
                "action_note": rating.action_note,
                "analysis_caveat": rating.analysis_caveat,
            }
        )
    return sorted(rows, key=lambda r: (r["selection_score"] or 0), reverse=True)


def _daily_list_candidates(leaderboard: List[dict]) -> List[dict]:
    """
    Filter leaderboard rows down to names worth surfacing in daily research.
    """
    required_rank = _confidence_to_rank(DAILY_LIST_MIN_CONFIDENCE)
    rows = [
        dict(row)
        for row in leaderboard
        if row.get("template_supported")
        and row.get("research_status") in {"Actionable", "Research Candidate"}
        and row.get("recommendation") in {"Buy Candidate", "Watchlist"}
        and _confidence_to_rank(row.get("confidence", "Low")) >= required_rank
    ]
    return sorted(rows, key=lambda r: (r.get("selection_score") or 0), reverse=True)


def daily_market_list_rows(leaderboard: List[dict]) -> List[dict]:
    """
    Build the mixed-market daily list with sector and financials caps.
    """
    sector_counts: Dict[str, int] = defaultdict(int)
    selected: List[dict] = []
    financial_total = 0
    bank_total = 0
    nbfc_total = 0

    for row in _daily_list_candidates(leaderboard):
        if len(selected) >= DAILY_LIST_MAX_NAMES:
            break
        sector = row.get("sector", "Unknown")
        template = row.get("template")
        if sector_counts[sector] >= DAILY_LIST_MAX_PER_SECTOR:
            continue
        if template in {"B", "C"} and financial_total >= DAILY_LIST_MAX_FINANCIAL_NAMES:
            continue
        if template == "B" and bank_total >= DAILY_LIST_MAX_BANK_NAMES:
            continue
        if template == "C" and nbfc_total >= DAILY_LIST_MAX_NBFC_NAMES:
            continue

        sector_counts[sector] += 1
        if template in {"B", "C"}:
            financial_total += 1
        if template == "B":
            bank_total += 1
        elif template == "C":
            nbfc_total += 1
        selected.append(row)
    return selected


def template_daily_rows(leaderboard: List[dict], template_code: str) -> List[dict]:
    """
    Build template-specific daily queues for banks or NBFCs/HFCs.
    """
    rows = [row for row in _daily_list_candidates(leaderboard) if row.get("template") == template_code]
    return rows[:DAILY_LIST_MAX_NAMES]


def portfolio_plan_rows(leaderboard: List[dict]) -> List[dict]:
    """
    Build a portfolio suggestion with sector and single-stock caps.
    """
    required_rank = _confidence_to_rank(PORTFOLIO_MIN_CONFIDENCE)
    candidates = [
        r for r in leaderboard
        if r.get("recommendation") == "Buy Candidate"
        and _confidence_to_rank(r.get("confidence", "Low")) >= required_rank
    ]
    candidates = sorted(candidates, key=lambda r: (r.get("selection_score") or 0), reverse=True)

    max_names = max(1, PORTFOLIO_MAX_HOLDINGS)
    base_weight = min(100.0 / max_names, PORTFOLIO_MAX_SINGLE_STOCK_WEIGHT_PCT)

    sector_weight: Dict[str, float] = defaultdict(float)
    selected: List[dict] = []
    for row in candidates:
        if len(selected) >= max_names:
            break
        sector = row.get("sector", "Unknown")
        if sector_weight[sector] + base_weight > PORTFOLIO_MAX_SECTOR_WEIGHT_PCT + 1e-9:
            continue
        sector_weight[sector] += base_weight
        selected.append(dict(row))

    if not selected:
        return []

    total = base_weight * len(selected)
    for row in selected:
        weight = base_weight * 100.0 / total
        row["suggested_weight_pct"] = round(min(weight, PORTFOLIO_MAX_SINGLE_STOCK_WEIGHT_PCT), 2)
        row["risk_budget_note"] = (
            f"sector_cap={PORTFOLIO_MAX_SECTOR_WEIGHT_PCT}%, "
            f"single_cap={PORTFOLIO_MAX_SINGLE_STOCK_WEIGHT_PCT}%"
        )
    return selected


def update_recommendation_history(
    run_date: str,
    ratings: Dict[str, StockRating],
    stocks: Dict[str, RawStockData],
    history_path: Path,
) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not history_path.exists()
    with open(history_path, "a", newline="") as f:
        fields = [
            "run_date",
            "ticker",
            "recommendation",
            "confidence",
            "selection_score",
            "opportunity_score",
            "close_price",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        for ticker, rating in ratings.items():
            writer.writerow(
                {
                    "run_date": run_date,
                    "ticker": ticker,
                    "recommendation": rating.recommendation,
                    "confidence": rating.recommendation_confidence,
                    "selection_score": rating.selection_score,
                    "opportunity_score": rating.opportunity_score,
                    "close_price": stocks[ticker].fundamentals.get("close_price"),
                }
            )


def evaluate_recommendation_outcomes(
    run_date_str: str,
    ratings: Dict[str, StockRating],
    stocks: Dict[str, RawStockData],
    history_path: Path,
) -> dict:
    """
    Computes simple outcome metrics by comparing current price against historical calls.
    """
    if not history_path.exists():
        return {"status": "insufficient_history"}

    today = date.fromisoformat(run_date_str)
    with open(history_path, newline="") as f:
        rows = list(csv.DictReader(f))

    matured = []
    for row in rows:
        try:
            call_date = date.fromisoformat(row["run_date"])
        except Exception:
            continue
        if (today - call_date).days < OUTCOME_HORIZON_DAYS:
            continue
        ticker = row.get("ticker")
        current = stocks.get(ticker)
        if not current:
            continue
        start_price = float(row["close_price"]) if row.get("close_price") not in {"", None} else None
        end_price = current.fundamentals.get("close_price")
        if start_price is None or end_price is None or start_price <= 0:
            continue
        ret = (end_price / start_price) - 1.0
        matured.append(
            {
                "ticker": ticker,
                "recommendation": row.get("recommendation", ""),
                "return_pct": ret * 100.0,
            }
        )

    if not matured:
        return {"status": "insufficient_matured_calls", "n_calls": 0}

    buy = [m for m in matured if m["recommendation"] == "Buy Candidate"]
    buy_hit = sum(1 for m in buy if m["return_pct"] > 0)
    buy_hit_rate = (buy_hit / len(buy)) if buy else None

    avg_all = sum(m["return_pct"] for m in matured) / len(matured)
    result = {
        "status": "ok",
        "n_calls": len(matured),
        "buy_calls": len(buy),
        "avg_return_pct_all_calls": round(avg_all, 2),
        "buy_hit_rate": round(buy_hit_rate, 3) if buy_hit_rate is not None else None,
        "recalibration_alert": bool(buy_hit_rate is not None and buy_hit_rate < BUY_HIT_RATE_ALERT_THRESHOLD),
    }
    return result
