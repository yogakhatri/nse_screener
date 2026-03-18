"""
Advanced overlays for recommendation quality, downside control, and portfolio safety.
"""
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from .config import (
    AUTO_BEAR_DRAWDOWN_RECOVERY_THRESHOLD,
    AUTO_BEAR_RETURN_6M_THRESHOLD,
    AUTO_BULL_DRAWDOWN_RECOVERY_THRESHOLD,
    AUTO_BULL_RETURN_6M_THRESHOLD,
    BEAR_GATE_MAX_DEFAULT_DISTRESS_RISK,
    BEAR_GATE_MAX_PROMOTER_PLEDGE_PCT,
    BEAR_GATE_MIN_RED_FLAGS_SCORE,
    BEAR_MODE_QUALITY_BONUS,
    BEAR_MODE_RISK_PENALTY,
    BUY_HIT_RATE_ALERT_THRESHOLD,
    BUY_POTENTIAL_THRESHOLD,
    BUY_VALUATION_GAP_THRESHOLD,
    CONFIDENCE_HIGH_THRESHOLD,
    CONFIDENCE_MEDIUM_THRESHOLD,
    DEFAULT_MARKET_MODE,
    ENTRY_STAGE_WEIGHTS,
    GATE_MAX_DEFAULT_DISTRESS_RISK,
    GATE_MAX_PROMOTER_PLEDGE_PCT,
    GATE_MIN_CONFIDENCE_FOR_BUY,
    GATE_MIN_LIQUIDITY_TURNOVER_CR,
    GATE_MIN_RED_FLAGS_SCORE,
    MAX_EXPECTED_DOWNSIDE_PCT,
    MAX_STOP_LOSS_PCT,
    MIN_EXPECTED_DOWNSIDE_PCT,
    MIN_STOP_LOSS_PCT,
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


def infer_market_mode(
    stocks: Dict[str, RawStockData],
    requested_mode: str | None = None,
) -> str:
    """
    Decide regime mode. In auto mode we infer from median 6M return + drawdown recovery.
    """
    mode = (requested_mode or DEFAULT_MARKET_MODE).lower().strip()
    if mode in {"bear", "bull", "neutral"}:
        return mode

    ret_6m = []
    dd_recovery = []
    for stock in stocks.values():
        r6 = stock.fundamentals.get("return_6m")
        dr = stock.fundamentals.get("drawdown_recovery")
        if r6 is not None:
            ret_6m.append(r6)
        if dr is not None:
            dd_recovery.append(dr)

    if not ret_6m and not dd_recovery:
        return "neutral"

    median_r6 = sorted(ret_6m)[len(ret_6m) // 2] if ret_6m else 0.0
    median_dd = sorted(dd_recovery)[len(dd_recovery) // 2] if dd_recovery else 50.0

    if median_r6 <= AUTO_BEAR_RETURN_6M_THRESHOLD or median_dd <= AUTO_BEAR_DRAWDOWN_RECOVERY_THRESHOLD:
        return "bear"
    if median_r6 >= AUTO_BULL_RETURN_6M_THRESHOLD and median_dd >= AUTO_BULL_DRAWDOWN_RECOVERY_THRESHOLD:
        return "bull"
    return "neutral"


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

    pledge = stock.fundamentals.get("promoter_pledge")
    if pledge is not None and pledge > pledge_max:
        reasons.append(f"High promoter pledge (>{pledge_max}%)")

    distress = stock.fundamentals.get("default_distress")
    if distress is not None and distress > distress_max:
        reasons.append(f"Default/distress risk too high (>{distress_max})")

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


def apply_advanced_overlays(
    ratings: Dict[str, StockRating],
    stocks: Dict[str, RawStockData],
    market_mode: str,
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

    for ticker, rating in ratings.items():
        stock = stocks[ticker]
        rating.market_mode = market_mode
        rating.sector_regime_score = sector_scores.get(rating.classification.sector, 50.0)
        rating.sector_regime_label = sector_labels.get(rating.classification.sector, "Mixed")

        rating.drawdown_resilience_score = _drawdown_resilience(rating)
        rating.valuation_confidence_score = _valuation_confidence(rating, stock)
        rating.recommendation_confidence = _confidence_from_score(
            _weighted_average(
                [
                    (rating.valuation_confidence_score, 0.35),
                    (rating.potential_score, 0.35),
                    (rating.red_flags.score, 0.30),
                ]
            )
        )

        upside, downside, rr_ratio, rr_score = _expected_upside_downside(rating, stock, market_mode)
        rating.expected_upside_pct = upside
        rating.expected_downside_pct = downside
        rating.risk_reward_ratio = rr_ratio
        rating.risk_reward_score = rr_score

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
            ) * (rating.red_flags.score or 50.0) / 100.0 / risk_penalty,
            2,
        )

        rating.staged_entry_plan = _staged_entry_plan(rating)
        gate_note = "Gate passed" if gate_passed else f"Gate failed: {', '.join(fail_reasons)}"
        rating.action_note = (
            f"{rating.sector_regime_label} sector regime; "
            f"RR={rating.risk_reward_ratio or 0:.2f}; {gate_note}"
        )


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
                "investability_status": rating.investability_status,
                "recommendation": rating.recommendation,
                "confidence": rating.recommendation_confidence,
                "market_mode": rating.market_mode,
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
                "action_note": rating.action_note,
            }
        )
    return sorted(rows, key=lambda r: (r["selection_score"] or 0), reverse=True)


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
