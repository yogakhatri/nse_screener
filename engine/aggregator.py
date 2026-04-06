"""
NSE Rating Engine – Aggregator
Computes Opportunity Score, applies Red Flag caps, assigns Investability status.
"""
from __future__ import annotations
from typing import Optional
from .config import (
    OPPORTUNITY_WEIGHTS,
    BEAR_OPPORTUNITY_WEIGHTS,
    RED_FLAG_CAPS,
    INVESTABILITY,
    MIN_RANKABLE_CARDS,
    BUY_POTENTIAL_THRESHOLD,
    BUY_VALUATION_GAP_THRESHOLD,
    WATCH_POTENTIAL_THRESHOLD,
    WATCH_VALUATION_GAP_THRESHOLD,
    POTENTIAL_SCORE_WEIGHTS,
    VALUATION_GAP_SCORE_WEIGHTS,
    CONFIDENCE_HIGH_THRESHOLD,
    CONFIDENCE_MEDIUM_THRESHOLD,
)
from .models import StockRating

def _rf_band(rf_score: float):
    for lo, hi, cap, status in RED_FLAG_CAPS:
        if lo <= rf_score < hi or (hi == 100 and rf_score == 100):
            return cap, status
    return None, "Avoid"

def compute_opportunity_score(rating: StockRating, market_mode: str = "auto") -> StockRating:
    """
    1. Compute raw Opportunity Score from 6 cards (no Red Flags).
    2. Apply Red Flag cap.
    3. Set Investability Status.
    4. Populate Top 3 Strengths & Weaknesses.
    Bear mode uses BEAR_OPPORTUNITY_WEIGHTS, which heavily weights contrarian signals.
    """
    cards = {
        "performance":   rating.performance,
        "valuation":     rating.valuation,
        "growth":        rating.growth,
        "profitability": rating.profitability,
        "entry_point":   rating.entry_point,
        "contrarian":    rating.contrarian,
    }
    opp_weights = BEAR_OPPORTUNITY_WEIGHTS if market_mode == "bear" else OPPORTUNITY_WEIGHTS

    # Check eligibility: at least MIN_RANKABLE_CARDS of 6 cards must be rankable
    all_six = list(cards.values()) + [rating.red_flags]
    n_rankable = sum(1 for c in all_six if c.is_rankable)
    if n_rankable < MIN_RANKABLE_CARDS:
        rating.is_eligible = False
        rating.investability_status = "Insufficient Data"
        return _set_phase_outputs(rating, cards)

    # Weighted average of available rankable cards
    total_w = 0.0
    weighted_sum = 0.0
    for card_name, card in cards.items():
        if card.is_rankable and card.score is not None:
            w = opp_weights[card_name]
            weighted_sum += card.score * w
            total_w += w

    raw_score = round(weighted_sum / total_w, 2) if total_w > 0 else 0.0

    # Red Flag penalty
    if rating.red_flags.is_rankable and rating.red_flags.score is not None:
        cap, rf_status = _rf_band(rating.red_flags.score)
        if rf_status == "Uninvestable":
            rating.opportunity_score = raw_score
            rating.investability_status = "Uninvestable"
            rating = _set_summary(rating, cards)
            return _set_phase_outputs(rating, cards)
        final_score = min(raw_score, cap) if cap is not None else raw_score
        rating.opportunity_score = round(final_score, 2)
    else:
        rating.opportunity_score = round(raw_score, 2)
        rf_status = None

    # Investability
    if rf_status == "Uninvestable":
        rating.investability_status = "Uninvestable"
    elif rf_status == "Avoid":
        rating.investability_status = "Avoid"
    else:
        for lo, hi, status in INVESTABILITY:
            if lo <= rating.opportunity_score <= hi:
                rating.investability_status = status
                break

    rating = _set_summary(rating, cards)
    return _set_phase_outputs(rating, cards)

def _set_summary(rating: StockRating, cards: dict) -> StockRating:
    """Rank cards by score to derive Top 3 Strengths & Weaknesses."""
    scored = {
        name: card.score for name, card in cards.items()
        if card.is_rankable and card.score is not None
    }
    # Also include red flags in summary pool
    if rating.red_flags.is_rankable and rating.red_flags.score is not None:
        scored["red_flags"] = rating.red_flags.score

    sorted_cards = sorted(scored, key=scored.get, reverse=True)
    label_map = {name: getattr(rating, name).label for name in scored}

    rating.strengths  = [f"{n.replace('_',' ').title()}: {label_map[n]}" for n in sorted_cards[:3]]
    rating.weaknesses = [f"{n.replace('_',' ').title()}: {label_map[n]}" for n in sorted_cards[-3:]]
    return rating

def _weighted_average(values: list[tuple[Optional[float], float]]) -> Optional[float]:
    total_w = 0.0
    weighted_sum = 0.0
    for value, weight in values:
        if value is None:
            continue
        weighted_sum += value * weight
        total_w += weight
    if total_w == 0:
        return None
    return round(weighted_sum / total_w, 2)

def _entry_signal(entry_score: Optional[float]) -> str:
    if entry_score is None:
        return "Unknown"
    if entry_score >= 75:
        return "Accumulation Zone"
    if entry_score >= 55:
        return "Constructive Pullback"
    if entry_score >= 40:
        return "Neutral - Wait for Setup"
    return "Wait / Expensive Entry"

def _confidence_label(rating: StockRating, cards: dict) -> str:
    rankable_cards = [c for c in list(cards.values()) + [rating.red_flags] if c.is_rankable]
    if not rankable_cards:
        return "Low"
    avg_coverage = sum(c.data_coverage for c in rankable_cards) / len(rankable_cards)
    peer_bonus = {
        "Basic Industry": 10.0,
        "Industry": 5.0,
        "Sector": 0.0,
    }.get(rating.peer_level.value, 0.0)
    confidence_score = avg_coverage * 100.0 + len(rankable_cards) * 4.0 + peer_bonus
    if confidence_score >= CONFIDENCE_HIGH_THRESHOLD:
        return "High"
    if confidence_score >= CONFIDENCE_MEDIUM_THRESHOLD:
        return "Medium"
    return "Low"

def _recommendation(rating: StockRating) -> str:
    if rating.investability_status in ("Uninvestable", "Avoid", "Insufficient Data"):
        return "Avoid"
    if rating.red_flags.score is not None and rating.red_flags.score < 40:
        return "Avoid"
    if (rating.potential_score is not None and rating.valuation_gap_score is not None
            and rating.potential_score >= BUY_POTENTIAL_THRESHOLD
            and rating.valuation_gap_score >= BUY_VALUATION_GAP_THRESHOLD):
        return "Buy Candidate"
    if (rating.potential_score is not None and rating.valuation_gap_score is not None
            and rating.potential_score >= WATCH_POTENTIAL_THRESHOLD
            and rating.valuation_gap_score >= WATCH_VALUATION_GAP_THRESHOLD):
        return "Watchlist"
    return "Watchlist"

def _set_phase_outputs(rating: StockRating, cards: dict) -> StockRating:
    rating.potential_score = _weighted_average([
        (rating.growth.score, POTENTIAL_SCORE_WEIGHTS["growth"]),
        (rating.profitability.score, POTENTIAL_SCORE_WEIGHTS["profitability"]),
        (rating.performance.score, POTENTIAL_SCORE_WEIGHTS["performance"]),
        (rating.red_flags.score, POTENTIAL_SCORE_WEIGHTS["red_flags"]),
    ])
    rating.valuation_gap_score = _weighted_average([
        (rating.valuation.score, VALUATION_GAP_SCORE_WEIGHTS["valuation"]),
        (rating.entry_point.score, VALUATION_GAP_SCORE_WEIGHTS["entry_point"]),
    ])
    rating.entry_signal = _entry_signal(rating.entry_point.score)
    rating.recommendation_confidence = _confidence_label(rating, cards)
    rating.recommendation = _recommendation(rating)
    return rating
