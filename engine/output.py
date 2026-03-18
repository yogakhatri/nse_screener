"""
NSE Rating Engine – Output Formatter
Serializes StockRating to the canonical JSON / dict structure.
"""
from __future__ import annotations
import json
from .config import template_display_name
from .models import StockRating

def to_dict(rating: StockRating) -> dict:
    def card_block(card):
        return {
            "score":         card.score,
            "label":         card.label,
            "data_coverage": f"{round(card.data_coverage * 100, 1)}%",
            "is_rankable":   card.is_rankable,
            "reason":        card.reason,
            "sub_scores":    {k: round(v, 2) if v is not None else None
                              for k, v in card.sub_scores.items()},
        }

    cls = rating.classification
    return {
        "stock_name":       rating.name,
        "ticker":           rating.ticker,
        "classification": {
            "macro_sector":   cls.macro_sector,
            "sector":         cls.sector,
            "industry":       cls.industry,
            "basic_industry": cls.basic_industry,
        },
        "template_used":    f"Template {rating.template.value} ({template_display_name(rating.template.value)})",
        "peer_group": {
            "level":   rating.peer_level.value,
            "n_peers": rating.n_peers,
            "tickers": rating.peer_group[:10],   # show first 10
        },
        "cards": {
            "performance":   card_block(rating.performance),
            "valuation":     card_block(rating.valuation),
            "growth":        card_block(rating.growth),
            "profitability": card_block(rating.profitability),
            "entry_point":   card_block(rating.entry_point),
            "red_flags":     card_block(rating.red_flags),
        },
        "final_opportunity_score": rating.opportunity_score,
        "investability_status":    rating.investability_status,
        "potential_score":         rating.potential_score,
        "valuation_gap_score":     rating.valuation_gap_score,
        "recommendation":          rating.recommendation,
        "recommendation_confidence": rating.recommendation_confidence,
        "entry_signal":            rating.entry_signal,
        "market_mode":             rating.market_mode,
        "sector_regime_score":     rating.sector_regime_score,
        "sector_regime_label":     rating.sector_regime_label,
        "drawdown_resilience_score": rating.drawdown_resilience_score,
        "valuation_confidence_score": rating.valuation_confidence_score,
        "expected_upside_pct":     rating.expected_upside_pct,
        "expected_downside_pct":   rating.expected_downside_pct,
        "risk_reward_ratio":       rating.risk_reward_ratio,
        "risk_reward_score":       rating.risk_reward_score,
        "selection_score":         rating.selection_score,
        "investability_gate_passed": rating.investability_gate_passed,
        "gate_fail_reasons":       rating.gate_fail_reasons,
        "template_supported":      rating.template_supported,
        "template_support_status": rating.template_support_status,
        "template_support_reasons": rating.template_support_reasons,
        "staged_entry_plan":       rating.staged_entry_plan,
        "action_note":             rating.action_note,
        "ranks": {
            "sector_rank": rating.sector_rank,
            "sector_percentile": rating.sector_percentile,
            "basic_industry_rank": rating.basic_industry_rank,
            "basic_industry_percentile": rating.basic_industry_percentile,
        },
        "summary": {
            "top_3_strengths":  rating.strengths,
            "top_3_weaknesses": rating.weaknesses,
        },
    }

def to_json(rating: StockRating, indent: int = 2) -> str:
    return json.dumps(to_dict(rating), indent=indent, default=str)
