"""
NSE Rating Engine – Main Orchestrator
Wires together: template routing → peer resolution → card scoring → aggregation → output.
"""
from __future__ import annotations
from collections import defaultdict
from typing import Dict, List, Optional
from .config import infer_template_code_from_basic_industry, validate_runtime_config
from .models import (RawStockData, StockRating, Template, NSEClassification, PeerLevel)
from .peer_group import resolve_peer_group
from .cards import (score_performance, score_valuation, score_growth,
                    score_profitability, score_entry_point, score_red_flags,
                    validate_metric_direction_map)
from .aggregator import compute_opportunity_score
from .advanced import infer_market_mode, apply_advanced_overlays
from .output import to_dict, to_json

def _assign_template(basic_industry: str) -> Template:
    return Template(infer_template_code_from_basic_industry(basic_industry))

class NSERatingEngine:
    """
    Main engine class. Call `.rate(ticker)` for a single stock,
    or `.rate_universe()` for all stocks.
    """

    def __init__(self, stock_data: Dict[str, RawStockData], market_mode: str = "auto"):
        """
        stock_data: dict of {ticker: RawStockData}
        All stocks must have ≥ 252 trading days of price history (pre-filtered).
        """
        validate_runtime_config()
        validate_metric_direction_map()
        self.stocks = stock_data
        self.requested_market_mode = market_mode
        self.market_mode = infer_market_mode(stock_data, market_mode)
        self._cls_map: Dict[str, NSEClassification] = {
            t: d.classification for t, d in stock_data.items()
        }

    def rate(self, ticker: str) -> StockRating:
        stock = self.stocks[ticker]
        template = _assign_template(stock.classification.basic_industry)
        peer_tickers, peer_level = resolve_peer_group(ticker, self._cls_map)
        peers = [self.stocks[t] for t in peer_tickers if t in self.stocks]

        rating = StockRating(
            ticker=ticker,
            name=stock.name,
            classification=stock.classification,
            template=template,
            peer_group=peer_tickers,
            peer_level=peer_level,
            n_peers=len(peers),
        )

        rating.performance   = score_performance(stock, peers, template)
        rating.valuation     = score_valuation(stock, peers, template)
        rating.growth        = score_growth(stock, peers, template)
        rating.profitability = score_profitability(stock, peers, template)
        rating.entry_point   = score_entry_point(stock, peers, template)
        rating.red_flags     = score_red_flags(stock, peers, template)

        rating = compute_opportunity_score(rating)
        return rating

    def rate_universe(self) -> Dict[str, StockRating]:
        results = {}
        for ticker in self.stocks:
            try:
                results[ticker] = self.rate(ticker)
            except Exception as e:
                print(f"[WARN] Skipping {ticker}: {e}")
        apply_advanced_overlays(results, self.stocks, self.market_mode)
        self._annotate_relative_ranks(results)
        return results

    def _annotate_relative_ranks(self, ratings: Dict[str, StockRating]) -> None:
        def percentile(rank: int, total: int) -> float:
            if total <= 1:
                return 100.0
            return round((1.0 - ((rank - 1) / (total - 1))) * 100.0, 2)

        by_sector: dict[str, list[StockRating]] = defaultdict(list)
        by_basic_industry: dict[str, list[StockRating]] = defaultdict(list)

        for rating in ratings.values():
            if rating.opportunity_score is None:
                continue
            by_sector[rating.classification.sector].append(rating)
            by_basic_industry[rating.classification.basic_industry].append(rating)

        for group in by_sector.values():
            group_sorted = sorted(
                group,
                key=lambda r: (r.opportunity_score if r.opportunity_score is not None else -1.0),
                reverse=True,
            )
            total = len(group_sorted)
            for idx, rating in enumerate(group_sorted, start=1):
                rating.sector_rank = idx
                rating.sector_percentile = percentile(idx, total)

        for group in by_basic_industry.values():
            group_sorted = sorted(
                group,
                key=lambda r: (r.opportunity_score if r.opportunity_score is not None else -1.0),
                reverse=True,
            )
            total = len(group_sorted)
            for idx, rating in enumerate(group_sorted, start=1):
                rating.basic_industry_rank = idx
                rating.basic_industry_percentile = percentile(idx, total)

    def to_leaderboard(
        self, ratings: Dict[str, StockRating],
        exclude_statuses=("Uninvestable", "Insufficient Data", "Unsupported Data")
    ) -> List[dict]:
        """Return sorted list of rating dicts by Opportunity Score (desc)."""
        rows = []
        for ticker, r in ratings.items():
            if r.investability_status in exclude_statuses:
                continue
            rows.append({
                "ticker":              ticker,
                "name":                r.name,
                "sector":              r.classification.sector,
                "basic_industry":      r.classification.basic_industry,
                "template":            r.template.value,
                "peer_level":          r.peer_level.value,
                "performance":         r.performance.score,
                "valuation":           r.valuation.score,
                "growth":              r.growth.score,
                "profitability":       r.profitability.score,
                "entry_point":         r.entry_point.score,
                "red_flags":           r.red_flags.score,
                "opportunity_score":   r.opportunity_score,
                "investability_status":r.investability_status,
                "potential_score":     r.potential_score,
                "valuation_gap_score": r.valuation_gap_score,
                "recommendation":      r.recommendation,
                "confidence":          r.recommendation_confidence,
                "entry_signal":        r.entry_signal,
                "market_mode":         r.market_mode,
                "sector_regime_score": r.sector_regime_score,
                "sector_regime_label": r.sector_regime_label,
                "drawdown_resilience_score": r.drawdown_resilience_score,
                "valuation_confidence_score": r.valuation_confidence_score,
                "expected_upside_pct": r.expected_upside_pct,
                "expected_downside_pct": r.expected_downside_pct,
                "risk_reward_ratio":   r.risk_reward_ratio,
                "risk_reward_score":   r.risk_reward_score,
                "selection_score":     r.selection_score,
                "gate_passed":         r.investability_gate_passed,
                "gate_fail_reasons":   "; ".join(r.gate_fail_reasons),
                "template_supported":  r.template_supported,
                "template_support_status": r.template_support_status,
                "template_support_reason": "; ".join(r.template_support_reasons),
                "staged_entry_plan":   r.staged_entry_plan,
                "action_note":         r.action_note,
                "sector_rank":         r.sector_rank,
                "sector_percentile":   r.sector_percentile,
                "basic_industry_rank": r.basic_industry_rank,
                "basic_industry_percentile": r.basic_industry_percentile,
            })
        return sorted(
            rows,
            key=lambda x: ((x.get("selection_score") or 0), (x.get("opportunity_score") or 0)),
            reverse=True,
        )

    def rate_to_json(self, ticker: str) -> str:
        return to_json(self.rate(ticker))
