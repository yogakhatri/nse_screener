"""
NSE Rating Engine – Main Orchestrator
Wires together: template routing → peer resolution → card scoring → aggregation → output.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import date
from typing import Dict, List, Optional
from .config import (
    PEER_MIN_BASIC_INDUSTRY,
    PEER_MIN_INDUSTRY,
    PEER_MIN_SECTOR,
    infer_template_code,
    validate_runtime_config,
)
from .models import (RawStockData, StockRating, Template, NSEClassification, PeerLevel)
from .peer_group import resolve_peer_group
from .cards import (score_performance, score_valuation, score_growth,
                    score_profitability, score_entry_point, score_red_flags,
                    score_contrarian, validate_metric_direction_map)
from .aggregator import compute_opportunity_score
from .advanced import infer_market_mode_details, apply_advanced_overlays
from .output import to_dict, to_json

def _assign_template(classification: NSEClassification) -> Template:
    return Template(
        infer_template_code(
            macro_sector=classification.macro_sector,
            sector=classification.sector,
            industry=classification.industry,
            basic_industry=classification.basic_industry,
        )
    )

class NSERatingEngine:
    """
    Main engine class. Call `.rate(ticker)` for a single stock,
    or `.rate_universe()` for all stocks.
    """

    def __init__(
        self,
        stock_data: Dict[str, RawStockData],
        market_mode: str = "auto",
        run_date: Optional[date] = None,
        investment_horizon: str | None = None,
    ):
        """
        stock_data: dict of {ticker: RawStockData}
        All stocks must have ≥ 252 trading days of price history (pre-filtered).
        """
        validate_runtime_config()
        validate_metric_direction_map()
        self.stocks = stock_data
        self.requested_market_mode = market_mode
        self.investment_horizon = investment_horizon
        (
            self.market_mode,
            self.market_regime_source,
            self.market_regime_confidence,
        ) = infer_market_mode_details(stock_data, market_mode, run_date=run_date)
        self._cls_map: Dict[str, NSEClassification] = {
            t: d.classification for t, d in stock_data.items()
        }

    def _peer_group_quality(self, peer_level: PeerLevel, n_peers: int) -> tuple[str, list[str]]:
        """Assess whether the chosen peer group is large enough for reliable percentile scoring."""
        minimum_by_level = {
            PeerLevel.BASIC_INDUSTRY: PEER_MIN_BASIC_INDUSTRY - 1,
            PeerLevel.INDUSTRY: PEER_MIN_INDUSTRY - 1,
            PeerLevel.SECTOR: PEER_MIN_SECTOR - 1,
        }
        minimum = minimum_by_level.get(peer_level, PEER_MIN_SECTOR - 1)
        if n_peers >= minimum:
            return "Strong", []
        return (
            "Weak",
            [
                f"{peer_level.value} peer group has {n_peers} peers; "
                f"minimum configured peer count is {minimum}"
            ],
        )

    def rate(self, ticker: str) -> StockRating:
        stock = self.stocks[ticker]
        template = _assign_template(stock.classification)
        peer_tickers, peer_level = resolve_peer_group(ticker, self._cls_map)
        peers = [self.stocks[t] for t in peer_tickers if t in self.stocks]
        provenance_summary = Counter(
            item.source
            for metric, item in stock.metric_provenance.items()
            if stock.fundamentals.get(metric) is not None
        )
        field_provenance = {
            metric: {
                "source": item.source,
                "source_field": item.source_field,
                "confidence": item.confidence,
                "freshness": item.freshness,
                "method": item.method,
            }
            for metric, item in stock.metric_provenance.items()
        }

        rating = StockRating(
            ticker=ticker,
            name=stock.name,
            classification=stock.classification,
            template=template,
            peer_group=peer_tickers,
            peer_level=peer_level,
            n_peers=len(peers),
            classification_source=stock.classification_source,
            classification_confidence=stock.classification_confidence,
            fundamentals_source=stock.fundamentals_source,
            price_source=stock.price_source,
            metric_source_summary=dict(provenance_summary),
            field_provenance=field_provenance,
        )
        rating.peer_group_quality, rating.peer_group_reasons = self._peer_group_quality(
            peer_level,
            len(peers),
        )

        rating.performance   = score_performance(stock, peers, template)
        rating.valuation     = score_valuation(stock, peers, template)
        rating.growth        = score_growth(stock, peers, template)
        rating.profitability = score_profitability(stock, peers, template)
        rating.entry_point   = score_entry_point(stock, peers, template)
        rating.contrarian    = score_contrarian(stock, peers, template)
        rating.red_flags     = score_red_flags(stock, peers, template)

        rating = compute_opportunity_score(
            rating,
            market_mode=self.market_mode,
            investment_horizon=self.investment_horizon,
        )
        return rating

    def rate_universe(self) -> Dict[str, StockRating]:
        results = {}
        for ticker in self.stocks:
            try:
                results[ticker] = self.rate(ticker)
            except Exception as e:
                print(f"[WARN] Skipping {ticker}: {e}")
        apply_advanced_overlays(
            results,
            self.stocks,
            self.market_mode,
            market_regime_source=self.market_regime_source,
            market_regime_confidence=self.market_regime_confidence,
        )
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
        self,
        ratings: Dict[str, StockRating],
        exclude_statuses=("Uninvestable", "Insufficient Data", "Unsupported Data"),
        *,
        include_all_rated: bool = False,
    ) -> List[dict]:
        """Return sorted list of rating dicts by selection score (desc)."""
        def _source_cell(rating: StockRating, metrics: tuple[str, ...]) -> str:
            """Compress selected metric provenance into a CSV-friendly cell."""
            parts = []
            for metric in metrics:
                meta = rating.field_provenance.get(metric) or {}
                if not meta:
                    continue
                source = meta.get("source", "")
                confidence = meta.get("confidence", "")
                freshness = meta.get("freshness", "")
                parts.append(f"{metric}:{source}/{confidence}/{freshness}")
            return "; ".join(parts)

        rows = []
        for ticker, r in ratings.items():
            if not include_all_rated:
                if r.investability_status in exclude_statuses:
                    continue
                if r.recommendation in {"Insufficient Data", "Unsupported"}:
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
                "contrarian":          r.contrarian.score,
                "red_flags":           r.red_flags.score,
                "opportunity_score":   r.opportunity_score,
                "investability_status":r.investability_status,
                "potential_score":     r.potential_score,
                "valuation_gap_score": r.valuation_gap_score,
                "recommendation":      r.recommendation,
                "confidence":          r.recommendation_confidence,
                "confidence_score":    r.recommendation_confidence_score,
                "reason_codes":        "; ".join(r.recommendation_reason_codes),
                "recommendation_reasons": " ".join(r.recommendation_reasons),
                "risk_flags":          "; ".join(r.recommendation_risk_flags),
                "entry_signal":        r.entry_signal,
                "market_mode":         r.market_mode,
                "market_regime_source": r.market_regime_source,
                "market_regime_confidence": r.market_regime_confidence,
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
                "peer_group_quality":  r.peer_group_quality,
                "peer_group_reasons":  "; ".join(r.peer_group_reasons),
                "template_supported":  r.template_supported,
                "template_support_status": r.template_support_status,
                "template_support_reason": "; ".join(r.template_support_reasons),
                "classification_source": r.classification_source,
                "classification_confidence": r.classification_confidence,
                "fundamentals_source": r.fundamentals_source,
                "price_source": r.price_source,
                "research_status": r.research_status,
                "research_status_reason": r.research_status_reason,
                "research_tier": r.research_tier,
                "research_tier_reason": r.research_tier_reason,
                "data_quality_score": r.data_quality_score,
                "data_quality_status": r.data_quality_status,
                "data_quality_reasons": "; ".join(r.data_quality_reasons),
                "missing_critical_fields": "; ".join(r.missing_critical_fields),
                "unknown_risk_flags": "; ".join(r.unknown_risk_flags),
                "value_trap_score": r.value_trap_score,
                "value_trap_flags": "; ".join(r.value_trap_flags),
                "calibration_status": r.calibration_status,
                "calibration_multiplier": r.calibration_multiplier,
                "user_profile": r.user_profile_name,
                "user_filter_passed": r.user_filter_passed,
                "user_filter_reasons": "; ".join(r.user_filter_reasons),
                "user_profile_score": r.user_profile_score,
                "user_profile_notes": "; ".join(r.user_profile_notes),
                "metric_source_summary": "; ".join(
                    f"{source}:{count}" for source, count in sorted(r.metric_source_summary.items())
                ),
                "valuation_metric_sources": _source_cell(r, ("iv_gap", "fair_value_gap", "pe_percentile", "pb_percentile")),
                "price_metric_sources": _source_cell(r, ("return_1y", "return_6m", "drawdown_recovery", "close_price")),
                "risk_metric_sources": _source_cell(r, ("promoter_pledge", "default_distress", "asm_gsm_risk", "liquidity_manipulation")),
                "staged_entry_plan":   r.staged_entry_plan,
                "action_note":         r.action_note,
                "analysis_caveat":     r.analysis_caveat,
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
