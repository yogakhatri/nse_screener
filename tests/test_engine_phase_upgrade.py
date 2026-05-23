import unittest

from engine import NSERatingEngine, RawStockData, NSEClassification
from engine.advanced import (
    _has_critical_value,
    _missing_critical_fields,
    action_sheet_rows,
    daily_market_list_rows,
    daily_research_queue_rows,
    data_incomplete_rows,
    portfolio_plan_rows,
)
from engine.bias_controls import BiasAudit
from engine.cards import score_red_flags
from engine.config import CARD_WEIGHTS
from engine.metric_definitions import compute_cagr_3y
from engine.models import Template
from scripts.run_engine import data_quality_summary_rows


def _make_stock(ticker: str, pe: float, growth: float, pledge: float = 0.0) -> RawStockData:
    fundamentals = {
        "return_1y": 18.0 + growth,
        "return_6m": 9.0 + growth / 2,
        "cagr_5y": 14.0 + growth / 3,
        "peer_price_strength": 60.0 + growth,
        "drawdown_recovery": 55.0,
        "forward_view": 12.0 + growth,
        "pe_percentile": pe,
        "pb_percentile": 3.1,
        "p_cfo_percentile": 19.0,
        "ev_ebitda_percentile": 14.0,
        "hist_val_band": 78.0,
        "fcf_yield": 3.7,
        "iv_gap": 15.0,
        "rev_cagr_3y": 12.0 + growth,
        "eps_cagr_3y": 14.0 + growth,
        "rev_growth_yoy": 11.0 + growth,
        "eps_growth_yoy": 13.0 + growth,
        "peer_growth_rank": 60.0 + growth,
        "growth_stability": 66.0,
        "roce_3y_median": 22.0,
        "ebitda_margin": 24.0,
        "cfo_pat_ratio": 1.2,
        "margin_trend": 1.1,
        "roa": 13.0,
        "fcf_consistency": 80.0,
        "discount_to_iv": 15.0,
        "rsi_state": 45.0,
        "price_vs_200dma": -3.0,
        "price_vs_50dma": -1.5,
        "volume_delivery": 57.0,
        "rs_turn": 51.0,
        "volatility_compression": 58.0,
        "promoter_pledge": pledge,
        "asm_gsm_risk": 0.0,
        "default_distress": 8.0,
        "accounting_quality": 0.0,
        "liquidity_manipulation": 5.0,
        "governance_event": 0.0,
        "interest_coverage": 4.0,
        "credit_rating_grade": 2.0,
        "avg_daily_turnover_cr": 9.0,
    }
    return RawStockData(
        ticker=ticker,
        name=ticker,
        classification=NSEClassification(
            macro_sector="Technology",
            sector="Technology",
            industry="Software",
            basic_industry="Computers - Software & Consulting",
        ),
        fundamentals=fundamentals,
        classification_source="unit_test_master",
        classification_confidence="High",
        fundamentals_source="unit_test",
        price_source="unit_test",
    )


class PhaseUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.universe = {
            "AAA": _make_stock("AAA", pe=24.0, growth=2.0),
            "BBB": _make_stock("BBB", pe=28.0, growth=1.0),
            "CCC": _make_stock("CCC", pe=30.0, growth=0.5),
        }

    def test_live_mode_bias_audit_not_blocked_by_holdout(self) -> None:
        audit = BiasAudit(list(self.universe.keys()), CARD_WEIGHTS)
        report = audit.run(as_of_date="2026-03-10", mode="live")
        self.assertTrue(report["all_clear"])
        self.assertIn("LIVE MODE", report["period_check"])
        audit.close()

    def test_rating_has_recommendation_and_serialization(self) -> None:
        engine = NSERatingEngine(self.universe)
        ratings = engine.rate_universe()
        rating = ratings["AAA"]
        payload = rating.to_dict()

        self.assertIn(rating.recommendation, {"Buy Candidate", "Watchlist", "Avoid", "Insufficient Data"})
        self.assertGreaterEqual(
            sum(1 for r in ratings.values() if r.recommendation == "Buy Candidate"),
            1,
            "quality test universe should surface at least one deserving buy candidate",
        )
        self.assertIn(rating.recommendation_confidence, {"High", "Medium", "Low"})
        self.assertIn("recommendation", payload)
        self.assertIn("potential_score", payload)
        self.assertIn("ranks", payload)

    def test_sector_ranks_populated(self) -> None:
        engine = NSERatingEngine(self.universe)
        ratings = engine.rate_universe()
        for rating in ratings.values():
            self.assertIsNotNone(rating.sector_rank)
            self.assertIsNotNone(rating.sector_percentile)
            self.assertIsNotNone(rating.basic_industry_rank)
            self.assertIsNotNone(rating.basic_industry_percentile)

    def test_advanced_outputs_present(self) -> None:
        engine = NSERatingEngine(self.universe, market_mode="bear")
        ratings = engine.rate_universe()
        one = ratings["AAA"]
        self.assertIn(one.market_mode, {"bear", "neutral", "bull"})
        self.assertIsNotNone(one.expected_downside_pct)
        self.assertIsNotNone(one.selection_score)
        self.assertIsInstance(one.investability_gate_passed, bool)
        self.assertTrue(isinstance(one.staged_entry_plan, str) and len(one.staged_entry_plan) > 0)

    def test_action_sheet_and_portfolio(self) -> None:
        engine = NSERatingEngine(self.universe, market_mode="neutral")
        ratings = engine.rate_universe()
        leaderboard = engine.to_leaderboard(ratings, exclude_statuses=("Insufficient Data",))
        actions = action_sheet_rows(ratings)
        self.assertGreaterEqual(len(actions), 1)
        self.assertIn("investability_status", actions[0])
        self.assertIn("template_support_status", actions[0])
        self.assertIn("research_status", actions[0])
        portfolio = portfolio_plan_rows(leaderboard)
        self.assertIsInstance(portfolio, list)

    def test_daily_market_list_caps_financials(self) -> None:
        leaderboard = [
            {
                "ticker": "BANK1",
                "sector": "Financial Services",
                "template": "B",
                "template_supported": True,
                "research_status": "Actionable",
                "research_tier": "High Confidence Research",
                "recommendation": "Buy Candidate",
                "confidence": "High",
                "selection_score": 90.0,
            },
            {
                "ticker": "BANK2",
                "sector": "Financial Services",
                "template": "B",
                "template_supported": True,
                "research_status": "Actionable",
                "research_tier": "High Confidence Research",
                "recommendation": "Buy Candidate",
                "confidence": "High",
                "selection_score": 89.0,
            },
            {
                "ticker": "BANK3",
                "sector": "Financial Services",
                "template": "B",
                "template_supported": True,
                "research_status": "Actionable",
                "research_tier": "High Confidence Research",
                "recommendation": "Buy Candidate",
                "confidence": "High",
                "selection_score": 88.0,
            },
            {
                "ticker": "NBFC1",
                "sector": "Financial Services",
                "template": "C",
                "template_supported": True,
                "research_status": "Actionable",
                "research_tier": "High Confidence Research",
                "recommendation": "Buy Candidate",
                "confidence": "High",
                "selection_score": 87.0,
            },
            {
                "ticker": "IND1",
                "sector": "Industrials",
                "template": "A",
                "template_supported": True,
                "research_status": "Actionable",
                "research_tier": "High Confidence Research",
                "recommendation": "Buy Candidate",
                "confidence": "High",
                "selection_score": 86.0,
            },
            {
                "ticker": "HC1",
                "sector": "Healthcare",
                "template": "A",
                "template_supported": True,
                "research_status": "Research Candidate",
                "research_tier": "Qualified Watchlist",
                "recommendation": "Watchlist",
                "confidence": "Medium",
                "selection_score": 85.0,
            },
        ]
        rows = daily_market_list_rows(leaderboard)
        financials = [row for row in rows if row["template"] in {"B", "C"}]
        self.assertLessEqual(len(financials), 3)

    def test_daily_market_list_excludes_data_incomplete_rows(self) -> None:
        leaderboard = [
            {
                "ticker": "AAA",
                "sector": "Industrials",
                "template": "A",
                "template_supported": True,
                "research_status": "Research Candidate",
                "research_tier": "Data Incomplete",
                "recommendation": "Watchlist",
                "confidence": "Medium",
                "selection_score": 90.0,
            },
            {
                "ticker": "BBB",
                "sector": "Healthcare",
                "template": "A",
                "template_supported": True,
                "research_status": "Actionable",
                "research_tier": "High Confidence Research",
                "recommendation": "Buy Candidate",
                "confidence": "High",
                "selection_score": 80.0,
            },
        ]
        self.assertEqual([row["ticker"] for row in daily_market_list_rows(leaderboard)], ["BBB"])
        self.assertEqual([row["ticker"] for row in daily_research_queue_rows(leaderboard)], ["AAA", "BBB"])
        self.assertEqual([row["ticker"] for row in data_incomplete_rows(leaderboard)], ["AAA"])

    def test_actionable_requires_usable_data_quality(self) -> None:
        stock = _make_stock("WEAKDATA", pe=22.0, growth=8.0)
        stock.classification_source = "unknown"
        stock.classification_confidence = "Low"
        stock.fundamentals_source = "unknown"
        stock.price_source = "missing"

        engine = NSERatingEngine({"WEAKDATA": stock}, market_mode="neutral")
        ratings = engine.rate_universe()
        rating = ratings["WEAKDATA"]

        self.assertLess(rating.data_quality_score, 65.0)
        self.assertIn(rating.data_quality_status, {"Weak Data", "Research Only Data"})
        self.assertNotEqual(rating.research_status, "Actionable")
        self.assertTrue(any("Data quality" in reason for reason in rating.gate_fail_reasons))

    def test_data_quality_summary_groups_sources(self) -> None:
        engine = NSERatingEngine(self.universe, market_mode="neutral")
        ratings = engine.rate_universe()
        rows = data_quality_summary_rows(ratings)
        groups = {row["group"] for row in rows}
        self.assertIn("data_quality_status", groups)
        self.assertIn("price_source", groups)

    def test_rating_exposes_metric_source_summary(self) -> None:
        engine = NSERatingEngine(self.universe, market_mode="neutral")
        ratings = engine.rate_universe()
        rating = ratings["AAA"]
        self.assertIsInstance(rating.metric_source_summary, dict)
        payload = rating.to_dict()
        self.assertIn("field_provenance", payload)

    def test_stage1_asm_is_not_forced_disqualifier(self) -> None:
        stock = _make_stock("ASM1", pe=25.0, growth=1.5)
        stock.fundamentals["asm_stage"] = 1
        stock.on_asm = True
        peers = [_make_stock("PEER1", pe=26.0, growth=1.0), _make_stock("PEER2", pe=24.0, growth=2.0)]
        card = score_red_flags(stock, peers, Template.GENERAL)
        self.assertNotEqual(card.label, "Severe")

    def test_stage3_asm_forces_disqualifier(self) -> None:
        stock = _make_stock("ASM3", pe=25.0, growth=1.5)
        stock.fundamentals["asm_stage"] = 3
        stock.on_asm = True
        peers = [_make_stock("PEER1", pe=26.0, growth=1.0), _make_stock("PEER2", pe=24.0, growth=2.0)]
        card = score_red_flags(stock, peers, Template.GENERAL)
        self.assertEqual(card.label, "Severe")

    def test_cagr_3y_returns_none_for_negative_latest_value(self) -> None:
        self.assertIsNone(compute_cagr_3y(-4.18, 2.28))

    def test_cagr_3y_still_computes_for_positive_values(self) -> None:
        val = compute_cagr_3y(27.0, 8.0)
        self.assertIsNotNone(val)
        self.assertGreater(val, 0)

    def test_derived_risk_metrics_satisfy_critical_field_checks(self) -> None:
        stock = _make_stock("PROXY", pe=24.0, growth=2.0)
        stock.fundamentals.pop("pledge_pct", None)
        stock.fundamentals.pop("asm_stage", None)
        stock.fundamentals.pop("gsm_stage", None)
        stock.fundamentals.pop("governance_events", None)
        stock.fundamentals["promoter_pledge"] = 5.0
        stock.fundamentals["asm_gsm_risk"] = 0.0
        stock.fundamentals["governance_event"] = 0.0
        stock.on_asm = False
        stock.on_gsm = False

        self.assertTrue(_has_critical_value(stock, "pledge_pct"))
        self.assertTrue(_has_critical_value(stock, "asm_stage"))
        self.assertTrue(_has_critical_value(stock, "gsm_stage"))
        self.assertTrue(_has_critical_value(stock, "governance_events"))

        engine = NSERatingEngine({"PROXY": stock}, market_mode="neutral")
        rating = engine.rate("PROXY")
        self.assertNotIn("pledge_pct", rating.missing_critical_fields)
        self.assertNotIn("asm_stage", rating.missing_critical_fields)
        self.assertNotIn("gsm_stage", rating.missing_critical_fields)
        self.assertNotIn("governance_events", rating.missing_critical_fields)


if __name__ == "__main__":
    unittest.main()
