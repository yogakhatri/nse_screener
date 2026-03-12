import unittest

from engine import NSERatingEngine, RawStockData, NSEClassification
from engine.advanced import action_sheet_rows, portfolio_plan_rows
from engine.bias_controls import BiasAudit
from engine.config import CARD_WEIGHTS


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

        self.assertIn(rating.recommendation, {"Buy Candidate", "Watchlist", "Avoid"})
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
        portfolio = portfolio_plan_rows(leaderboard)
        self.assertIsInstance(portfolio, list)


if __name__ == "__main__":
    unittest.main()
