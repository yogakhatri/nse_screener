import unittest
from pathlib import Path

from engine.models import CardResult, NSEClassification, PeerLevel, RawStockData, StockRating, Template
from engine.preferences import (
    ResearchPreferences,
    apply_research_preferences,
    filter_preference_rows,
    load_research_preferences,
)
from scripts.run_engine import resolve_output_dir


def _stock_and_rating() -> tuple[RawStockData, StockRating]:
    classification = NSEClassification(
        macro_sector="Technology",
        sector="Information Technology",
        industry="Software",
        basic_industry="IT - Software",
    )
    stock = RawStockData(
        ticker="AAA",
        name="AAA",
        classification=classification,
        fundamentals={
            "market_cap_cr": 12_000.0,
            "pe_percentile": 28.0,
            "pb_percentile": 5.0,
            "fcf_yield": 3.0,
            "iv_gap": 20.0,
            "rev_growth_yoy": 12.0,
            "eps_growth_yoy": 10.0,
            "rev_cagr_3y": 11.0,
            "debt_to_equity": 0.4,
            "interest_coverage": 6.0,
            "roce_3y_median": 18.0,
            "roe": 16.0,
            "cfo_pat_ratio": 1.1,
            "dividend_yield": 1.2,
        },
    )
    rating = StockRating(
        ticker="AAA",
        name="AAA",
        classification=classification,
        template=Template.GENERAL,
        peer_group=[],
        peer_level=PeerLevel.BASIC_INDUSTRY,
        n_peers=10,
        valuation=CardResult("valuation", 70.0, "Attractive"),
        growth=CardResult("growth", 68.0, "Good"),
        profitability=CardResult("profitability", 72.0, "Good"),
        entry_point=CardResult("entry_point", 62.0, "Good"),
        red_flags=CardResult("red_flags", 82.0, "None"),
    )
    rating.selection_score = 65.0
    rating.potential_score = 72.0
    rating.valuation_gap_score = 68.0
    rating.risk_reward_score = 70.0
    rating.expected_upside_pct = 22.0
    rating.data_quality_status = "Actionable Data"
    rating.value_trap_score = 10.0
    return stock, rating


class ResearchPreferenceTests(unittest.TestCase):
    def test_example_profile_is_valid(self) -> None:
        profile = load_research_preferences("config/research_profile.example.json")
        self.assertEqual(profile.profile_name, "quality_value_1y")

    def test_all_repository_profiles_are_valid(self) -> None:
        for path in Path("config").glob("research_profile*.json"):
            with self.subTest(path=str(path)):
                self.assertIsNotNone(load_research_preferences(str(path)).profile_name)

    def test_invalid_profile_rejects_unknown_weight(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported keys"):
            ResearchPreferences(custom_weights={"bad_metric": 1.0}).validate()

    def test_invalid_profile_rejects_nonfinite_weight(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite numeric"):
            ResearchPreferences(custom_weights={"selection_score": float("nan")}).validate()

    def test_invalid_profile_rejects_negative_safety_threshold(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_pe cannot be negative"):
            ResearchPreferences(max_pe=-1).validate()

    def test_preferences_annotate_passed_stock(self) -> None:
        stock, rating = _stock_and_rating()
        preferences = ResearchPreferences(
            sector_preference=("Information Technology",),
            market_cap_preference="mid",
            max_pe=35,
            min_roce=15,
        ).validate()
        apply_research_preferences({"AAA": rating}, {"AAA": stock}, preferences)
        self.assertTrue(rating.user_filter_passed)
        self.assertIsNotNone(rating.user_profile_score)

    def test_profile_file_values_survive_when_no_override_is_supplied(self) -> None:
        profile = load_research_preferences("config/research_profile.strict_quality.json")
        self.assertEqual(profile.risk_level, "conservative")
        self.assertEqual(profile.market_cap_preference, "exclude_micro")

    def test_non_default_profile_uses_dedicated_output_dir(self) -> None:
        self.assertEqual(str(resolve_output_dir("2026-04-09", "default")), "runs/2026-04-09")
        self.assertEqual(
            str(resolve_output_dir("2026-04-09", "Quality Value 1Y")),
            "runs/2026-04-09/profiles/Quality_Value_1Y",
        )

    def test_preferences_explain_failed_filter(self) -> None:
        stock, rating = _stock_and_rating()
        preferences = ResearchPreferences(sector_preference=("Healthcare",), max_pe=20).validate()
        apply_research_preferences({"AAA": rating}, {"AAA": stock}, preferences)
        self.assertFalse(rating.user_filter_passed)
        self.assertTrue(any("sector" in reason for reason in rating.user_filter_reasons))
        self.assertTrue(any("P/E" in reason for reason in rating.user_filter_reasons))

    def test_filter_preference_rows_sorts_by_profile_score(self) -> None:
        rows = [
            {"ticker": "BBB", "user_filter_passed": True, "user_profile_score": 60, "selection_score": 90},
            {"ticker": "AAA", "user_filter_passed": True, "user_profile_score": 80, "selection_score": 50},
            {"ticker": "CCC", "user_filter_passed": False, "user_profile_score": 100, "selection_score": 100},
        ]
        filtered = filter_preference_rows(rows)
        self.assertEqual([row["ticker"] for row in filtered], ["AAA", "BBB"])


if __name__ == "__main__":
    unittest.main()
