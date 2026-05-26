import unittest

from engine.shortlist import build_top_picks


def _row(**kwargs) -> dict:
    base = {
        "ticker": "AAA",
        "name": "AAA Ltd",
        "sector": "Information Technology",
        "template": "A",
        "template_supported": True,
        "research_status": "Research Candidate",
        "research_tier": "Qualified Watchlist",
        "recommendation": "Watchlist",
        "confidence": "Medium",
        "gate_passed": True,
        "selection_score": 62.0,
        "user_profile_score": 65.0,
        "user_filter_passed": True,
        "potential_score": 70.0,
        "valuation_gap_score": 68.0,
        "red_flags": 75.0,
        "roce_3y_median": 18.0,
        "rev_cagr_3y": 12.0,
        "dividend_yield": 1.5,
        "debt_to_equity": 0.3,
    }
    base.update(kwargs)
    return base


class ShortlistTests(unittest.TestCase):
    def test_research_shortlist_returns_up_to_three_primary(self) -> None:
        rows = [_row(ticker=f"T{i}", selection_score=60 + i) for i in range(6)]
        result = build_top_picks(rows, research_mode="research_shortlist", return_persona="quality_value")
        self.assertLessEqual(len(result["primary"]), 3)
        self.assertGreaterEqual(len(result["primary"]), 1)

    def test_high_conviction_excludes_without_gate(self) -> None:
        rows = [_row(gate_passed=False, research_tier="High Confidence Research")]
        result = build_top_picks(rows, research_mode="high_conviction")
        self.assertEqual(len(result["primary"]), 0)

    def test_unsupported_tier_not_surfaced_in_shortlist(self) -> None:
        rows = [_row(research_tier="Unsupported", template_supported=False)]
        result = build_top_picks(rows, research_mode="research_shortlist")
        self.assertEqual(len(result["primary"]), 0)

    def test_thematic_requires_policy_match(self) -> None:
        rows = [
            _row(sector="Information Technology"),
            _row(ticker="B", sector="Utilities", selection_score=90.0),
        ]
        result = build_top_picks(
            rows,
            research_mode="thematic",
            policy_themes=("digital_india",),
        )
        self.assertTrue(all("Information Technology" in (r.get("sector") or "") for r in result["primary"]))


if __name__ == "__main__":
    unittest.main()
