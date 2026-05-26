"""Analyst queue CSV field alignment."""
import unittest

from engine.analyst_workflow import build_analyst_research_queue, worksheet_fieldnames


class TestAnalystWorkflow(unittest.TestCase):
    def test_queue_rows_match_worksheet_fieldnames(self):
        lb = [
            {
                "ticker": "ABC",
                "name": "ABC Ltd",
                "sector": "IT",
                "recommendation": "Watchlist",
                "research_tier": "Qualified Watchlist",
                "research_status": "ok",
                "gate_passed": False,
                "gate_fail_reasons": "pledge",
                "selection_score": 72,
                "potential_score": 71,
                "valuation_gap_score": 65,
                "value_trap_score": 10,
                "analysis_caveat": "test",
            }
        ]
        queue = build_analyst_research_queue(lb, top_pick_tickers=("ABC",))
        fields = set(worksheet_fieldnames())
        for row in queue:
            self.assertLessEqual(set(row.keys()), fields)


if __name__ == "__main__":
    unittest.main()
