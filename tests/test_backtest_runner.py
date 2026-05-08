import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.backtest import calibration_notes, load_recommendations


class BacktestRunnerTests(unittest.TestCase):
    def test_load_recommendations_accepts_buy_candidate_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            pd.DataFrame(
                [
                    {
                        "ticker": "AAA",
                        "recommendation": "Buy Candidate",
                        "research_status": "Actionable",
                        "selection_score": 80,
                    },
                    {
                        "ticker": "BBB",
                        "recommendation": "Avoid",
                        "research_status": "Rejected",
                        "selection_score": 90,
                    },
                ]
            ).to_csv(run_dir / "buy_candidates.csv", index=False)
            recs = load_recommendations(run_dir)
            self.assertEqual(len(recs), 1)
            self.assertEqual(recs[0]["ticker"], "AAA")

    def test_calibration_notes_flags_weak_performance(self) -> None:
        notes = calibration_notes({"hit_rate": 40, "mean_return": -2, "sharpe_like": 0.1})
        self.assertTrue(any("hit rate" in note for note in notes))
        self.assertTrue(any("mean return" in note for note in notes))


if __name__ == "__main__":
    unittest.main()
