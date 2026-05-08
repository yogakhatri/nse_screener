import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.build_classification_master import build_master
from scripts.prepare_universe import _merge_classification


class ClassificationPipelineTests(unittest.TestCase):
    def test_build_master_prefers_recent_public_scrape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            screener_dir = tmp / "screener"
            cache_dir = screener_dir / "cache"
            screener_dir.mkdir()
            cache_dir.mkdir()

            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "HDFCBANK",
                        "Name": "HDFC Bank Ltd",
                        "Macro Sector": "Financial Services",
                        "Sector": "Financial Services",
                        "Industry": "Private Sector Bank",
                        "Basic Industry": "Banks",
                    }
                ]
            ).to_csv(screener_dir / "screener_export_2026-04-09.csv", index=False)

            (cache_dir / "HDFCBANK.json").write_text(
                json.dumps(
                    {
                        "__cache_schema_version": 3,
                        "data": {
                            "NSE Symbol": "HDFCBANK",
                            "Name": "HDFC Bank Ltd",
                            "Macro Sector": "Financial Services",
                            "Sector": "Financial Services",
                            "Industry": "Private Sector Bank",
                            "Basic Industry": "Banks",
                        },
                    }
                )
            )

            rows = build_master(
                screener_csvs=[screener_dir / "screener_export_2026-04-09.csv"],
                cache_dir=cache_dir,
                existing_master=None,
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["NSE Symbol"], "HDFCBANK")
            self.assertEqual(rows[0]["Sector"], "Financial Services")
            self.assertEqual(rows[0]["Classification Confidence"], "High")

    def test_prepare_universe_prefers_classification_master_over_fundamentals(self) -> None:
        universe_df = pd.DataFrame(
            [
                {
                    "NSE Symbol": "HDFCBANK",
                    "Name": "HDFC BANK LTD",
                    "Macro Sector": "",
                    "Sector": "",
                    "Industry": "",
                    "Basic Industry": "",
                    "Classification Source": "",
                    "Classification Confidence": "",
                }
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "classification.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "HDFCBANK",
                        "Name": "HDFC Bank Ltd",
                        "Macro Sector": "Financial Services",
                        "Sector": "Financial Services",
                        "Industry": "Private Sector Bank",
                        "Basic Industry": "Banks",
                        "Classification Source": "screener_csv",
                        "Classification Confidence": "High",
                    }
                ]
            ).to_csv(path, index=False)

            merged, matched = _merge_classification(universe_df, path)
            self.assertEqual(matched, 1)
            self.assertEqual(merged.iloc[0]["Sector"], "Financial Services")
            self.assertEqual(merged.iloc[0]["Classification Source"], "screener_csv")
            self.assertEqual(merged.iloc[0]["Classification Confidence"], "High")


if __name__ == "__main__":
    unittest.main()
