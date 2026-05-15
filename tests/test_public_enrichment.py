import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.merge_public_enrichment import merge_public_enrichment
from scripts.load_data import load_from_screener


class PublicEnrichmentTests(unittest.TestCase):
    def test_merges_public_evidence_without_overwriting_existing_values(self) -> None:
        """Public enrichment should fill missing critical fields and preserve manual values by default."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            screener = root / "screener.csv"
            shareholding = root / "shareholding.csv"
            governance = root / "governance.csv"
            financial_risk = root / "financial_risk.csv"

            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "AAA",
                        "Name": "AAA Ltd",
                        "Pledged percentage": "",
                        "Promoter Holding %": "",
                        "GNPA %": "",
                    },
                    {
                        "NSE Symbol": "BBB",
                        "Name": "BBB Bank",
                        "Pledged percentage": "5",
                        "GNPA %": "",
                    },
                ]
            ).to_csv(screener, index=False)
            pd.DataFrame(
                [
                    {
                        "symbol": "AAA.NS",
                        "pledge_pct": "0",
                        "promoter_holding_pct": "51.2",
                    },
                    {
                        "symbol": "BBB",
                        "pledge_pct": "12",
                        "promoter_holding_pct": "42.0",
                    },
                ]
            ).to_csv(shareholding, index=False)
            pd.DataFrame(
                [
                    {"symbol": "AAA", "event": "none", "governance_risk": "0"},
                    {"symbol": "BBB", "event": "Auditor resignation", "governance_risk": "75"},
                ]
            ).to_csv(governance, index=False)
            pd.DataFrame(
                [
                    {
                        "ticker": "BBB",
                        "gnpa_pct": "2.4",
                        "nnpa_pct": "0.8",
                        "car_pct": "18.5",
                        "credit_cost": "0.7",
                        "nim": "3.2",
                    }
                ]
            ).to_csv(financial_risk, index=False)

            report = merge_public_enrichment(
                screener,
                shareholding_csv=shareholding,
                governance_csv=governance,
                financial_risk_csv=financial_risk,
            )

            out = pd.read_csv(screener, dtype=str).fillna("")
            aaa = out[out["NSE Symbol"] == "AAA"].iloc[0]
            bbb = out[out["NSE Symbol"] == "BBB"].iloc[0]

            self.assertEqual(aaa["Pledged percentage"], "0")
            self.assertEqual(aaa["Promoter Holding %"], "51.2")
            self.assertEqual(aaa["Governance Events"], "none")
            self.assertEqual(aaa["Governance Risk"], "0")
            self.assertEqual(bbb["Pledged percentage"], "5")
            self.assertEqual(bbb["GNPA %"], "2.4")
            self.assertEqual(bbb["NNPA %"], "0.8")
            self.assertEqual(bbb["CAR %"], "18.5")
            self.assertGreater(report["total_updated_cells"], 0)
            self.assertIn("shareholding", bbb["Public Enrichment Source"])
            self.assertIn("financial_risk", bbb["Public Enrichment Source"])

    def test_overwrite_mode_replaces_existing_values(self) -> None:
        """Analysts should be able to force-refresh stale enrichment values explicitly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            screener = root / "screener.csv"
            shareholding = root / "shareholding.csv"
            pd.DataFrame([{"NSE Symbol": "AAA", "Pledged percentage": "10"}]).to_csv(screener, index=False)
            pd.DataFrame([{"symbol": "AAA", "pledge_pct": "2"}]).to_csv(shareholding, index=False)

            merge_public_enrichment(screener, shareholding_csv=shareholding, overwrite=True)

            out = pd.read_csv(screener, dtype=str).fillna("")
            self.assertEqual(out.iloc[0]["Pledged percentage"], "2")

    def test_missing_optional_files_are_reported_not_fatal(self) -> None:
        """Missing optional evidence files should not block diagnostics or manual workflows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            screener = root / "screener.csv"
            pd.DataFrame([{"NSE Symbol": "AAA"}]).to_csv(screener, index=False)

            report = merge_public_enrichment(
                screener,
                shareholding_csv=root / "missing_shareholding.csv",
                governance_csv=root / "missing_governance.csv",
            )

            statuses = {source["source_id"]: source["status"] for source in report["sources"]}
            self.assertEqual(statuses["shareholding"], "missing_optional")
            self.assertEqual(statuses["governance"], "missing_optional")

    def test_enrichment_fields_reach_loader_and_provenance(self) -> None:
        """Merged public evidence must become normalized fundamentals, not just extra CSV columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            screener = root / "screener.csv"
            shareholding = root / "shareholding.csv"
            governance = root / "governance.csv"
            financial_risk = root / "financial_risk.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "AAA",
                        "Name": "AAA Ltd",
                        "Macro Sector": "Financial Services",
                        "Sector": "Financial Services",
                        "Industry": "Banking",
                        "Basic Industry": "Private Sector Bank",
                    }
                ]
            ).to_csv(screener, index=False)
            pd.DataFrame([{"symbol": "AAA", "pledge_pct": "0"}]).to_csv(shareholding, index=False)
            pd.DataFrame([{"symbol": "AAA", "event": "none", "governance_risk": "0"}]).to_csv(governance, index=False)
            pd.DataFrame(
                [
                    {
                        "ticker": "AAA",
                        "gnpa_pct": "1.1",
                        "nnpa_pct": "0.2",
                        "car_pct": "18",
                        "pcr_pct": "72",
                        "credit_cost": "0.4",
                    }
                ]
            ).to_csv(financial_risk, index=False)

            merge_public_enrichment(
                screener,
                shareholding_csv=shareholding,
                governance_csv=governance,
                financial_risk_csv=financial_risk,
            )
            universe = load_from_screener(str(screener))
            stock = universe["AAA"]

            self.assertEqual(stock.fundamentals["pledge_pct"], 0.0)
            self.assertEqual(stock.fundamentals["gnpa_pct"], 1.1)
            self.assertEqual(stock.fundamentals["nnpa_pct"], 0.2)
            self.assertEqual(stock.fundamentals["capital_adequacy_stress"], 0.0)
            self.assertEqual(stock.fundamentals["governance_events"], ["none"])
            self.assertIn("pledge_pct", stock.metric_provenance)
            self.assertIn("governance_events", stock.metric_provenance)


if __name__ == "__main__":
    unittest.main()
