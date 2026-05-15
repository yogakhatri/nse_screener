import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from scripts.fetch_governance_events import (
    classify_governance_event,
    load_rows_from_json,
    parse_nse_announcements,
    save_governance_events,
)


class GovernanceEventTests(unittest.TestCase):
    def test_classifies_high_risk_public_announcement_text(self) -> None:
        """Keyword rules should prioritize material long-term governance risks."""
        label, risk = classify_governance_event("Company receives SEBI adjudication order") or ("", 0)
        self.assertEqual(label, "Regulatory enforcement / SEBI event")
        self.assertGreaterEqual(risk, 80)

    def test_parser_extracts_only_governance_relevant_rows(self) -> None:
        """Routine announcements should not pollute governance-risk evidence."""
        payload = {
            "data": [
                {
                    "symbol": "AAA",
                    "desc": "Resignation of statutory auditor with immediate effect",
                    "an_dt": "09-May-2026",
                    "attchmntFile": "/corporate/AAA.pdf",
                },
                {
                    "symbol": "BBB",
                    "desc": "Board meeting intimation for quarterly results",
                    "an_dt": "09-May-2026",
                },
            ]
        }
        rows = parse_nse_announcements(payload, as_of=date(2026, 5, 9))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["NSE Symbol"], "AAA")
        self.assertIn("Auditor", rows[0]["Governance Events"])
        self.assertTrue(rows[0]["Source URL"].startswith("https://www.nseindia.com/"))

    def test_offline_json_loader_and_save_use_contract_columns(self) -> None:
        """Downloaded JSON payloads should be parseable without network access."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload_path = root / "announcements.json"
            output_csv = root / "governance.csv"
            payload_path.write_text(
                """
                {
                  "data": [
                    {
                      "SYMBOL": "CCC",
                      "attchmntText": "Disclosure of default on debt servicing",
                      "announcementDate": "2026-05-09"
                    }
                  ]
                }
                """
            )
            rows = load_rows_from_json(payload_path, as_of=date(2026, 5, 9))
            saved = save_governance_events(rows, date(2026, 5, 9), output_csv)
            df = pd.read_csv(saved)

            self.assertEqual(list(df.columns)[0], "NSE Symbol")
            self.assertEqual(df.iloc[0]["NSE Symbol"], "CCC")
            self.assertIn("Default", df.iloc[0]["Governance Events"])


if __name__ == "__main__":
    unittest.main()
