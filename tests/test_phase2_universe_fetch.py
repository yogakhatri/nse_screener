import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.fetch_nse_universe import (
    apply_classification,
    build_archive_url,
    build_universe_from_bhavcopy,
    classification_stats,
    default_zip_path,
    legacy_zip_path,
    load_classification_master,
    read_bhavcopy_zip,
    write_missing_classification,
)


class Phase2UniverseFetchTests(unittest.TestCase):
    def test_build_archive_url(self) -> None:
        import datetime as dt

        legacy_url = build_archive_url(
            dt.date(2026, 3, 12),
            "https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MMM}/cm{DD}{MMM}{YYYY}bhav.csv.zip",
        )
        self.assertEqual(
            legacy_url,
            "https://archives.nseindia.com/content/historical/EQUITIES/2026/MAR/cm12MAR2026bhav.csv.zip",
        )

        current_url = build_archive_url(
            dt.date(2026, 3, 12),
            "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip",
        )
        self.assertEqual(
            current_url,
            "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20260312_F_0000.csv.zip",
        )

    def test_default_and_legacy_zip_paths(self) -> None:
        import datetime as dt

        self.assertEqual(
            str(default_zip_path(dt.date(2026, 3, 12))),
            "data/raw/prices/bhavcopy/BhavCopy_NSE_CM_0_0_0_20260312_F_0000.csv.zip",
        )
        self.assertEqual(
            str(legacy_zip_path(dt.date(2026, 3, 12))),
            "data/raw/prices/bhavcopy/cm12MAR2026bhav.csv.zip",
        )

    def test_read_and_build_universe_from_zip(self) -> None:
        csv_bytes = (
            b"SYMBOL,SERIES,NAME OF COMPANY\n"
            b"RELIANCE,EQ,Reliance Industries\n"
            b"TESTBE,BE,Test BE Company\n"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "sample.zip"
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("cm12MAR2026bhav.csv", io.BytesIO(csv_bytes).getvalue())

            df = read_bhavcopy_zip(zip_path)
            universe_eq = build_universe_from_bhavcopy(df, include_non_eq=False)
            universe_all = build_universe_from_bhavcopy(df, include_non_eq=True)

            self.assertEqual(len(universe_eq), 1)
            self.assertEqual(universe_eq.iloc[0]["NSE Symbol"], "RELIANCE")
            self.assertEqual(len(universe_all), 2)

    def test_classification_enrichment_and_missing_report(self) -> None:
        import pandas as pd

        universe = pd.DataFrame(
            [
                {"NSE Symbol": "RELIANCE", "Name": "Reliance", "SERIES": "EQ", "Macro Sector": "", "Sector": "", "Industry": "", "Basic Industry": ""},
                {"NSE Symbol": "UNMAPPED", "Name": "Unmapped", "SERIES": "EQ", "Macro Sector": "", "Sector": "", "Industry": "", "Basic Industry": ""},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            class_path = Path(tmpdir) / "class.csv"
            missing_path = Path(tmpdir) / "missing.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "RELIANCE",
                        "Macro Sector": "Energy",
                        "Sector": "Oil & Gas",
                        "Industry": "Refineries",
                        "Basic Industry": "Refineries",
                    }
                ]
            ).to_csv(class_path, index=False)

            class_df = load_classification_master(class_path)
            enriched = apply_classification(universe, class_df)
            stats = classification_stats(enriched)
            missing_count = write_missing_classification(enriched, missing_path)

            self.assertEqual(enriched.loc[0, "Sector"], "Oil & Gas")
            self.assertEqual(stats["sector_pct"], 50.0)
            self.assertEqual(missing_count, 1)


if __name__ == "__main__":
    unittest.main()
