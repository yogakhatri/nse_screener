import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from scripts.source_registry import SourceSpec, build_registry, inspect_source


class SourceRegistryTests(unittest.TestCase):
    def test_file_source_records_shape_hash_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sample_2026-04-09.csv"
            pd.DataFrame([{"symbol": "AAA", "price": 10}, {"symbol": "BBB", "price": 20}]).to_csv(
                path, index=False
            )
            record = inspect_source(
                SourceSpec(
                    source_id="sample",
                    category="test",
                    path=path,
                    source_type="file",
                    required=True,
                    max_age_days=365,
                    expected_min_rows=2,
                )
            )
            self.assertEqual(record.status, "ok")
            self.assertEqual(record.rows, 2)
            self.assertEqual(record.columns, 2)
            self.assertTrue(record.hash)
            self.assertEqual(record.quality_status, "usable")
            self.assertEqual(record.data_date, "2026-04-09")

    def test_required_low_coverage_blocks_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "small.csv"
            pd.DataFrame([{"symbol": "AAA"}]).to_csv(path, index=False)
            spec = SourceSpec(
                source_id="small_required",
                category="test",
                path=path,
                source_type="file",
                required=True,
                max_age_days=365,
                expected_min_rows=10,
            )
            registry = build_registry(date(2026, 4, 9), specs=[spec])
            self.assertEqual(registry["overall_status"], "blocked")
            self.assertEqual(registry["sources"][0]["status"], "low_coverage_required")

    def test_optional_missing_does_not_block_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = SourceSpec(
                source_id="optional_missing",
                category="test",
                path=Path(tmpdir) / "missing",
                source_type="directory",
                required=False,
            )
            registry = build_registry(date(2026, 4, 9), specs=[spec])
            self.assertEqual(registry["overall_status"], "ok")
            self.assertEqual(registry["sources"][0]["status"], "missing_optional")

    def test_default_registry_includes_public_enrichment_sources(self) -> None:
        registry = build_registry(date(2026, 4, 9), screener_csv=Path("missing.csv"))
        source_ids = {source["source_id"] for source in registry["sources"]}
        self.assertIn("governance_events", source_ids)
        self.assertIn("financial_asset_quality", source_ids)
        self.assertIn("shareholding", source_ids)


if __name__ == "__main__":
    unittest.main()
