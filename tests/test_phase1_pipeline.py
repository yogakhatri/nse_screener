import unittest
import tempfile
from pathlib import Path

import pandas as pd

from engine import NSEClassification, RawStockData, Template
from engine.cards import score_red_flags
from engine.config import CARD_WEIGHTS, infer_template_code, validate_runtime_config
from engine.metric_definitions import compute_default_distress
from engine.scoring import score_metric
from scripts.load_data import load_from_screener, metric_coverage, validate_loader_support
from scripts.prepare_universe import _build_universe_frame, _finalize_output, _merge_fundamentals
from scripts.run_engine import (
    apply_template_support_overrides,
    input_quality_blockers,
    input_quality_report,
    template_quality_report,
)


def _stock(ticker: str, fundamentals: dict) -> RawStockData:
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


class Phase1PipelineTests(unittest.TestCase):
    def test_runtime_config_validates(self) -> None:
        validate_runtime_config()
        validate_loader_support()

    def test_build_universe_filters_non_eq(self) -> None:
        df = pd.DataFrame(
            [
                {"SYMBOL": "RELIANCE", "SERIES": "EQ", "NAME": "Reliance"},
                {"SYMBOL": "TESTBE", "SERIES": "BE", "NAME": "Test BE"},
            ]
        )
        out = _build_universe_frame(df, include_non_eq=False)
        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0]["NSE Symbol"], "RELIANCE")

    def test_finalize_output_adds_template_columns(self) -> None:
        df = pd.DataFrame([{"NSE Symbol": "INFY", "Name": "Infosys"}])
        out = _finalize_output(df)
        for column in [
            "NSE Symbol",
            "Name",
            "Macro Sector",
            "Sector",
            "Industry",
            "Basic Industry",
        ]:
            self.assertIn(column, out.columns)

    def test_merge_fundamentals_handles_prior_fund_columns(self) -> None:
        universe_df = pd.DataFrame(
            [{"NSE Symbol": "RELIANCE", "Name": "", "Macro Sector": "", "Sector": "", "Industry": "", "Basic Industry": ""}]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "fund.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "RELIANCE",
                        "Name": "Reliance Industries",
                        "Name_fund": "Legacy Value",
                        "Current Price": "1200",
                    }
                ]
            ).to_csv(path, index=False)
            merged, matched = _merge_fundamentals(universe_df, path)
            self.assertEqual(matched, 1)
            self.assertEqual(merged.iloc[0]["Name"], "Reliance Industries")
            self.assertEqual(merged.iloc[0]["Current Price"], "1200")
            self.assertIn("fund__Name_fund", merged.columns)

    def test_loader_falls_back_to_fund_prefixed_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "self_merged.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "INFY",
                        "Name": "Infosys",
                        "Macro Sector": "",
                        "Sector": "",
                        "Industry": "",
                        "Basic Industry": "",
                        "P/E": "",
                        "Current Price": "",
                        "Margin Trend": "",
                        "FCF Consistency": "",
                        "Growth Stability": "",
                        "fund__Sector": "Information Technology",
                        "fund__Industry": "Computers - Software & Consulting",
                        "fund__Basic Industry": "IT - Software",
                        "fund__P/E": "22.5",
                        "fund__Current Price": "1500",
                        "fund__Margin Trend": "1.25",
                        "fund__FCF Consistency": "80.0",
                        "fund__Growth Stability": "72.0",
                    }
                ]
            ).to_csv(path, index=False)
            universe = load_from_screener(str(path))
            stock = universe["INFY"]
            self.assertEqual(stock.classification.sector, "Information Technology")
            self.assertEqual(stock.classification.basic_industry, "IT - Software")
            self.assertEqual(stock.fundamentals["pe_percentile"], 22.5)
            self.assertEqual(stock.fundamentals["margin_trend"], 1.25)
            self.assertEqual(stock.fundamentals["fcf_consistency"], 80.0)
            self.assertEqual(stock.fundamentals["growth_stability"], 72.0)

    def test_score_metric_returns_neutral_when_no_peers(self) -> None:
        self.assertEqual(score_metric(10.0, [], True), 50.0)

    def test_quality_gate_blocks_sparse_input(self) -> None:
        universe = {"AAA": _stock("AAA", fundamentals={})}
        quality = input_quality_report(universe)
        blockers = input_quality_blockers(
            quality=quality,
            min_universe_size=2,
            min_avg_core_rankable_pct=5.0,
            min_core_cards_with_rankable=2,
            min_classification_coverage_pct=50.0,
        )
        self.assertTrue(any("Universe too small" in b for b in blockers))
        self.assertTrue(any("Core coverage too low" in b for b in blockers))

    def test_quality_gate_passes_with_filled_core_metrics(self) -> None:
        fundamentals = {}
        for card in ["performance", "valuation", "growth", "profitability", "entry_point"]:
            for metric in CARD_WEIGHTS["A"][card]:
                fundamentals[metric] = 10.0
        universe = {
            "AAA": _stock("AAA", fundamentals=fundamentals),
            "BBB": _stock("BBB", fundamentals=fundamentals),
            "CCC": _stock("CCC", fundamentals=fundamentals),
        }
        quality = input_quality_report(universe)
        blockers = input_quality_blockers(
            quality=quality,
            min_universe_size=3,
            min_avg_core_rankable_pct=5.0,
            min_core_cards_with_rankable=3,
            min_classification_coverage_pct=80.0,
        )
        self.assertEqual(blockers, [])

    def test_quality_gate_blocks_diversified_taxonomy(self) -> None:
        stock = RawStockData(
            ticker="AAA",
            name="AAA",
            classification=NSEClassification(
                macro_sector="Diversified",
                sector="Diversified",
                industry="Diversified",
                basic_industry="Diversified",
            ),
            fundamentals={},
        )
        quality = input_quality_report({"AAA": stock, "BBB": stock})
        blockers = input_quality_blockers(
            quality=quality,
            min_universe_size=1,
            min_avg_core_rankable_pct=0.0,
            min_core_cards_with_rankable=0,
            min_classification_coverage_pct=50.0,
        )
        self.assertTrue(any("Classification coverage too low" in b for b in blockers))

    def test_metric_coverage_is_template_aware(self) -> None:
        general = _stock("AAA", fundamentals={metric: 10.0 for metric in CARD_WEIGHTS["A"]["performance"]})
        bank = RawStockData(
            ticker="BANK1",
            name="BANK1",
            classification=NSEClassification(
                macro_sector="Financial Services",
                sector="Financial Services",
                industry="Banking",
                basic_industry="Private Sector Bank",
            ),
            fundamentals={metric: 10.0 for metric in CARD_WEIGHTS["B"]["performance"]},
        )
        coverage = metric_coverage({"AAA": general, "BANK1": bank})
        self.assertEqual(coverage["A"]["performance"]["n_stocks"], 1)
        self.assertEqual(coverage["B"]["performance"]["n_stocks"], 1)
        self.assertEqual(coverage["C"]["performance"]["n_stocks"], 0)

    def test_template_inference_handles_current_financial_taxonomy_labels(self) -> None:
        self.assertEqual(
            infer_template_code(
                macro_sector="Financial Services",
                sector="Financial Services",
                industry="Private Sector Bank",
                basic_industry="Banks",
            ),
            "B",
        )
        self.assertEqual(
            infer_template_code(
                macro_sector="Financial Services",
                sector="Financial Services",
                industry="Non Banking Financial Company (NBFC)",
                basic_industry="Finance",
            ),
            "C",
        )
        self.assertEqual(
            infer_template_code(
                macro_sector="Financial Services",
                sector="Financial Services",
                industry="Investment Company",
                basic_industry="Finance",
            ),
            "A",
        )

    def test_template_quality_report_flags_unsupported_template(self) -> None:
        general_fundamentals = {
            metric: 10.0
            for card in ["performance", "valuation", "growth", "profitability",
                         "entry_point", "red_flags", "contrarian"]
            for metric in CARD_WEIGHTS["A"][card]
        }
        general = _stock("AAA", fundamentals=general_fundamentals)
        bank = RawStockData(
            ticker="BANK1",
            name="BANK1",
            classification=NSEClassification(
                macro_sector="Financial Services",
                sector="Financial Services",
                industry="Banking",
                basic_industry="Private Sector Bank",
            ),
            fundamentals={metric: 10.0 for metric in CARD_WEIGHTS["B"]["performance"]},
        )
        report = template_quality_report({"AAA": general, "BANK1": bank})
        self.assertTrue(report["A"]["supported"])
        self.assertFalse(report["B"]["supported"])
        self.assertTrue(any("valuation" in blocker for blocker in report["B"]["blockers"]))

    def test_apply_template_support_override_marks_rating_unsupported(self) -> None:
        bank_fundamentals = {metric: 10.0 for metric in CARD_WEIGHTS["B"]["performance"]}
        universe = {
            "BANK1": RawStockData(
                ticker="BANK1",
                name="BANK1",
                classification=NSEClassification(
                    macro_sector="Financial Services",
                    sector="Financial Services",
                    industry="Banking",
                    basic_industry="Private Sector Bank",
                ),
                fundamentals=bank_fundamentals,
            ),
            "BANK2": RawStockData(
                ticker="BANK2",
                name="BANK2",
                classification=NSEClassification(
                    macro_sector="Financial Services",
                    sector="Financial Services",
                    industry="Banking",
                    basic_industry="Private Sector Bank",
                ),
                fundamentals=bank_fundamentals,
            ),
        }
        from engine import NSERatingEngine

        engine = NSERatingEngine(universe)
        ratings = engine.rate_universe()
        apply_template_support_overrides(ratings, template_quality_report(universe), enforce=True)
        self.assertEqual(ratings["BANK1"].investability_status, "Unsupported Data")
        self.assertEqual(ratings["BANK1"].recommendation, "Unsupported")
        self.assertFalse(ratings["BANK1"].template_supported)

    def test_apply_template_support_override_debug_mode_preserves_rating(self) -> None:
        bank_fundamentals = {metric: 10.0 for metric in CARD_WEIGHTS["B"]["performance"]}
        universe = {
            "BANK1": RawStockData(
                ticker="BANK1",
                name="BANK1",
                classification=NSEClassification(
                    macro_sector="Financial Services",
                    sector="Financial Services",
                    industry="Banking",
                    basic_industry="Private Sector Bank",
                ),
                fundamentals=bank_fundamentals,
            ),
            "BANK2": RawStockData(
                ticker="BANK2",
                name="BANK2",
                classification=NSEClassification(
                    macro_sector="Financial Services",
                    sector="Financial Services",
                    industry="Banking",
                    basic_industry="Private Sector Bank",
                ),
                fundamentals=bank_fundamentals,
            ),
        }
        from engine import NSERatingEngine

        engine = NSERatingEngine(universe)
        ratings = engine.rate_universe()
        before = ratings["BANK1"]
        before_status = before.investability_status
        before_recommendation = before.recommendation
        before_gate = before.investability_gate_passed
        before_note = before.action_note

        apply_template_support_overrides(ratings, template_quality_report(universe), enforce=False)

        self.assertEqual(ratings["BANK1"].investability_status, before_status)
        self.assertEqual(ratings["BANK1"].recommendation, before_recommendation)
        self.assertEqual(ratings["BANK1"].investability_gate_passed, before_gate)
        self.assertFalse(ratings["BANK1"].template_supported)
        self.assertEqual(ratings["BANK1"].template_support_status, "Unsupported Template Coverage")
        self.assertIn("Template unsupported (debug only):", ratings["BANK1"].action_note)
        if before_note:
            self.assertIn(before_note, ratings["BANK1"].action_note)

    def test_load_from_screener_prefers_price_history_metrics(self) -> None:
        history = pd.DataFrame(
            {
                "date": pd.date_range("2025-08-01", periods=130, freq="D"),
                "open": [100.0 + i for i in range(130)],
                "high": [101.0 + i for i in range(130)],
                "low": [99.0 + i for i in range(130)],
                "close": [100.0 + i for i in range(130)],
                "prev_close": [99.0 + i for i in range(130)],
                "volume": [1000.0 + i for i in range(130)],
                "traded_value": [10000000.0 + i * 1000 for i in range(130)],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "screener.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "AAA",
                        "Name": "AAA",
                        "Macro Sector": "Technology",
                        "Sector": "Technology",
                        "Industry": "Software",
                        "Basic Industry": "Computers - Software & Consulting",
                        "Current Price": "10",
                        "6 Month Return": "999",
                        "Price vs 50 DMA": "999",
                    }
                ]
            ).to_csv(csv_path, index=False)
            universe = load_from_screener(str(csv_path), price_history_map={"AAA": history})
            stock = universe["AAA"]
            self.assertEqual(stock.fundamentals["close_price"], 229.0)
            self.assertNotEqual(stock.fundamentals["return_6m"], 999.0)
            self.assertNotEqual(stock.fundamentals["price_vs_50dma"], 999.0)
            self.assertIsNotNone(stock.price_history)

    def test_default_distress_treats_unrated_as_conservative_not_default(self) -> None:
        risk, is_disq = compute_default_distress(
            debt_to_equity=1.2,
            interest_coverage_ttm=2.5,
            credit_rating_grade=None,
        )
        self.assertEqual(risk, 25.0)
        self.assertFalse(is_disq)

    def test_load_from_screener_prefers_internal_iv_over_external_intrinsic_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "screener.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "AAA",
                        "Name": "AAA",
                        "Macro Sector": "Technology",
                        "Sector": "Technology",
                        "Industry": "Software",
                        "Basic Industry": "Computers - Software & Consulting",
                        "Current Price": "100",
                        "Intrinsic Value": "500",
                        "EPS FY0": "10",
                        "EPS FY1": "11",
                        "EPS FY2": "9",
                        "EPS TTM": "10",
                        "Book Value Per Share": "40",
                    }
                ]
            ).to_csv(csv_path, index=False)
            universe = load_from_screener(str(csv_path), price_history_map={})
            stock = universe["AAA"]
            self.assertAlmostEqual(stock.fundamentals["iv_gap"], -20.0, places=2)
            self.assertAlmostEqual(stock.fundamentals["discount_to_iv"], -20.0, places=2)

    def test_load_from_screener_derives_bank_roe_adjusted_pb(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "screener.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "BANK1",
                        "Name": "BANK1",
                        "Macro Sector": "Financial Services",
                        "Sector": "Financial Services",
                        "Industry": "Banking",
                        "Basic Industry": "Private Sector Bank",
                        "Current Price": "100",
                        "Price to Book value": "2.0",
                        "ROE": "16",
                    }
                ]
            ).to_csv(csv_path, index=False)
            universe = load_from_screener(str(csv_path), price_history_map={})
            stock = universe["BANK1"]
            self.assertAlmostEqual(stock.fundamentals["roe_adj_pb"], 0.125, places=6)
            self.assertIsNotNone(stock.fundamentals["fair_value_gap"])
            self.assertIsNotNone(stock.fundamentals["discount_to_fair_pb"])

    def test_load_from_screener_derives_general_roe_adjusted_pb(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "screener.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "AAA",
                        "Name": "AAA",
                        "Macro Sector": "Technology",
                        "Sector": "Technology",
                        "Industry": "Software",
                        "Basic Industry": "Computers - Software & Consulting",
                        "Price to Book value": "3.0",
                        "ROE": "15",
                    }
                ]
            ).to_csv(csv_path, index=False)
            universe = load_from_screener(str(csv_path), price_history_map={})
            stock = universe["AAA"]
            self.assertAlmostEqual(stock.fundamentals["roe_adj_pb"], 0.2, places=6)
            self.assertIsNone(stock.fundamentals.get("fair_value_gap"))

    def test_load_from_screener_excludes_fund_like_instruments(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "screener.csv"
            pd.DataFrame(
                [
                    {
                        "NSE Symbol": "ETF1",
                        "Name": "Sample Banking ETF",
                    },
                    {
                        "NSE Symbol": "AAA",
                        "Name": "Operating Company Ltd",
                        "Macro Sector": "Technology",
                        "Sector": "Technology",
                        "Industry": "Software",
                        "Basic Industry": "Computers - Software & Consulting",
                    },
                ]
            ).to_csv(csv_path, index=False)
            universe = load_from_screener(str(csv_path), price_history_map={})
            self.assertNotIn("ETF1", universe)
            self.assertIn("AAA", universe)

    def test_red_flags_uses_raw_pledge_percentage_for_disqualifier(self) -> None:
        stock = _stock(
            "AAA",
            fundamentals={
                metric: 10.0
                for metric in CARD_WEIGHTS["A"]["red_flags"]
            },
        )
        stock.fundamentals.update(
            {
                "promoter_pledge": 75.0,
                "pledge_pct": 20.0,
                "asm_stage": 0,
                "gsm_stage": 0,
                "interest_coverage": 4.0,
                "credit_rating_grade": 2.0,
                "avg_daily_turnover_cr": 10.0,
                "governance_events": [],
            }
        )
        peers = [
            _stock("PEER1", fundamentals={metric: 15.0 for metric in CARD_WEIGHTS["A"]["red_flags"]}),
            _stock("PEER2", fundamentals={metric: 5.0 for metric in CARD_WEIGHTS["A"]["red_flags"]}),
        ]
        card = score_red_flags(stock, peers, Template.GENERAL)
        self.assertNotEqual(card.label, "Severe")


if __name__ == "__main__":
    unittest.main()
