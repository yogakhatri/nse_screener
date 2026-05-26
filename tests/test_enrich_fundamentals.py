import unittest

import pandas as pd

from scripts.enrich_fundamentals import (
    _cagr_to_approx_yoy,
    build_column_map,
    compute_rev_growth_yoy,
    enrich_dataframe,
)


class EnrichFundamentalsTests(unittest.TestCase):
    def test_cagr_to_yoy_proxy(self) -> None:
        yoy = _cagr_to_approx_yoy(12.0)
        self.assertIsNotNone(yoy)
        self.assertGreater(yoy, 0)

    def test_enrich_fills_sales_growth_from_cagr(self) -> None:
        df = pd.DataFrame(
            [{
                "NSE Symbol": "AAA",
                "Sales growth 3Years": 15.0,
                "Profit growth 3Years": 12.0,
                "OPM": 24.0,
                "FCF Yield": 2.5,
                "ROCE 3Years": 22.0,
            }]
        )
        out = enrich_dataframe(df)
        col_map = build_column_map(out)
        row = out.iloc[0]
        yoy = compute_rev_growth_yoy(row, col_map)
        self.assertIsNotNone(yoy)
        self.assertIn("Sales growth", out.columns)
        self.assertFalse(pd.isna(out.iloc[0]["Sales growth"]))


if __name__ == "__main__":
    unittest.main()
