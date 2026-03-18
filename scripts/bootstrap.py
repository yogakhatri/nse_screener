#!/usr/bin/env python3
"""
Bootstrap helper for first-time setup after cloning from GitHub.

What it does:
1) Creates required project directories.
2) Generates a Screener CSV template for first run.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import ensure_folders


TEMPLATE_COLUMNS = [
    "NSE Symbol",
    "Name",
    "Macro Sector",
    "Sector",
    "Industry",
    "Basic Industry",
    "P/E",
    "Price to Book value",
    "EV / EBITDA",
    "FCF Yield",
    "Sales growth 3Years",
    "Profit growth 3Years",
    "ROCE 3Years",
    "OPM",
    "ROA",
    "Pledged percentage",
    "1 Year Return",
    "6 Month Return",
    "5 Year CAGR",
    "Relative Strength",
    "Drawdown Recovery",
    "Forward Growth",
    "Current Price",
    "Intrinsic Value",
    "RSI",
    "Price vs 200 DMA",
    "Price vs 50 DMA",
    "Delivery Score",
    "RS Turn",
    "Volatility Compression",
    "Debt to equity",
    "Interest Coverage",
    "Credit Rating Grade",
    "Avg Daily Turnover Cr",
    "ASM Stage",
    "GSM Stage",
]


SAMPLE_ROW = {
    "NSE Symbol": "RELIANCE",
    "Name": "Reliance Industries",
    "Macro Sector": "Energy",
    "Sector": "Oil & Gas",
    "Industry": "Refineries",
    "Basic Industry": "Refineries",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--template-path",
        default="data/raw/fundamentals/screener/screener_export_TEMPLATE.csv",
        help="Where to write template CSV",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite template if it already exists",
    )
    parser.add_argument(
        "--sample-row",
        action="store_true",
        help="Include one sample row in template",
    )
    return parser.parse_args()


def write_template(path: Path, force: bool, sample_row: bool) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        return path
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TEMPLATE_COLUMNS)
        writer.writeheader()
        if sample_row:
            row = {col: "" for col in TEMPLATE_COLUMNS}
            row.update(SAMPLE_ROW)
            writer.writerow(row)
    return path


def main() -> None:
    args = parse_args()
    ensure_folders()
    template_path = write_template(Path(args.template_path), force=args.force, sample_row=args.sample_row)
    print("Bootstrap complete.")
    print(f"Template ready: {template_path}")
    print("Next:")
    print("  1) Fill the template with your Screener export metrics")
    print("  2) Run: make run RUN_DATE=YYYY-MM-DD SCREENER_CSV=<your_csv_path>")
    print("     (or python scripts/run_engine.py --mode live --market-mode auto --screener-csv <your_csv_path>)")


if __name__ == "__main__":
    main()
