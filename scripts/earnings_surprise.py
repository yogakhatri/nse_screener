#!/usr/bin/env python3
"""
Earnings Surprise Detector
============================
Detects earnings beats/misses by analyzing recent quarterly results
relative to trailing averages. Stocks with consistent positive surprises
tend to outperform significantly.

Approach:
  Since we don't have consensus estimates, we use a "self-estimate" approach:
  - Compare latest quarter's metrics with trailing 4-quarter averages
  - Revenue surprise: actual vs trailing avg revenue growth
  - Profit surprise: actual vs trailing avg profit growth
  - Margin surprise: latest OPM vs trailing average OPM

Data source: Screener.in quarterly data (if scraped) or CSV fundamentals.

Usage:
  python scripts/earnings_surprise.py --screener-csv <csv> --date 2026-03-12
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))


def compute_earnings_surprise(row: dict) -> Dict[str, Optional[float]]:
    """
    Compute earnings surprise metrics from available data.

    Without actual quarterly estimates, we approximate surprise by comparing
    short-term metrics against long-term trends:
    - If YoY revenue growth >> 3Y CAGR, that's a positive surprise
    - If current OPM >> sector avg OPM, that's margin surprise
    - Acceleration in growth = surprise
    """
    result: Dict[str, Optional[float]] = {}

    # Revenue acceleration (YoY growth vs 3Y CAGR)
    rev_yoy = _safe_float(row.get("Sales growth", row.get("rev_growth_yoy")))
    rev_3y = _safe_float(row.get("Sales growth 3Years", row.get("rev_cagr_3y")))

    if rev_yoy is not None and rev_3y is not None:
        # If YoY is much higher than 3Y trend, it's a positive surprise
        acceleration = rev_yoy - rev_3y
        # Scale to 0-100: -20% acceleration -> 20, 0% -> 50, +20% -> 80
        result["revenue_surprise"] = round(float(np.clip(acceleration * 1.5 + 50, 0, 100)), 2)
    else:
        result["revenue_surprise"] = None

    # Profit acceleration
    profit_yoy = _safe_float(row.get("Profit growth", row.get("eps_growth_yoy")))
    profit_3y = _safe_float(row.get("Profit growth 3Years", row.get("eps_cagr_3y")))

    if profit_yoy is not None and profit_3y is not None:
        acceleration = profit_yoy - profit_3y
        result["profit_surprise"] = round(float(np.clip(acceleration * 1.5 + 50, 0, 100)), 2)
    else:
        result["profit_surprise"] = None

    # Margin surprise (OPM relative to historical)
    opm = _safe_float(row.get("OPM", row.get("Operating Profit Margin")))
    opm_5y = _safe_float(row.get("Average OPM 5Yrs", row.get("OPM 5Yr")))

    if opm is not None and opm_5y is not None and opm_5y > 0:
        margin_delta = opm - opm_5y
        result["margin_surprise"] = round(float(np.clip(margin_delta * 3 + 50, 0, 100)), 2)
    else:
        result["margin_surprise"] = None

    # Combined earnings quality score
    scores = [v for v in result.values() if v is not None]
    if scores:
        result["earnings_surprise_composite"] = round(float(np.mean(scores)), 2)
    else:
        result["earnings_surprise_composite"] = None

    # Earnings momentum: consecutive quarters of acceleration
    # (Needs quarterly data; using proxy from YoY vs 3Y)
    if rev_yoy is not None and rev_3y is not None and profit_yoy is not None and profit_3y is not None:
        # Both revenue AND profit accelerating = strong momentum
        rev_accel = rev_yoy > rev_3y
        profit_accel = profit_yoy > profit_3y
        if rev_accel and profit_accel:
            result["earnings_momentum"] = 80.0
        elif rev_accel or profit_accel:
            result["earnings_momentum"] = 60.0
        else:
            result["earnings_momentum"] = 30.0
    else:
        result["earnings_momentum"] = None

    return result


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(str(val).strip().replace(",", "").replace("%", ""))
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


def process_universe(screener_csv: Path) -> Dict[str, dict]:
    """Process entire universe for earnings surprise signals."""
    df = pd.read_csv(screener_csv, dtype=str).fillna("")

    sym_col = None
    for alias in ["NSE Symbol", "Symbol", "Ticker"]:
        if alias in df.columns:
            sym_col = alias
            break
    if not sym_col:
        return {}

    results = {}
    for _, row in df.iterrows():
        symbol = str(row.get(sym_col, "")).strip().upper()
        if not symbol:
            continue
        results[symbol] = compute_earnings_surprise(row.to_dict())

    return results


def save_earnings_surprise(scores: Dict[str, dict], run_date: date) -> Path:
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"earnings_surprise_{run_date.isoformat()}.csv"

    fieldnames = ["symbol", "revenue_surprise", "profit_surprise", "margin_surprise",
                  "earnings_surprise_composite", "earnings_momentum"]

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for sym, metrics in sorted(scores.items()):
            w.writerow({"symbol": sym, **metrics})

    return output_path


def merge_into_screener_csv(screener_csv: Path, scores: Dict[str, dict]) -> int:
    if not screener_csv.exists() or not scores:
        return 0

    df = pd.read_csv(screener_csv)
    sym_col = None
    for alias in ["NSE Symbol", "Symbol", "Ticker"]:
        if alias in df.columns:
            sym_col = alias
            break
    if not sym_col:
        return 0

    col_map = {
        "earnings_surprise_composite": "Earnings Surprise",
        "earnings_momentum": "Earnings Momentum",
    }

    for col in col_map.values():
        if col not in df.columns:
            df[col] = np.nan

    updated = 0
    for idx, row in df.iterrows():
        sym = str(row[sym_col]).strip().upper()
        if sym in scores:
            for key, col_name in col_map.items():
                val = scores[sym].get(key)
                if val is not None:
                    df.at[idx, col_name] = val
            updated += 1

    df.to_csv(screener_csv, index=False)
    return updated


def parse_args():
    parser = argparse.ArgumentParser(description="Detect earnings surprises")
    parser.add_argument("--screener-csv", required=True)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--merge", action="store_true", help="Merge scores back into CSV")
    return parser.parse_args()


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.date)
    csv_path = Path(args.screener_csv)

    print(f"[Earnings] Processing {csv_path}...")
    scores = process_universe(csv_path)
    print(f"  Computed surprise scores for {len(scores)} stocks")

    # Stats
    composites = [s["earnings_surprise_composite"] for s in scores.values()
                  if s.get("earnings_surprise_composite") is not None]
    if composites:
        print(f"  Avg composite: {np.mean(composites):.1f}")
        print(f"  Top beats (>70): {sum(1 for c in composites if c > 70)}")
        print(f"  Misses (<30): {sum(1 for c in composites if c < 30)}")

    output_path = save_earnings_surprise(scores, run_date)
    print(f"  Saved: {output_path}")

    if args.merge:
        merged = merge_into_screener_csv(csv_path, scores)
        print(f"  Merged {merged} scores into {csv_path}")


if __name__ == "__main__":
    main()
