#!/usr/bin/env python3
"""
Institutional Tracking Module
==============================
Tracks MF / FII / DII holding patterns and changes as institutional flow signals.
Key insight: stocks being accumulated by institutions tend to outperform.

Data sources:
  - BSE shareholding patterns (fetch_shareholding.py output)
  - NSE bulk/block deals (fetched here)
  - AMFI mutual fund portfolio data (monthly)

Metrics computed:
  - mf_holding_change     : MF holding % change QoQ
  - fii_holding_change    : FII holding % change QoQ
  - institutional_interest : Combined FII+MF+DII holding level
  - bulk_deal_signal      : Recent bulk/block deal activity direction
  - fresh_mf_entry        : Flag if MF newly entered the stock

Usage:
  python scripts/institutional_tracking.py --date 2026-03-18
  python scripts/institutional_tracking.py --date 2026-03-18 --merge-csv <csv>
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP

SHAREHOLDING_DIR = FOLDER_MAP["shareholding"]

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html",
    "Referer": "https://www.nseindia.com/",
}


def fetch_bulk_deals(run_date: date, lookback_days: int = 30) -> List[dict]:
    """Fetch recent bulk/block deal data from NSE."""
    import requests

    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    deals = []

    try:
        session.get("https://www.nseindia.com/", timeout=10)
    except Exception:
        pass

    # Try NSE API for bulk deals
    try:
        url = "https://www.nseindia.com/api/snapshot-capital-market-largedeal"
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict):
                for deal_type in ["BULK_DEALS_DATA", "BLOCK_DEALS_DATA"]:
                    if deal_type in data and isinstance(data[deal_type], list):
                        for item in data[deal_type]:
                            try:
                                deals.append({
                                    "symbol": str(item.get("symbol", "")).strip().upper(),
                                    "deal_type": "bulk" if "BULK" in deal_type else "block",
                                    "client": str(item.get("clientName", "")),
                                    "buy_sell": str(item.get("buySell", "")).upper(),
                                    "quantity": float(str(item.get("quantityTraded", 0)).replace(",", "")),
                                    "price": float(str(item.get("tradedPrice", 0)).replace(",", "")),
                                    "date": str(item.get("dealDate", run_date.isoformat())),
                                })
                            except (ValueError, TypeError):
                                continue
    except Exception:
        pass

    return deals


def load_shareholding_history(
    symbol: str,
    shp_dir: Path = SHAREHOLDING_DIR,
) -> List[dict]:
    """Load historical shareholding data for a symbol from saved CSVs."""
    records = []

    for csv_path in sorted(shp_dir.glob("shareholding_*.csv")):
        try:
            date_str = csv_path.stem.replace("shareholding_", "")
            file_date = date.fromisoformat(date_str)
        except ValueError:
            continue

        try:
            df = pd.read_csv(csv_path, dtype=str).fillna("")
            for _, row in df.iterrows():
                sym = str(row.get("symbol", "")).strip().upper()
                if sym == symbol.upper():
                    record = {"date": file_date, "symbol": sym}
                    for col in df.columns:
                        if col != "symbol":
                            try:
                                record[col] = float(str(row[col]).replace(",", ""))
                            except (ValueError, TypeError):
                                record[col] = None
                    records.append(record)
        except Exception:
            continue

    return sorted(records, key=lambda r: r["date"])


def compute_institutional_metrics(
    current_holdings: Dict[str, dict],
    previous_holdings: Optional[Dict[str, dict]] = None,
    bulk_deals: Optional[List[dict]] = None,
) -> Dict[str, dict]:
    """Compute institutional tracking metrics for each stock."""
    results = {}

    # Build bulk deal signal map
    deal_signals: Dict[str, float] = {}
    if bulk_deals:
        for deal in bulk_deals:
            sym = deal.get("symbol", "")
            if not sym:
                continue

            qty = deal.get("quantity", 0)
            direction = 1 if deal.get("buy_sell", "").startswith("B") else -1
            deal_signals[sym] = deal_signals.get(sym, 0) + (qty * direction)

    for symbol, holdings in current_holdings.items():
        metrics: dict = {}

        promoter = holdings.get("promoter_holding_pct")
        fii = holdings.get("fii_holding_pct")
        mf = holdings.get("mf_holding_pct")
        dii = holdings.get("dii_holding_pct")
        insurance = holdings.get("insurance_holding_pct")
        public = holdings.get("public_holding_pct")

        # Institutional interest: combined FII + MF + DII + Insurance
        inst_total = 0
        inst_count = 0
        for val in [fii, mf, dii, insurance]:
            if val is not None:
                inst_total += val
                inst_count += 1

        if inst_count > 0:
            # Score 0-100: higher institutional holding = higher score
            # 0% inst -> 20, 50%+ inst -> 85
            metrics["institutional_interest"] = round(
                float(np.clip(inst_total * 1.3 + 20, 0, 100)), 2
            )
        else:
            metrics["institutional_interest"] = None

        # QoQ changes
        if previous_holdings and symbol in previous_holdings:
            prev = previous_holdings[symbol]

            if fii is not None and prev.get("fii_holding_pct") is not None:
                change = fii - prev["fii_holding_pct"]
                # Score: -5% change -> 20, 0 change -> 50, +5% change -> 80
                metrics["fii_holding_change"] = round(
                    float(np.clip(change * 6 + 50, 0, 100)), 2
                )
            else:
                metrics["fii_holding_change"] = None

            if mf is not None and prev.get("mf_holding_pct") is not None:
                change = mf - prev["mf_holding_pct"]
                metrics["mf_holding_change"] = round(
                    float(np.clip(change * 6 + 50, 0, 100)), 2
                )

                # Fresh MF entry detection
                if prev["mf_holding_pct"] < 0.1 and mf >= 0.5:
                    metrics["fresh_mf_entry"] = 80.0
                else:
                    metrics["fresh_mf_entry"] = None
            else:
                metrics["mf_holding_change"] = None
                metrics["fresh_mf_entry"] = None
        else:
            metrics["fii_holding_change"] = None
            metrics["mf_holding_change"] = None
            metrics["fresh_mf_entry"] = None

        # Bulk deal signal
        if symbol in deal_signals:
            net_qty = deal_signals[symbol]
            if net_qty > 0:
                metrics["bulk_deal_signal"] = 75.0  # Net buying
            elif net_qty < 0:
                metrics["bulk_deal_signal"] = 25.0  # Net selling
            else:
                metrics["bulk_deal_signal"] = 50.0  # Neutral
        else:
            metrics["bulk_deal_signal"] = None

        results[symbol] = metrics

    return results


def save_institutional_scores(scores: Dict[str, dict], run_date: date) -> Path:
    """Save institutional scores as dated CSV."""
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"institutional_scores_{run_date.isoformat()}.csv"

    fieldnames = ["symbol", "institutional_interest", "fii_holding_change",
                  "mf_holding_change", "fresh_mf_entry", "bulk_deal_signal"]

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for ticker, metrics in sorted(scores.items()):
            row = {"symbol": ticker, **metrics}
            w.writerow(row)

    return output_path


def merge_into_screener_csv(screener_csv: Path, scores: Dict[str, dict]) -> int:
    """Merge institutional scores into screener CSV."""
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
        "institutional_interest": "Institutional Interest",
        "fii_holding_change": "FII Holding Change",
        "mf_holding_change": "MF Holding Change",
        "bulk_deal_signal": "Bulk Deal Signal",
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
    parser = argparse.ArgumentParser(description="Institutional tracking")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--merge-csv", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.date)

    print(f"[Institutional] Loading shareholding data for {run_date}...")

    # Load current shareholding data
    current_holdings = {}
    latest_shp = None
    for csv_path in sorted(SHAREHOLDING_DIR.glob("shareholding_*.csv"), reverse=True):
        latest_shp = csv_path
        break

    if latest_shp:
        try:
            df = pd.read_csv(latest_shp, dtype=str).fillna("")
            for _, row in df.iterrows():
                sym = str(row.get("symbol", "")).strip().upper()
                if sym:
                    holdings = {}
                    for col in df.columns:
                        if col != "symbol" and "pct" in col.lower():
                            try:
                                holdings[col] = float(str(row[col]).replace(",", ""))
                            except (ValueError, TypeError):
                                pass
                    current_holdings[sym] = holdings
            print(f"  Loaded {len(current_holdings)} stocks from {latest_shp.name}")
        except Exception as e:
            print(f"  Error loading shareholding: {e}")
    else:
        print("  No shareholding data found")

    # Fetch bulk deals
    print("[Institutional] Fetching bulk/block deals...")
    bulk_deals = fetch_bulk_deals(run_date)
    print(f"  Found {len(bulk_deals)} deals")

    # Compute metrics
    print("[Institutional] Computing institutional metrics...")
    scores = compute_institutional_metrics(current_holdings, None, bulk_deals)
    print(f"  Computed scores for {len(scores)} stocks")

    if scores:
        output_path = save_institutional_scores(scores, run_date)
        print(f"  Saved: {output_path}")

    if args.merge_csv and scores:
        merged = merge_into_screener_csv(Path(args.merge_csv), scores)
        print(f"  Merged {merged} institutional scores into {args.merge_csv}")


if __name__ == "__main__":
    main()
