#!/usr/bin/env python3
"""
Fetch Nifty 500 (and other NSE index) EOD closing values.

Source URL pattern:
  https://archives.nseindia.com/content/indices/ind_close_all_{DD}{Mon}{YYYY}.csv

The fetched data is used by compute_rs_turn() and compute_peer_price_strength()
in the engine to compute relative-strength metrics.

Usage:
  python scripts/fetch_index_data.py --end-date 2026-03-18 --sessions 260
  python scripts/fetch_index_data.py --end-date 2026-03-18 --index "Nifty 500"
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP

INDICES_DIR = FOLDER_MAP["indices"]
INDICES_DIR.mkdir(parents=True, exist_ok=True)

# NSE archive URL templates for index data
INDEX_URL_TEMPLATES = [
    "https://archives.nseindia.com/content/indices/ind_close_all_{date_str}.csv",
    "https://www.niftyindices.com/Backpage.aspx/getHistoricaldatatabletoaliaboraliaboraliaboraliaboraliabo",
]

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}

# Target indices to track
TARGET_INDICES = {
    "NIFTY 500": "nifty500",
    "NIFTY 50": "nifty50",
    "NIFTY NEXT 50": "niftynext50",
    "NIFTY MIDCAP 150": "niftymidcap150",
    "NIFTY SMALLCAP 250": "niftysmallcap250",
    "NIFTY BANK": "niftybank",
}


def download_index_file(target_date: date) -> Optional[Path]:
    """Download NSE index EOD file for a given date."""
    import requests

    # NSE uses format like "12Mar2026"
    date_str = target_date.strftime("%d%b%Y")
    url = f"https://archives.nseindia.com/content/indices/ind_close_all_{date_str}.csv"
    dest_path = INDICES_DIR / f"ind_close_all_{date_str}.csv"

    # Check cache
    if dest_path.exists() and dest_path.stat().st_size > 100:
        return dest_path

    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    try:
        session.get("https://www.nseindia.com/", timeout=10)
    except Exception:
        pass

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 100:
            dest_path.write_bytes(resp.content)
            return dest_path
    except Exception:
        pass

    return None


def read_index_csv(file_path: Path) -> Dict[str, float]:
    """Read an index EOD CSV and return index_name -> close_value mapping."""
    try:
        df = pd.read_csv(file_path, dtype=str).fillna("")
    except Exception:
        return {}

    # Find columns
    index_col = None
    close_col = None

    for col in df.columns:
        norm = col.strip().lower().replace(" ", "")
        if norm in ("indexname", "index"):
            index_col = col
        elif norm in ("closingindexvalue", "closingvalue", "close", "closevalue"):
            close_col = col

    if not index_col or not close_col:
        return {}

    result = {}
    for _, row in df.iterrows():
        name = str(row[index_col]).strip().upper()
        try:
            val = float(str(row[close_col]).strip().replace(",", ""))
            result[name] = val
        except (ValueError, TypeError):
            continue

    return result


def load_local_index_history(
    run_date: date,
    index_name: str = "NIFTY 500",
    lookback_sessions: int = 260,
) -> List[dict]:
    """Load index closing values from local cache."""
    if not INDICES_DIR.exists():
        return []

    dated_files = []
    for fpath in sorted(INDICES_DIR.glob("ind_close_all_*.csv")):
        # Extract date from filename
        name = fpath.stem  # e.g. ind_close_all_12Mar2026
        date_part = name.replace("ind_close_all_", "")
        try:
            fdate = datetime.strptime(date_part, "%d%b%Y").date()
        except ValueError:
            continue
        if fdate > run_date:
            continue
        dated_files.append((fdate, fpath))

    if not dated_files:
        return []

    dated_files.sort(key=lambda x: x[0])
    selected = dated_files[-lookback_sessions:]

    records = []
    target_upper = index_name.strip().upper()

    for fdate, fpath in selected:
        values = read_index_csv(fpath)
        if target_upper in values:
            records.append({
                "date": fdate,
                "close": values[target_upper],
            })

    return records


def build_index_series(run_date: date, lookback: int = 260) -> Dict[str, List[float]]:
    """Build close-price series for all target indices."""
    result = {}

    for idx_name, short_name in TARGET_INDICES.items():
        records = load_local_index_history(run_date, idx_name, lookback)
        if records:
            result[short_name] = [r["close"] for r in sorted(records, key=lambda r: r["date"])]

    return result


def save_index_summary(index_series: Dict[str, List[float]], run_date: date) -> Path:
    """Save a summary CSV of index trending data."""
    output_path = INDICES_DIR / f"index_summary_{run_date.isoformat()}.csv"

    fieldnames = ["index", "sessions", "latest_close", "return_1m", "return_3m", "return_6m", "return_1y"]

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for idx_name, closes in sorted(index_series.items()):
            if not closes:
                continue
            latest = closes[-1]
            row = {
                "index": idx_name,
                "sessions": len(closes),
                "latest_close": round(latest, 2),
                "return_1m": round((latest / closes[-22] - 1) * 100, 2) if len(closes) >= 22 else "",
                "return_3m": round((latest / closes[-66] - 1) * 100, 2) if len(closes) >= 66 else "",
                "return_6m": round((latest / closes[-130] - 1) * 100, 2) if len(closes) >= 130 else "",
                "return_1y": round((latest / closes[-252] - 1) * 100, 2) if len(closes) >= 252 else "",
            }
            w.writerow(row)

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch NSE index EOD data")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--sessions", type=int, default=260, help="Sessions to fetch (default: 260)")
    parser.add_argument("--max-calendar-days", type=int, default=520, help="Max calendar days to search back")
    parser.add_argument("--index", default="Nifty 500", help="Primary index name (default: Nifty 500)")
    parser.add_argument("--skip-download", action="store_true", help="Only process existing locally cached files")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between downloads (seconds)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    end_date = date.fromisoformat(args.end_date)

    if not args.skip_download:
        print(f"[Index] Fetching up to {args.sessions} sessions ending {end_date}...")
        fetched = 0
        errors = 0
        candidate_date = end_date

        for _ in range(args.max_calendar_days):
            if fetched >= args.sessions:
                break
            if candidate_date.weekday() >= 5:
                candidate_date -= timedelta(days=1)
                continue

            result = download_index_file(candidate_date)
            if result:
                fetched += 1
                if fetched % 20 == 0:
                    print(f"  [{fetched}/{args.sessions}] downloaded...")
            else:
                errors += 1

            candidate_date -= timedelta(days=1)
            time.sleep(args.delay)

        print(f"  Downloaded {fetched} files ({errors} missing/failed)")

    # Build index series
    print("[Index] Building index time series...")
    index_series = build_index_series(end_date, args.sessions)

    for idx_name, closes in index_series.items():
        print(f"  {idx_name}: {len(closes)} data points", end="")
        if closes:
            print(f"  latest={closes[-1]:.2f}")
        else:
            print()

    if index_series:
        summary_path = save_index_summary(index_series, end_date)
        print(f"  Saved summary: {summary_path}")
    else:
        print("  No index data found. Run without --skip-download to fetch data.")


if __name__ == "__main__":
    main()
