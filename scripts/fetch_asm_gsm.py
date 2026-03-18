#!/usr/bin/env python3
"""
Fetch ASM (Additional Surveillance Measure) and GSM (Graded Surveillance Measure)
lists from NSE India and save them to data/raw/redflags/.

NSE publishes these lists as CSVs/Excel on their website.
This script fetches the current surveillance lists and outputs them in a format
the engine's load_data.py can consume.

Usage:
  python scripts/fetch_asm_gsm.py --date 2026-03-18
  python scripts/fetch_asm_gsm.py --date 2026-03-18 --merge-csv <screener_csv>
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP

ASM_DIR = FOLDER_MAP["asm"]
GSM_DIR = FOLDER_MAP["gsm"]

# NSE API endpoints for surveillance data
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json,text/html",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


def _get_nse_session():
    """Create a session with NSE cookies."""
    import requests
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    # Hit the main page first to get cookies
    try:
        session.get("https://www.nseindia.com/", timeout=10)
        time.sleep(1)
    except Exception:
        pass
    return session


def fetch_asm_list(session=None) -> list[dict]:
    """Fetch current ASM list from NSE website.
    Returns list of dicts with 'symbol' and 'stage' keys."""
    import requests
    if session is None:
        session = _get_nse_session()

    results = []

    # Try NSE ASM API endpoint
    urls = [
        "https://www.nseindia.com/api/reportASM",
        "https://www.nseindia.com/api/live-analysis-asm",
    ]

    for url in urls:
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            # Parse the response based on structure
            if isinstance(data, dict):
                for key in ["data", "asm", "ASM"]:
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        symbol = item.get("symbol", item.get("Symbol", item.get("SYMBOL", "")))
                        stage = item.get("stage", item.get("Stage", item.get("asmStage", 1)))
                        if symbol:
                            try:
                                stage_int = int(str(stage).strip().replace("Stage", "").strip())
                            except (ValueError, TypeError):
                                stage_int = 1
                            results.append({
                                "symbol": str(symbol).strip().upper(),
                                "stage": stage_int,
                            })
                if results:
                    return results
        except Exception as e:
            print(f"  [WARN] ASM fetch from {url}: {e}")
            continue

    # Fallback: try to parse from NSE circulars page
    try:
        resp = session.get(
            "https://www.nseindia.com/api/merged-daily-reports?key=favASM",
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                for item in data:
                    symbol = item.get("symbol", "")
                    if symbol:
                        results.append({"symbol": str(symbol).strip().upper(), "stage": 1})
    except Exception:
        pass

    return results


def fetch_gsm_list(session=None) -> list[dict]:
    """Fetch current GSM list from NSE website."""
    import requests
    if session is None:
        session = _get_nse_session()

    results = []
    urls = [
        "https://www.nseindia.com/api/reportGSM",
        "https://www.nseindia.com/api/live-analysis-gsm",
    ]

    for url in urls:
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if isinstance(data, dict):
                for key in ["data", "gsm", "GSM"]:
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        symbol = item.get("symbol", item.get("Symbol", ""))
                        stage = item.get("stage", item.get("Stage", item.get("gsmStage", 1)))
                        if symbol:
                            try:
                                stage_int = int(str(stage).strip().replace("Stage", "").strip())
                            except (ValueError, TypeError):
                                stage_int = 1
                            results.append({
                                "symbol": str(symbol).strip().upper(),
                                "stage": stage_int,
                            })
                if results:
                    return results
        except Exception as e:
            print(f"  [WARN] GSM fetch from {url}: {e}")
            continue

    return results


def save_surveillance_data(
    asm_list: list[dict],
    gsm_list: list[dict],
    run_date: date,
) -> tuple[Path, Path]:
    """Save ASM and GSM lists as dated CSVs."""
    ASM_DIR.mkdir(parents=True, exist_ok=True)
    GSM_DIR.mkdir(parents=True, exist_ok=True)

    asm_path = ASM_DIR / f"asm_list_{run_date.isoformat()}.csv"
    gsm_path = GSM_DIR / f"gsm_list_{run_date.isoformat()}.csv"

    with open(asm_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "stage"])
        w.writeheader()
        w.writerows(asm_list)

    with open(gsm_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "stage"])
        w.writeheader()
        w.writerows(gsm_list)

    return asm_path, gsm_path


def merge_into_screener_csv(
    screener_csv: Path,
    asm_list: list[dict],
    gsm_list: list[dict],
) -> int:
    """Merge ASM/GSM data into the screener CSV's ASM Stage and GSM Stage columns."""
    if not screener_csv.exists():
        return 0

    df = pd.read_csv(screener_csv)

    # Find symbol column
    sym_col = None
    for alias in ["NSE Symbol", "Symbol", "Ticker"]:
        if alias in df.columns:
            sym_col = alias
            break
    if not sym_col:
        return 0

    asm_map = {item["symbol"]: item["stage"] for item in asm_list}
    gsm_map = {item["symbol"]: item["stage"] for item in gsm_list}

    updated = 0
    asm_col = "ASM Stage" if "ASM Stage" in df.columns else None
    gsm_col = "GSM Stage" if "GSM Stage" in df.columns else None

    if not asm_col:
        df["ASM Stage"] = 0
        asm_col = "ASM Stage"
    if not gsm_col:
        df["GSM Stage"] = 0
        gsm_col = "GSM Stage"

    for idx, row in df.iterrows():
        sym = str(row[sym_col]).strip().upper()
        if sym in asm_map:
            df.at[idx, asm_col] = asm_map[sym]
            updated += 1
        if sym in gsm_map:
            df.at[idx, gsm_col] = gsm_map[sym]
            updated += 1

    df.to_csv(screener_csv, index=False)
    return updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch ASM/GSM surveillance lists from NSE")
    parser.add_argument("--date", default=date.today().isoformat(), help="Run date YYYY-MM-DD")
    parser.add_argument("--merge-csv", default=None, help="Merge surveillance data into screener CSV")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.date)

    print(f"[ASM/GSM] Fetching surveillance lists for {run_date}...")

    try:
        session = _get_nse_session()
    except ImportError:
        print("[ERROR] requests library required. Install: pip install requests")
        sys.exit(1)

    asm_list = fetch_asm_list(session)
    print(f"  ASM: {len(asm_list)} stocks under surveillance")
    time.sleep(2)

    gsm_list = fetch_gsm_list(session)
    print(f"  GSM: {len(gsm_list)} stocks under surveillance")

    asm_path, gsm_path = save_surveillance_data(asm_list, gsm_list, run_date)
    print(f"  Saved: {asm_path}")
    print(f"  Saved: {gsm_path}")

    if args.merge_csv:
        merge_path = Path(args.merge_csv)
        updated = merge_into_screener_csv(merge_path, asm_list, gsm_list)
        print(f"  Merged {updated} surveillance flags into {merge_path}")


if __name__ == "__main__":
    main()
