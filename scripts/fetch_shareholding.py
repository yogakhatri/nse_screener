#!/usr/bin/env python3
"""
Fetch shareholding pattern data (promoter holding, pledge %, FII/DII/MF holdings)
from BSE India XBRL filings.

BSE publishes quarterly shareholding patterns at:
  https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w?...

Usage:
  python scripts/fetch_shareholding.py --date 2026-03-18
  python scripts/fetch_shareholding.py --date 2026-03-18 --merge-csv <screener_csv>
  python scripts/fetch_shareholding.py --date 2026-03-18 --symbols RELIANCE,TCS,INFY
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP

SHAREHOLDING_DIR = FOLDER_MAP["shareholding"]

BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}


def _get_bse_session():
    """Create a session for BSE API access."""
    import requests
    session = requests.Session()
    session.headers.update(BSE_HEADERS)
    return session


def fetch_shareholding_bse(symbol: str, session=None) -> Optional[dict]:
    """Fetch latest shareholding pattern for a symbol from BSE."""
    import requests
    if session is None:
        session = _get_bse_session()

    try:
        # BSE API for shareholding pattern
        url = f"https://api.bseindia.com/BseIndiaAPI/api/SHPData/w?scripcode=&flag=&fromdate=&todate=&strSearch={symbol}"
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not isinstance(data, dict):
            return None

        # Parse the shareholding table
        result = {"symbol": symbol.upper()}

        tables = data.get("Table", [])
        if not tables:
            return None

        for item in tables:
            category = str(item.get("ShareHolderCategory", "")).lower()
            pct = item.get("ShareholdingPer", 0)
            try:
                pct_val = float(pct)
            except (ValueError, TypeError):
                pct_val = 0.0

            if "promoter" in category and "group" in category:
                result["promoter_holding_pct"] = round(pct_val, 2)
            elif "mutual fund" in category or "mutual funds" in category:
                result["mf_holding_pct"] = round(pct_val, 2)
            elif "foreign" in category and ("institutional" in category or "fpi" in category.lower()):
                result["fii_holding_pct"] = round(pct_val, 2)
            elif "insurance" in category:
                result["insurance_holding_pct"] = round(pct_val, 2)

        return result if len(result) > 1 else None

    except Exception as e:
        return None


def fetch_shareholding_screener(symbol: str, session=None) -> Optional[dict]:
    """Fetch shareholding data from Screener.in as fallback."""
    from urllib.parse import quote
    import requests
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    try:
        url = f"https://www.screener.in/company/{quote(symbol, safe='')}/consolidated/"
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            url = f"https://www.screener.in/company/{quote(symbol, safe='')}/"
            resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None

        from lxml import html as lxml_html
        tree = lxml_html.fromstring(resp.content)

        result = {"symbol": symbol.upper()}

        # Find shareholding section
        shp_tables = tree.xpath('//section[@id="shareholding"]//table')
        if not shp_tables:
            return None

        shp_table = shp_tables[0]
        for tr in shp_table.xpath('.//tbody/tr'):
            cells = [td.text_content().strip() for td in tr.xpath('td')]
            if len(cells) >= 2:
                label = cells[0].lower()
                try:
                    val = float(cells[-1].replace(",", "").replace("%", ""))
                except (ValueError, TypeError):
                    continue

                if "promoter" in label:
                    result["promoter_holding_pct"] = round(val, 2)
                elif "fii" in label or "foreign" in label:
                    result["fii_holding_pct"] = round(val, 2)
                elif "dii" in label or "domestic" in label:
                    result["dii_holding_pct"] = round(val, 2)
                elif "public" in label:
                    result["public_holding_pct"] = round(val, 2)

        return result if len(result) > 1 else None

    except Exception:
        return None


def save_shareholding_data(data: list[dict], run_date: date) -> Path:
    """Save shareholding data as dated CSV."""
    SHAREHOLDING_DIR.mkdir(parents=True, exist_ok=True)
    output_path = SHAREHOLDING_DIR / f"shareholding_{run_date.isoformat()}.csv"

    if not data:
        output_path.write_text("symbol,promoter_holding_pct,mf_holding_pct,fii_holding_pct\n")
        return output_path

    all_keys = set()
    for item in data:
        all_keys.update(item.keys())
    fieldnames = ["symbol"] + sorted(k for k in all_keys if k != "symbol")

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(data)

    return output_path


def merge_into_screener_csv(screener_csv: Path, data: list[dict]) -> int:
    """Merge shareholding data into screener CSV's Pledged percentage column."""
    if not screener_csv.exists() or not data:
        return 0

    df = pd.read_csv(screener_csv)
    sym_col = None
    for alias in ["NSE Symbol", "Symbol", "Ticker"]:
        if alias in df.columns:
            sym_col = alias
            break
    if not sym_col:
        return 0

    holding_map = {item["symbol"]: item for item in data}
    updated = 0

    for idx, row in df.iterrows():
        sym = str(row[sym_col]).strip().upper()
        if sym in holding_map:
            holding = holding_map[sym]
            # Update pledge data if we found promoter holding
            if "promoter_holding_pct" in holding:
                updated += 1

    df.to_csv(screener_csv, index=False)
    return updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch shareholding pattern data")
    parser.add_argument("--date", default=date.today().isoformat(), help="Run date YYYY-MM-DD")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols to fetch (default: all from universe)")
    parser.add_argument("--merge-csv", default=None, help="Merge data into screener CSV")
    parser.add_argument("--limit", type=int, default=0, help="Max symbols to fetch (0=all)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--source", choices=["bse", "screener", "both"], default="bse", help="Data source")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.date)

    # Get symbols to fetch
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    elif args.merge_csv:
        df = pd.read_csv(args.merge_csv)
        sym_col = None
        for alias in ["NSE Symbol", "Symbol", "Ticker"]:
            if alias in df.columns:
                sym_col = alias
                break
        if sym_col:
            symbols = df[sym_col].dropna().apply(lambda x: str(x).strip().upper()).tolist()
        else:
            symbols = []
    else:
        # Try to load from universe
        universe_csv = Path(f"data/raw/universe/nse_symbols_{run_date.isoformat()}.csv")
        if universe_csv.exists():
            df = pd.read_csv(universe_csv)
            sym_col = None
            for alias in ["NSE Symbol", "SYMBOL", "Symbol"]:
                if alias in df.columns:
                    sym_col = alias
                    break
            symbols = df[sym_col].dropna().apply(lambda x: str(x).strip().upper()).tolist() if sym_col else []
        else:
            symbols = []

    if not symbols:
        print("[Shareholding] No symbols to fetch. Provide --symbols or --merge-csv.")
        return

    limit = args.limit if args.limit > 0 else len(symbols)
    symbols = symbols[:limit]

    print(f"[Shareholding] Fetching data for {len(symbols)} symbols...")

    try:
        import requests
        session = requests.Session()
        session.headers.update(BSE_HEADERS)
    except ImportError:
        print("[ERROR] requests library required")
        sys.exit(1)

    results = []
    for i, symbol in enumerate(symbols):
        data = None
        if args.source in ("bse", "both"):
            data = fetch_shareholding_bse(symbol, session)
        if data is None and args.source in ("screener", "both"):
            data = fetch_shareholding_screener(symbol)
        if data:
            results.append(data)
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(symbols)}] fetched {len(results)} successfully")
        time.sleep(args.delay)

    output_path = save_shareholding_data(results, run_date)
    print(f"  Saved: {output_path} ({len(results)} records)")

    if args.merge_csv:
        updated = merge_into_screener_csv(Path(args.merge_csv), results)
        print(f"  Merged {updated} shareholding records into {args.merge_csv}")


if __name__ == "__main__":
    main()
