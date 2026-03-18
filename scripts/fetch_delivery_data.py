#!/usr/bin/env python3
"""
Fetch and process NSE Security-wise Delivery Position data.

Downloads daily delivery CSVs from NSE archives, computes 20D and 60D average
delivery percentages per symbol, and outputs a volume_delivery score.

Source URL pattern:
  https://archives.nseindia.com/products/content/sec_del_eq_{DDMMYYYY}.zip

Usage:
  python scripts/fetch_delivery_data.py --end-date 2026-03-18 --sessions 60
  python scripts/fetch_delivery_data.py --end-date 2026-03-18 --merge-csv <screener_csv>
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP

DELIVERY_DIR = FOLDER_MAP["delivery"]
DELIVERY_DIR.mkdir(parents=True, exist_ok=True)

# NSE archive URL template for delivery data
DELIVERY_URL_TEMPLATE = (
    "https://archives.nseindia.com/products/content/sec_del_eq_{date_str}.zip"
)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}

# Column aliases for delivery CSVs (NSE has changed formats over time)
SYMBOL_ALIASES = ("SYMBOL", "TckrSymb", "SCRIP_NAME", "Symbol")
SERIES_ALIASES = ("SERIES", "SctySrs")
DATE_ALIASES = ("DATE1", "TradDt", "DATE")
TRADED_QTY_ALIASES = ("QTY_PER_TRADE", "TRADED_QTY", "TOTTRDQTY", "TtlTradgVol",
                      "TradedQty", "TotalTradedQuantity")
DELIVERABLE_QTY_ALIASES = ("DELIVERABLE_QTY", "DELIV_QTY", "DlvrblQty",
                           "DeliverableQuantity", "Deliv_Qty")
DELIVERY_PCT_ALIASES = ("DELIV_PER", "DELIVERY_PCT", "DlvryPctge",
                        "Pct_Deliv_Qty_to_TradedQty", "%DlyQt_to_TrdQty",
                        "DeliverablePercentage")


def _norm(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _find_col(columns: Iterable[str], aliases: Iterable[str]) -> Optional[str]:
    normalized = {_norm(col): col for col in columns}
    for alias in aliases:
        hit = normalized.get(_norm(alias))
        if hit is not None:
            return hit
    return None


def _as_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"nan", "na", "none", "-"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def download_delivery_file(target_date: date, dest_dir: Path = DELIVERY_DIR) -> Optional[Path]:
    """Download NSE delivery position file for a given date."""
    import requests

    date_str = target_date.strftime("%d%m%Y")
    url = DELIVERY_URL_TEMPLATE.format(date_str=date_str)
    dest_path = dest_dir / f"sec_del_eq_{date_str}.zip"

    # Check cache first
    if dest_path.exists() and dest_path.stat().st_size > 100:
        return dest_path

    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    # Visit NSE homepage first to get cookies
    try:
        session.get("https://www.nseindia.com/", timeout=10)
    except Exception:
        pass

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 100:
            dest_path.write_bytes(resp.content)
            return dest_path
        # Try alternate URL pattern
        alt_url = f"https://archives.nseindia.com/archives/equities/mto/MTO_{target_date.strftime('%d%m%Y')}.DAT"
        resp = session.get(alt_url, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 100:
            alt_path = dest_dir / f"MTO_{date_str}.DAT"
            alt_path.write_bytes(resp.content)
            return alt_path
    except Exception:
        pass

    return None


def read_delivery_csv(file_path: Path) -> pd.DataFrame:
    """Read a delivery position file (ZIP or DAT)."""
    if file_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(file_path) as zf:
            members = [n for n in zf.namelist()
                       if n.lower().endswith((".csv", ".dat", ".txt"))]
            if not members:
                return pd.DataFrame()
            with zf.open(members[0]) as fh:
                content = fh.read().decode("utf-8", errors="replace")
    else:
        content = file_path.read_text(errors="replace")

    # Try standard CSV parsing
    try:
        df = pd.read_csv(io.StringIO(content), dtype=str).fillna("")
        if len(df.columns) > 2:
            return df
    except Exception:
        pass

    # Try with different separators (some old files use pipe or fixed-width)
    for sep in ["|", "\t", ","]:
        try:
            df = pd.read_csv(io.StringIO(content), sep=sep, dtype=str).fillna("")
            if len(df.columns) > 2:
                return df
        except Exception:
            continue

    return pd.DataFrame()


def parse_delivery_date_from_filename(path: Path) -> Optional[date]:
    """Extract date from delivery file name."""
    patterns = [
        re.compile(r"sec_del_eq_(\d{8})", re.IGNORECASE),
        re.compile(r"MTO_(\d{8})", re.IGNORECASE),
    ]
    for pat in patterns:
        m = pat.search(path.name)
        if m:
            try:
                return datetime.strptime(m.group(1), "%d%m%Y").date()
            except ValueError:
                pass
    return None


def load_local_delivery_history(
    run_date: date,
    tickers: Optional[Iterable[str]] = None,
    delivery_dir: Path = DELIVERY_DIR,
    lookback_sessions: int = 60,
) -> Dict[str, list]:
    """Load delivery data from local cache for all tickers."""
    if not delivery_dir.exists():
        return {}

    wanted = {str(t).strip().upper() for t in tickers or [] if str(t).strip()}
    has_filter = bool(wanted)

    dated_files = []
    for fpath in sorted(delivery_dir.iterdir()):
        if fpath.suffix.lower() not in (".zip", ".dat", ".csv"):
            continue
        fdate = parse_delivery_date_from_filename(fpath)
        if fdate is None or fdate > run_date:
            continue
        dated_files.append((fdate, fpath))

    if not dated_files:
        return {}

    dated_files.sort(key=lambda x: x[0])
    selected = dated_files[-lookback_sessions:]

    data_by_ticker: dict[str, list[dict]] = defaultdict(list)

    for fdate, fpath in selected:
        df = read_delivery_csv(fpath)
        if df.empty:
            continue

        sym_col = _find_col(df.columns, SYMBOL_ALIASES)
        series_col = _find_col(df.columns, SERIES_ALIASES)
        traded_col = _find_col(df.columns, TRADED_QTY_ALIASES)
        deliv_col = _find_col(df.columns, DELIVERABLE_QTY_ALIASES)
        pct_col = _find_col(df.columns, DELIVERY_PCT_ALIASES)

        if not sym_col:
            continue

        # Filter EQ series only
        if series_col:
            df = df[df[series_col].astype(str).str.strip().str.upper() == "EQ"]

        if has_filter:
            df = df[df[sym_col].astype(str).str.strip().str.upper().isin(wanted)]

        for _, row in df.iterrows():
            symbol = str(row.get(sym_col, "")).strip().upper()
            if not symbol:
                continue

            # Get delivery percentage directly or compute it
            delivery_pct = None
            if pct_col:
                delivery_pct = _as_float(row.get(pct_col))

            if delivery_pct is None and traded_col and deliv_col:
                traded = _as_float(row.get(traded_col))
                delivered = _as_float(row.get(deliv_col))
                if traded and traded > 0 and delivered is not None:
                    delivery_pct = (delivered / traded) * 100.0

            if delivery_pct is not None:
                data_by_ticker[symbol].append({
                    "date": fdate,
                    "delivery_pct": delivery_pct,
                })

    return data_by_ticker


def compute_delivery_metrics(
    delivery_data: Dict[str, list],
) -> Dict[str, dict]:
    """Compute 20D and 60D average delivery percentages for each ticker."""
    results = {}

    for ticker, records in delivery_data.items():
        if not records:
            continue

        # Sort by date, take latest
        records.sort(key=lambda r: r["date"])
        pcts = [r["delivery_pct"] for r in records if r["delivery_pct"] is not None]

        if not pcts:
            continue

        avg_20d = np.mean(pcts[-20:]) if len(pcts) >= 5 else None
        avg_60d = np.mean(pcts[-60:]) if len(pcts) >= 10 else None

        # Compute volume_delivery score (same as metric_definitions)
        volume_delivery = None
        if avg_20d is not None and avg_60d is not None and avg_60d > 0:
            volume_delivery = float(np.clip((avg_20d / avg_60d) * 50.0, 0.0, 100.0))

        results[ticker] = {
            "delivery_pct_20d_avg": round(avg_20d, 2) if avg_20d is not None else None,
            "delivery_pct_60d_avg": round(avg_60d, 2) if avg_60d is not None else None,
            "volume_delivery": round(volume_delivery, 2) if volume_delivery is not None else None,
            "sessions_available": len(pcts),
        }

    return results


def save_delivery_metrics(metrics: Dict[str, dict], run_date: date) -> Path:
    """Save computed delivery metrics as dated CSV."""
    output_path = DELIVERY_DIR / f"delivery_metrics_{run_date.isoformat()}.csv"

    fieldnames = ["symbol", "delivery_pct_20d_avg", "delivery_pct_60d_avg",
                  "volume_delivery", "sessions_available"]

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for ticker, data in sorted(metrics.items()):
            row = {"symbol": ticker, **data}
            w.writerow(row)

    return output_path


def merge_into_screener_csv(screener_csv: Path, metrics: Dict[str, dict]) -> int:
    """Merge volume_delivery scores into screener CSV."""
    if not screener_csv.exists() or not metrics:
        return 0

    df = pd.read_csv(screener_csv)
    sym_col = None
    for alias in ["NSE Symbol", "Symbol", "Ticker"]:
        if alias in df.columns:
            sym_col = alias
            break
    if not sym_col:
        return 0

    # Add/update Delivery Score column
    if "Delivery Score" not in df.columns:
        df["Delivery Score"] = np.nan

    updated = 0
    for idx, row in df.iterrows():
        sym = str(row[sym_col]).strip().upper()
        if sym in metrics and metrics[sym].get("volume_delivery") is not None:
            df.at[idx, "Delivery Score"] = metrics[sym]["volume_delivery"]
            updated += 1

    df.to_csv(screener_csv, index=False)
    return updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and process NSE delivery data")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--sessions", type=int, default=60, help="Number of sessions to fetch (default: 60)")
    parser.add_argument("--max-calendar-days", type=int, default=120, help="Max calendar days to search back")
    parser.add_argument("--merge-csv", default=None, help="Merge delivery scores into screener CSV")
    parser.add_argument("--skip-download", action="store_true", help="Only process existing local files")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between downloads (seconds)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    end_date = date.fromisoformat(args.end_date)

    if not args.skip_download:
        print(f"[Delivery] Fetching up to {args.sessions} sessions ending {end_date}...")
        fetched = 0
        errors = 0
        candidate_date = end_date

        for _ in range(args.max_calendar_days):
            if fetched >= args.sessions:
                break
            if candidate_date.weekday() >= 5:  # Skip weekends
                candidate_date -= timedelta(days=1)
                continue

            result = download_delivery_file(candidate_date)
            if result:
                fetched += 1
                if fetched % 10 == 0:
                    print(f"  [{fetched}/{args.sessions}] downloaded...")
            else:
                errors += 1

            candidate_date -= timedelta(days=1)
            time.sleep(args.delay)

        print(f"  Downloaded {fetched} files ({errors} missing/failed)")

    # Load and compute metrics
    print("[Delivery] Computing delivery metrics...")
    delivery_data = load_local_delivery_history(end_date, lookback_sessions=args.sessions)
    metrics = compute_delivery_metrics(delivery_data)
    print(f"  Computed metrics for {len(metrics)} symbols")

    if metrics:
        output_path = save_delivery_metrics(metrics, end_date)
        print(f"  Saved: {output_path}")

    if args.merge_csv and metrics:
        merged = merge_into_screener_csv(Path(args.merge_csv), metrics)
        print(f"  Merged {merged} delivery scores into {args.merge_csv}")
    elif not metrics:
        print("  No delivery data found. Run with --sessions to download data first.")


if __name__ == "__main__":
    main()
