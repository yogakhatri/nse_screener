#!/usr/bin/env python3
"""
Screener.in Full Fundamentals Scraper
======================================
Scrapes ALL metrics needed by the engine from public Screener.in company pages.
No login or paid account required.

For each stock it fetches:
  - Valuation:    P/E, P/B, EV/EBITDA, FCF Yield, Intrinsic Value
  - Growth:       Sales growth 3Y CAGR, Profit growth 3Y CAGR, YoY growths
  - Profitability:ROCE 3Y, OPM, ROA
  - Entry Point:  Current Price, RSI, Price vs 200DMA, Price vs 50DMA
  - Red Flags:    Debt/Equity, Interest Coverage, Pledged %, GNPA/NNPA (banks)
  - Classification: Sector, Industry

Output: a dated screener_export_YYYY-MM-DD.csv matching the exact schema
the engine expects.

Usage:
  python scripts/fetch_fundamentals_screener.py \\
      --universe data/raw/universe/nse_symbols_2026-03-18.csv \\
      --output data/raw/fundamentals/screener/screener_export_2026-03-18.csv \\
      --date 2026-03-18 \\
      --delay 1.5 \\
      --limit 50           # test with 50 stocks first
      --workers 4          # parallel requests (keep low to avoid blocks)
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd

try:
    import requests
    from lxml import html as lxml_html
except ImportError:
    print("ERROR: requests and lxml are required. Run: pip install requests lxml")
    sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Output columns — exactly what screener_export CSV should contain
# ──────────────────────────────────────────────────────────────────────────────
OUTPUT_COLUMNS = [
    "NSE Symbol", "Name",
    "Macro Sector", "Sector", "Industry", "Basic Industry",
    "P/E", "Price to Book value", "EV / EBITDA", "FCF Yield",
    "Sales growth 3Years", "Profit growth 3Years",
    "Sales growth", "Profit growth",          # YoY
    "ROCE 3Years", "OPM", "ROA",
    "Pledged percentage",
    "1 Year Return", "6 Month Return", "5 Year CAGR",
    "Relative Strength", "Drawdown Recovery",
    "Forward Growth", "Current Price", "Intrinsic Value",
    "RSI", "Price vs 200 DMA", "Price vs 50 DMA",
    "Delivery Score", "RS Turn", "Volatility Compression",
    "Debt to equity", "Interest Coverage",
    "Credit Rating Grade", "Avg Daily Turnover Cr",
    "ASM Stage", "GSM Stage",
    # Bank-specific extras (empty for non-banks)
    "NIM", "GNPA %", "NNPA %", "CAR %", "PCR %",
    "Margin Trend", "CFO/PAT", "FCF Consistency", "Growth Stability",
]


# ──────────────────────────────────────────────────────────────────────────────
# HTTP session setup
# ──────────────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────────────────────────────────────
def _f(value) -> Optional[float]:
    """Safe float parse from any string."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("%", "").replace("₹", "").replace("Cr", "").strip()
    if not text or text.lower() in {"na", "nan", "none", "-", "—", ""}:
        return None
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def _table_to_dict(table_el) -> dict[str, list[str]]:
    """Convert an HTML table element to {row_label: [col_values...]}."""
    rows = {}
    for tr in table_el.xpath('.//tbody/tr'):
        cells = [td.text_content().strip().replace(",", "") for td in tr.xpath('td')]
        if cells and cells[0]:
            rows[cells[0].strip()] = cells[1:]
    return rows


def _ratios_value(tree, label: str) -> Optional[float]:
    """Extract a value from the #ratios / .company-ratios section by label."""
    # Try the ratios section first
    for li in tree.xpath('//ul[contains(@class,"ratios") or @id="ratios"]/li'):
        name = li.xpath('.//span[@class="name"]')
        val = li.xpath('.//span[@class="nowrap number"]')
        if not val:
            val = li.xpath('.//span[contains(@class,"number")]')
        if name and val:
            if label.lower() in name[0].text_content().strip().lower():
                return _f(val[0].text_content())
    # Fallback: search all ratio-style elements
    for el in tree.xpath('//*[contains(@class,"ratio")]'):
        text = el.text_content()
        if label.lower() in text.lower():
            # Try to find the numeric sibling
            for span in el.xpath('.//span'):
                v = _f(span.text_content())
                if v is not None:
                    return v
    return None


def _top_ratios(tree) -> dict:
    """Extract the headline ratios box (Market Cap, Stock P/E, etc.)."""
    out = {}
    # Screener displays these in <li> elements with a .name and .number span
    for li in tree.xpath('//ul[@id="top-ratios"]/li | //ul[contains(@class,"top-ratios")]/li'):
        spans = li.xpath('.//span')
        if len(spans) >= 2:
            key = spans[0].text_content().strip().rstrip(":")
            val = spans[-1].text_content().strip().replace(",", "").replace("₹", "").replace("%", "")
            out[key] = val
    return out


def _find_row(table_dict: dict, *candidates: str) -> Optional[list]:
    """Find a row by trying multiple candidate labels."""
    tbl_keys_lower = {k.lower(): k for k in table_dict}
    for c in candidates:
        hit = tbl_keys_lower.get(c.lower())
        if hit is not None:
            return table_dict[hit]
    return None


def _linear_trend(values: list) -> Optional[float]:
    """Return slope of linear regression over the values (most recent first → reversed)."""
    vals = [_f(v) for v in reversed(values)]
    valid = [(i, v) for i, v in enumerate(vals) if v is not None]
    if len(valid) < 3:
        return None
    xs = np.array([i for i, _ in valid], dtype=float)
    ys = np.array([v for _, v in valid], dtype=float)
    if np.std(ys) < 1e-9:
        return 0.0
    return float(np.polyfit(xs, ys, 1)[0])


def _pct_positive(values: list) -> Optional[float]:
    """Percentage of non-None values that are positive."""
    vals = [_f(v) for v in values if _f(v) is not None]
    if len(vals) < 2:
        return None
    return round(sum(1 for v in vals if v > 0) / len(vals) * 100, 1)


# ──────────────────────────────────────────────────────────────────────────────
# Core scraper — single stock
# ──────────────────────────────────────────────────────────────────────────────
def scrape_stock(symbol: str, session: requests.Session) -> dict:
    """
    Scrape all available metrics from Screener.in for one symbol.
    Returns a flat dict with OUTPUT_COLUMNS as keys.
    """
    row: dict = {"NSE Symbol": symbol}

    # Try consolidated first, then standalone
    for url in [
        f"https://www.screener.in/company/{quote(symbol, safe='')}/consolidated/",
        f"https://www.screener.in/company/{quote(symbol, safe='')}/",
    ]:
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                break
        except Exception:
            continue
    else:
        return row  # all attempts failed

    try:
        tree = lxml_html.fromstring(resp.content)
    except Exception:
        return row

    # ── Company name ──────────────────────────────────────────────────────────
    name_el = tree.xpath('//h1[@class="h2 shrink-text"] | //h1[contains(@class,"company-name")]')
    if name_el:
        row["Name"] = name_el[0].text_content().strip()

    # ── Classification from breadcrumbs / company info ────────────────────────
    for a in tree.xpath('//div[contains(@class,"company-info")]//a | //div[@id="company-info"]//a'):
        href = a.get("href", "")
        text = a.text_content().strip()
        if "/industries/" in href:
            row.setdefault("Industry", text)
        elif "/sectors/" in href:
            row.setdefault("Sector", text)

    # ── Top-ratios box ─────────────────────────────────────────────────────────
    top = _top_ratios(tree)
    # Map known top-ratio labels
    ratio_map = {
        "Stock P/E":            "P/E",
        "P/E":                  "P/E",
        "Price to Earning":     "P/E",
        "Book Value":           "_book_value",  # need price to compute P/B
        "Dividend Yield":       "_div_yield",
        "ROCE":                 "ROCE 3Years",
        "ROE":                  "_roe",
        "Face Value":           "_face_value",
        "Market Cap":           "_market_cap_cr",
        "Current Price":        "Current Price",
        "52 Week High":         "_52w_high",
        "52 Week Low":          "_52w_low",
        "Debt to equity":       "Debt to equity",
        "Debt / Equity":        "Debt to equity",
        "Interest Coverage":    "Interest Coverage",
        "Intrinsic Value":      "Intrinsic Value",
    }
    for src, dst in ratio_map.items():
        for k, v in top.items():
            if src.lower() in k.lower():
                row[dst] = _f(v) if _f(v) is not None else v
                break

    # Current Price from the price element
    price_el = tree.xpath('//div[@id="company-info"]//span[@class="number"][1] | //*[@id="current-price"]')
    if price_el and "Current Price" not in row:
        row["Current Price"] = _f(price_el[0].text_content())

    # Compute P/B from Price and Book Value
    cp = _f(row.get("Current Price"))
    bv = _f(row.get("_book_value"))
    if cp and bv and bv > 0:
        row["Price to Book value"] = round(cp / bv, 2)

    # 52-week hi/lo → Drawdown Recovery
    hi = _f(row.get("_52w_high"))
    lo = _f(row.get("_52w_low"))
    if cp and hi and hi > 0:
        row["1 Year Return"] = round((cp / hi - 1) * 100, 1)  # distance from 52w high as proxy
    if cp and lo and lo > 0:
        row["Drawdown Recovery"] = round((cp / lo - 1) * 100, 1)

    # ── Profit & Loss table ───────────────────────────────────────────────────
    pl_tables = tree.xpath('//section[@id="profit-loss"]//table')
    if pl_tables:
        pl = _table_to_dict(pl_tables[0])

        # Headers for year labels
        pl_headers = [th.text_content().strip() for th in pl_tables[0].xpath('.//thead/tr/th')]

        # Sales / Revenue
        sales_row = _find_row(pl, "Sales", "Revenue", "Net Sales", "Total Revenue")
        if sales_row and len(sales_row) >= 4:
            s = [_f(v) for v in sales_row]
            s_valid = [v for v in s if v is not None]
            if len(s_valid) >= 4:
                # 3Y CAGR
                oldest = s_valid[-4] if len(s_valid) >= 4 else None
                newest = s_valid[-1]
                if oldest and oldest > 0 and newest:
                    row["Sales growth 3Years"] = round(((newest / oldest) ** (1/3) - 1) * 100, 1)
                # YoY
                if len(s_valid) >= 2 and s_valid[-2] > 0:
                    row["Sales growth"] = round((s_valid[-1] / s_valid[-2] - 1) * 100, 1)

        # Net Profit
        pat_row = _find_row(pl, "Net Profit", "PAT", "Profit after tax")
        if pat_row:
            p = [_f(v) for v in pat_row]
            p_valid = [v for v in p if v is not None]
            if len(p_valid) >= 4:
                oldest = p_valid[-4]
                newest = p_valid[-1]
                if oldest and abs(oldest) > 0 and newest:
                    row["Profit growth 3Years"] = round(((newest / oldest) ** (1/3) - 1) * 100, 1)
            if len(p_valid) >= 2 and abs(p_valid[-2]) > 0:
                row["Profit growth"] = round((p_valid[-1] / p_valid[-2] - 1) * 100, 1)

        # OPM %
        opm_row = _find_row(pl, "OPM %", "EBITDA Margin", "Operating Profit Margin", "OPM")
        if opm_row:
            opm_vals = [_f(v) for v in opm_row]
            opm_valid = [v for v in opm_vals if v is not None]
            if opm_valid:
                row["OPM"] = opm_valid[-1]
                slope = _linear_trend(opm_row)
                if slope is not None:
                    row["Margin Trend"] = round(slope, 2)

        # ROA (if directly present)
        roa_row = _find_row(pl, "ROA", "Return on Assets")
        if roa_row:
            roa_vals = [_f(v) for v in roa_row if _f(v) is not None]
            if roa_vals:
                row["ROA"] = roa_vals[-1]

        # EPS / 5Y price CAGR proxy from EPS
        eps_row = _find_row(pl, "EPS in Rs", "EPS", "Basic EPS")
        if eps_row:
            eps_vals = [_f(v) for v in eps_row if _f(v) is not None]
            if len(eps_vals) >= 5 and abs(eps_vals[0]) > 0 and eps_vals[-1]:
                row["5 Year CAGR"] = round(((eps_vals[-1] / eps_vals[0]) ** 0.2 - 1) * 100, 1)

        # Dividend payout
        div_row = _find_row(pl, "Dividend Payout %", "Dividend %")
        if div_row:
            div_vals = [_f(v) for v in div_row if _f(v) is not None]
            # used internally if needed

        # Growth stability from 5Y sales CAGR consistency
        if sales_row:
            row["Growth Stability"] = _pct_positive(sales_row[-5:])

    # ── Balance Sheet ─────────────────────────────────────────────────────────
    bs_tables = tree.xpath('//section[@id="balance-sheet"]//table')
    if bs_tables:
        bs = _table_to_dict(bs_tables[0])

        borrow_row = _find_row(bs, "Borrowings", "Total Debt", "Long-term borrowings")
        equity_row = _find_row(bs, "Equity Capital", "Total Equity", "Shareholders Equity")

        if borrow_row and equity_row and "Debt to equity" not in row:
            b = _f(borrow_row[-1]) if borrow_row else None
            e = _f(equity_row[-1]) if equity_row else None
            if b is not None and e and e > 0:
                row["Debt to equity"] = round(b / e, 2)

    # ── Cash Flow ─────────────────────────────────────────────────────────────
    cf_tables = tree.xpath('//section[@id="cash-flow"]//table')
    if cf_tables:
        cf = _table_to_dict(cf_tables[0])

        cfo_row = _find_row(cf, "Cash from Operating Activity", "Operating Cash Flow", "CFO")
        inv_row = _find_row(cf, "Cash from Investing Activity", "Investing Cash Flow")
        pat_row_cf = _find_row(cf, "Net Profit", "PAT")

        if cfo_row:
            cfo_vals = [_f(v) for v in cfo_row if _f(v) is not None]
            if inv_row:
                inv_vals = [_f(v) for v in inv_row if _f(v) is not None]
                # FCF = CFO + Investing (investing is typically negative capex)
                fcf_pairs = list(zip(cfo_vals[-5:], inv_vals[-5:])) if len(inv_vals) >= 2 else []
                if fcf_pairs:
                    row["FCF Consistency"] = round(
                        sum(1 for c, i in fcf_pairs if c + i > 0) / len(fcf_pairs) * 100, 1
                    )
                    # FCF Yield proxy: latest FCF / Market Cap
                    mc = _f(row.get("_market_cap_cr"))
                    if mc and mc > 0 and fcf_pairs:
                        latest_fcf = fcf_pairs[-1][0] + fcf_pairs[-1][1]
                        row["FCF Yield"] = round(latest_fcf / mc * 100, 2)

            # CFO/PAT from last available year
            if pat_row_cf:
                pat_cf_vals = [_f(v) for v in pat_row_cf if _f(v) is not None]
            else:
                pat_cf_vals = []

            if cfo_vals and pat_cf_vals:
                cfo_last = cfo_vals[-1]
                pat_last = pat_cf_vals[-1]
                if pat_last and pat_last != 0:
                    row["CFO/PAT"] = round(cfo_last / pat_last, 2)

    # ── Quarters table ────────────────────────────────────────────────────────
    q_tables = tree.xpath('//section[@id="quarters"]//table')
    if q_tables:
        q = _table_to_dict(q_tables[0])

        # RSI proxy from quarterly price changes (not available on screener pages)
        # Skip — RSI is better from price data

        # Latest OPM if not already set from P&L
        if "OPM" not in row:
            opm_q = _find_row(q, "OPM %", "OPM")
            if opm_q:
                vals = [_f(v) for v in opm_q if _f(v) is not None]
                if vals:
                    row["OPM"] = vals[0]

    # ── ROCE from ratios / data section ───────────────────────────────────────
    roce_el = tree.xpath(
        '//*[contains(text(),"ROCE")]/../following-sibling::*[1] | '
        '//*[contains(@class,"roce")]'
    )
    if "ROCE 3Years" not in row and roce_el:
        for el in roce_el:
            v = _f(el.text_content())
            if v is not None:
                row["ROCE 3Years"] = v
                break

    # ── Shareholding — Promoter Pledge ───────────────────────────────────────
    shp_tables = tree.xpath('//section[@id="shareholding"]//table')
    if shp_tables:
        shp = _table_to_dict(shp_tables[0])
        pledge_row = _find_row(shp, "Pledged percentage", "Pledged %", "Pledge")
        if pledge_row:
            pledge_vals = [_f(v) for v in pledge_row if _f(v) is not None]
            if pledge_vals:
                row["Pledged percentage"] = pledge_vals[0]

    # ── Bank-specific — GNPA/NIM/CAR (from key metrics section) ──────────────
    for li in tree.xpath('//section[@id="peer-comparison"]//li | //ul[contains(@class,"data-table")]//li'):
        label = li.xpath('.//span[@class="name"]')
        val = li.xpath('.//span[@class="number"]')
        if label and val:
            lbl = label[0].text_content().strip()
            v = _f(val[0].text_content())
            if "NIM" in lbl:
                row["NIM"] = v
            elif "GNPA" in lbl or "Gross NPA" in lbl:
                row["GNPA %"] = v
            elif "NNPA" in lbl or "Net NPA" in lbl:
                row["NNPA %"] = v
            elif "CAR" in lbl or "Capital Adequacy" in lbl:
                row["CAR %"] = v

    # ── Intrinsic value (shown as "Intrinsic Value" in top box) ──────────────
    if "Intrinsic Value" not in row:
        iv_el = tree.xpath('//*[contains(text(),"Intrinsic Value")]/..//span[@class="number"]')
        if iv_el:
            row["Intrinsic Value"] = _f(iv_el[0].text_content())

    # Clean internal temp keys
    for k in ["_book_value", "_div_yield", "_roe", "_face_value", "_market_cap_cr", "_52w_high", "_52w_low"]:
        row.pop(k, None)

    return row


# ──────────────────────────────────────────────────────────────────────────────
# Load universe symbols
# ──────────────────────────────────────────────────────────────────────────────
def load_symbols(universe_csv: Path) -> list[str]:
    df = pd.read_csv(universe_csv)
    for col in ["NSE Symbol", "Symbol", "Ticker", "SYMBOL"]:
        if col in df.columns:
            return df[col].dropna().astype(str).str.strip().tolist()
    raise ValueError(f"Cannot find symbol column in {universe_csv}")


# ──────────────────────────────────────────────────────────────────────────────
# Cache helpers  (one JSON per symbol, in data/raw/fundamentals/screener/cache/)
# ──────────────────────────────────────────────────────────────────────────────
def cache_path(symbol: str, cache_dir: Path) -> Path:
    safe = symbol.replace("/", "_").replace("\\", "_")
    return cache_dir / f"{safe}.json"


def load_cache(symbol: str, cache_dir: Path) -> Optional[dict]:
    p = cache_path(symbol, cache_dir)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return None


def save_cache(symbol: str, data: dict, cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path(symbol, cache_dir).write_text(json.dumps(data, default=str))


# ──────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────────────────────────────────────
def run(
    symbols: list[str],
    output_csv: Path,
    delay: float,
    limit: int,
    workers: int,
    cache_dir: Path,
    force_refresh: bool,
) -> None:
    if limit > 0:
        symbols = symbols[:limit]
        print(f"Limiting to {limit} symbols")

    print(f"Fetching {len(symbols)} stocks from screener.in ...")
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    results = []
    total = len(symbols)
    done = 0
    failed = 0

    # ── Single-threaded mode with delay ──────────────────────────────────────
    if workers <= 1:
        session = make_session()
        for i, symbol in enumerate(symbols, 1):
            # Try cache first
            if not force_refresh:
                cached = load_cache(symbol, cache_dir)
                if cached:
                    results.append(cached)
                    done += 1
                    print(f"  [{i}/{total}] {symbol:20s} (cached)", flush=True)
                    continue

            try:
                data = scrape_stock(symbol, session)
                save_cache(symbol, data, cache_dir)
                results.append(data)
                done += 1
                status = "ok" if len(data) > 3 else "partial"
                print(f"  [{i}/{total}] {symbol:20s} {status} ({len(data)} fields)", flush=True)
            except Exception as e:
                failed += 1
                print(f"  [{i}/{total}] {symbol:20s} FAILED: {e}", flush=True)
                results.append({"NSE Symbol": symbol})

            if i < total:
                time.sleep(delay + random.uniform(0, delay * 0.3))

    # ── Multi-threaded mode (lower delay, but easier to get blocked) ──────────
    else:
        session_pool = [make_session() for _ in range(workers)]

        def fetch_worker(args):
            idx, symbol = args
            if not force_refresh:
                cached = load_cache(symbol, cache_dir)
                if cached:
                    return idx, symbol, cached, True
            sess = session_pool[idx % workers]
            data = scrape_stock(symbol, sess)
            save_cache(symbol, data, cache_dir)
            time.sleep(delay + random.uniform(0, delay * 0.5))
            return idx, symbol, data, False

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_worker, (i, s)): s for i, s in enumerate(symbols)}
            for fut in as_completed(futures):
                try:
                    idx, sym, data, from_cache = fut.result()
                    results.append(data)
                    done += 1
                    src = "cached" if from_cache else f"{len(data)} fields"
                    print(f"  [{done}/{total}] {sym:20s} ({src})", flush=True)
                except Exception as e:
                    sym = futures[fut]
                    failed += 1
                    results.append({"NSE Symbol": sym})
                    print(f"  FAILED: {sym}: {e}", flush=True)

    # ── Build output DataFrame ────────────────────────────────────────────────
    df = pd.DataFrame(results)
    # Ensure all output columns exist
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[OUTPUT_COLUMNS]

    # Sort by NSE Symbol
    df = df.sort_values("NSE Symbol").reset_index(drop=True)

    df.to_csv(output_csv, index=False)
    print(f"\nDone. {done} ok, {failed} failed.")
    print(f"Output: {output_csv}  ({len(df)} rows, {len(df.columns)} columns)")

    # Coverage summary
    core_cols = ["P/E", "Price to Book value", "Sales growth 3Years", "Profit growth 3Years",
                 "ROCE 3Years", "OPM", "Current Price", "Debt to equity"]
    print("\nCoverage (non-null) for key columns:")
    for col in core_cols:
        if col in df.columns:
            pct = df[col].notna().mean() * 100
            print(f"  {col:30s}  {pct:5.1f}%")


def parse_args():
    p = argparse.ArgumentParser(description="Scrape fundamentals from screener.in")
    p.add_argument("--universe", required=True, help="Universe CSV with NSE symbols")
    p.add_argument("--output", required=True, help="Output CSV path")
    p.add_argument("--date", default=date.today().isoformat())
    p.add_argument("--delay", type=float, default=1.5,
                   help="Seconds between requests (default 1.5; be respectful)")
    p.add_argument("--limit", type=int, default=0,
                   help="Max symbols to fetch (0=all). Use 50 for testing.")
    p.add_argument("--workers", type=int, default=1,
                   help="Parallel threads (default 1; keep ≤3 to avoid blocks)")
    p.add_argument("--cache-dir", default=None,
                   help="Cache directory (default: data/raw/fundamentals/screener/cache/)")
    p.add_argument("--force-refresh", action="store_true",
                   help="Ignore cache and re-scrape everything")
    return p.parse_args()


def main():
    args = parse_args()
    universe = Path(args.universe)
    output = Path(args.output)
    cache_dir = Path(args.cache_dir) if args.cache_dir else \
        Path("data/raw/fundamentals/screener/cache")

    if not universe.exists():
        print(f"ERROR: Universe file not found: {universe}")
        sys.exit(1)

    symbols = load_symbols(universe)
    print(f"Universe: {len(symbols)} symbols from {universe}")

    run(
        symbols=symbols,
        output_csv=output,
        delay=args.delay,
        limit=args.limit,
        workers=args.workers,
        cache_dir=cache_dir,
        force_refresh=args.force_refresh,
    )


if __name__ == "__main__":
    main()
