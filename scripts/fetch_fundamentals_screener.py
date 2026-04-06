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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd

from engine.metric_definitions import (
    compute_cagr_3y,
    compute_drawdown_recovery,
    compute_fcf_consistency,
    compute_growth_stability,
    compute_margin_trend,
    compute_yoy_growth,
)

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
    "ROCE 3Years", "OPM", "ROA", "ROE",
    "Pledged percentage",
    "1 Year Return", "6 Month Return", "5 Year CAGR",
    "Relative Strength", "Drawdown Recovery",
    "Forward Growth", "Current Price", "Intrinsic Value",
    "Book Value Per Share", "EPS TTM", "EPS FY0", "EPS FY1", "EPS FY2",
    "Dividend Yield", "Current Ratio", "Current Ratio Prev Year",
    "Gross Block", "Gross Block 3Y Ago",
    "Asset Turnover", "Asset Turnover Prev Year",
    "Cash from Operations", "ROA Prev Year",
    "Promoter Holding %", "Promoter Holding Prev %", "DII %",
    "RSI", "Price vs 200 DMA", "Price vs 50 DMA",
    "Delivery Score", "RS Turn", "Volatility Compression",
    "Debt to equity", "D/E Prev Year", "Interest Coverage",
    "Credit Rating Grade", "Avg Daily Turnover Cr",
    "ASM Stage", "GSM Stage",
    # Bank-specific extras (empty for non-banks)
    "NIM", "GNPA %", "NNPA %", "CAR %", "PCR %",
    "Advances Growth", "Deposit Growth", "NII Growth", "Fee Income Growth", "Earnings Growth",
    "AUM Growth", "Cost to Income", "Credit Cost", "Slippage Ratio",
    "Margin Trend", "CFO/PAT", "FCF Consistency", "Growth Stability",
]

CACHE_SCHEMA_VERSION = 3


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
            # Normalize: screener.in appends '\xa0+' footnote markers to many row labels
            key = cells[0].strip().replace("\xa0", " ").rstrip(" +").strip()
            rows[key] = cells[1:]
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


def _annual_values(values: list, headers: list[str]) -> list[Optional[float]]:
    """Extract annual values from a list of values and corresponding headers."""
    data_headers = headers[1:1 + len(values)] if headers else []
    annual = []
    for idx, value in enumerate(values):
        header = data_headers[idx] if idx < len(data_headers) else ""
        if header and "TTM" in header.upper():
            continue
        annual.append(_f(value))
    return annual


def _latest_yoy_pct(values: list[Optional[float]]) -> Optional[float]:
    valid = [v for v in values if v is not None]
    if len(valid) < 2:
        return None
    growth = compute_yoy_growth(valid[-1], valid[-2])
    if growth is None:
        return None
    return round(growth * 100.0, 1)


def _quarterly_ttm_yoy_pct(values: list) -> Optional[float]:
    """Compute YoY growth from quarterly TTM values."""
    parsed = [_f(v) for v in values[:8]]
    if len(parsed) < 8 or any(v is None for v in parsed):
        return None
    growth = compute_yoy_growth(sum(parsed[:4]), sum(parsed[4:8]))
    if growth is None:
        return None
    return round(growth * 100.0, 1)


def _quarterly_spot_yoy_pct(values: list) -> Optional[float]:
    """Compute YoY growth from quarterly spot values."""
    parsed = [_f(v) for v in values[:5]]
    if len(parsed) < 5:
        return None
    latest = parsed[0]
    year_ago = parsed[4]
    if latest is None or year_ago is None or year_ago <= 0:
        return None
    growth = compute_yoy_growth(latest, year_ago)
    if growth is None:
        return None
    return round(growth * 100.0, 1)


# ──────────────────────────────────────────────────────────────────────────────
# Core scraper — single stock
# ──────────────────────────────────────────────────────────────────────────────
def scrape_stock(symbol: str, session: requests.Session) -> dict:
    """
    Scrape all available metrics from Screener.in for one symbol.
    Returns a flat dict with OUTPUT_COLUMNS as keys.
    """
    row: dict = {"NSE Symbol": symbol}

    # Try consolidated first, then standalone; retry on 429/503
    resp = None
    for url in [
        f"https://www.screener.in/company/{quote(symbol, safe='')}/consolidated/",
        f"https://www.screener.in/company/{quote(symbol, safe='')}/",
    ]:
        for attempt in range(4):
            try:
                r = session.get(url, timeout=20)
                if r.status_code == 200:
                    resp = r
                    break
                if r.status_code in (429, 503):
                    backoff = (2 ** attempt) * 5 + random.uniform(0, 2)
                    time.sleep(backoff)
                    continue
                break  # 404 or other — no point retrying this URL
            except Exception:
                break
        if resp is not None:
            break
    if resp is None:
        return row

    try:
        tree = lxml_html.fromstring(resp.content)
    except Exception:
        return row

    # ── Company name ──────────────────────────────────────────────────────────
    name_el = tree.xpath('//h1[@class="h2 shrink-text"] | //h1[contains(@class,"company-name")]')
    if name_el:
        row["Name"] = name_el[0].text_content().strip()

    # ── Classification — screener.in uses title= attributes on /market/ links
    for title_attr, col in [
        ("Broad Sector", "Macro Sector"),
        ("Sector", "Sector"),
        ("Broad Industry", "Basic Industry"),
        ("Industry", "Industry"),
    ]:
        els = tree.xpath(f'//a[@title="{title_attr}"]')
        if els:
            row[col] = els[0].text_content().strip()

    # ── Top-ratios box ─────────────────────────────────────────────────────────
    top = _top_ratios(tree)
    # Map known top-ratio labels
    ratio_map = {
        "Stock P/E":            "P/E",
        "P/E":                  "P/E",
        "Price to Earning":     "P/E",
        "EV / EBITDA":         "EV / EBITDA",
        "EV/EBITDA":           "EV / EBITDA",
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
        row["Book Value Per Share"] = round(bv, 2)
    div_yield_val = _f(row.get("_div_yield"))
    if div_yield_val is not None:
        row["Dividend Yield"] = div_yield_val
    roe_val = _f(row.get("_roe"))
    if roe_val is not None:
        row["ROE"] = roe_val
    pe_val = _f(row.get("P/E"))
    if cp and pe_val and pe_val > 0 and "EPS TTM" not in row:
        row["EPS TTM"] = round(cp / pe_val, 2)

    # 52-week hi/lo → Drawdown Recovery
    hi = _f(row.get("_52w_high"))
    lo = _f(row.get("_52w_low"))
    if cp is not None and hi is not None and lo is not None:
        drawdown = compute_drawdown_recovery(cp, hi, lo)
        if drawdown is not None:
            row["Drawdown Recovery"] = round(drawdown, 1)

    # ── Profit & Loss table ───────────────────────────────────────────────────
    pl_tables = tree.xpath('//section[@id="profit-loss"]//table')
    if pl_tables:
        pl = _table_to_dict(pl_tables[0])

        # Headers for year labels
        pl_headers = [th.text_content().strip() for th in pl_tables[0].xpath('.//thead/tr/th')]
        valid_sales: list[float] = []
        valid_pat: list[float] = []

        # Sales / Revenue
        sales_row = _find_row(pl, "Sales", "Revenue", "Net Sales", "Total Revenue")
        if sales_row and len(sales_row) >= 4:
            annual_sales = _annual_values(sales_row, pl_headers)
            valid_sales = [v for v in annual_sales if v is not None]
            if valid_sales:
                row["_sales_fy0"] = valid_sales[-1]
            if len(valid_sales) >= 2:
                row["_sales_fy1"] = valid_sales[-2]
            if len(valid_sales) >= 4:
                sales_cagr = compute_cagr_3y(valid_sales[-1], valid_sales[-4])
                if sales_cagr is not None:
                    row["Sales growth 3Years"] = round(sales_cagr * 100.0, 1)
            if len(valid_sales) >= 5:
                stability = compute_growth_stability(valid_sales[-5:])
                if stability is not None:
                    row["Growth Stability"] = round(stability, 1)

        expense_row = _find_row(pl, "Expenses", "Total Expenses", "Operating Expenses")
        if expense_row and "Cost to Income" not in row:
            annual_expenses = _annual_values(expense_row, pl_headers)
            valid_expenses = [v for v in annual_expenses if v is not None]
            if valid_expenses and valid_sales and valid_sales[-1] > 0:
                row["Cost to Income"] = round(valid_expenses[-1] / valid_sales[-1] * 100.0, 2)

        financing_margin_row = _find_row(pl, "Financing Margin %", "NIM", "Net Interest Margin")
        if financing_margin_row and "NIM" not in row:
            annual_nim = _annual_values(financing_margin_row, pl_headers)
            valid_nim = [v for v in annual_nim if v is not None]
            if valid_nim:
                row["NIM"] = round(valid_nim[-1], 2)

        # Net Profit
        pat_row = _find_row(pl, "Net Profit", "PAT", "Profit after tax")
        if pat_row:
            annual_pat = _annual_values(pat_row, pl_headers)
            valid_pat = [v for v in annual_pat if v is not None]
            if valid_pat:
                row["_pat_fy0"] = valid_pat[-1]
            if len(valid_pat) >= 2:
                row["_pat_fy1"] = valid_pat[-2]
            if len(valid_pat) >= 4:
                pat_cagr = compute_cagr_3y(valid_pat[-1], valid_pat[-4])
                if pat_cagr is not None:
                    row["Profit growth 3Years"] = round(pat_cagr * 100.0, 1)

        # OPM %
        opm_row = _find_row(pl, "OPM %", "EBITDA Margin", "Operating Profit Margin", "OPM")
        if opm_row:
            annual_opm = _annual_values(opm_row, pl_headers)
            opm_valid = [v for v in annual_opm if v is not None]
            if opm_valid:
                row["OPM"] = opm_valid[-1]
                slope = compute_margin_trend(opm_valid[-3:])
                if slope is not None:
                    row["Margin Trend"] = round(slope, 2)
            if len(opm_valid) >= 2:
                row["OPM FY1"] = opm_valid[-2]
            if len(opm_valid) >= 3:
                row["OPM FY2"] = opm_valid[-3]

        # ROA (if directly present)
        roa_row = _find_row(pl, "ROA", "Return on Assets")
        if roa_row:
            roa_vals = [_f(v) for v in roa_row if _f(v) is not None]
            if roa_vals:
                row["ROA"] = roa_vals[-1]
            if len(roa_vals) >= 2:
                row["ROA Prev Year"] = roa_vals[-2]

        # EPS / 5Y price CAGR proxy from EPS
        eps_row = _find_row(pl, "EPS in Rs", "EPS", "Basic EPS")
        if eps_row:
            header_vals = pl_headers[1:1 + len(eps_row)] if pl_headers else []
            annual_eps = []
            for idx, value in enumerate(eps_row):
                header = header_vals[idx] if idx < len(header_vals) else ""
                parsed = _f(value)
                if header and "TTM" in header.upper():
                    if parsed is not None:
                        row["EPS TTM"] = parsed
                    continue
                annual_eps.append(parsed)
            annual_eps = [v for v in annual_eps if v is not None]
            if annual_eps:
                row["EPS FY0"] = annual_eps[-1]
            if len(annual_eps) >= 2:
                row["EPS FY1"] = annual_eps[-2]
            if len(annual_eps) >= 3:
                row["EPS FY2"] = annual_eps[-3]

        # Dividend payout
        div_row = _find_row(pl, "Dividend Payout %", "Dividend %")
        if div_row:
            div_vals = [_f(v) for v in div_row if _f(v) is not None]

    # ── Balance Sheet ─────────────────────────────────────────────────────────
    bs_tables = tree.xpath('//section[@id="balance-sheet"]//table')
    if bs_tables:
        bs = _table_to_dict(bs_tables[0])
        bs_headers = [th.text_content().strip() for th in bs_tables[0].xpath('.//thead/tr/th')]

        borrow_row = _find_row(bs, "Borrowings", "Total Debt", "Long-term borrowings")
        equity_row = _find_row(bs, "Equity Capital", "Total Equity", "Shareholders Equity")

        if borrow_row and equity_row and "Debt to equity" not in row:
            b = _f(borrow_row[-1]) if borrow_row else None
            e = _f(equity_row[-1]) if equity_row else None
            if b is not None and e and e > 0:
                row["Debt to equity"] = round(b / e, 2)
            # D/E previous year for Piotroski F5
            if len(borrow_row) >= 2 and len(equity_row) >= 2:
                b1 = _f(borrow_row[-2]) if len(borrow_row) >= 2 else None
                e1 = _f(equity_row[-2]) if len(equity_row) >= 2 else None
                if b1 is not None and e1 is not None and e1 > 0:
                    row["D/E Prev Year"] = round(b1 / e1, 2)

        # Current assets & liabilities → Current Ratio
        ca_row = _find_row(bs, "Other Assets", "Current Assets", "Total Current Assets")
        cl_row = _find_row(bs, "Other Liabilities", "Current Liabilities", "Total Current Liabilities")
        if ca_row and cl_row:
            ca = _f(ca_row[-1])
            cl = _f(cl_row[-1])
            if ca is not None and cl is not None and cl > 0:
                row["Current Ratio"] = round(ca / cl, 2)
            if len(ca_row) >= 2 and len(cl_row) >= 2:
                ca1 = _f(ca_row[-2])
                cl1 = _f(cl_row[-2])
                if ca1 is not None and cl1 is not None and cl1 > 0:
                    row["Current Ratio Prev Year"] = round(ca1 / cl1, 2)

        # Gross Block (FY0 and FY3 for Operating Leverage Score)
        gb_row = _find_row(bs, "Fixed Assets", "Gross Block", "Net Fixed Assets")
        if gb_row:
            gb_annual = _annual_values(gb_row, bs_headers)
            gb_valid = [v for v in gb_annual if v is not None]
            if gb_valid:
                row["Gross Block"] = gb_valid[-1]
            if len(gb_valid) >= 4:
                row["Gross Block 3Y Ago"] = gb_valid[-4]

        # Asset Turnover = Revenue / Total Assets
        ta_row = _find_row(bs, "Total Assets", "Balance Sheet Total")
        if ta_row:
            ta_annual = _annual_values(ta_row, bs_headers)
            ta_vals = [v for v in ta_annual if v is not None]
            sales_fy0 = _f(row.get("_sales_fy0"))
            sales_fy1 = _f(row.get("_sales_fy1"))
            if ta_vals and sales_fy0 and ta_vals[-1] and ta_vals[-1] > 0:
                row["Asset Turnover"] = round(sales_fy0 / ta_vals[-1], 2)
            if len(ta_vals) >= 2 and sales_fy1 and ta_vals[-2] and ta_vals[-2] > 0:
                row["Asset Turnover Prev Year"] = round(sales_fy1 / ta_vals[-2], 2)
            pat_fy0 = _f(row.get("_pat_fy0"))
            pat_fy1 = _f(row.get("_pat_fy1"))
            if pat_fy0 is not None and ta_vals and ta_vals[-1] > 0 and "ROA" not in row:
                row["ROA"] = round(pat_fy0 / ta_vals[-1] * 100.0, 2)
            if pat_fy1 is not None and len(ta_vals) >= 2 and ta_vals[-2] > 0 and "ROA Prev Year" not in row:
                row["ROA Prev Year"] = round(pat_fy1 / ta_vals[-2] * 100.0, 2)

        deposits_row = _find_row(bs, "Deposits", "Total Deposits")
        if deposits_row and "Deposit Growth" not in row:
            deposits_annual = _annual_values(deposits_row, bs_headers)
            deposit_growth = _latest_yoy_pct(deposits_annual)
            if deposit_growth is not None:
                row["Deposit Growth"] = deposit_growth

        aum_row = _find_row(bs, "AUM", "Assets Under Management")
        if aum_row and "AUM Growth" not in row:
            aum_annual = _annual_values(aum_row, bs_headers)
            aum_growth = _latest_yoy_pct(aum_annual)
            if aum_growth is not None:
                row["AUM Growth"] = aum_growth

    # ── Cash Flow ─────────────────────────────────────────────────────────────
    cf_tables = tree.xpath('//section[@id="cash-flow"]//table')
    if cf_tables:
        cf = _table_to_dict(cf_tables[0])

        cfo_row = _find_row(cf, "Cash from Operating Activity", "Operating Cash Flow", "CFO")
        inv_row = _find_row(cf, "Cash from Investing Activity", "Investing Cash Flow")
        pat_row_cf = _find_row(cf, "Net Profit", "PAT")

        cfo_vals = []
        if cfo_row:
            cfo_vals = [_f(v) for v in cfo_row if _f(v) is not None]
            if inv_row:
                inv_vals = [_f(v) for v in inv_row if _f(v) is not None]
                fcf_annual = [c + i for c, i in zip(cfo_vals[-5:], inv_vals[-5:])]
                fcf_consistency = compute_fcf_consistency(fcf_annual)
                if fcf_consistency is not None:
                    row["FCF Consistency"] = round(fcf_consistency, 1)
                mc = _f(row.get("_market_cap_cr"))
                if mc and mc > 0 and fcf_annual:
                    latest_fcf = fcf_annual[-1]
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
            row["Cash from Operations"] = cfo_last

    # ── Quarters table ────────────────────────────────────────────────────────
    q_tables = tree.xpath('//section[@id="quarters"]//table')
    if q_tables:
        q = _table_to_dict(q_tables[0])

        sales_q = _find_row(q, "Sales", "Revenue", "Net Sales", "Total Revenue")
        if sales_q:
            sales_growth = _quarterly_ttm_yoy_pct(sales_q)
            if sales_growth is not None:
                row["Sales growth"] = sales_growth

        profit_q = _find_row(q, "Net Profit", "PAT", "Profit after tax")
        if profit_q:
            profit_growth = _quarterly_ttm_yoy_pct(profit_q)
            if profit_growth is not None:
                row["Profit growth"] = profit_growth
                row["Earnings Growth"] = profit_growth

        nii_q = _find_row(q, "Net Interest Income", "NII")
        if nii_q:
            nii_growth = _quarterly_ttm_yoy_pct(nii_q)
            if nii_growth is not None:
                row["NII Growth"] = nii_growth

        fee_q = _find_row(q, "Fee Income", "Non Interest Income", "Other Income")
        if fee_q:
            fee_growth = _quarterly_ttm_yoy_pct(fee_q)
            if fee_growth is not None:
                row["Fee Income Growth"] = fee_growth

        advances_q = _find_row(q, "Advances", "Gross Advances", "Loan Book")
        if advances_q:
            advances_growth = _quarterly_spot_yoy_pct(advances_q)
            if advances_growth is not None:
                row["Advances Growth"] = advances_growth

        deposits_q = _find_row(q, "Deposits", "Total Deposits")
        if deposits_q:
            deposit_growth = _quarterly_spot_yoy_pct(deposits_q)
            if deposit_growth is not None:
                row["Deposit Growth"] = deposit_growth

        aum_q = _find_row(q, "AUM", "Assets Under Management")
        if aum_q:
            aum_growth = _quarterly_spot_yoy_pct(aum_q)
            if aum_growth is not None:
                row["AUM Growth"] = aum_growth

        # RSI proxy from quarterly price changes (not available on screener pages)
        # Skip — RSI is better from price data

        # Latest OPM if not already set from P&L
        if "OPM" not in row:
            opm_q = _find_row(q, "OPM %", "OPM")
            if opm_q:
                vals = [_f(v) for v in opm_q if _f(v) is not None]
                if vals:
                    row["OPM"] = vals[0]

    # ── Shareholding — Promoter/DII holding QoQ + Pledge ─────────────────────
    shp_tables = tree.xpath('//section[@id="shareholding"]//table')
    if shp_tables:
        shp = _table_to_dict(shp_tables[0])
        pledge_row = _find_row(shp, "Pledged percentage", "Pledged %", "Pledge")
        if pledge_row:
            pledge_vals = [_f(v) for v in pledge_row if _f(v) is not None]
            if pledge_vals:
                row["Pledged percentage"] = pledge_vals[0]

        promoter_row = _find_row(shp, "Promoters", "Promoter", "Promoter & PAC")
        if promoter_row:
            p_vals = [_f(v) for v in promoter_row if _f(v) is not None]
            if p_vals:
                row["Promoter Holding %"] = p_vals[0]
            if len(p_vals) >= 2:
                row["Promoter Holding Prev %"] = p_vals[1]

        dii_row = _find_row(shp, "DII", "Domestic Institutions", "Domestic Institutional")
        if dii_row:
            d_vals = [_f(v) for v in dii_row if _f(v) is not None]
            if d_vals:
                row["DII %"] = d_vals[0]

        # Dividend Yield from key ratios or ratios table
        for li in tree.xpath(
            '//section[@id="top-ratios"]//li | //div[contains(@class,"top-ratios")]//li'
        ):
            lbl_el = li.xpath('.//span[@class="name"]')
            val_el = li.xpath('.//span[@class="number"]')
            if lbl_el and val_el:
                lbl = lbl_el[0].text_content().strip()
                if "Dividend Yield" in lbl or "Div Yield" in lbl:
                    dv = _f(val_el[0].text_content())
                    if dv is not None:
                        row["Dividend Yield"] = dv

    # ── Bank-specific — GNPA/NIM/CAR (from key metrics section) ──────────────
    for li in tree.xpath('//section[@id="peer-comparison"]//li | //ul[contains(@class,"data-table")]//li'):
        label = li.xpath('.//span[@class="name"]')
        val = li.xpath('.//span[@class="number"]')
        if label and val:
            lbl = label[0].text_content().strip()
            v = _f(val[0].text_content())
            if "NIM" in lbl:
                row["NIM"] = v
            elif "ROE" in lbl:
                row["ROE"] = v
            elif "GNPA" in lbl or "Gross NPA" in lbl:
                row["GNPA %"] = v
            elif "NNPA" in lbl or "Net NPA" in lbl:
                row["NNPA %"] = v
            elif "CAR" in lbl or "Capital Adequacy" in lbl:
                row["CAR %"] = v
            elif "PCR" in lbl or "Provision Coverage" in lbl:
                row["PCR %"] = v
            elif "Cost to Income" in lbl or "Cost/Income" in lbl:
                row["Cost to Income"] = v
            elif "Credit Cost" in lbl:
                row["Credit Cost"] = v
            elif "Slippage" in lbl:
                row["Slippage Ratio"] = v
            elif "Advances Growth" in lbl or "Loan Book Growth" in lbl:
                row["Advances Growth"] = v
            elif "Deposit Growth" in lbl:
                row["Deposit Growth"] = v
            elif "NII Growth" in lbl or "Net Interest Income Growth" in lbl:
                row["NII Growth"] = v
            elif "Fee Income Growth" in lbl or "Non Interest Income Growth" in lbl:
                row["Fee Income Growth"] = v
            elif "AUM Growth" in lbl:
                row["AUM Growth"] = v
            elif "Earnings Growth" in lbl:
                row["Earnings Growth"] = v

    # ── Intrinsic value (shown as "Intrinsic Value" in top box) ──────────────
    if "Intrinsic Value" not in row:
        iv_el = tree.xpath('//*[contains(text(),"Intrinsic Value")]/..//span[@class="number"]')
        if iv_el:
            row["Intrinsic Value"] = _f(iv_el[0].text_content())

    # Clean internal temp keys
    for k in [
        "_book_value",
        "_div_yield",
        "_roe",
        "_face_value",
        "_market_cap_cr",
        "_52w_high",
        "_52w_low",
        "_pat_fy0",
        "_pat_fy1",
    ]:
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
            payload = json.loads(p.read_text())
            if not isinstance(payload, dict):
                return None
            if payload.get("__cache_schema_version") != CACHE_SCHEMA_VERSION:
                return None
            data = payload.get("data")
            return data if isinstance(data, dict) else None
        except Exception:
            return None
    return None


def save_cache(symbol: str, data: dict, cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path(symbol, cache_dir).write_text(
        json.dumps({"__cache_schema_version": CACHE_SCHEMA_VERSION, "data": data}, default=str)
    )


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

    # ── Multi-threaded mode ───────────────────────────────────────────────────
    else:
        _local = threading.local()

        def fetch_worker(args):
            idx, symbol = args
            # First call per thread: create a dedicated session and stagger startup
            # so workers don't all fire at t=0 (slot 0 starts immediately,
            # slot 1 waits delay/workers, slot 2 waits 2*delay/workers, etc.)
            if not hasattr(_local, "session"):
                slot = idx % workers
                _local.session = make_session()
                if slot > 0:
                    time.sleep(slot * delay / workers)

            if not force_refresh:
                cached = load_cache(symbol, cache_dir)
                if cached:
                    return idx, symbol, cached, True

            data = scrape_stock(symbol, _local.session)
            save_cache(symbol, data, cache_dir)
            time.sleep(delay + random.uniform(0, delay * 0.3))
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
