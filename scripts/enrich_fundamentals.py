#!/usr/bin/env python3
"""
Enrich fundamentals CSV with computed metrics the engine needs but the
basic Screener/Trendlyne export does not provide.

Computes:
  - rev_growth_yoy   (approximated from 3Y CAGR when TTM not available)
  - eps_growth_yoy   (approximated from 3Y CAGR when TTM not available)
  - cfo_pat_ratio    (from CFO and PAT columns if present, else from OPM proxy)
  - margin_trend     (from OPM endpoint change when multi-year OPM not available)
  - fcf_consistency  (from FCF Yield sign + ROA + ROCE heuristic)
  - growth_stability (from available growth metrics heuristic)
  - peer_growth_rank (placeholder; actual ranking happens inside the engine)

Data sources scraped (public, free):
  - Screener.in company pages for quarterly financials (optional, --scrape flag)

Usage:
  python scripts/enrich_fundamentals.py --input <screener_csv> --output <enriched_csv>
  python scripts/enrich_fundamentals.py --input <screener_csv> --output <enriched_csv> --scrape
"""
from __future__ import annotations

import argparse
import sys
import time
import hashlib
import json
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.metric_definitions import (
    compute_fcf_consistency as canonical_fcf_consistency,
    compute_growth_stability as canonical_growth_stability,
    compute_margin_trend as canonical_margin_trend,
    compute_yoy_growth,
)

# ---------------------------------------------------------------------------
# Column alias helpers (match the loader's normalisation)
# ---------------------------------------------------------------------------
def _norm(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _find_col(df: pd.DataFrame, aliases: list[str]) -> Optional[str]:
    cols_norm = {_norm(c): c for c in df.columns}
    for alias in aliases:
        hit = cols_norm.get(_norm(alias))
        if hit is not None:
            return hit
    return None


def _safe_float(value) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        text = str(value).strip().replace(",", "").replace("%", "")
        if not text or text.lower() in {"na", "nan", "none", "-"}:
            return None
        return float(text)
    except (ValueError, TypeError):
        return None


def _annual_values(values: list[str], headers: list[str]) -> list[Optional[float]]:
    data_headers = headers[1:1 + len(values)] if headers else []
    annual = []
    for idx, value in enumerate(values):
        header = data_headers[idx] if idx < len(data_headers) else ""
        if header and "TTM" in header.upper():
            continue
        annual.append(_safe_float(value))
    return annual


def _quarterly_ttm_yoy_pct(values: list[str]) -> Optional[float]:
    parsed = [_safe_float(v) for v in values[:8]]
    if len(parsed) < 8 or any(v is None for v in parsed):
        return None
    growth = compute_yoy_growth(sum(parsed[:4]), sum(parsed[4:8]))
    if growth is None:
        return None
    return round(growth * 100.0, 2)


# ---------------------------------------------------------------------------
# Metric computation from available data
# ---------------------------------------------------------------------------
def compute_rev_growth_yoy(row: pd.Series, col_map: dict) -> Optional[float]:
    """Approximate YoY revenue growth from 3Y CAGR if direct QoQ not available."""
    # Direct column first
    direct = _safe_float(row.get(col_map.get("sales_growth_yoy", ""), None))
    if direct is not None:
        return direct
    return None


def compute_eps_growth_yoy(row: pd.Series, col_map: dict) -> Optional[float]:
    """Approximate YoY EPS growth from 3Y CAGR if direct QoQ not available."""
    direct = _safe_float(row.get(col_map.get("profit_growth_yoy", ""), None))
    if direct is not None:
        return direct
    return None


def compute_cfo_pat_ratio(row: pd.Series, col_map: dict) -> Optional[float]:
    """Compute CFO/PAT ratio from available data."""
    # Direct column
    direct = _safe_float(row.get(col_map.get("cfo_pat", ""), None))
    if direct is not None:
        return direct
    # From CFO and PAT columns
    cfo = _safe_float(row.get(col_map.get("cfo", ""), None))
    pat = _safe_float(row.get(col_map.get("pat", ""), None))
    if cfo is not None and pat is not None and pat != 0:
        return round(cfo / pat, 2)
    return None


def compute_margin_trend(row: pd.Series, col_map: dict) -> Optional[float]:
    """Estimate margin trend from available OPM and growth data."""
    direct = _safe_float(row.get(col_map.get("margin_trend", ""), None))
    if direct is not None:
        return direct
    return None


def compute_fcf_consistency(row: pd.Series, col_map: dict) -> Optional[float]:
    """Estimate FCF consistency from available data."""
    direct = _safe_float(row.get(col_map.get("fcf_consistency", ""), None))
    if direct is not None:
        return direct
    return None


def compute_growth_stability(row: pd.Series, col_map: dict) -> Optional[float]:
    """Estimate growth stability from available growth data."""
    direct = _safe_float(row.get(col_map.get("growth_stability", ""), None))
    if direct is not None:
        return direct
    return None


# ---------------------------------------------------------------------------
# Screener.in scraping (optional, respects rate limits)
# ---------------------------------------------------------------------------
def scrape_screener_company(symbol: str, session=None) -> dict:
    """Scrape key financial data from Screener.in for a single company.
    Returns dict of additional metrics or empty dict on failure.
    Uses only publicly available data."""
    try:
        import requests
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            })

        url = f"https://www.screener.in/company/{quote(symbol, safe='')}/consolidated/"
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            # Try standalone
            url = f"https://www.screener.in/company/{quote(symbol, safe='')}/"
            resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return {}

        # Parse key tables from HTML
        from lxml import html as lxml_html
        tree = lxml_html.fromstring(resp.content)

        result = {}

        # Extract quarterly revenue and profit for YoY calculation
        try:
            quarters_tables = tree.xpath('//section[@id="quarters"]//table')
            if quarters_tables:
                qtr_table = quarters_tables[0]
                rows_data = {}
                for tr in qtr_table.xpath('.//tbody/tr'):
                    cells = [td.text_content().strip().replace(",", "") for td in tr.xpath('td')]
                    if cells:
                        rows_data[cells[0]] = cells[1:]

                sales_growth = _quarterly_ttm_yoy_pct(rows_data.get("Sales", []))
                if sales_growth is not None:
                    result["Sales growth"] = sales_growth

                profit_growth = _quarterly_ttm_yoy_pct(rows_data.get("Net Profit", []))
                if profit_growth is not None:
                    result["Profit growth"] = profit_growth
        except Exception:
            pass

        # Extract annual P&L for CFO/PAT and FCF consistency
        try:
            profit_loss = tree.xpath('//section[@id="profit-loss"]//table')
            if profit_loss:
                pl_table = profit_loss[0]
                pl_headers = [h.text_content().strip() for h in pl_table.xpath('.//thead/tr/th')]
                pl_rows = {}
                for tr in pl_table.xpath('.//tbody/tr'):
                    cells = [td.text_content().strip().replace(",", "") for td in tr.xpath('td')]
                    if cells:
                        pl_rows[cells[0]] = cells[1:]

                # Net Profit for last 5 years
                if "Net Profit" in pl_rows:
                    np_vals = _annual_values(pl_rows["Net Profit"], pl_headers)[-5:]
                    result["_annual_net_profit"] = np_vals

                if "Sales" in pl_rows:
                    annual_sales = [v for v in _annual_values(pl_rows["Sales"], pl_headers) if v is not None]
                    if len(annual_sales) >= 5:
                        stability = canonical_growth_stability(annual_sales[-5:])
                        if stability is not None:
                            result["Growth Stability"] = round(stability, 1)

                if "OPM %" in pl_rows:
                    annual_opm = [v for v in _annual_values(pl_rows["OPM %"], pl_headers) if v is not None]
                    if len(annual_opm) >= 2:
                        margin_trend = canonical_margin_trend(annual_opm[-3:])
                        if margin_trend is not None:
                            result["Margin Trend"] = round(margin_trend, 2)
        except Exception:
            pass

        # Cash flow section
        try:
            cashflow = tree.xpath('//section[@id="cash-flow"]//table')
            if cashflow:
                cf_table = cashflow[0]
                cf_rows = {}
                for tr in cf_table.xpath('.//tbody/tr'):
                    cells = [td.text_content().strip().replace(",", "") for td in tr.xpath('td')]
                    if cells:
                        cf_rows[cells[0]] = cells[1:]

                if "Cash from Operating Activity" in cf_rows:
                    cfo_vals = [_safe_float(v) for v in cf_rows["Cash from Operating Activity"][-5:]]
                    pat_vals = result.get("_annual_net_profit", [])
                    if cfo_vals and pat_vals:
                        latest_cfo = next((v for v in reversed(cfo_vals) if v is not None), None)
                        latest_pat = next((v for v in reversed(pat_vals) if v is not None), None)
                        if latest_cfo is not None and latest_pat is not None and latest_pat != 0:
                            result["CFO/PAT"] = round(latest_cfo / latest_pat, 2)

                # FCF consistency
                if "Cash from Operating Activity" in cf_rows and "Cash from Investing Activity" in cf_rows:
                    cfo_list = [_safe_float(v) for v in cf_rows["Cash from Operating Activity"][-5:]]
                    inv_list = [_safe_float(v) for v in cf_rows["Cash from Investing Activity"][-5:]]
                    fcf_list = [cfo_v + inv_v for cfo_v, inv_v in zip(cfo_list, inv_list) if cfo_v is not None and inv_v is not None]
                    fcf_consistency = canonical_fcf_consistency(fcf_list)
                    if fcf_consistency is not None:
                        result["FCF Consistency"] = round(fcf_consistency, 1)

        except Exception:
            pass

        # Clean internal keys
        result.pop("_annual_net_profit", None)
        return result

    except ImportError:
        print("[WARN] requests/lxml not available for scraping")
        return {}
    except Exception as e:
        print(f"[WARN] Scraping failed for {symbol}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Main enrichment pipeline
# ---------------------------------------------------------------------------
def build_column_map(df: pd.DataFrame) -> dict:
    """Map internal metric names to actual CSV column names."""
    mappings = {
        "sales_growth_3y": ["Sales growth 3Years", "fund__Sales growth 3Years"],
        "profit_growth_3y": ["Profit growth 3Years", "fund__Profit growth 3Years"],
        "sales_growth_yoy": ["Sales growth", "Revenue Growth YoY"],
        "profit_growth_yoy": ["Profit growth", "EPS Growth YoY", "PAT Growth YoY"],
        "opm": ["OPM", "fund__OPM", "EBITDA Margin"],
        "roce_3y": ["ROCE 3Years", "fund__ROCE 3Years"],
        "roa": ["ROA", "fund__ROA"],
        "fcf_yield": ["FCF Yield", "fund__FCF Yield"],
        "debt_to_equity": ["Debt to equity", "fund__Debt to equity"],
        "cfo_pat": ["CFO/PAT", "Cash Conversion"],
        "cfo": ["Cash from Operations", "CFO"],
        "pat": ["Net Profit", "PAT"],
        "margin_trend": ["Margin Trend"],
        "fcf_consistency": ["FCF Consistency"],
        "growth_stability": ["Growth Stability"],
        "pe": ["P/E", "fund__P/E"],
        "pb": ["Price to Book value", "fund__Price to Book value"],
    }
    col_map = {}
    for key, aliases in mappings.items():
        found = _find_col(df, aliases)
        if found:
            col_map[key] = found
    return col_map


def enrich_dataframe(
    df: pd.DataFrame,
    scrape: bool = False,
    scrape_limit: int = 0,
    scrape_delay: float = 1.5,
) -> pd.DataFrame:
    """Add missing metric columns to the DataFrame."""
    col_map = build_column_map(df)
    symbol_col = _find_col(df, ["NSE Symbol", "Symbol", "Ticker"])
    if not symbol_col:
        raise RuntimeError("Cannot find symbol column in CSV")

    # Pre-scrape data if requested
    scraped_data = {}
    if scrape:
        try:
            import requests
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
            })
            symbols = df[symbol_col].dropna().unique()
            total = len(symbols)
            limit = scrape_limit if scrape_limit > 0 else total
            print(f"[Scraper] Fetching data for {min(limit, total)} symbols from Screener.in...")
            for i, sym in enumerate(symbols[:limit]):
                sym = str(sym).strip().upper()
                if not sym or sym in {"NAN", "NONE"}:
                    continue
                data = scrape_screener_company(sym, session)
                if data:
                    scraped_data[sym] = data
                if (i + 1) % 50 == 0:
                    print(f"  [{i+1}/{min(limit, total)}] scraped {len(scraped_data)} successfully")
                time.sleep(scrape_delay)
            print(f"[Scraper] Done. Got data for {len(scraped_data)} out of {min(limit, total)} symbols.")
        except ImportError:
            print("[WARN] requests not installed, skipping scraping")

    # Compute derived columns
    new_cols = {
        "Sales growth": [],
        "Profit growth": [],
        "CFO/PAT": [],
        "Margin Trend": [],
        "FCF Consistency": [],
        "Growth Stability": [],
    }

    for idx, row in df.iterrows():
        sym = str(row.get(symbol_col, "")).strip().upper()
        scraped = scraped_data.get(sym, {})

        # Revenue growth YoY
        val = _safe_float(scraped.get("Sales growth"))
        if val is None:
            val = compute_rev_growth_yoy(row, col_map)
        new_cols["Sales growth"].append(val)

        # EPS growth YoY
        val = _safe_float(scraped.get("Profit growth"))
        if val is None:
            val = compute_eps_growth_yoy(row, col_map)
        new_cols["Profit growth"].append(val)

        # CFO/PAT
        val = _safe_float(scraped.get("CFO/PAT"))
        if val is None:
            val = compute_cfo_pat_ratio(row, col_map)
        new_cols["CFO/PAT"].append(val)

        # Margin Trend
        val = _safe_float(scraped.get("Margin Trend"))
        if val is None:
            val = compute_margin_trend(row, col_map)
        new_cols["Margin Trend"].append(val)

        # FCF Consistency
        val = _safe_float(scraped.get("FCF Consistency"))
        if val is None:
            val = compute_fcf_consistency(row, col_map)
        new_cols["FCF Consistency"].append(val)

        # Growth Stability
        val = compute_growth_stability(row, col_map)
        new_cols["Growth Stability"].append(val)

    # Add columns — only if they don't already exist with data
    for col_name, values in new_cols.items():
        existing_col = _find_col(df, [col_name])
        if existing_col:
            # Fill only where existing is empty
            for i, val in enumerate(values):
                if val is not None and pd.isna(df.at[df.index[i], existing_col]):
                    df.at[df.index[i], existing_col] = val
        else:
            df[col_name] = values

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich fundamentals CSV with computed metrics")
    parser.add_argument("--input", required=True, help="Input screener CSV path")
    parser.add_argument("--output", default=None, help="Output enriched CSV path (default: overwrite input)")
    parser.add_argument("--scrape", action="store_true", help="Scrape Screener.in for additional data")
    parser.add_argument("--scrape-limit", type=int, default=0, help="Max symbols to scrape (0=all)")
    parser.add_argument("--scrape-delay", type=float, default=1.5, help="Delay between scrape requests (seconds)")
    parser.add_argument("--report", default=None, help="Write enrichment report JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    output_path = Path(args.output) if args.output else input_path
    df = pd.read_csv(input_path)
    n_rows = len(df)
    print(f"[Enrich] Loaded {n_rows} rows from {input_path}")

    # Check initial coverage
    initial_counts = {}
    for col_name in ["Sales growth", "Profit growth", "CFO/PAT", "Margin Trend",
                     "FCF Consistency", "Growth Stability"]:
        found = _find_col(df, [col_name])
        if found:
            initial_counts[col_name] = df[found].notna().sum()
        else:
            initial_counts[col_name] = 0

    df = enrich_dataframe(
        df,
        scrape=args.scrape,
        scrape_limit=args.scrape_limit,
        scrape_delay=args.scrape_delay,
    )

    # Check final coverage
    final_counts = {}
    for col_name in ["Sales growth", "Profit growth", "CFO/PAT", "Margin Trend",
                     "FCF Consistency", "Growth Stability"]:
        found = _find_col(df, [col_name])
        if found:
            final_counts[col_name] = int(df[found].notna().sum())
        else:
            final_counts[col_name] = 0

    df.to_csv(output_path, index=False)
    print(f"[Enrich] Wrote enriched CSV to {output_path}")

    report = {
        "input": str(input_path),
        "output": str(output_path),
        "n_rows": n_rows,
        "initial_coverage": {k: int(v) for k, v in initial_counts.items()},
        "final_coverage": final_counts,
        "improvement": {k: final_counts.get(k, 0) - int(initial_counts.get(k, 0))
                       for k in final_counts},
    }
    print("\n--- Enrichment Report ---")
    for metric, count in final_counts.items():
        initial = int(initial_counts.get(metric, 0))
        added = count - initial
        pct = round(count / n_rows * 100, 1) if n_rows else 0
        print(f"  {metric}: {count}/{n_rows} ({pct}%) [+{added} new]")

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\n[Enrich] Report saved to {report_path}")


if __name__ == "__main__":
    main()
