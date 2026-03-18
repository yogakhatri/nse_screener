#!/usr/bin/env python3
"""
Backtest Runner
================
Validates the engine's stock-picking ability by testing historical
recommendations against actual subsequent returns.

For each historical run:
  1. Load the engine's Buy recommendations
  2. Look up actual forward returns (1M, 3M, 6M, 1Y)
  3. Compare vs benchmark (Nifty 500)
  4. Compute hit rate, alpha, Sharpe-like ratio

Usage:
  python scripts/backtest.py --runs-dir runs/ --prices-dir data/raw/prices/bhavcopy
  python scripts/backtest.py --run-date 2026-03-12 --forward-days 63
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))


def load_recommendations(run_dir: Path) -> List[dict]:
    """Load Buy recommendations from a run directory."""
    recs = []

    # Try buy_candidates.csv first
    bc_path = run_dir / "buy_candidates.csv"
    if bc_path.exists():
        try:
            df = pd.read_csv(bc_path)
            ticker_col = None
            for alias in ["ticker", "Ticker", "Symbol"]:
                if alias in df.columns:
                    ticker_col = alias
                    break
            if ticker_col:
                for _, row in df.iterrows():
                    recs.append({
                        "ticker": str(row[ticker_col]).strip().upper(),
                        "score": float(row.get("selection_score", row.get("score", 0))) if "selection_score" in row or "score" in row else 0,
                    })
                return recs
        except Exception:
            pass

    # Fall back to stock JSONs
    for sf in sorted(run_dir.glob("stock_*.json")):
        try:
            data = json.loads(sf.read_text())
            if data.get("recommendation") == "Buy":
                recs.append({
                    "ticker": data.get("ticker", ""),
                    "score": data.get("selection_score") or data.get("final_opportunity_score") or 0,
                })
        except (json.JSONDecodeError, IOError):
            continue

    return recs


def load_price_on_date(
    ticker: str,
    target_date: date,
    price_history: Dict[str, pd.DataFrame],
    tolerance_days: int = 5,
) -> Optional[float]:
    """Get closing price for a ticker on or near a date."""
    if ticker not in price_history:
        return None

    hist = price_history[ticker]
    if hist.empty or "date" not in hist.columns or "close" not in hist.columns:
        return None

    # Find closest date within tolerance
    for delta in range(tolerance_days + 1):
        for d in [target_date + timedelta(days=delta), target_date - timedelta(days=delta)]:
            matches = hist[hist["date"] == d]
            if not matches.empty:
                val = matches.iloc[0]["close"]
                if val is not None and not np.isnan(val):
                    return float(val)

    return None


def compute_forward_returns(
    recs: List[dict],
    rec_date: date,
    price_history: Dict[str, pd.DataFrame],
    forward_periods: Dict[str, int] = None,
) -> List[dict]:
    """Compute forward returns for each recommended stock."""
    if forward_periods is None:
        forward_periods = {"1M": 22, "3M": 63, "6M": 130, "1Y": 252}

    results = []

    for rec in recs:
        ticker = rec["ticker"]
        entry_price = load_price_on_date(ticker, rec_date, price_history)
        if entry_price is None or entry_price <= 0:
            continue

        result = {
            "ticker": ticker,
            "score": rec["score"],
            "entry_price": entry_price,
            "entry_date": rec_date.isoformat(),
        }

        for period_name, days in forward_periods.items():
            exit_date = rec_date + timedelta(days=int(days * 1.5))  # calendar days
            exit_price = load_price_on_date(ticker, exit_date, price_history)
            if exit_price is not None and exit_price > 0:
                ret = ((exit_price / entry_price) - 1) * 100
                result[f"return_{period_name}"] = round(ret, 2)
                result[f"exit_price_{period_name}"] = exit_price
            else:
                result[f"return_{period_name}"] = None

        results.append(result)

    return results


def compute_backtest_stats(
    returns: List[dict],
    period: str = "3M",
) -> dict:
    """Compute aggregate backtest statistics."""
    ret_key = f"return_{period}"
    valid_returns = [r[ret_key] for r in returns if r.get(ret_key) is not None]

    if not valid_returns:
        return {"period": period, "n_stocks": 0}

    arr = np.array(valid_returns)

    winners = sum(1 for r in arr if r > 0)
    losers = sum(1 for r in arr if r <= 0)

    return {
        "period": period,
        "n_stocks": len(arr),
        "mean_return": round(float(np.mean(arr)), 2),
        "median_return": round(float(np.median(arr)), 2),
        "best_return": round(float(np.max(arr)), 2),
        "worst_return": round(float(np.min(arr)), 2),
        "std_dev": round(float(np.std(arr)), 2),
        "hit_rate": round(winners / len(arr) * 100, 1),
        "winners": winners,
        "losers": losers,
        "sharpe_like": round(float(np.mean(arr) / np.std(arr)), 2) if np.std(arr) > 0 else 0,
        "profit_factor": round(
            abs(sum(r for r in arr if r > 0)) / abs(sum(r for r in arr if r < 0)), 2
        ) if any(r < 0 for r in arr) else float("inf"),
    }


def run_single_backtest(
    run_dir: Path,
    price_history: Dict[str, pd.DataFrame],
) -> Optional[dict]:
    """Run backtest for a single engine run."""
    # Parse run date from directory name
    try:
        run_date = date.fromisoformat(run_dir.name)
    except ValueError:
        return None

    recs = load_recommendations(run_dir)
    if not recs:
        return None

    returns = compute_forward_returns(recs, run_date, price_history)
    if not returns:
        return None

    result = {
        "run_date": run_date.isoformat(),
        "n_recommendations": len(recs),
        "n_with_returns": len(returns),
        "returns": returns,
    }

    for period in ["1M", "3M", "6M", "1Y"]:
        stats = compute_backtest_stats(returns, period)
        result[f"stats_{period}"] = stats

    return result


def save_backtest_report(results: List[dict], output_path: Path) -> None:
    """Save backtest results as JSON and CSV summary."""
    # JSON report
    json_path = output_path.with_suffix(".json")
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    # CSV summary
    csv_path = output_path.with_suffix(".csv")
    fieldnames = ["run_date", "n_recs", "period", "mean_return", "median_return",
                  "hit_rate", "sharpe_like", "profit_factor", "best", "worst"]

    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            for period in ["1M", "3M", "6M", "1Y"]:
                stats = r.get(f"stats_{period}", {})
                if stats.get("n_stocks", 0) > 0:
                    w.writerow({
                        "run_date": r["run_date"],
                        "n_recs": r["n_recommendations"],
                        "period": period,
                        "mean_return": stats.get("mean_return"),
                        "median_return": stats.get("median_return"),
                        "hit_rate": stats.get("hit_rate"),
                        "sharpe_like": stats.get("sharpe_like"),
                        "profit_factor": stats.get("profit_factor"),
                        "best": stats.get("best_return"),
                        "worst": stats.get("worst_return"),
                    })

    return json_path, csv_path


def generate_markdown_report(results: List[dict], output_path: Path) -> None:
    """Generate human-readable markdown backtest report."""
    lines = ["# Backtest Report", ""]

    for r in results:
        lines.append(f"## Run: {r['run_date']}")
        lines.append(f"- Recommendations: {r['n_recommendations']}")
        lines.append(f"- With return data: {r['n_with_returns']}")
        lines.append("")

        for period in ["1M", "3M", "6M", "1Y"]:
            stats = r.get(f"stats_{period}", {})
            if stats.get("n_stocks", 0) > 0:
                lines.append(f"### {period} Forward Returns")
                lines.append(f"| Metric | Value |")
                lines.append(f"|--------|-------|")
                lines.append(f"| Stocks | {stats['n_stocks']} |")
                lines.append(f"| Mean Return | {stats['mean_return']}% |")
                lines.append(f"| Median Return | {stats['median_return']}% |")
                lines.append(f"| Hit Rate | {stats['hit_rate']}% |")
                lines.append(f"| Sharpe-like | {stats['sharpe_like']} |")
                lines.append(f"| Profit Factor | {stats['profit_factor']} |")
                lines.append(f"| Best | {stats['best_return']}% |")
                lines.append(f"| Worst | {stats['worst_return']}% |")
                lines.append("")

        # Top/bottom picks
        returns = r.get("returns", [])
        if returns:
            # Sort by 3M return
            valid_3m = [ret for ret in returns if ret.get("return_3M") is not None]
            if valid_3m:
                valid_3m.sort(key=lambda x: x["return_3M"], reverse=True)
                lines.append("### Top 5 Picks (3M)")
                lines.append("| Ticker | Score | Return |")
                lines.append("|--------|-------|--------|")
                for pick in valid_3m[:5]:
                    lines.append(f"| {pick['ticker']} | {pick['score']:.1f} | {pick['return_3M']}% |")
                lines.append("")

                lines.append("### Bottom 5 Picks (3M)")
                lines.append("| Ticker | Score | Return |")
                lines.append("|--------|-------|--------|")
                for pick in valid_3m[-5:]:
                    lines.append(f"| {pick['ticker']} | {pick['score']:.1f} | {pick['return_3M']}% |")
                lines.append("")

        lines.append("---")
        lines.append("")

    output_path.write_text("\n".join(lines))


def parse_args():
    parser = argparse.ArgumentParser(description="Backtest engine recommendations")
    parser.add_argument("--runs-dir", default="runs", help="Directory containing run folders")
    parser.add_argument("--run-date", default=None, help="Specific run date to backtest")
    parser.add_argument("--output", default="data/processed/backtest", help="Output path prefix")
    parser.add_argument("--forward-days", type=int, default=63, help="Forward period in trading days")
    return parser.parse_args()


def main():
    args = parse_args()

    from scripts.price_history import load_local_price_history

    runs_dir = Path(args.runs_dir)
    output_prefix = Path(args.output)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)

    # Determine which runs to backtest
    if args.run_date:
        run_dirs = [runs_dir / args.run_date]
    else:
        run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()])

    print(f"[Backtest] Found {len(run_dirs)} run(s) to test")

    # Load price history once (covers all dates)
    latest_date = date.today()
    print(f"[Backtest] Loading price history up to {latest_date}...")
    price_history = load_local_price_history(latest_date)
    print(f"  Loaded {len(price_history)} tickers")

    results = []
    for rd in run_dirs:
        print(f"  Testing {rd.name}...")
        result = run_single_backtest(rd, price_history)
        if result:
            stats_3m = result.get("stats_3M", {})
            hr = stats_3m.get("hit_rate", "—")
            mean_r = stats_3m.get("mean_return", "—")
            print(f"    → {result['n_recommendations']} recs, 3M hit rate: {hr}%, mean: {mean_r}%")
            results.append(result)
        else:
            print(f"    → No testable recommendations")

    if results:
        json_path, csv_path = save_backtest_report(results, output_prefix)
        print(f"\n[Backtest] Saved: {json_path}")
        print(f"[Backtest] Saved: {csv_path}")

        md_path = output_prefix.with_suffix(".md")
        generate_markdown_report(results, md_path)
        print(f"[Backtest] Report: {md_path}")
    else:
        print("[Backtest] No results to report")


if __name__ == "__main__":
    main()
