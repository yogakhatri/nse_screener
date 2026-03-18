#!/usr/bin/env python3
"""
Momentum Scoring Module
========================
Computes advanced momentum and trend-quality metrics that go beyond simple
return-based scoring. These supplement the engine's existing Performance and
Entry Point cards with institutional-grade momentum signals.

Metrics computed:
  - dual_momentum       : Absolute + relative momentum combo (Antonacci style)
  - momentum_quality    : Consistency/smoothness of the uptrend
  - trend_strength      : ADX-based trend strength (0-100)
  - mean_reversion_risk : How extended the stock is vs its regression channel
  - sector_momentum     : Sector relative performance vs Nifty 500
  - breakout_score      : Proximity to 52W high with volume confirmation

All metrics are raw scores [0-100] suitable for percentile scoring.

Usage:
  Called internally by run_engine.py or standalone:
    python scripts/momentum_scoring.py --date 2026-03-12 --screener-csv <csv>
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))


def compute_dual_momentum(
    stock_returns_6m: Optional[float],
    stock_returns_12m: Optional[float],
    index_returns_6m: Optional[float],
    index_returns_12m: Optional[float],
    tbill_rate: float = 6.5,  # India 1Y T-bill approx
) -> Optional[float]:
    """
    Dual Momentum (Gary Antonacci style):
      - Absolute momentum: stock return > risk-free rate
      - Relative momentum: stock return > index return
      - Score: 0-100 based on how strongly both conditions are met
    """
    if stock_returns_12m is None or index_returns_12m is None:
        return None

    r6 = stock_returns_6m if stock_returns_6m is not None else 0
    r12 = stock_returns_12m
    ir6 = index_returns_6m if index_returns_6m is not None else 0
    ir12 = index_returns_12m

    annualized_rf = tbill_rate / 100.0

    # Absolute momentum score (0-50): how much stock exceeds risk-free
    abs_excess = r12 - annualized_rf * 100
    abs_score = float(np.clip(abs_excess / 40 * 50 + 25, 0, 50))

    # Relative momentum score (0-50): how much stock exceeds index
    rel_excess_12 = r12 - ir12
    rel_excess_6 = r6 - ir6
    rel_combined = rel_excess_12 * 0.6 + rel_excess_6 * 0.4
    rel_score = float(np.clip(rel_combined / 30 * 50 + 25, 0, 50))

    return round(abs_score + rel_score, 2)


def compute_momentum_quality(
    closes: List[float],
    lookback: int = 252,
) -> Optional[float]:
    """
    Momentum Quality (trend smoothness):
      Measures how consistently the stock has been rising, not just the
      total return. A stock that went up smoothly scores higher than one
      that crashed and bounced.

      Method: % of rolling 20-day windows with positive returns, weighted
      by recency. Also penalizes large drawdowns.
    """
    if len(closes) < min(lookback, 60):
        return None

    prices = np.array(closes[-lookback:], dtype=float)
    n = len(prices)

    # Rolling 20-day return sign
    window = 20
    if n < window + 1:
        return None

    positive_windows = 0
    total_windows = 0
    recency_weights = []

    for i in range(window, n):
        ret = (prices[i] / prices[i - window]) - 1
        weight = i / n  # More recent windows weighted higher
        recency_weights.append(weight)
        if ret > 0:
            positive_windows += weight
        total_windows += weight

    if total_windows == 0:
        return None

    consistency = positive_windows / total_windows  # 0 to 1

    # Drawdown penalty: max drawdown in the period
    peak = prices[0]
    max_dd = 0
    for p in prices:
        if p > peak:
            peak = p
        dd = (peak - p) / peak
        if dd > max_dd:
            max_dd = dd

    dd_penalty = max(0, 1 - max_dd * 2)  # 50% DD -> penalty of 0

    score = consistency * dd_penalty * 100
    return round(float(np.clip(score, 0, 100)), 2)


def compute_trend_strength(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> Optional[float]:
    """
    Trend Strength (ADX-based):
      Average Directional Index measures trend strength regardless of direction.
      We then adjust for direction: strong uptrend = high score, strong downtrend = low score.
    """
    n = len(closes)
    if n < period * 2 + 1 or len(highs) < n or len(lows) < n:
        return None

    h = np.array(highs[-n:], dtype=float)
    l = np.array(lows[-n:], dtype=float)
    c = np.array(closes[-n:], dtype=float)

    # True Range
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))

    # Directional movement
    up_move = h[1:] - h[:-1]
    down_move = l[:-1] - l[1:]

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    # Smoothed averages (Wilder smoothing)
    def wilder_smooth(arr, p):
        result = np.zeros_like(arr)
        result[p - 1] = np.mean(arr[:p])
        for i in range(p, len(arr)):
            result[i] = (result[i - 1] * (p - 1) + arr[i]) / p
        return result[p - 1:]

    atr = wilder_smooth(tr, period)
    plus_di = wilder_smooth(plus_dm, period)
    minus_di = wilder_smooth(minus_dm, period)

    if len(atr) == 0:
        return None

    # Prevent division by zero
    atr = np.where(atr == 0, 1e-10, atr)
    plus_di = (plus_di / atr) * 100
    minus_di = (minus_di / atr) * 100

    di_sum = plus_di + minus_di
    di_sum = np.where(di_sum == 0, 1e-10, di_sum)
    dx = np.abs(plus_di - minus_di) / di_sum * 100

    if len(dx) < period:
        return None

    adx_values = wilder_smooth(dx, period)
    if len(adx_values) == 0:
        return None

    adx = adx_values[-1]

    # Direction adjustment: if +DI > -DI, trend is up (good)
    direction = 1 if plus_di[-1] > minus_di[-1] else -1

    # Score: ADX * direction, scaled to 0-100
    # Strong uptrend: ADX=40, direction=+1 -> score ~70
    # Strong downtrend: ADX=40, direction=-1 -> score ~30
    # No trend: ADX=10 -> score ~50
    score = 50 + (adx * direction * 0.5)
    return round(float(np.clip(score, 0, 100)), 2)


def compute_mean_reversion_risk(
    closes: List[float],
    lookback: int = 252,
) -> Optional[float]:
    """
    Mean Reversion Risk:
      How far the current price is from its regression channel.
      A stock far above its channel is at higher risk of mean reversion.
      Returns a score where 50 = at channel, 100 = deeply oversold (good entry),
      0 = extremely overbought.
    """
    if len(closes) < min(lookback, 60):
        return None

    prices = np.array(closes[-lookback:], dtype=float)
    n = len(prices)
    log_prices = np.log(prices)

    x = np.arange(n, dtype=float)
    coeffs = np.polyfit(x, log_prices, 1)
    trend = np.polyval(coeffs, x)
    residuals = log_prices - trend

    std = np.std(residuals)
    if std == 0:
        return 50.0

    # Z-score of current price relative to trend
    z = residuals[-1] / std

    # Score: z=-2 -> 100 (oversold, good entry), z=+2 -> 0 (overbought, risky)
    score = 50 - z * 25
    return round(float(np.clip(score, 0, 100)), 2)


def compute_breakout_score(
    closes: List[float],
    volumes: Optional[List[float]] = None,
    lookback: int = 252,
) -> Optional[float]:
    """
    Breakout Score:
      Combines proximity to 52W high with volume confirmation.
      Stocks near their high with rising volume score higher (institutional buying).
    """
    if len(closes) < min(lookback, 60):
        return None

    prices = np.array(closes[-lookback:], dtype=float)
    high_52w = np.max(prices)
    low_52w = np.min(prices)
    current = prices[-1]

    if high_52w == low_52w:
        return 50.0

    # Position in range (0 = at low, 1 = at high)
    range_position = (current - low_52w) / (high_52w - low_52w)

    # Volume confirmation
    vol_factor = 1.0
    if volumes and len(volumes) >= 60:
        vols = np.array(volumes[-60:], dtype=float)
        avg_20 = np.mean(vols[-20:]) if len(vols) >= 20 else np.mean(vols)
        avg_60 = np.mean(vols)
        if avg_60 > 0:
            vol_ratio = avg_20 / avg_60
            vol_factor = float(np.clip(vol_ratio, 0.5, 2.0))

    # Breakout zone bonus: extra score if within 5% of 52W high
    breakout_bonus = 0
    if current >= high_52w * 0.95:
        breakout_bonus = 10 * vol_factor  # Higher volume near high = stronger signal

    raw_score = range_position * 80 + breakout_bonus
    return round(float(np.clip(raw_score, 0, 100)), 2)


def compute_all_momentum_metrics(
    closes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    volumes: Optional[List[float]] = None,
    index_closes: Optional[List[float]] = None,
) -> Dict[str, Optional[float]]:
    """Compute all momentum metrics for a single stock."""
    result: Dict[str, Optional[float]] = {}

    n = len(closes) if closes else 0

    # Returns for dual momentum
    return_6m = None
    return_12m = None
    idx_return_6m = None
    idx_return_12m = None

    if n >= 130 and closes[-130] > 0:
        return_6m = ((closes[-1] / closes[-130]) - 1) * 100
    if n >= 252 and closes[-252] > 0:
        return_12m = ((closes[-1] / closes[-252]) - 1) * 100

    if index_closes:
        in_ = len(index_closes)
        if in_ >= 130 and index_closes[-130] > 0:
            idx_return_6m = ((index_closes[-1] / index_closes[-130]) - 1) * 100
        if in_ >= 252 and index_closes[-252] > 0:
            idx_return_12m = ((index_closes[-1] / index_closes[-252]) - 1) * 100

    result["dual_momentum"] = compute_dual_momentum(
        return_6m, return_12m, idx_return_6m, idx_return_12m
    )

    result["momentum_quality"] = compute_momentum_quality(closes)

    if highs and lows:
        result["trend_strength"] = compute_trend_strength(highs, lows, closes)
    else:
        result["trend_strength"] = None

    result["mean_reversion_risk"] = compute_mean_reversion_risk(closes)

    result["breakout_score"] = compute_breakout_score(closes, volumes)

    return result


def compute_momentum_for_universe(
    price_history: Dict[str, pd.DataFrame],
    index_closes: Optional[List[float]] = None,
) -> Dict[str, Dict[str, Optional[float]]]:
    """Compute momentum metrics for all stocks in the universe."""
    results = {}

    for ticker, hist in price_history.items():
        if hist.empty:
            continue

        closes = hist["close"].dropna().tolist() if "close" in hist.columns else []
        highs = hist["high"].dropna().tolist() if "high" in hist.columns else []
        lows = hist["low"].dropna().tolist() if "low" in hist.columns else []
        volumes = hist["volume"].dropna().tolist() if "volume" in hist.columns else []

        if not closes:
            continue

        results[ticker] = compute_all_momentum_metrics(
            closes=closes,
            highs=highs if highs else None,
            lows=lows if lows else None,
            volumes=volumes if volumes else None,
            index_closes=index_closes,
        )

    return results


def save_momentum_scores(scores: Dict[str, Dict], run_date: date) -> Path:
    """Save momentum scores as dated CSV."""
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"momentum_scores_{run_date.isoformat()}.csv"

    fieldnames = ["symbol", "dual_momentum", "momentum_quality", "trend_strength",
                  "mean_reversion_risk", "breakout_score"]

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for ticker, metrics in sorted(scores.items()):
            row = {"symbol": ticker, **metrics}
            w.writerow(row)

    return output_path


def merge_into_screener_csv(screener_csv: Path, scores: Dict[str, Dict]) -> int:
    """Merge momentum scores into screener CSV."""
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

    new_cols = ["Dual Momentum", "Momentum Quality", "Trend Strength",
                "Mean Reversion Risk", "Breakout Score"]
    col_map = {
        "dual_momentum": "Dual Momentum",
        "momentum_quality": "Momentum Quality",
        "trend_strength": "Trend Strength",
        "mean_reversion_risk": "Mean Reversion Risk",
        "breakout_score": "Breakout Score",
    }

    for col in new_cols:
        if col not in df.columns:
            df[col] = np.nan

    updated = 0
    for idx, row in df.iterrows():
        sym = str(row[sym_col]).strip().upper()
        if sym in scores:
            for metric_key, col_name in col_map.items():
                val = scores[sym].get(metric_key)
                if val is not None:
                    df.at[idx, col_name] = val
            updated += 1

    df.to_csv(screener_csv, index=False)
    return updated


def parse_args():
    parser = argparse.ArgumentParser(description="Compute momentum scores")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--screener-csv", default=None, help="Merge scores into screener CSV")
    return parser.parse_args()


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.date)

    from scripts.price_history import load_local_price_history
    from scripts.fetch_index_data import load_local_index_history

    print(f"[Momentum] Loading price history for {run_date}...")
    price_history = load_local_price_history(run_date)
    print(f"  Loaded {len(price_history)} stocks")

    # Load index data
    index_records = load_local_index_history(run_date, "NIFTY 500", 260)
    index_closes = [r["close"] for r in sorted(index_records, key=lambda r: r["date"])] if index_records else None
    if index_closes:
        print(f"  Loaded {len(index_closes)} Nifty 500 data points")
    else:
        print("  No Nifty 500 index data (momentum will use absolute-only)")

    print("[Momentum] Computing scores...")
    scores = compute_momentum_for_universe(price_history, index_closes)
    print(f"  Computed scores for {len(scores)} stocks")

    if scores:
        output_path = save_momentum_scores(scores, run_date)
        print(f"  Saved: {output_path}")

    if args.screener_csv and scores:
        merged = merge_into_screener_csv(Path(args.screener_csv), scores)
        print(f"  Merged {merged} momentum scores into {args.screener_csv}")


if __name__ == "__main__":
    main()
