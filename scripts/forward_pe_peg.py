#!/usr/bin/env python3
"""
Forward PE / PEG Estimation
=============================
Estimates forward metrics using available growth data since we don't have
analyst consensus estimates:
  - Forward PE: Current PE * (1 / (1 + expected earnings growth))
  - PEG Ratio: PE / EPS growth rate
  - PEG Quality: PEG adjusted for consistency

These are critical for identifying GARP (Growth At Reasonable Price) stocks,
which is the approach Ticker Tape and successful investors use.

Usage:
  python scripts/forward_pe_peg.py --screener-csv <csv> --date 2026-03-12
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(str(val).strip().replace(",", "").replace("%", ""))
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


def compute_forward_metrics(row: dict) -> Dict[str, Optional[float]]:
    """Compute forward PE and PEG metrics from available fundamentals."""
    result: Dict[str, Optional[float]] = {}

    # Current PE
    pe = _safe_float(row.get("Stock P/E", row.get("PE")))

    # Growth rates (use best available)
    eps_yoy = _safe_float(row.get("Profit growth", row.get("eps_growth_yoy")))
    eps_3y = _safe_float(row.get("Profit growth 3Years", row.get("eps_cagr_3y")))
    rev_yoy = _safe_float(row.get("Sales growth", row.get("rev_growth_yoy")))
    rev_3y = _safe_float(row.get("Sales growth 3Years", row.get("rev_cagr_3y")))

    # Best estimate of forward growth: blend YoY with 3Y trend
    # Weight 3Y CAGR more for stability
    fwd_eps_growth = None
    if eps_yoy is not None and eps_3y is not None:
        fwd_eps_growth = eps_3y * 0.6 + eps_yoy * 0.4  # Weighted blend
    elif eps_3y is not None:
        fwd_eps_growth = eps_3y
    elif eps_yoy is not None:
        fwd_eps_growth = eps_yoy

    fwd_rev_growth = None
    if rev_yoy is not None and rev_3y is not None:
        fwd_rev_growth = rev_3y * 0.6 + rev_yoy * 0.4
    elif rev_3y is not None:
        fwd_rev_growth = rev_3y
    elif rev_yoy is not None:
        fwd_rev_growth = rev_yoy

    # Forward PE estimation
    if pe is not None and pe > 0 and fwd_eps_growth is not None and fwd_eps_growth > -80:
        growth_factor = 1 + (fwd_eps_growth / 100)
        if growth_factor > 0.1:  # Avoid negative/zero division
            fwd_pe = pe / growth_factor
            result["forward_pe"] = round(fwd_pe, 2)

            # Score: lower forward PE is better
            # FwdPE < 10 -> 90, FwdPE 10-20 -> 70, FwdPE 20-40 -> 50, FwdPE > 40 -> 20
            fwd_pe_score = float(np.clip(100 - fwd_pe * 1.5, 5, 95))
            result["forward_pe_score"] = round(fwd_pe_score, 2)
        else:
            result["forward_pe"] = None
            result["forward_pe_score"] = None
    else:
        result["forward_pe"] = None
        result["forward_pe_score"] = None

    # PEG Ratio
    if pe is not None and pe > 0 and fwd_eps_growth is not None and fwd_eps_growth > 1:
        peg = pe / fwd_eps_growth
        result["peg_ratio"] = round(peg, 2)

        # PEG Score: PEG < 0.5 -> 90 (very cheap growth), PEG 0.5-1 -> 70 (GARP sweet spot)
        # PEG 1-2 -> 50, PEG > 2 -> 20 (overpriced growth)
        if peg <= 0:
            peg_score = 10.0
        elif peg < 0.5:
            peg_score = 90.0
        elif peg < 1.0:
            peg_score = 80.0 - (peg - 0.5) * 20  # 80 -> 70
        elif peg < 1.5:
            peg_score = 70.0 - (peg - 1.0) * 30  # 70 -> 55
        elif peg < 2.0:
            peg_score = 55.0 - (peg - 1.5) * 30  # 55 -> 40
        else:
            peg_score = max(10, 40.0 - (peg - 2.0) * 10)

        result["peg_score"] = round(peg_score, 2)
    else:
        result["peg_ratio"] = None
        result["peg_score"] = None

    # GARP Score (Growth At Reasonable Price)
    # Combines forward PE, PEG, and growth quality
    garp_components = []

    if result.get("forward_pe_score") is not None:
        garp_components.append(result["forward_pe_score"] * 0.3)
    if result.get("peg_score") is not None:
        garp_components.append(result["peg_score"] * 0.4)

    # Growth quality bonus
    if fwd_eps_growth is not None and fwd_eps_growth > 15:
        growth_quality = float(np.clip(fwd_eps_growth, 0, 50))
        garp_components.append(growth_quality * 0.3)

    if garp_components:
        total_weight = sum([0.3, 0.4, 0.3][:len(garp_components)])
        result["garp_score"] = round(sum(garp_components) / total_weight * len(garp_components) / len(garp_components), 2)
    else:
        result["garp_score"] = None

    # Estimated growth rate (useful reference)
    result["est_eps_growth"] = round(fwd_eps_growth, 2) if fwd_eps_growth is not None else None
    result["est_rev_growth"] = round(fwd_rev_growth, 2) if fwd_rev_growth is not None else None

    return result


def process_universe(screener_csv: Path) -> Dict[str, dict]:
    df = pd.read_csv(screener_csv, dtype=str).fillna("")

    sym_col = None
    for alias in ["NSE Symbol", "Symbol", "Ticker"]:
        if alias in df.columns:
            sym_col = alias
            break
    if not sym_col:
        return {}

    results = {}
    for _, row in df.iterrows():
        symbol = str(row.get(sym_col, "")).strip().upper()
        if not symbol:
            continue
        results[symbol] = compute_forward_metrics(row.to_dict())

    return results


def save_forward_metrics(scores: Dict[str, dict], run_date: date) -> Path:
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"forward_pe_peg_{run_date.isoformat()}.csv"

    fieldnames = ["symbol", "forward_pe", "forward_pe_score", "peg_ratio",
                  "peg_score", "garp_score", "est_eps_growth", "est_rev_growth"]

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for sym, metrics in sorted(scores.items()):
            w.writerow({"symbol": sym, **metrics})

    return output_path


def merge_into_screener_csv(screener_csv: Path, scores: Dict[str, dict]) -> int:
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
        "forward_pe": "Forward PE",
        "peg_ratio": "PEG Ratio",
        "peg_score": "PEG Score",
        "garp_score": "GARP Score",
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
    parser = argparse.ArgumentParser(description="Forward PE/PEG estimation")
    parser.add_argument("--screener-csv", required=True)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--merge", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.date)
    csv_path = Path(args.screener_csv)

    print(f"[Forward PE/PEG] Processing {csv_path}...")
    scores = process_universe(csv_path)
    print(f"  Computed for {len(scores)} stocks")

    # Stats
    pegs = [s["peg_ratio"] for s in scores.values() if s.get("peg_ratio") is not None and 0 < s["peg_ratio"] < 5]
    if pegs:
        print(f"  Median PEG: {np.median(pegs):.2f}")
        print(f"  GARP zone (PEG < 1): {sum(1 for p in pegs if p < 1)}")
        print(f"  Overvalued (PEG > 2): {sum(1 for p in pegs if p > 2)}")

    fwd_pes = [s["forward_pe"] for s in scores.values() if s.get("forward_pe") is not None and 0 < s["forward_pe"] < 200]
    if fwd_pes:
        print(f"  Median Forward PE: {np.median(fwd_pes):.1f}")

    output_path = save_forward_metrics(scores, run_date)
    print(f"  Saved: {output_path}")

    if args.merge:
        merged = merge_into_screener_csv(csv_path, scores)
        print(f"  Merged {merged} forward metrics into {csv_path}")


if __name__ == "__main__":
    main()
