#!/usr/bin/env python3
"""
"Why This Stock?" Explainer
=============================
Generates human-readable investment thesis summaries for top-ranked stocks.
Similar to Ticker Tape's "Why to Buy" feature.

For each stock, constructs a 3-5 sentence narrative covering:
  1. Core strength (highest scoring card)
  2. Valuation context
  3. Growth trajectory
  4. Key risk factors
  5. Entry timing signal

Reads stock JSON files from a run directory and outputs enriched JSONs
with a 'thesis' field and a standalone human-readable report.

Usage:
  python scripts/stock_explainer.py --run-dir runs/2026-03-12
  python scripts/stock_explainer.py --run-dir runs/2026-03-12 --top 20
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))


# Card score interpretation
SCORE_LABELS = {
    (80, 101): "exceptional",
    (65, 80): "strong",
    (50, 65): "above average",
    (40, 50): "moderate",
    (25, 40): "below average",
    (0, 25): "weak",
}

VALUATION_LABELS = {
    (80, 101): "deeply undervalued",
    (65, 80): "undervalued",
    (50, 65): "fairly valued",
    (40, 50): "slightly expensive",
    (25, 40): "expensive",
    (0, 25): "very expensive",
}

ENTRY_LABELS = {
    (70, 101): "excellent entry point",
    (55, 70): "favorable entry",
    (40, 55): "neutral timing",
    (25, 40): "unfavorable entry",
    (0, 25): "poor timing (wait)",
}

GROWTH_LABELS = {
    (80, 101): "high-growth",
    (60, 80): "above-average growth",
    (40, 60): "moderate growth",
    (20, 40): "low growth",
    (0, 20): "declining",
}

RISK_LABELS = {
    (80, 101): "minimal risk flags",
    (60, 80): "low risk",
    (40, 60): "moderate risk",
    (20, 40): "elevated risk",
    (0, 20): "high risk",
}


def _label(score: Optional[float], labels: dict) -> str:
    if score is None:
        return "insufficient data"
    for (lo, hi), label in labels.items():
        if lo <= score < hi:
            return label
    return "unranked"


def _top_sub_metrics(card_data: dict, n: int = 2) -> List[Tuple[str, float]]:
    """Get top N sub-metrics by score for a card."""
    subs = card_data.get("sub_scores", {})
    scored = [(k, v) for k, v in subs.items() if v is not None]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:n]


def _bottom_sub_metrics(card_data: dict, n: int = 2) -> List[Tuple[str, float]]:
    """Get bottom N sub-metrics by score for a card."""
    subs = card_data.get("sub_scores", {})
    scored = [(k, v) for k, v in subs.items() if v is not None]
    scored.sort(key=lambda x: x[1])
    return scored[:n]


def _metric_name(key: str) -> str:
    """Human-readable metric name from snake_case key."""
    names = {
        "return_1y": "1-year return",
        "return_6m": "6-month return",
        "cagr_5y": "5-year CAGR",
        "peer_price_strength": "peer relative strength",
        "drawdown_recovery": "drawdown recovery",
        "forward_view": "forward outlook",
        "pe_percentile": "P/E valuation",
        "pb_percentile": "P/B valuation",
        "p_cfo_percentile": "price-to-cash-flow",
        "ev_ebitda_percentile": "EV/EBITDA",
        "hist_val_band": "historical valuation band",
        "fcf_yield": "free cash flow yield",
        "iv_gap": "intrinsic value gap",
        "rev_cagr_3y": "3Y revenue CAGR",
        "eps_cagr_3y": "3Y EPS CAGR",
        "rev_growth_yoy": "YoY revenue growth",
        "eps_growth_yoy": "YoY EPS growth",
        "peer_growth_rank": "peer growth rank",
        "growth_stability": "growth consistency",
        "roce_3y_median": "3Y median ROCE",
        "ebitda_margin": "EBITDA margin",
        "cfo_pat_ratio": "cash-flow-to-profit ratio",
        "margin_trend": "margin trend",
        "roa": "return on assets",
        "fcf_consistency": "FCF consistency",
        "discount_to_iv": "discount to intrinsic value",
        "rsi_state": "RSI momentum",
        "price_vs_200dma": "price vs 200-DMA",
        "price_vs_50dma": "price vs 50-DMA",
        "volume_delivery": "delivery volume",
        "rs_turn": "relative strength turn",
        "volatility_compression": "volatility compression",
        "promoter_pledge": "promoter pledge",
        "asm_gsm_risk": "ASM/GSM surveillance",
        "default_distress": "default/distress risk",
        "accounting_quality": "accounting quality",
        "liquidity_manipulation": "liquidity risk",
        "governance_event": "governance risk",
    }
    return names.get(key, key.replace("_", " "))


def generate_thesis(stock_data: dict) -> dict:
    """Generate investment thesis for a stock from its JSON data."""
    ticker = stock_data.get("ticker", "Unknown")
    name = stock_data.get("stock_name", ticker)
    cards = stock_data.get("cards", {})
    recommendation = stock_data.get("recommendation", "Unknown")
    opp_score = stock_data.get("final_opportunity_score")
    selection_score = stock_data.get("selection_score")
    expected_upside = stock_data.get("expected_upside_pct")
    risk_reward = stock_data.get("risk_reward_ratio")
    sector = stock_data.get("classification", {}).get("sector", "")
    industry = stock_data.get("classification", {}).get("basic_industry", "")
    market_mode = stock_data.get("market_mode", "neutral")

    # Extract card scores
    perf_score = cards.get("performance", {}).get("score")
    val_score = cards.get("valuation", {}).get("score")
    growth_score = cards.get("growth", {}).get("score")
    profit_score = cards.get("profitability", {}).get("score")
    entry_score = cards.get("entry_point", {}).get("score")
    risk_score = cards.get("red_flags", {}).get("score")

    # Build thesis components
    parts = []

    # 1. Opening line with overall assessment
    if opp_score is not None:
        overall_label = _label(opp_score, SCORE_LABELS)
        parts.append(
            f"{name} ({ticker}) scores {opp_score:.1f}/100 overall — "
            f"an {overall_label} opportunity in the {industry or sector} space."
        )

    # 2. Core strength (highest scoring card)
    scored_cards = []
    for card_name, label_map in [
        ("performance", SCORE_LABELS),
        ("valuation", VALUATION_LABELS),
        ("growth", GROWTH_LABELS),
        ("profitability", SCORE_LABELS),
        ("entry_point", ENTRY_LABELS),
    ]:
        s = cards.get(card_name, {}).get("score")
        if s is not None:
            scored_cards.append((card_name, s))
    scored_cards.sort(key=lambda x: x[1], reverse=True)

    if scored_cards:
        best_card, best_score = scored_cards[0]
        top_metrics = _top_sub_metrics(cards.get(best_card, {}), 2)
        metric_strs = [f"{_metric_name(m)} ({v:.0f})" for m, v in top_metrics]

        card_labels = {
            "performance": "price performance",
            "valuation": "valuation",
            "growth": "growth trajectory",
            "profitability": "profitability",
            "entry_point": "entry timing",
        }
        parts.append(
            f"Its strongest suit is {card_labels.get(best_card, best_card)} "
            f"({best_score:.0f}/100), driven by {' and '.join(metric_strs)}."
        )

    # 3. Valuation context
    if val_score is not None:
        val_label = _label(val_score, VALUATION_LABELS)
        parts.append(f"Valuation looks {val_label} ({val_score:.0f}/100).")

    # 4. Growth trajectory
    if growth_score is not None:
        growth_label = _label(growth_score, GROWTH_LABELS)
        parts.append(f"Growth profile: {growth_label} ({growth_score:.0f}/100).")

    # 5. Key risks
    if risk_score is not None:
        risk_label = _label(risk_score, RISK_LABELS)
        bottom_risks = _bottom_sub_metrics(cards.get("red_flags", {}), 2)
        risk_details = [f"{_metric_name(m)} ({v:.0f})" for m, v in bottom_risks if v < 50]
        risk_str = f"Risk assessment: {risk_label} ({risk_score:.0f}/100)"
        if risk_details:
            risk_str += f" — watch {', '.join(risk_details)}"
        risk_str += "."
        parts.append(risk_str)

    # 6. Entry timing
    if entry_score is not None:
        entry_label = _label(entry_score, ENTRY_LABELS)
        parts.append(f"Entry timing: {entry_label} ({entry_score:.0f}/100).")

    # 7. Risk/reward summary
    if expected_upside is not None and risk_reward is not None:
        rr_quality = "favorable" if risk_reward > 1.5 else "acceptable" if risk_reward > 1.0 else "unfavorable"
        parts.append(
            f"Expected upside of {expected_upside:.1f}% with a "
            f"{rr_quality} risk-reward ratio of {risk_reward:.2f}."
        )

    # 8. Market context
    if market_mode != "neutral":
        parts.append(f"Note: market is currently in {market_mode} mode.")

    thesis_text = " ".join(parts)

    # Build structured highlights
    strengths = []
    weaknesses = []
    for card_name in ["performance", "valuation", "growth", "profitability", "entry_point"]:
        s = cards.get(card_name, {}).get("score")
        if s is not None:
            if s >= 65:
                strengths.append(f"{card_name.replace('_', ' ').title()}: {s:.0f}/100")
            elif s < 35:
                weaknesses.append(f"{card_name.replace('_', ' ').title()}: {s:.0f}/100")

    # Top metrics across all cards
    all_top = []
    for card_name in ["performance", "valuation", "growth", "profitability", "entry_point"]:
        for metric, val in _top_sub_metrics(cards.get(card_name, {}), 1):
            if val >= 70:
                all_top.append((_metric_name(metric), val))
    all_top.sort(key=lambda x: x[1], reverse=True)

    return {
        "thesis": thesis_text,
        "thesis_highlights": {
            "strengths": strengths[:5],
            "weaknesses": weaknesses[:5],
            "top_metrics": [{"name": n, "score": s} for n, s in all_top[:5]],
            "recommendation": recommendation,
            "entry_signal": stock_data.get("entry_signal", ""),
        },
    }


def process_run_directory(run_dir: Path, top_n: int = 0) -> List[dict]:
    """Process all stock JSONs in a run directory, add thesis to each."""
    stock_files = sorted(run_dir.glob("stock_*.json"))

    if not stock_files:
        print(f"No stock JSON files found in {run_dir}")
        return []

    # If top_n specified, read leaderboard first to know which stocks to process
    priority_tickers = set()
    if top_n > 0:
        leaderboard = run_dir / "leaderboard.csv"
        if leaderboard.exists():
            import csv
            with open(leaderboard) as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= top_n:
                        break
                    ticker = row.get("ticker", row.get("Ticker", "")).strip().upper()
                    if ticker:
                        priority_tickers.add(ticker)

    results = []
    for sf in stock_files:
        try:
            data = json.loads(sf.read_text())
        except (json.JSONDecodeError, IOError):
            continue

        ticker = data.get("ticker", "")
        if top_n > 0 and priority_tickers and ticker.upper() not in priority_tickers:
            continue

        thesis = generate_thesis(data)
        data.update(thesis)

        # Write enriched JSON back
        sf.write_text(json.dumps(data, indent=2, default=str))
        results.append(data)

    return results


def generate_report(results: List[dict], output_path: Path) -> None:
    """Generate a human-readable summary report of top stock theses."""
    lines = ["# Stock Analysis Report", ""]

    # Sort by opportunity score
    results.sort(
        key=lambda x: x.get("selection_score") or x.get("final_opportunity_score") or 0,
        reverse=True,
    )

    for i, data in enumerate(results, 1):
        ticker = data.get("ticker", "?")
        name = data.get("stock_name", ticker)
        opp = data.get("final_opportunity_score")
        rec = data.get("recommendation", "?")

        lines.append(f"## {i}. {name} ({ticker})")
        lines.append(f"**Recommendation:** {rec} | **Score:** {opp:.1f}/100" if opp else f"**Recommendation:** {rec}")
        lines.append("")
        lines.append(data.get("thesis", "No thesis generated."))
        lines.append("")

        highlights = data.get("thesis_highlights", {})
        if highlights.get("strengths"):
            lines.append("**Strengths:** " + " | ".join(highlights["strengths"]))
        if highlights.get("weaknesses"):
            lines.append("**Weaknesses:** " + " | ".join(highlights["weaknesses"]))
        lines.append("")
        lines.append("---")
        lines.append("")

    output_path.write_text("\n".join(lines))


def parse_args():
    parser = argparse.ArgumentParser(description="Generate stock investment theses")
    parser.add_argument("--run-dir", required=True, help="Path to run directory")
    parser.add_argument("--top", type=int, default=0, help="Process only top N stocks from leaderboard (0=all)")
    parser.add_argument("--report", default=None, help="Output markdown report path")
    return parser.parse_args()


def main():
    args = parse_args()
    run_dir = Path(args.run_dir)

    if not run_dir.exists():
        print(f"Run directory not found: {run_dir}")
        sys.exit(1)

    print(f"[Explainer] Processing {run_dir}...")
    results = process_run_directory(run_dir, args.top)
    print(f"  Generated theses for {len(results)} stocks")

    report_path = Path(args.report) if args.report else run_dir / "stock_analysis_report.md"
    generate_report(results, report_path)
    print(f"  Report: {report_path}")


if __name__ == "__main__":
    main()
