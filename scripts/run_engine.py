#!/usr/bin/env python3
"""
run_engine.py — Production runner for NSE sector-wise stock analysis.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from engine import NSERatingEngine
from engine.advanced import (
    action_sheet_rows,
    evaluate_recommendation_outcomes,
    portfolio_plan_rows,
    update_recommendation_history,
)
from engine.bias_controls import BiasAudit
from engine.config import CARD_WEIGHTS
from scripts.load_data import load_from_screener, metric_coverage
from scripts.local_storage import RunLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="Run date YYYY-MM-DD")
    parser.add_argument(
        "--screener-csv",
        default=None,
        help="Path to Screener export CSV (default: data/raw/fundamentals/screener/screener_export_<date>.csv)",
    )
    parser.add_argument(
        "--mode",
        choices=["live", "backtest"],
        default="live",
        help="live: current ranking; backtest: historical evaluation with holdout guard",
    )
    parser.add_argument("--backtest-start", default="2019-04-01", help="Backtest start YYYY-MM-DD")
    parser.add_argument("--backtest-end", default=None, help="Backtest end YYYY-MM-DD (default: --date)")
    parser.add_argument("--tickers", default=None, help="Comma-separated tickers (optional universe filter)")
    parser.add_argument(
        "--strict-freshness",
        action="store_true",
        help="Fail run if screener CSV is stale (>120 days old)",
    )
    parser.add_argument(
        "--market-mode",
        choices=["auto", "bear", "neutral", "bull"],
        default="auto",
        help="Regime override for down-market/bull-market behavior",
    )
    return parser.parse_args()


def resolve_run_date(value: str | None) -> dt.date:
    return dt.date.fromisoformat(value) if value else dt.date.today()


def resolve_screener_csv(run_date: dt.date, arg_path: str | None) -> Path:
    if arg_path:
        return Path(arg_path)
    return Path("data/raw/fundamentals/screener") / f"screener_export_{run_date.isoformat()}.csv"


def validate_screener_freshness(path: Path, run_date: dt.date, strict: bool) -> List[str]:
    warnings: List[str] = []
    if not path.exists():
        return warnings
    file_date = dt.date.fromtimestamp(path.stat().st_mtime)
    age_days = (run_date - file_date).days
    if age_days > 120:
        msg = f"Screener CSV looks stale ({age_days} days old; updated {file_date.isoformat()})."
        if strict:
            raise RuntimeError(msg)
        warnings.append(msg)
    return warnings


def filter_universe(universe: Dict[str, object], tickers_arg: str | None) -> Dict[str, object]:
    if not tickers_arg:
        return universe
    selected = {t.strip().upper() for t in tickers_arg.split(",") if t.strip()}
    return {ticker: stock for ticker, stock in universe.items() if ticker in selected}


LEADERBOARD_COLUMNS = [
    "ticker",
    "name",
    "sector",
    "basic_industry",
    "template",
    "peer_level",
    "performance",
    "valuation",
    "growth",
    "profitability",
    "entry_point",
    "red_flags",
    "opportunity_score",
    "investability_status",
    "potential_score",
    "valuation_gap_score",
    "recommendation",
    "confidence",
    "entry_signal",
    "market_mode",
    "sector_regime_score",
    "sector_regime_label",
    "drawdown_resilience_score",
    "valuation_confidence_score",
    "expected_upside_pct",
    "expected_downside_pct",
    "risk_reward_ratio",
    "risk_reward_score",
    "selection_score",
    "gate_passed",
    "gate_fail_reasons",
    "staged_entry_plan",
    "action_note",
    "sector_rank",
    "sector_percentile",
    "basic_industry_rank",
    "basic_industry_percentile",
]


def _write_csv(path: Path, rows: List[dict], fieldnames: Optional[List[str]] = None) -> None:
    if not rows and not fieldnames:
        path.write_text("")
        return
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)


def _ranked_sector_views(leaderboard: List[dict]) -> tuple[List[dict], List[dict]]:
    by_sector: Dict[str, List[dict]] = {}
    for row in leaderboard:
        by_sector.setdefault(row["sector"], []).append(row)

    sector_top: List[dict] = []
    sector_summary: List[dict] = []
    for sector, rows in sorted(by_sector.items(), key=lambda x: x[0]):
        rows_sorted = sorted(rows, key=lambda r: r.get("opportunity_score") or 0, reverse=True)
        buy_count = sum(1 for r in rows_sorted if r.get("recommendation") == "Buy Candidate")
        avg_opp = round(sum((r.get("opportunity_score") or 0) for r in rows_sorted) / len(rows_sorted), 2)
        sector_summary.append(
            {
                "sector": sector,
                "n_stocks": len(rows_sorted),
                "n_buy_candidates": buy_count,
                "avg_opportunity_score": avg_opp,
            }
        )
        for i, row in enumerate(rows_sorted[:10], start=1):
            out = dict(row)
            out["sector_top_rank"] = i
            sector_top.append(out)
    return sector_top, sector_summary


def _coverage_rows(universe: Dict[str, object]) -> List[dict]:
    rows: List[dict] = []
    for template, cards in metric_coverage(universe).items():
        for card, stats in cards.items():
            rows.append(
                {
                    "template": template,
                    "card": card,
                    "avg_coverage": stats["avg_coverage"],
                    "rankable_pct": stats["rankable_pct"],
                }
            )
    return rows


def _build_action_lists(leaderboard: List[dict]) -> tuple[List[dict], List[dict], List[dict]]:
    buy_candidates = [
        row for row in leaderboard
        if row.get("recommendation") == "Buy Candidate"
        and row.get("investability_status") == "Investable"
    ]
    undervalued_high_potential = sorted(
        [
            row for row in leaderboard
            if row.get("potential_score") is not None and row.get("valuation_gap_score") is not None
        ],
        key=lambda r: (
            r.get("valuation_gap_score") or 0,
            r.get("potential_score") or 0,
            r.get("opportunity_score") or 0,
        ),
        reverse=True,
    )[:200]
    red_flag_exclusions = [
        row for row in leaderboard
        if (row.get("red_flags") is not None and row["red_flags"] < 40)
        or row.get("investability_status") in {"Uninvestable", "Avoid"}
    ]
    return buy_candidates, undervalued_high_potential, red_flag_exclusions


def main() -> None:
    args = parse_args()
    run_date = resolve_run_date(args.date)
    run_date_str = run_date.isoformat()
    run_mode = args.mode.lower()
    backtest_end = args.backtest_end or run_date_str

    screener_csv = resolve_screener_csv(run_date, args.screener_csv)
    if not screener_csv.exists():
        raise FileNotFoundError(
            f"Screener CSV not found: {screener_csv}. Provide --screener-csv or place the dated file."
        )

    freshness_warnings = validate_screener_freshness(screener_csv, run_date, args.strict_freshness)
    universe = load_from_screener(str(screener_csv))
    universe = filter_universe(universe, args.tickers)
    if not universe:
        raise RuntimeError("Universe is empty after load/filter. Check CSV and --tickers input.")

    logger = RunLogger(run_date=run_date)
    logger.start()
    logger.log_input(
        source_id="screener_export",
        file_path=screener_csv,
        freshness_ts=dt.datetime.fromtimestamp(screener_csv.stat().st_mtime).isoformat(timespec="seconds"),
    )

    audit = BiasAudit(list(universe.keys()), CARD_WEIGHTS)
    try:
        report = audit.run(
            as_of_date=run_date_str,
            backtest_start=args.backtest_start,
            backtest_end=backtest_end,
            mode=run_mode,
        )
    finally:
        audit.close()
    if not report["all_clear"]:
        print("BIAS AUDIT FAILED — blocking run:")
        for blocker in report["blockers"]:
            print(f"  • {blocker}")
        sys.exit(1)

    for warning in freshness_warnings + report["warnings"]:
        print(f"⚠️  {warning}")

    engine = NSERatingEngine(universe, market_mode=args.market_mode)
    ratings = engine.rate_universe()
    leaderboard = engine.to_leaderboard(ratings, exclude_statuses=("Insufficient Data",))

    out_dir = Path("runs") / run_date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    for ticker, rating in ratings.items():
        with open(out_dir / f"stock_{ticker}.json", "w") as f:
            json.dump(rating.to_dict(), f, indent=2)

    _write_csv(out_dir / "leaderboard.csv", leaderboard, fieldnames=LEADERBOARD_COLUMNS)
    _write_csv(
        out_dir / "coverage_by_template_card.csv",
        _coverage_rows(universe),
        fieldnames=["template", "card", "avg_coverage", "rankable_pct"],
    )

    buy_candidates, undervalued, red_flag_exclusions = _build_action_lists(leaderboard)
    _write_csv(out_dir / "buy_candidates.csv", buy_candidates, fieldnames=LEADERBOARD_COLUMNS)
    _write_csv(
        out_dir / "undervalued_high_potential.csv",
        undervalued,
        fieldnames=LEADERBOARD_COLUMNS,
    )
    _write_csv(
        out_dir / "red_flag_exclusions.csv",
        red_flag_exclusions,
        fieldnames=LEADERBOARD_COLUMNS,
    )

    sector_top, sector_summary = _ranked_sector_views(leaderboard)
    _write_csv(
        out_dir / "sector_top_10.csv",
        sector_top,
        fieldnames=LEADERBOARD_COLUMNS + ["sector_top_rank"],
    )
    _write_csv(
        out_dir / "sector_summary.csv",
        sector_summary,
        fieldnames=["sector", "n_stocks", "n_buy_candidates", "avg_opportunity_score"],
    )

    action_sheet = action_sheet_rows(ratings)
    _write_csv(
        out_dir / "action_sheet.csv",
        action_sheet,
        fieldnames=[
            "ticker",
            "name",
            "sector",
            "recommendation",
            "confidence",
            "market_mode",
            "sector_regime",
            "selection_score",
            "potential_score",
            "valuation_gap_score",
            "expected_upside_pct",
            "expected_downside_pct",
            "risk_reward_ratio",
            "entry_signal",
            "staged_entry_plan",
            "gate_passed",
            "gate_fail_reasons",
            "action_note",
        ],
    )

    portfolio_plan = portfolio_plan_rows(leaderboard)
    _write_csv(
        out_dir / "portfolio_plan.csv",
        portfolio_plan,
        fieldnames=LEADERBOARD_COLUMNS + ["suggested_weight_pct", "risk_budget_note"],
    )

    with open(out_dir / "coverage_snapshot.json", "w") as f:
        json.dump(metric_coverage(universe), f, indent=2)

    history_path = Path("logs") / "recommendation_history.csv"
    update_recommendation_history(run_date_str, ratings, universe, history_path)
    monitoring = evaluate_recommendation_outcomes(run_date_str, ratings, universe, history_path)
    with open(out_dir / "model_monitoring.json", "w") as f:
        json.dump(monitoring, f, indent=2)

    logger.log_scores(leaderboard)
    logger.n_rated = len(ratings)
    logger.n_excluded = max(0, len(ratings) - len(leaderboard))
    logger.finish(leaderboard=leaderboard)

    print(f"Run complete ({run_mode}) on {run_date_str}")
    print(f"Stocks rated: {len(ratings)}")
    print(f"Buy candidates: {len(buy_candidates)}")
    print(f"Market mode: {engine.market_mode}")
    print(f"Portfolio picks: {len(portfolio_plan)}")
    print(f"Outputs: {out_dir}")


if __name__ == "__main__":
    main()
