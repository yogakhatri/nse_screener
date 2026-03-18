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
from engine.config import (
    CARD_WEIGHTS,
    MIN_TEMPLATE_AVG_CORE_RANKABLE_PCT,
    MIN_TEMPLATE_CARD_RANKABLE_PCT,
    MIN_TEMPLATE_RED_FLAGS_RANKABLE_PCT,
    QUALITY_GATE_REQUIRE_ALL_CORE_CARDS,
    configured_core_cards,
    validate_runtime_config,
)
from scripts.load_data import load_from_screener, metric_coverage
from scripts.local_storage import RunLogger

CORE_CARDS = list(configured_core_cards())


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
    parser.add_argument(
        "--min-universe-size",
        type=int,
        default=250,
        help="Minimum symbols required after loading/filtering",
    )
    parser.add_argument(
        "--min-avg-core-rankable-pct",
        type=float,
        default=8.0,
        help="Minimum average rankable%% across configured core cards",
    )
    parser.add_argument(
        "--min-core-cards-with-rankable",
        type=int,
        default=3,
        help="Minimum number of core cards with rankable%% > 0",
    )
    parser.add_argument(
        "--min-classification-coverage-pct",
        type=float,
        default=90.0,
        help="Minimum %% of symbols with non-generic Sector/Industry/Basic Industry taxonomy",
    )
    parser.add_argument(
        "--skip-quality-gate",
        action="store_true",
        help="Skip input quality gate checks (not recommended for live use)",
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


def input_quality_report(universe: Dict[str, object]) -> dict:
    coverage = metric_coverage(universe)
    core_rankable_pct = {}
    for card in CORE_CARDS:
        n_stocks = 0
        n_rankable = 0
        for template_cards in coverage.values():
            stats = template_cards.get(card, {})
            n_stocks += int(stats.get("n_stocks", 0))
            n_rankable += int(stats.get("n_rankable", 0))
        pct = round((n_rankable / n_stocks) * 100.0, 2) if n_stocks else 0.0
        core_rankable_pct[card] = pct
    avg_core_rankable_pct = round(
        sum(core_rankable_pct.values()) / len(core_rankable_pct), 2
    ) if core_rankable_pct else 0.0
    n_core_cards_with_rankable = sum(1 for value in core_rankable_pct.values() if value > 0.0)
    n_classified = 0
    for stock in universe.values():
        cls = getattr(stock, "classification", None)
        sector = (getattr(cls, "sector", "") or "").strip().lower()
        industry = (getattr(cls, "industry", "") or "").strip().lower()
        basic_industry = (getattr(cls, "basic_industry", "") or "").strip().lower()
        if sector and industry and basic_industry and "diversified" not in {sector, industry, basic_industry}:
            n_classified += 1
    classification_coverage_pct = round((n_classified / len(universe) * 100.0), 2) if universe else 0.0
    return {
        "n_symbols_loaded": len(universe),
        "core_rankable_pct": core_rankable_pct,
        "avg_core_rankable_pct": avg_core_rankable_pct,
        "n_core_cards_with_rankable": n_core_cards_with_rankable,
        "classification_coverage_pct": classification_coverage_pct,
        "n_symbols_classified": n_classified,
    }


def input_quality_blockers(
    quality: dict,
    min_universe_size: int,
    min_avg_core_rankable_pct: float,
    min_core_cards_with_rankable: int,
    min_classification_coverage_pct: float,
) -> List[str]:
    blockers: List[str] = []
    if quality["n_symbols_loaded"] < min_universe_size:
        blockers.append(
            f"Universe too small ({quality['n_symbols_loaded']} < {min_universe_size})."
        )
    if quality["avg_core_rankable_pct"] < min_avg_core_rankable_pct:
        blockers.append(
            "Core coverage too low "
            f"({quality['avg_core_rankable_pct']}% < {min_avg_core_rankable_pct}%)."
        )
    if quality["n_core_cards_with_rankable"] < min_core_cards_with_rankable:
        blockers.append(
            "Too few core cards have any rankable stocks "
            f"({quality['n_core_cards_with_rankable']} < {min_core_cards_with_rankable})."
        )
    if quality["classification_coverage_pct"] < min_classification_coverage_pct:
        blockers.append(
            "Classification coverage too low "
            f"({quality['classification_coverage_pct']}% < {min_classification_coverage_pct}%)."
        )
    return blockers


def template_quality_report(universe: Dict[str, object]) -> dict:
    coverage = metric_coverage(universe)
    report: dict = {}
    min_core_cards = len(CORE_CARDS) if QUALITY_GATE_REQUIRE_ALL_CORE_CARDS else max(3, len(CORE_CARDS) - 1)
    for template, cards in coverage.items():
        n_stocks = max((int(stats.get("n_stocks", 0)) for stats in cards.values()), default=0)
        core_rankable_pct = {
            card: round(float(cards.get(card, {}).get("rankable_pct", 0.0)), 2)
            for card in CORE_CARDS
        }
        avg_core_rankable_pct = round(
            sum(core_rankable_pct.values()) / len(core_rankable_pct), 2
        ) if core_rankable_pct else 0.0
        n_core_cards_with_rankable = sum(1 for value in core_rankable_pct.values() if value > 0.0)
        red_flags_rankable_pct = round(float(cards.get("red_flags", {}).get("rankable_pct", 0.0)), 2)
        blockers: List[str] = []
        if n_stocks > 0:
            if avg_core_rankable_pct < MIN_TEMPLATE_AVG_CORE_RANKABLE_PCT:
                blockers.append(
                    "avg core rankable% too low "
                    f"({avg_core_rankable_pct}% < {MIN_TEMPLATE_AVG_CORE_RANKABLE_PCT}%)"
                )
            if n_core_cards_with_rankable < min_core_cards:
                blockers.append(
                    "too few core cards rankable "
                    f"({n_core_cards_with_rankable} < {min_core_cards})"
                )
            for card, pct in core_rankable_pct.items():
                if pct < MIN_TEMPLATE_CARD_RANKABLE_PCT:
                    blockers.append(
                        f"{card} rankable% too low ({pct}% < {MIN_TEMPLATE_CARD_RANKABLE_PCT}%)"
                    )
            if red_flags_rankable_pct < MIN_TEMPLATE_RED_FLAGS_RANKABLE_PCT:
                blockers.append(
                    "red_flags rankable% too low "
                    f"({red_flags_rankable_pct}% < {MIN_TEMPLATE_RED_FLAGS_RANKABLE_PCT}%)"
                )
        report[template] = {
            "active": n_stocks > 0,
            "supported": n_stocks > 0 and not blockers,
            "n_stocks": n_stocks,
            "core_rankable_pct": core_rankable_pct,
            "avg_core_rankable_pct": avg_core_rankable_pct,
            "n_core_cards_with_rankable": n_core_cards_with_rankable,
            "red_flags_rankable_pct": red_flags_rankable_pct,
            "blockers": blockers,
        }
    return report


def template_quality_blockers(template_quality: dict) -> List[str]:
    blockers: List[str] = []
    for template, info in template_quality.items():
        if not info.get("active"):
            continue
        if info.get("supported"):
            continue
        blockers.append(
            f"Template {template} unsupported: " + "; ".join(info.get("blockers", []))
        )
    return blockers


def apply_template_support_overrides(ratings: Dict[str, object], template_quality: dict) -> None:
    for rating in ratings.values():
        info = template_quality.get(rating.template.value, {})
        supported = bool(info.get("supported", False))
        blockers = list(info.get("blockers", []))
        rating.template_supported = supported
        rating.template_support_status = "Supported" if supported else "Unsupported Template Coverage"
        rating.template_support_reasons = blockers
        if supported:
            continue
        rating.investability_status = "Unsupported Data"
        rating.recommendation = "Unsupported"
        rating.investability_gate_passed = False
        reasons = list(rating.gate_fail_reasons)
        reasons.extend(blockers)
        deduped = []
        for reason in reasons:
            if reason not in deduped:
                deduped.append(reason)
        rating.gate_fail_reasons = deduped
        rating.action_note = "Template unsupported: " + "; ".join(blockers)


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
    "template_supported",
    "template_support_status",
    "template_support_reason",
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


def _template_support_rows(template_quality: dict) -> List[dict]:
    rows: List[dict] = []
    for template, info in sorted(template_quality.items()):
        rows.append(
            {
                "template": template,
                "active": info.get("active", False),
                "supported": info.get("supported", False),
                "n_stocks": info.get("n_stocks", 0),
                "avg_core_rankable_pct": info.get("avg_core_rankable_pct", 0.0),
                "n_core_cards_with_rankable": info.get("n_core_cards_with_rankable", 0),
                "red_flags_rankable_pct": info.get("red_flags_rankable_pct", 0.0),
                "blockers": "; ".join(info.get("blockers", [])),
            }
        )
    return rows


def _unsupported_rows(ratings: Dict[str, object]) -> List[dict]:
    rows: List[dict] = []
    for rating in ratings.values():
        if rating.template_supported:
            continue
        rows.append(
            {
                "ticker": rating.ticker,
                "name": rating.name,
                "template": rating.template.value,
                "sector": rating.classification.sector,
                "basic_industry": rating.classification.basic_industry,
                "support_status": rating.template_support_status,
                "support_reason": "; ".join(rating.template_support_reasons),
                "investability_status": rating.investability_status,
                "recommendation": rating.recommendation,
            }
        )
    return sorted(rows, key=lambda r: (r["template"], r["ticker"]))


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
        or row.get("investability_status") in {"Uninvestable", "Avoid", "Unsupported Data"}
    ]
    return buy_candidates, undervalued_high_potential, red_flag_exclusions


def main() -> None:
    args = parse_args()
    validate_runtime_config()
    run_date = resolve_run_date(args.date)
    run_date_str = run_date.isoformat()
    run_mode = args.mode.lower()
    backtest_end = args.backtest_end or run_date_str

    screener_csv = resolve_screener_csv(run_date, args.screener_csv)
    if not screener_csv.exists():
        raise FileNotFoundError(
            "\n".join(
                [
                    f"Screener CSV not found: {screener_csv}",
                    "Fix: run `make prepare-universe RUN_DATE=... NSE_UNIVERSE_CSV=... [FUNDAMENTALS_CSV=...]`",
                    "Or run `make prepare-csv RUN_DATE=...` and fill it manually.",
                ]
            )
        )

    freshness_warnings = validate_screener_freshness(screener_csv, run_date, args.strict_freshness)
    universe = load_from_screener(str(screener_csv), run_date=run_date)
    universe = filter_universe(universe, args.tickers)
    if not universe:
        raise RuntimeError("Universe is empty after load/filter. Check CSV and --tickers input.")

    quality = input_quality_report(universe)
    template_quality = template_quality_report(universe)
    if not args.skip_quality_gate:
        blockers = input_quality_blockers(
            quality=quality,
            min_universe_size=args.min_universe_size,
            min_avg_core_rankable_pct=args.min_avg_core_rankable_pct,
            min_core_cards_with_rankable=args.min_core_cards_with_rankable,
            min_classification_coverage_pct=args.min_classification_coverage_pct,
        )
        blockers.extend(template_quality_blockers(template_quality))
        if blockers:
            details = ", ".join(
                f"{card}={pct:.1f}%" for card, pct in quality["core_rankable_pct"].items()
            )
            template_details = [
                f"Template {template}: active={info['active']}, supported={info['supported']}, "
                f"avg_core={info['avg_core_rankable_pct']}%, red_flags={info['red_flags_rankable_pct']}%, "
                f"blockers={'; '.join(info['blockers']) or 'none'}"
                for template, info in sorted(template_quality.items())
                if info.get("active")
            ]
            raise RuntimeError(
                "\n".join(
                    [
                        "INPUT QUALITY GATE FAILED:",
                        *[f"- {msg}" for msg in blockers],
                        f"- Core rankable% by card: {details}",
                        "- Classification coverage (non-Diversified taxonomy): "
                        f"{quality['classification_coverage_pct']}% "
                        f"({quality['n_symbols_classified']}/{quality['n_symbols_loaded']})",
                        "- Template support snapshot:",
                        *[f"  * {line}" for line in template_details],
                        "Fix: build a full daily universe + fundamentals CSV, then rerun.",
                        "Hint: make prepare-universe RUN_DATE=... NSE_UNIVERSE_CSV=... FUNDAMENTALS_CSV=...",
                        "Override only for debugging: --skip-quality-gate",
                    ]
                )
            )

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
    apply_template_support_overrides(ratings, template_quality)
    leaderboard = engine.to_leaderboard(
        ratings,
        exclude_statuses=("Insufficient Data", "Unsupported Data", "Uninvestable"),
    )

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
    _write_csv(
        out_dir / "template_support.csv",
        _template_support_rows(template_quality),
        fieldnames=[
            "template",
            "active",
            "supported",
            "n_stocks",
            "avg_core_rankable_pct",
            "n_core_cards_with_rankable",
            "red_flags_rankable_pct",
            "blockers",
        ],
    )
    with open(out_dir / "input_quality.json", "w") as f:
        json.dump(quality, f, indent=2)
    with open(out_dir / "template_support.json", "w") as f:
        json.dump(template_quality, f, indent=2)

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
    _write_csv(
        out_dir / "unsupported_stocks.csv",
        _unsupported_rows(ratings),
        fieldnames=[
            "ticker",
            "name",
            "template",
            "sector",
            "basic_industry",
            "support_status",
            "support_reason",
            "investability_status",
            "recommendation",
        ],
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
            "template",
            "template_supported",
            "template_support_status",
            "template_support_reason",
            "investability_status",
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
