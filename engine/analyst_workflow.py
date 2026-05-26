"""
Generate analyst research queue artifacts (no scoring changes).

Produces structured rows for manual bottom-up work: moat, catalysts,
macro risks, and promotion criteria from Watchlist → Buy Candidate.
"""
from __future__ import annotations

from typing import Any, Dict, List


WORKSHEET_PROMPTS = [
    ("business_model", "What does the company sell? Pricing power vs peers?"),
    ("moat", "Sustainable advantage (brand, cost, network, regulation)?"),
    ("management", "Capital allocation track record; promoter integrity?"),
    ("catalysts", "Next 12–36m triggers (orders, capacity, policy, exports)?"),
    ("risks", "Top 3 kill scenarios (customer, regulation, cycle, FX)?"),
    ("valuation", "Base / bull / bear fair value vs current price?"),
    ("macro_shocks", "Rates, oil, geopolitics, sector-specific policy impact?"),
    ("promotion", "What evidence upgrades Watchlist → Buy (pledge, governance, results)?"),
]


def _row_from_leaderboard(row: dict) -> Dict[str, Any]:
    ticker = row.get("ticker", "")
    return {
        "ticker": ticker,
        "name": row.get("name", ""),
        "sector": row.get("sector", ""),
        "recommendation": row.get("recommendation", ""),
        "research_tier": row.get("research_tier", ""),
        "research_status": row.get("research_status", ""),
        "gate_passed": row.get("gate_passed"),
        "gate_fail_reasons": row.get("gate_fail_reasons", ""),
        "missing_critical_fields": row.get("missing_critical_fields", ""),
        "selection_score": row.get("selection_score"),
        "potential_score": row.get("potential_score"),
        "valuation_gap_score": row.get("valuation_gap_score"),
        "value_trap_score": row.get("value_trap_score"),
        "analysis_caveat": row.get("analysis_caveat", ""),
        "worksheet_status": "pending",
        "priority": _priority(row),
    }


def _priority(row: dict) -> str:
    if row.get("recommendation") == "Buy Candidate" and row.get("gate_passed"):
        return "high"
    if row.get("recommendation") in {"Buy Candidate", "Watchlist"}:
        if row.get("research_tier") in {"High Confidence Research", "Qualified Watchlist"}:
            return "medium"
    return "low"


def build_analyst_research_queue(
    leaderboard: List[dict],
    *,
    top_pick_tickers: tuple[str, ...] = (),
    max_rows: int = 50,
) -> List[Dict[str, Any]]:
    """
    Queue for human follow-up: top picks first, then gate-failed watchlist names.
    """
    top_set = {t.upper() for t in top_pick_tickers if t}
    seen: set[str] = set()
    queue: List[Dict[str, Any]] = []

    def add(row: dict) -> None:
        t = str(row.get("ticker", "")).upper()
        if not t or t in seen:
            return
        seen.add(t)
        queue.append(_row_from_leaderboard(row))

    for row in leaderboard:
        if str(row.get("ticker", "")).upper() in top_set:
            add(row)

    for row in sorted(
        leaderboard,
        key=lambda r: (r.get("selection_score") or 0),
        reverse=True,
    ):
        if row.get("recommendation") not in {"Buy Candidate", "Watchlist"}:
            continue
        if row.get("gate_passed"):
            continue
        if row.get("research_tier") in {"Rejected", "Unsupported"}:
            continue
        add(row)
        if len(queue) >= max_rows:
            break

    for row in sorted(
        leaderboard,
        key=lambda r: (r.get("selection_score") or 0),
        reverse=True,
    ):
        if row.get("recommendation") not in {"Buy Candidate", "Watchlist"}:
            continue
        add(row)
        if len(queue) >= max_rows:
            break

    return queue[:max_rows]


def worksheet_fieldnames() -> List[str]:
    base = [
        "ticker",
        "name",
        "sector",
        "priority",
        "recommendation",
        "research_tier",
        "research_status",
        "gate_passed",
        "gate_fail_reasons",
        "missing_critical_fields",
        "selection_score",
        "potential_score",
        "valuation_gap_score",
        "value_trap_score",
        "analysis_caveat",
        "worksheet_status",
    ]
    return base + [key for key, _ in WORKSHEET_PROMPTS]
