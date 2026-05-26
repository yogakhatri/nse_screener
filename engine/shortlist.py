"""
Build user-facing Top 3 + Next 5 shortlists from leaderboard rows.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from .policy_themes import policy_theme_boost, sector_matches_themes
from .research_modes import (
    MODE_CONFIG,
    NEXT_TIER_COUNT,
    PERSONA_CONFIG,
    TOP_PICK_COUNT,
    normalize_research_mode,
    normalize_return_persona,
)


def _confidence_rank(label: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get((label or "").lower(), 0)


def _persona_rank_score(row: dict, persona: str) -> float:
    cfg = PERSONA_CONFIG[normalize_return_persona(persona)]
    base = float(row.get("user_profile_score") or row.get("selection_score") or 0.0)
    boost = 0.0
    for metric, delta in cfg.get("rank_boost", {}).items():
        val = row.get(metric)
        if val is not None:
            boost += float(delta) * (float(val) / 100.0)
    themes = row.get("_policy_themes") or ()
    boost += policy_theme_boost(str(row.get("sector", "")), themes) / 100.0 * 5.0
    return base + boost * 100.0


def _persona_filter_reasons(row: dict, persona: str) -> List[str]:
    cfg = PERSONA_CONFIG[normalize_return_persona(persona)]
    reasons: List[str] = []
    if cfg.get("min_roce") is not None:
        roce = row.get("roce_3y_median") or row.get("roe")
        if roce is None or float(roce) < float(cfg["min_roce"]):
            reasons.append(f"ROCE/ROE below persona minimum ({cfg['min_roce']})")
    if cfg.get("min_rev_cagr_3y") is not None:
        g = row.get("rev_cagr_3y")
        if g is None or float(g) < float(cfg["min_rev_cagr_3y"]):
            reasons.append(f"Revenue CAGR below persona minimum ({cfg['min_rev_cagr_3y']})")
    if cfg.get("min_red_flags_score") is not None:
        rf = row.get("red_flags")
        if rf is None or float(rf) < float(cfg["min_red_flags_score"]):
            reasons.append("Red flags score below compounder minimum")
    if cfg.get("min_dividend_yield") is not None:
        dy = row.get("dividend_yield")
        if dy is None or float(dy) < float(cfg["min_dividend_yield"]):
            reasons.append("Dividend yield below steady-income minimum")
    if cfg.get("max_debt_to_equity") is not None:
        dte = row.get("debt_to_equity")
        if dte is not None and float(dte) > float(cfg["max_debt_to_equity"]):
            reasons.append("Debt/equity above steady-income maximum")
    return reasons


def _mode_eligible(row: dict, mode: str, policy_themes: tuple[str, ...]) -> tuple[bool, str]:
    cfg = MODE_CONFIG[normalize_research_mode(mode)]
    mode_key = normalize_research_mode(mode)
    tier = row.get("research_tier", "")
    if tier not in cfg["allowed_tiers"]:
        if not (
            mode_key == "thematic"
            and tier == "Data Incomplete"
            and row.get("recommendation") in {"Buy Candidate", "Watchlist"}
        ):
            return False, f"research_tier={tier}"
    if cfg.get("require_gate_passed") and not row.get("gate_passed"):
        return False, "gate not passed"
    if _confidence_rank(row.get("confidence", "Low")) < int(cfg["min_confidence_rank"]):
        return False, "confidence too low"
    if cfg.get("require_policy_theme_match") and policy_themes:
        if not sector_matches_themes(str(row.get("sector", "")), policy_themes):
            return False, "sector not in active policy themes"
    if row.get("recommendation") not in {"Buy Candidate", "Watchlist"}:
        return False, f"recommendation={row.get('recommendation')}"
    if not row.get("template_supported", True):
        if mode_key == "high_conviction":
            return False, "template unsupported"
        if mode_key == "research_shortlist":
            return False, "template unsupported — complete growth/profitability data first"
    if row.get("research_status") == "Unsupported":
        return False, "research_status unsupported"
    return True, ""


def _enrich_row_for_shortlist(row: dict, policy_themes: tuple[str, ...]) -> dict:
    out = dict(row)
    out["_policy_themes"] = policy_themes
    return out


def build_top_picks(
    rows: List[dict],
    *,
    research_mode: str = "research_shortlist",
    return_persona: str = "quality_value",
    policy_themes: tuple[str, ...] = (),
    max_primary: int = TOP_PICK_COUNT,
    max_secondary: int = NEXT_TIER_COUNT,
) -> Dict[str, Any]:
    """
    Return primary (Top N), secondary (next tier), and diagnostic summary.
    """
    mode = normalize_research_mode(research_mode)
    persona = normalize_return_persona(return_persona)
    mode_cfg = MODE_CONFIG[mode]

    candidates: List[dict] = []
    rejected: List[dict] = []
    for row in rows:
        if not row.get("user_filter_passed", True):
            continue
        enriched = _enrich_row_for_shortlist(row, policy_themes)
        ok, reason = _mode_eligible(enriched, mode, policy_themes)
        persona_reasons = _persona_filter_reasons(enriched, persona)
        if not ok:
            rejected.append({**enriched, "shortlist_reject_reason": reason})
            continue
        if persona_reasons and mode != "thematic":
            rejected.append({**enriched, "shortlist_reject_reason": "; ".join(persona_reasons)})
            continue
        enriched["shortlist_rank_score"] = round(_persona_rank_score(enriched, persona), 2)
        enriched["shortlist_persona_notes"] = persona_reasons
        if not enriched.get("template_supported", True):
            enriched["shortlist_caveat"] = (
                (enriched.get("shortlist_caveat") or "")
                + " Template coverage incomplete — verify growth/profitability metrics. "
            ).strip()
        if enriched.get("research_tier") in {"Unsupported", "Data Incomplete"}:
            enriched["shortlist_caveat"] = (
                (enriched.get("shortlist_caveat") or "")
                + f" Tier={enriched.get('research_tier')} — manual verification required."
            ).strip()
        candidates.append(enriched)

    candidates.sort(
        key=lambda r: (
            r.get("shortlist_rank_score") or 0.0,
            r.get("selection_score") or 0.0,
        ),
        reverse=True,
    )

    sector_counts: Dict[str, int] = defaultdict(int)
    primary: List[dict] = []
    secondary: List[dict] = []

    for row in candidates:
        sector = row.get("sector", "Unknown")
        if len(primary) < max_primary and sector_counts[sector] < 2:
            primary.append(row)
            sector_counts[sector] += 1
        elif len(secondary) < max_secondary:
            secondary.append(row)

    # Thematic mode: if primary empty, allow data-incomplete with caveat in secondary first
    if not primary and mode == "thematic" and mode_cfg.get("allow_data_incomplete_primary"):
        for row in sorted(
            [r for r in rows if r.get("user_filter_passed", True)],
            key=lambda r: r.get("selection_score") or 0.0,
            reverse=True,
        ):
            if not policy_themes or sector_matches_themes(str(row.get("sector", "")), policy_themes):
                if row.get("research_tier") == "Data Incomplete" and len(primary) < max_primary:
                    primary.append(
                        _enrich_row_for_shortlist(
                            {
                                **row,
                                "shortlist_rank_score": row.get("selection_score"),
                                "shortlist_caveat": "Data incomplete — verify pledge/governance before investing",
                            },
                            policy_themes,
                        )
                    )

    summary = {
        "research_mode": mode,
        "return_persona": persona,
        "policy_themes": list(policy_themes),
        "mode_label": mode_cfg["label"],
        "primary_count": len(primary),
        "secondary_count": len(secondary),
        "candidates_considered": len(candidates),
        "rejected_count": len(rejected),
        "empty_reason": _empty_reason(primary, candidates, rows, policy_themes),
    }
    return {
        "primary": primary,
        "secondary": secondary,
        "summary": summary,
        "rejected_sample": rejected[:10],
    }


def _empty_reason(
    primary: List[dict],
    candidates: List[dict],
    all_rows: List[dict],
    policy_themes: tuple[str, ...],
) -> Optional[str]:
    if primary:
        return None
    if not all_rows:
        return "No stocks in leaderboard."
    filtered = [r for r in all_rows if r.get("user_filter_passed", True)]
    if not filtered:
        return "User profile filters removed all stocks; relax profile thresholds."
    if not candidates:
        missing = []
        for r in filtered[:50]:
            if r.get("missing_critical_fields"):
                missing.extend(str(r.get("missing_critical_fields")).split("; "))
        if missing:
            top = sorted({m.strip() for m in missing if m.strip()})[:5]
            return (
                "No names passed mode/persona gates. Most common missing evidence: "
                + ", ".join(top)
                + ". Run enrichment (pledge, governance, financial risk) or use research_shortlist mode."
            )
        if policy_themes:
            return "No stocks matched active policy themes in the filtered universe."
        return "No stocks passed research mode gates; try research_shortlist mode or complete enrichment CSVs."
    return "Sector caps filled primary slots; see secondary tier."
