"""
Policy and macro theme tags for sector-aware shortlisting.

Themes narrow the universe; they never bypass red-flag or investability gates.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Set, Tuple

# theme_id -> (label, sector keywords matched case-insensitively against NSE sector names)
POLICY_THEMES: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "infrastructure": (
        "Infrastructure & capex cycle",
        ("construction", "infrastructure", "cement", "capital goods", "engineering"),
    ),
    "defence": (
        "Defence & aerospace",
        ("aerospace", "defence", "defense"),
    ),
    "renewables": (
        "Renewables & power transition",
        ("power", "renewable", "solar", "wind", "utilities"),
    ),
    "manufacturing_pli": (
        "Manufacturing & PLI beneficiaries",
        ("automobile", "auto", "chemical", "metal", "steel", "electronics"),
    ),
    "digital_india": (
        "Digital & IT services",
        ("information technology", "it ", "software", "technology"),
    ),
    "financial_inclusion": (
        "Financial inclusion (banks & NBFC)",
        ("financial services", "bank", "nbfc", "housing finance"),
    ),
    "healthcare": (
        "Healthcare & pharma",
        ("healthcare", "pharma", "hospital", "diagnostic"),
    ),
}

VALID_POLICY_THEMES = frozenset(POLICY_THEMES.keys())


def normalize_policy_themes(value: str | Iterable[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = [p.strip().lower() for p in value.split(",")]
    else:
        parts = [str(p).strip().lower() for p in value]
    unknown = sorted({p for p in parts if p and p not in VALID_POLICY_THEMES})
    if unknown:
        raise ValueError(
            f"Unknown policy_themes: {unknown}. Valid: {sorted(VALID_POLICY_THEMES)}"
        )
    return tuple(sorted({p for p in parts if p}))


def sector_matches_themes(sector: str, themes: Iterable[str]) -> bool:
    """Return True if sector text matches any configured theme keyword set."""
    if not themes:
        return True
    sector_l = (sector or "").strip().lower()
    if not sector_l:
        return False
    for theme_id in themes:
        _, keywords = POLICY_THEMES[theme_id]
        if any(kw in sector_l for kw in keywords):
            return True
    return False


def active_theme_labels(themes: Iterable[str]) -> List[str]:
    return [POLICY_THEMES[t][0] for t in themes if t in POLICY_THEMES]


def policy_theme_boost(sector: str, themes: Iterable[str]) -> float:
    """Small ranking boost (0–5 points) when sector aligns with active themes."""
    if not themes:
        return 0.0
    return 5.0 if sector_matches_themes(sector, themes) else 0.0
