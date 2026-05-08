#!/usr/bin/env python3
"""
Build a local classification master from public scrape artifacts.

Why this exists:
- The daily bhavcopy universe does not carry sector taxonomy.
- Old fundamentals files can carry wrong sector labels and corrupt peer groups.
- A locally maintained master lets us stabilize taxonomy using recent public
  Screener exports and cached company pages.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import ensure_folders


SYMBOL_ALIASES = ["NSE Symbol", "SYMBOL", "Symbol", "Ticker"]
NAME_ALIASES = ["Name", "Company Name"]
MACRO_SECTOR_ALIASES = ["Macro Sector", "Macro"]
SECTOR_ALIASES = ["Sector"]
INDUSTRY_ALIASES = ["Industry"]
BASIC_INDUSTRY_ALIASES = ["Basic Industry", "BasicIndustry"]

MASTER_COLUMNS = [
    "NSE Symbol",
    "Name",
    "Macro Sector",
    "Sector",
    "Industry",
    "Basic Industry",
    "Classification Source",
    "Classification Confidence",
    "Last Seen Date",
]

DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")
MISSING_TOKENS = {"", "nan", "none", "-", "unknown"}
GENERIC_TOKENS = {"diversified", "other", "others", "miscellaneous"}


@dataclass
class ClassificationObservation:
    symbol: str
    name: str
    macro_sector: str
    sector: str
    industry: str
    basic_industry: str
    source: str
    seen_date: str
    weight: int


def _norm(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _find_col(columns: Iterable[str], aliases: Iterable[str]) -> Optional[str]:
    normalized = {_norm(c): c for c in columns}
    for alias in aliases:
        hit = normalized.get(_norm(alias))
        if hit is not None:
            return hit
    return None


def _clean_symbol(value: object) -> str:
    text = str(value or "").strip().upper().replace(".NS", "")
    if not text or text in {"NAN", "NONE", "-"}:
        return ""
    return text


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in MISSING_TOKENS else text


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str).fillna("")


def _classification_tuple(obs: ClassificationObservation) -> Tuple[str, str, str, str]:
    return (
        obs.macro_sector.strip(),
        obs.sector.strip(),
        obs.industry.strip(),
        obs.basic_industry.strip(),
    )


def _is_useful_classification(
    macro_sector: str,
    sector: str,
    industry: str,
    basic_industry: str,
) -> bool:
    values = [macro_sector, sector, industry, basic_industry]
    cleaned = [str(v or "").strip().lower() for v in values]
    if any(value in MISSING_TOKENS for value in cleaned[1:]):
        return False
    if all(value in GENERIC_TOKENS for value in cleaned if value):
        return False
    return True


def _extract_date_hint(path: Path) -> str:
    match = DATE_IN_NAME.search(path.name)
    if match:
        return match.group(1)
    return datetime.fromtimestamp(path.stat().st_mtime).date().isoformat()


def _row_observation(
    row: dict,
    *,
    source: str,
    seen_date: str,
    weight: int,
) -> Optional[ClassificationObservation]:
    symbol = _clean_symbol(
        row.get("NSE Symbol") or row.get("SYMBOL") or row.get("Symbol") or row.get("Ticker")
    )
    if not symbol:
        return None

    obs = ClassificationObservation(
        symbol=symbol,
        name=_clean_text(row.get("Name") or row.get("Company Name")),
        macro_sector=_clean_text(row.get("Macro Sector") or row.get("Macro")),
        sector=_clean_text(row.get("Sector")),
        industry=_clean_text(row.get("Industry")),
        basic_industry=_clean_text(row.get("Basic Industry") or row.get("BasicIndustry")),
        source=source,
        seen_date=seen_date,
        weight=weight,
    )
    if not _is_useful_classification(*_classification_tuple(obs)):
        return None
    return obs


def _csv_observations(csv_path: Path) -> List[ClassificationObservation]:
    if not csv_path.exists():
        return []
    df = _read_csv(csv_path)
    observed: List[ClassificationObservation] = []
    seen_date = _extract_date_hint(csv_path)
    source = f"screener_csv:{csv_path.name}"
    for row in df.to_dict("records"):
        obs = _row_observation(row, source=source, seen_date=seen_date, weight=3)
        if obs is not None:
            observed.append(obs)
    return observed


def _cache_observations(cache_dir: Path) -> List[ClassificationObservation]:
    if not cache_dir.exists():
        return []
    observed: List[ClassificationObservation] = []
    for json_path in sorted(cache_dir.glob("*.json")):
        try:
            payload = json.loads(json_path.read_text())
        except Exception:
            continue
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            continue
        obs = _row_observation(
            data,
            source="screener_cache",
            seen_date=_extract_date_hint(json_path),
            weight=2,
        )
        if obs is not None:
            observed.append(obs)
    return observed


def _existing_master_observations(master_path: Path) -> List[ClassificationObservation]:
    if not master_path.exists():
        return []
    df = _read_csv(master_path)
    observed: List[ClassificationObservation] = []
    for row in df.to_dict("records"):
        obs = _row_observation(
            row,
            source="existing_master",
            seen_date=_clean_text(row.get("Last Seen Date")) or _extract_date_hint(master_path),
            weight=1,
        )
        if obs is not None:
            observed.append(obs)
    return observed


def _pick_best(observations: List[ClassificationObservation]) -> Optional[dict]:
    if not observations:
        return None

    grouped: Dict[Tuple[str, str, str, str], dict] = {}
    for obs in observations:
        key = _classification_tuple(obs)
        bucket = grouped.setdefault(
            key,
            {
                "weight": 0,
                "sources": set(),
                "latest": "0000-00-00",
                "name": "",
            },
        )
        bucket["weight"] += obs.weight
        bucket["sources"].add(obs.source.split(":")[0])
        bucket["latest"] = max(bucket["latest"], obs.seen_date)
        if obs.name:
            bucket["name"] = obs.name

    best_key, best_meta = max(
        grouped.items(),
        key=lambda item: (item[1]["weight"], len(item[1]["sources"]), item[1]["latest"]),
    )
    source_types = sorted(best_meta["sources"])
    if "screener_csv" in source_types and "screener_cache" in source_types:
        confidence = "High"
    elif "screener_csv" in source_types:
        confidence = "High"
    elif len(source_types) >= 2:
        confidence = "Medium"
    else:
        confidence = "Low"

    return {
        "Name": best_meta["name"],
        "Macro Sector": best_key[0],
        "Sector": best_key[1],
        "Industry": best_key[2],
        "Basic Industry": best_key[3],
        "Classification Source": ",".join(source_types),
        "Classification Confidence": confidence,
        "Last Seen Date": best_meta["latest"],
    }


def build_master(
    screener_csvs: Iterable[Path],
    cache_dir: Path,
    existing_master: Optional[Path] = None,
) -> List[dict]:
    """
    Build master rows keyed by NSE symbol.
    """
    by_symbol: Dict[str, List[ClassificationObservation]] = defaultdict(list)
    for csv_path in screener_csvs:
        for obs in _csv_observations(csv_path):
            by_symbol[obs.symbol].append(obs)
    for obs in _cache_observations(cache_dir):
        by_symbol[obs.symbol].append(obs)
    if existing_master is not None:
        for obs in _existing_master_observations(existing_master):
            by_symbol[obs.symbol].append(obs)

    rows: List[dict] = []
    for symbol, observations in sorted(by_symbol.items()):
        best = _pick_best(observations)
        if best is None:
            continue
        rows.append({"NSE Symbol": symbol, **best})
    return rows


def _discover_csvs(explicit_csv: Optional[Path], screener_dir: Path) -> List[Path]:
    if explicit_csv is not None and explicit_csv.exists():
        return [explicit_csv]
    csvs = sorted(
        [
            path for path in screener_dir.glob("screener_export_*.csv")
            if "enriched" not in path.name.lower() and "template" not in path.name.lower()
        ]
    )
    return csvs[-10:]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local NSE classification master from public scrape artifacts.")
    parser.add_argument(
        "--output-csv",
        default="data/raw/classification/nse_symbol_classification_master.csv",
        help="Output classification master path",
    )
    parser.add_argument(
        "--screener-csv",
        default=None,
        help="Optional specific screener export CSV to prioritize",
    )
    parser.add_argument(
        "--screener-dir",
        default="data/raw/fundamentals/screener",
        help="Directory holding dated screener exports",
    )
    parser.add_argument(
        "--cache-dir",
        default="data/raw/fundamentals/screener/cache",
        help="Directory holding per-symbol screener cache JSON",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output if it already exists",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_folders()

    output_csv = Path(args.output_csv)
    screener_dir = Path(args.screener_dir)
    cache_dir = Path(args.cache_dir)
    explicit_csv = Path(args.screener_csv) if args.screener_csv else None

    if output_csv.exists() and not args.force:
        raise RuntimeError(f"Output exists: {output_csv}. Use --force to overwrite.")

    screener_csvs = _discover_csvs(explicit_csv, screener_dir)
    rows = build_master(screener_csvs=screener_csvs, cache_dir=cache_dir, existing_master=output_csv)
    if not rows:
        raise RuntimeError("No usable classification observations found from screener exports or cache.")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=MASTER_COLUMNS).to_csv(output_csv, index=False)

    print(f"Classification master built: {output_csv}")
    print(f"Rows: {len(rows)}")
    if screener_csvs:
        print(f"CSV sources used: {len(screener_csvs)}")
    print(f"Cache dir: {cache_dir}")


if __name__ == "__main__":
    main()
