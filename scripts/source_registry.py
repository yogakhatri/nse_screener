#!/usr/bin/env python3
"""
Source registry for public-data research runs.

The registry records source freshness, file counts, hashes, and lightweight
quality metadata so downstream scoring can distinguish strong data from stale,
missing, or fallback data.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP, file_hash


@dataclass(frozen=True)
class SourceSpec:
    """Contract for a public data source consumed by the engine."""

    source_id: str
    category: str
    path: Path
    source_type: str
    pattern: str = "*"
    required: bool = False
    max_age_days: int = 7
    expected_min_rows: int = 0


@dataclass
class SourceRecord:
    """Normalized runtime status for one source input."""

    source_id: str
    category: str
    source_type: str
    path: str
    status: str
    required: bool
    file_count: int = 0
    latest_file: str = ""
    latest_mtime: str = ""
    data_date: str = ""
    age_days: Optional[int] = None
    size_bytes: int = 0
    hash: str = ""
    rows: Optional[int] = None
    columns: Optional[int] = None
    quality_status: str = "unknown"
    notes: str = ""


def _dated_path(template: str, run_date: date) -> Path:
    """Expand a run-date path template."""
    return Path(template.format(date=run_date.isoformat()))


def default_source_specs(run_date: date, screener_csv: Optional[Path] = None) -> list[SourceSpec]:
    """Return the standard public-source registry contract for a run."""
    screener_path = screener_csv or _dated_path(
        "data/raw/fundamentals/screener/screener_export_{date}.csv", run_date
    )
    return [
        SourceSpec(
            source_id="screener_export",
            category="fundamentals",
            path=screener_path,
            source_type="file",
            required=True,
            max_age_days=7,
            expected_min_rows=250,
        ),
        SourceSpec(
            source_id="nse_universe",
            category="universe",
            path=_dated_path("data/raw/universe/nse_symbols_{date}.csv", run_date),
            source_type="file",
            required=True,
            max_age_days=7,
            expected_min_rows=250,
        ),
        SourceSpec(
            source_id="classification_master",
            category="classification",
            path=FOLDER_MAP["classification"] / "nse_symbol_classification_master.csv",
            source_type="file",
            required=True,
            max_age_days=30,
            expected_min_rows=250,
        ),
        SourceSpec(
            source_id="nse_bhavcopy_history",
            category="prices",
            path=FOLDER_MAP["bhavcopy"],
            source_type="directory",
            pattern="*.zip",
            required=True,
            max_age_days=10,
        ),
        SourceSpec(
            source_id="nse_delivery",
            category="delivery",
            path=FOLDER_MAP["delivery"],
            source_type="directory",
            pattern="*.zip",
            required=False,
            max_age_days=14,
        ),
        SourceSpec(
            source_id="nse_indices",
            category="indices",
            path=FOLDER_MAP["indices"],
            source_type="directory",
            pattern="*.csv",
            required=False,
            max_age_days=14,
        ),
        SourceSpec(
            source_id="asm_list",
            category="redflags",
            path=FOLDER_MAP["asm"],
            source_type="directory",
            pattern="*.csv",
            required=False,
            max_age_days=14,
        ),
        SourceSpec(
            source_id="gsm_list",
            category="redflags",
            path=FOLDER_MAP["gsm"],
            source_type="directory",
            pattern="*.csv",
            required=False,
            max_age_days=14,
        ),
        SourceSpec(
            source_id="shareholding",
            category="ownership",
            path=FOLDER_MAP["shareholding"],
            source_type="directory",
            pattern="*.csv",
            required=False,
            max_age_days=120,
        ),
        SourceSpec(
            source_id="governance_events",
            category="redflags",
            path=FOLDER_MAP["governance"],
            source_type="directory",
            pattern="*.csv",
            required=False,
            max_age_days=120,
        ),
        SourceSpec(
            source_id="financial_asset_quality",
            category="fundamentals",
            path=FOLDER_MAP["financial_risk"],
            source_type="directory",
            pattern="*.csv",
            required=False,
            max_age_days=120,
        ),
        SourceSpec(
            source_id="yahoo_price_cache",
            category="prices",
            path=FOLDER_MAP["yfinance"],
            source_type="directory",
            pattern="*",
            required=False,
            max_age_days=14,
        ),
    ]


def _csv_shape(path: Path) -> tuple[Optional[int], Optional[int], str]:
    """Return CSV row/column counts without making the registry fatal."""
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        return None, None, f"csv_read_error={exc}"
    return int(len(df)), int(len(df.columns)), ""


def _infer_source_data_date(path: Path) -> Optional[date]:
    """Infer the market/source date from common NSE/Screener filenames."""
    name = path.name
    iso_match = re.search(r"(20\d{2}-\d{2}-\d{2})", name)
    if iso_match:
        try:
            return date.fromisoformat(iso_match.group(1))
        except ValueError:
            pass

    nse_match = re.search(r"BhavCopy_NSE_CM_0_0_0_(\d{8})_F_0000", name, re.IGNORECASE)
    if nse_match:
        try:
            return datetime.strptime(nse_match.group(1), "%Y%m%d").date()
        except ValueError:
            pass

    legacy_match = re.search(r"cm(\d{2}[A-Z]{3}\d{4})bhav", name, re.IGNORECASE)
    if legacy_match:
        try:
            return datetime.strptime(legacy_match.group(1).upper(), "%d%b%Y").date()
        except ValueError:
            pass

    index_match = re.search(r"ind_close_all_(\d{2}[A-Za-z]{3}\d{4})", name)
    if index_match:
        try:
            return datetime.strptime(index_match.group(1), "%d%b%Y").date()
        except ValueError:
            pass

    return None


def _status_from_age(required: bool, exists: bool, age_days: Optional[int], max_age_days: int) -> str:
    """Classify freshness status for a source."""
    if not exists:
        return "missing_required" if required else "missing_optional"
    if age_days is not None and age_days > max_age_days:
        return "stale_required" if required else "stale_optional"
    return "ok"


def inspect_source(spec: SourceSpec, now: Optional[datetime] = None) -> SourceRecord:
    """Inspect one configured source and return normalized metadata."""
    now = now or datetime.now()
    path = spec.path
    if spec.source_type == "file":
        if not path.exists():
            return SourceRecord(
                source_id=spec.source_id,
                category=spec.category,
                source_type=spec.source_type,
                path=str(path),
                status=_status_from_age(spec.required, False, None, spec.max_age_days),
                required=spec.required,
                quality_status="missing",
                notes="file not found",
            )
        stat = path.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime)
        data_date = _infer_source_data_date(path)
        freshness_date = data_date or mtime.date()
        age_days = max(0, (now.date() - freshness_date).days)
        status = _status_from_age(spec.required, True, age_days, spec.max_age_days)
        rows = columns = None
        notes = ""
        if path.suffix.lower() == ".csv":
            rows, columns, notes = _csv_shape(path)
        if rows is not None and spec.expected_min_rows and rows < spec.expected_min_rows:
            status = "low_coverage_required" if spec.required else "low_coverage_optional"
            notes = (notes + "; " if notes else "") + f"rows {rows} < {spec.expected_min_rows}"
        quality_status = "usable" if status == "ok" else "degraded"
        return SourceRecord(
            source_id=spec.source_id,
            category=spec.category,
            source_type=spec.source_type,
            path=str(path),
            status=status,
            required=spec.required,
            file_count=1,
            latest_file=path.name,
            latest_mtime=mtime.isoformat(timespec="seconds"),
            data_date=data_date.isoformat() if data_date else "",
            age_days=age_days,
            size_bytes=stat.st_size,
            hash=file_hash(path),
            rows=rows,
            columns=columns,
            quality_status=quality_status,
            notes=notes,
        )

    if not path.exists():
        return SourceRecord(
            source_id=spec.source_id,
            category=spec.category,
            source_type=spec.source_type,
            path=str(path),
            status=_status_from_age(spec.required, False, None, spec.max_age_days),
            required=spec.required,
            quality_status="missing",
            notes="directory not found",
        )

    files = [p for p in path.glob(spec.pattern) if p.is_file()]
    if not files:
        return SourceRecord(
            source_id=spec.source_id,
            category=spec.category,
            source_type=spec.source_type,
            path=str(path),
            status=_status_from_age(spec.required, False, None, spec.max_age_days),
            required=spec.required,
            quality_status="missing",
            notes=f"no files matching {spec.pattern}",
        )

    latest = max(
        files,
        key=lambda p: (
            _infer_source_data_date(p) or date.min,
            datetime.fromtimestamp(p.stat().st_mtime),
        ),
    )
    stat = latest.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime)
    data_date = _infer_source_data_date(latest)
    freshness_date = data_date or mtime.date()
    age_days = max(0, (now.date() - freshness_date).days)
    status = _status_from_age(spec.required, True, age_days, spec.max_age_days)
    quality_status = "usable" if status == "ok" else "degraded"
    return SourceRecord(
        source_id=spec.source_id,
        category=spec.category,
        source_type=spec.source_type,
        path=str(path),
        status=status,
        required=spec.required,
        file_count=len(files),
        latest_file=latest.name,
        latest_mtime=mtime.isoformat(timespec="seconds"),
        data_date=data_date.isoformat() if data_date else "",
        age_days=age_days,
        size_bytes=stat.st_size,
        hash=file_hash(latest),
        quality_status=quality_status,
    )


def build_registry(
    run_date: date,
    screener_csv: Optional[Path] = None,
    specs: Optional[Iterable[SourceSpec]] = None,
) -> dict:
    """Build a full source registry document for one engine run."""
    source_specs = list(specs or default_source_specs(run_date, screener_csv))
    as_of = datetime.combine(run_date, time.max)
    records = [inspect_source(spec, now=as_of) for spec in source_specs]
    required_bad = [r for r in records if r.required and r.status != "ok"]
    optional_bad = [r for r in records if not r.required and r.status not in {"ok", "missing_optional"}]
    return {
        "run_date": run_date.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": "blocked" if required_bad else ("degraded" if optional_bad else "ok"),
        "required_blockers": [asdict(r) for r in required_bad],
        "optional_warnings": [asdict(r) for r in optional_bad],
        "sources": [asdict(r) for r in records],
    }


def write_registry(registry: dict, out_json: Path, out_csv: Optional[Path] = None) -> None:
    """Persist registry as JSON plus an analyst-friendly CSV."""
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(registry, indent=2))
    if out_csv is None:
        return
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    rows = registry.get("sources", [])
    fieldnames = list(rows[0].keys()) if rows else ["source_id", "status"]
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build run source registry")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--screener-csv", default=None)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when required sources are degraded")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.date)
    screener_csv = Path(args.screener_csv) if args.screener_csv else None
    registry = build_registry(run_date=run_date, screener_csv=screener_csv)
    out_json = Path(args.output_json) if args.output_json else Path("runs") / run_date.isoformat() / "source_registry.json"
    out_csv = Path(args.output_csv) if args.output_csv else Path("runs") / run_date.isoformat() / "source_registry.csv"
    write_registry(registry, out_json, out_csv)
    print(f"Source registry: {registry['overall_status']} -> {out_json}")
    if args.strict and registry["required_blockers"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
