#!/usr/bin/env python3
"""
Data Freshness Monitor
=======================
Validates the freshness and quality of all data sources before engine runs.
Produces a health report that flags stale data, missing files, and quality issues.

Checks:
  - Screener CSV age and completeness
  - Bhavcopy price data recency
  - Universe file freshness
  - Delivery data availability
  - Index data availability
  - ASM/GSM data recency
  - Shareholding data age

Usage:
  python scripts/data_freshness.py --date 2026-03-12
  python scripts/data_freshness.py --date 2026-03-12 --strict
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP


def check_file_freshness(
    file_path: Path,
    max_age_days: int = 7,
) -> dict:
    """Check if a file exists and is fresh enough."""
    if not file_path.exists():
        return {"status": "missing", "path": str(file_path), "age_days": None}

    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    age = (datetime.now() - mtime).days
    size = file_path.stat().st_size

    status = "ok" if age <= max_age_days else "stale"
    if size < 100:
        status = "empty"

    return {
        "status": status,
        "path": str(file_path),
        "age_days": age,
        "size_bytes": size,
        "last_modified": mtime.isoformat(),
    }


def check_csv_quality(csv_path: Path) -> dict:
    """Check CSV file quality: row count, completeness, etc."""
    import pandas as pd

    if not csv_path.exists():
        return {"status": "missing"}

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        return {"status": "error", "error": str(e)}

    total_rows = len(df)
    total_cols = len(df.columns)

    # Check completeness per column
    completeness = {}
    for col in df.columns:
        non_null = df[col].notna().sum()
        completeness[col] = round(non_null / total_rows * 100, 1) if total_rows > 0 else 0

    # Key columns for NSE screener
    key_cols = ["Stock P/E", "OPM", "ROCE 3Yr", "Sales growth 3Years", "CMP"]
    missing_key = [c for c in key_cols if c not in df.columns]
    low_coverage = {c: v for c, v in completeness.items() if v < 50 and c in key_cols}

    return {
        "status": "ok",
        "rows": total_rows,
        "columns": total_cols,
        "missing_key_columns": missing_key,
        "low_coverage_columns": low_coverage,
        "avg_completeness": round(sum(completeness.values()) / len(completeness), 1) if completeness else 0,
    }


def check_directory_freshness(
    dir_path: Path,
    expected_pattern: str = "*",
    max_age_days: int = 7,
) -> dict:
    """Check directory for recent files."""
    if not dir_path.exists():
        return {"status": "missing", "path": str(dir_path), "file_count": 0}

    files = list(dir_path.glob(expected_pattern))
    if not files:
        return {"status": "empty", "path": str(dir_path), "file_count": 0}

    # Find most recent file
    latest = max(files, key=lambda f: f.stat().st_mtime)
    latest_mtime = datetime.fromtimestamp(latest.stat().st_mtime)
    age = (datetime.now() - latest_mtime).days

    return {
        "status": "ok" if age <= max_age_days else "stale",
        "path": str(dir_path),
        "file_count": len(files),
        "latest_file": latest.name,
        "latest_age_days": age,
    }


def run_full_check(run_date: date, strict: bool = False) -> dict:
    """Run all freshness checks and produce a health report."""
    report = {
        "check_date": run_date.isoformat(),
        "check_time": datetime.now().isoformat(),
        "checks": {},
        "overall_status": "ok",
        "warnings": [],
        "errors": [],
    }

    # 1. Screener CSV
    screener_csv = Path(f"data/raw/fundamentals/screener/screener_export_{run_date.isoformat()}.csv")
    enriched_csv = Path(f"data/raw/fundamentals/screener/screener_export_{run_date.isoformat()}_enriched.csv")

    # Try enriched first, then regular
    target_csv = enriched_csv if enriched_csv.exists() else screener_csv
    csv_check = check_file_freshness(target_csv, max_age_days=7)
    csv_quality = check_csv_quality(target_csv)
    report["checks"]["screener_csv"] = {**csv_check, "quality": csv_quality}

    if csv_check["status"] != "ok":
        report["errors"].append(f"Screener CSV: {csv_check['status']}")
    elif csv_quality.get("rows", 0) < 100:
        report["warnings"].append(f"Screener CSV has only {csv_quality.get('rows', 0)} rows")

    # 2. Bhavcopy prices
    bhavcopy_check = check_directory_freshness(
        FOLDER_MAP["bhavcopy"], "*.zip", max_age_days=3,
    )
    report["checks"]["bhavcopy"] = bhavcopy_check
    if bhavcopy_check["status"] != "ok":
        report["warnings"].append(f"Bhavcopy: {bhavcopy_check['status']}")

    # 3. Universe file
    universe_csv = Path(f"data/raw/universe/nse_symbols_{run_date.isoformat()}.csv")
    universe_check = check_file_freshness(universe_csv, max_age_days=7)
    report["checks"]["universe"] = universe_check
    if universe_check["status"] != "ok":
        report["warnings"].append(f"Universe CSV: {universe_check['status']}")

    # 4. Delivery data
    delivery_check = check_directory_freshness(
        FOLDER_MAP["delivery"], "*.zip", max_age_days=7,
    )
    report["checks"]["delivery"] = delivery_check
    if delivery_check["file_count"] == 0:
        report["warnings"].append("No delivery data available")

    # 5. Index data
    index_check = check_directory_freshness(
        FOLDER_MAP["indices"], "ind_close_all_*.csv", max_age_days=7,
    )
    report["checks"]["indices"] = index_check
    if index_check["file_count"] == 0:
        report["warnings"].append("No index data available")

    # 6. ASM/GSM
    asm_check = check_directory_freshness(FOLDER_MAP["asm"], "*.csv", max_age_days=14)
    gsm_check = check_directory_freshness(FOLDER_MAP["gsm"], "*.csv", max_age_days=14)
    report["checks"]["asm"] = asm_check
    report["checks"]["gsm"] = gsm_check

    # 7. Shareholding
    shp_check = check_directory_freshness(FOLDER_MAP["shareholding"], "*.csv", max_age_days=90)
    report["checks"]["shareholding"] = shp_check

    # 8. Classification
    cls_check = check_file_freshness(
        FOLDER_MAP["classification"] / "nse_symbol_classification_master.csv",
        max_age_days=30,
    )
    report["checks"]["classification"] = cls_check

    # Overall status
    if report["errors"]:
        report["overall_status"] = "error"
    elif report["warnings"]:
        report["overall_status"] = "warning"

    return report


def print_report(report: dict) -> None:
    """Print human-readable report to stdout."""
    status_emoji = {
        "ok": "✅", "stale": "⚠️", "missing": "❌", "empty": "❌", "error": "❌", "warning": "⚠️",
    }

    print(f"\n{'='*60}")
    print(f"  Data Freshness Report — {report['check_date']}")
    print(f"{'='*60}")

    overall = report["overall_status"]
    print(f"\n  Overall: {status_emoji.get(overall, '❓')} {overall.upper()}\n")

    for name, check in report["checks"].items():
        status = check.get("status", "unknown")
        emoji = status_emoji.get(status, "❓")
        details = ""

        if "file_count" in check:
            details = f"  ({check['file_count']} files"
            if check.get("latest_age_days") is not None:
                details += f", latest {check['latest_age_days']}d old"
            details += ")"
        elif "age_days" in check and check["age_days"] is not None:
            details = f"  ({check['age_days']}d old)"

        quality = check.get("quality", {})
        if quality.get("rows"):
            details += f"  [{quality['rows']} rows, {quality.get('avg_completeness', 0):.0f}% complete]"

        print(f"  {emoji} {name:20s} {status:8s}{details}")

    if report["warnings"]:
        print(f"\n  Warnings:")
        for w in report["warnings"]:
            print(f"    ⚠️  {w}")

    if report["errors"]:
        print(f"\n  Errors:")
        for e in report["errors"]:
            print(f"    ❌ {e}")

    print(f"\n{'='*60}\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Data freshness monitoring")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--strict", action="store_true", help="Fail on warnings too")
    parser.add_argument("--output", default=None, help="Save report JSON to file")
    return parser.parse_args()


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.date)

    report = run_full_check(run_date, args.strict)
    print_report(report)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, default=str))
        print(f"Report saved: {output_path}")

    # Exit code
    if report["overall_status"] == "error":
        sys.exit(1)
    if args.strict and report["overall_status"] == "warning":
        sys.exit(1)


if __name__ == "__main__":
    main()
