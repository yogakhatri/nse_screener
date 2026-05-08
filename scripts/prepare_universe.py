#!/usr/bin/env python3
"""
Prepare a dated NSE universe Screener CSV for engine runs.

Phase 1 objective:
1) Build a daily symbol universe from a local NSE universe CSV.
2) Optionally merge a fundamentals CSV onto that universe.
3) Write a dated Screener CSV the engine can consume directly.
4) Emit a prep report with match/coverage diagnostics.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.bootstrap import TEMPLATE_COLUMNS
from scripts.local_storage import ensure_folders


SYMBOL_ALIASES = ["NSE Symbol", "SYMBOL", "Symbol", "Ticker", "ticker"]
NAME_ALIASES = ["Name", "Company Name", "NAME"]
SERIES_ALIASES = ["SERIES", "Series", "series"]
MACRO_SECTOR_ALIASES = ["Macro Sector", "Macro", "MacroSector"]
SECTOR_ALIASES = ["Sector", "SECTOR"]
INDUSTRY_ALIASES = ["Industry", "INDUSTRY"]
BASIC_INDUSTRY_ALIASES = ["Basic Industry", "BasicIndustry", "BASIC INDUSTRY"]
CLASSIFICATION_SOURCE_ALIASES = ["Classification Source", "ClassificationSource"]
CLASSIFICATION_CONFIDENCE_ALIASES = ["Classification Confidence", "ClassificationConfidence"]


def _norm(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _find_col(columns: Iterable[str], aliases: Iterable[str]) -> str | None:
    normalized = {_norm(c): c for c in columns}
    for alias in aliases:
        hit = normalized.get(_norm(alias))
        if hit:
            return hit
    return None


def _clean_symbol(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "-"}:
        return ""
    return text.replace(".NS", "")


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError as exc:
        raise RuntimeError(
            "\n".join(
                [
                    f"CSV is empty: {path}",
                    "Add a header row + data rows, or run without --fundamentals-csv for debug flow.",
                ]
            )
        ) from exc
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"Unable to read CSV: {path} ({exc})") from exc


def _parse_args() -> argparse.Namespace:
    today = dt.date.today().isoformat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=today, help="Run date YYYY-MM-DD")
    parser.add_argument(
        "--universe-csv",
        default=None,
        help="CSV containing NSE symbols (required if --output-csv does not already exist)",
    )
    parser.add_argument(
        "--fundamentals-csv",
        default=None,
        help="Optional full fundamentals CSV to merge on symbol",
    )
    parser.add_argument(
        "--classification-csv",
        default=None,
        help="Optional classification master CSV to merge before fundamentals fallback",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Output Screener CSV path (default: data/raw/fundamentals/screener/screener_export_<date>.csv)",
    )
    parser.add_argument(
        "--report-json",
        default=None,
        help="Prep diagnostics output (default: data/processed/universe/universe_prep_<date>.json)",
    )
    parser.add_argument(
        "--missing-symbols-csv",
        default=None,
        help="Optional output for symbols missing fundamentals merge",
    )
    parser.add_argument(
        "--include-non-eq",
        action="store_true",
        help="Include non-EQ series symbols when a series column is available",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite output files if they already exist",
    )
    return parser.parse_args()


def _build_universe_frame(df: pd.DataFrame, include_non_eq: bool) -> pd.DataFrame:
    symbol_col = _find_col(df.columns, SYMBOL_ALIASES)
    if not symbol_col:
        raise RuntimeError("Universe CSV missing symbol column. Expected one of: " + ", ".join(SYMBOL_ALIASES))

    series_col = _find_col(df.columns, SERIES_ALIASES)
    if series_col and not include_non_eq:
        df = df[df[series_col].astype(str).str.strip().str.upper() == "EQ"]

    out = pd.DataFrame()
    out["NSE Symbol"] = df[symbol_col].map(_clean_symbol)

    for canonical, aliases in [
        ("Name", NAME_ALIASES),
        ("Macro Sector", MACRO_SECTOR_ALIASES),
        ("Sector", SECTOR_ALIASES),
        ("Industry", INDUSTRY_ALIASES),
        ("Basic Industry", BASIC_INDUSTRY_ALIASES),
        ("Classification Source", CLASSIFICATION_SOURCE_ALIASES),
        ("Classification Confidence", CLASSIFICATION_CONFIDENCE_ALIASES),
    ]:
        col = _find_col(df.columns, aliases)
        out[canonical] = df[col].astype(str).str.strip() if col else ""

    out = out[out["NSE Symbol"] != ""]
    out = out.drop_duplicates(subset=["NSE Symbol"], keep="first")
    out = out.sort_values("NSE Symbol").reset_index(drop=True)
    return out


def _merge_classification(universe_df: pd.DataFrame, classification_path: Path) -> tuple[pd.DataFrame, int]:
    """
    Merge the local classification master onto the daily universe.

    The classification master is the preferred source of truth because it is
    stable across runs and protects the engine from stale fundamentals files
    carrying the wrong sector taxonomy.
    """
    class_df = _read_csv(classification_path)
    symbol_col = _find_col(class_df.columns, SYMBOL_ALIASES)
    if not symbol_col:
        raise RuntimeError(
            f"Classification CSV missing symbol column ({classification_path}). "
            f"Expected one of: {', '.join(SYMBOL_ALIASES)}"
        )

    class_df = class_df.copy()
    class_df["__symbol"] = class_df[symbol_col].map(_clean_symbol)
    class_df = class_df[class_df["__symbol"] != ""]
    class_df = class_df.drop_duplicates(subset=["__symbol"], keep="first")

    rename_map = {}
    for canonical, aliases in [
        ("Name", NAME_ALIASES),
        ("Macro Sector", MACRO_SECTOR_ALIASES),
        ("Sector", SECTOR_ALIASES),
        ("Industry", INDUSTRY_ALIASES),
        ("Basic Industry", BASIC_INDUSTRY_ALIASES),
        ("Classification Source", CLASSIFICATION_SOURCE_ALIASES),
        ("Classification Confidence", CLASSIFICATION_CONFIDENCE_ALIASES),
    ]:
        hit = _find_col(class_df.columns, aliases)
        if hit:
            rename_map[hit] = f"class__{canonical}"

    class_df = class_df.rename(columns=rename_map)
    keep_cols = ["__symbol"] + [col for col in class_df.columns if col.startswith("class__")]
    class_df = class_df[keep_cols]
    class_df = class_df.loc[:, ~class_df.columns.duplicated()]

    merged = universe_df.copy()
    merged["__symbol"] = merged["NSE Symbol"]
    merged = merged.merge(class_df, how="left", on="__symbol")

    missing_tokens = {"", "nan", "none", "-", "None"}
    for canonical in [
        "Name",
        "Macro Sector",
        "Sector",
        "Industry",
        "Basic Industry",
        "Classification Source",
        "Classification Confidence",
    ]:
        class_col = f"class__{canonical}"
        if class_col not in merged.columns:
            continue
        if canonical not in merged.columns:
            merged[canonical] = ""
        merged[canonical] = merged[canonical].where(
            ~merged[canonical].astype(str).str.strip().isin(missing_tokens),
            merged[class_col].astype(str),
        )

    matched = 0
    match_col = "class__Sector" if "class__Sector" in merged.columns else None
    if match_col:
        for value in merged[match_col].astype(str):
            if value.strip() not in missing_tokens:
                matched += 1

    drop_cols = [col for col in merged.columns if col.startswith("class__")]
    merged = merged.drop(columns=drop_cols + ["__symbol"])
    return merged, matched


def _merge_fundamentals(universe_df: pd.DataFrame, fundamentals_path: Path) -> tuple[pd.DataFrame, int]:
    fund = _read_csv(fundamentals_path)
    symbol_col = _find_col(fund.columns, SYMBOL_ALIASES)
    if not symbol_col:
        raise RuntimeError(
            f"Fundamentals CSV missing symbol column ({fundamentals_path}). "
            f"Expected one of: {', '.join(SYMBOL_ALIASES)}"
        )

    fund = fund.copy()
    fund["__symbol"] = fund[symbol_col].map(_clean_symbol)
    fund = fund[fund["__symbol"] != ""]
    fund = fund.drop_duplicates(subset=["__symbol"], keep="first")
    fund = fund.drop(columns=[symbol_col], errors="ignore")

    # Prefix fundamentals columns once to avoid merge-name collisions.
    def _prefixed(col: str) -> str:
        base = str(col)
        while base.startswith("fund__"):
            base = base[len("fund__"):]
        return f"fund__{base}"

    prefixed_map = {col: _prefixed(col) for col in fund.columns if col != "__symbol"}
    fund = fund.rename(columns=prefixed_map)
    fund = fund.loc[:, ~fund.columns.duplicated()]

    base = universe_df.copy()
    base["__symbol"] = base["NSE Symbol"]

    merged = base.merge(fund, how="left", on="__symbol")

    missing_tokens = {"", "nan", "none", "-", "None"}

    # Prefer non-empty universe classification fields; fallback to fundamentals fields.
    for canonical in ["Name", "Macro Sector", "Sector", "Industry", "Basic Industry"]:
        fallback = f"fund__{canonical}"
        if fallback not in merged.columns:
            continue
        if canonical in {"Macro Sector", "Sector", "Industry", "Basic Industry"}:
            fallback_mask = merged[canonical].astype(str).str.strip().isin(missing_tokens)
            if "Classification Source" not in merged.columns:
                merged["Classification Source"] = ""
            if "Classification Confidence" not in merged.columns:
                merged["Classification Confidence"] = ""
            merged.loc[fallback_mask, "Classification Source"] = merged.loc[
                fallback_mask, "Classification Source"
            ].replace({"": "fundamentals_csv"}).fillna("fundamentals_csv")
            merged.loc[fallback_mask, "Classification Confidence"] = merged.loc[
                fallback_mask, "Classification Confidence"
            ].replace({"": "Low"}).fillna("Low")
        merged[canonical] = merged[canonical].where(
            ~merged[canonical].astype(str).str.strip().isin(missing_tokens),
            merged[fallback].astype(str),
        )

    if "Fundamentals Source" not in merged.columns:
        merged["Fundamentals Source"] = ""
    merged["Fundamentals Source"] = "fundamentals_csv"

    # Populate canonical Screener metric columns from fundamentals when universe values are blank.
    # This ensures downstream loader reads expected headers instead of fund__* columns.
    for canonical in TEMPLATE_COLUMNS:
        if canonical in {"NSE Symbol", "Name", "Macro Sector", "Sector", "Industry", "Basic Industry"}:
            continue
        fallback = f"fund__{canonical}"
        if fallback not in merged.columns:
            continue
        if canonical not in merged.columns:
            merged[canonical] = ""
        merged[canonical] = merged[canonical].where(
            ~merged[canonical].astype(str).str.strip().isin(missing_tokens),
            merged[fallback].astype(str),
        )

    matched = 0
    fund_cols = [c for c in fund.columns if c != "__symbol"]
    if fund_cols:
        for _, row in merged[fund_cols].iterrows():
            if any(str(v).strip() not in missing_tokens for v in row):
                matched += 1

    merged = merged.drop(columns=["__symbol"])
    return merged, matched


def _finalize_output(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["NSE Symbol"] = out["NSE Symbol"].map(_clean_symbol)
    out = out[out["NSE Symbol"] != ""]
    out = out.drop_duplicates(subset=["NSE Symbol"], keep="first")

    for col in TEMPLATE_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    for col, default in [
        ("Classification Source", ""),
        ("Classification Confidence", ""),
        ("Fundamentals Source", ""),
    ]:
        if col not in out.columns:
            out[col] = default

    ordered = TEMPLATE_COLUMNS + [c for c in out.columns if c not in TEMPLATE_COLUMNS]
    out = out[ordered]
    out = out.sort_values("NSE Symbol").reset_index(drop=True)
    return out


def main() -> None:
    args = _parse_args()
    run_date = dt.date.fromisoformat(args.date)

    output_csv = Path(args.output_csv) if args.output_csv else Path(
        f"data/raw/fundamentals/screener/screener_export_{run_date.isoformat()}.csv"
    )
    report_json = Path(args.report_json) if args.report_json else Path(
        f"data/processed/universe/universe_prep_{run_date.isoformat()}.json"
    )
    missing_symbols_csv = Path(args.missing_symbols_csv) if args.missing_symbols_csv else Path(
        f"data/processed/universe/missing_fundamentals_{run_date.isoformat()}.csv"
    )

    ensure_folders()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    missing_symbols_csv.parent.mkdir(parents=True, exist_ok=True)

    if output_csv.exists() and not args.force:
        raise RuntimeError(f"Output exists: {output_csv}. Use --force to overwrite.")
    if report_json.exists() and not args.force:
        raise RuntimeError(f"Report exists: {report_json}. Use --force to overwrite.")

    if not args.universe_csv:
        raise RuntimeError(
            "Missing --universe-csv. Provide a daily NSE symbols file "
            "(for example data/raw/universe/nse_symbols_<date>.csv)."
        )

    universe_path = Path(args.universe_csv)
    if not universe_path.exists():
        raise FileNotFoundError(f"Universe CSV not found: {universe_path}")

    universe_df = _build_universe_frame(_read_csv(universe_path), include_non_eq=args.include_non_eq)
    if universe_df.empty:
        raise RuntimeError("Universe CSV resolved to 0 symbols after cleaning/filters.")

    classification_rows = 0
    matched_rows = 0
    full_df = universe_df.copy()
    if args.classification_csv:
        classification_path = Path(args.classification_csv)
        if not classification_path.exists():
            raise FileNotFoundError(f"Classification CSV not found: {classification_path}")
        full_df, classification_rows = _merge_classification(full_df, classification_path)
    if args.fundamentals_csv:
        fundamentals_path = Path(args.fundamentals_csv)
        if not fundamentals_path.exists():
            raise FileNotFoundError(f"Fundamentals CSV not found: {fundamentals_path}")
        full_df, matched_rows = _merge_fundamentals(full_df, fundamentals_path)

    final_df = _finalize_output(full_df)
    final_df.to_csv(output_csv, index=False)

    if args.fundamentals_csv:
        metric_cols = [
            c for c in final_df.columns
            if c not in {
                "NSE Symbol",
                "Name",
                "Macro Sector",
                "Sector",
                "Industry",
                "Basic Industry",
                "Classification Source",
                "Classification Confidence",
                "Fundamentals Source",
            }
        ]
        missing_tokens = {"", "nan", "none", "-", "None"}
        missing_mask = []
        for _, row in final_df[metric_cols].iterrows():
            missing_mask.append(not any(str(v).strip() not in missing_tokens for v in row))
        missing_df = final_df.loc[missing_mask, ["NSE Symbol", "Name", "Sector", "Industry", "Basic Industry"]]
        missing_df.to_csv(missing_symbols_csv, index=False)
    else:
        pd.DataFrame(columns=["NSE Symbol", "Name", "Sector", "Industry", "Basic Industry"]).to_csv(
            missing_symbols_csv, index=False
        )

    report = {
        "run_date": run_date.isoformat(),
        "universe_csv": str(universe_path),
        "classification_csv": str(args.classification_csv) if args.classification_csv else None,
        "fundamentals_csv": str(args.fundamentals_csv) if args.fundamentals_csv else None,
        "output_csv": str(output_csv),
        "missing_symbols_csv": str(missing_symbols_csv),
        "n_universe_symbols": int(len(universe_df)),
        "n_output_rows": int(len(final_df)),
        "n_classification_matched": int(classification_rows),
        "classification_match_pct": round((classification_rows / len(universe_df) * 100.0), 2) if len(universe_df) else 0.0,
        "n_fundamentals_matched": int(matched_rows),
        "fundamentals_match_pct": round((matched_rows / len(universe_df) * 100.0), 2) if len(universe_df) else 0.0,
        "include_non_eq": bool(args.include_non_eq),
    }
    with open(report_json, "w") as f:
        json.dump(report, f, indent=2)

    print("Universe prep complete.")
    print(f"Output: {output_csv}")
    print(f"Report: {report_json}")
    if args.fundamentals_csv:
        print(f"Missing fundamentals list: {missing_symbols_csv}")
    print(f"Symbols prepared: {len(final_df)}")


if __name__ == "__main__":
    main()
