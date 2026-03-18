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
    ]:
        col = _find_col(df.columns, aliases)
        out[canonical] = df[col].astype(str).str.strip() if col else ""

    out = out[out["NSE Symbol"] != ""]
    out = out.drop_duplicates(subset=["NSE Symbol"], keep="first")
    out = out.sort_values("NSE Symbol").reset_index(drop=True)
    return out


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
        merged[canonical] = merged[canonical].where(
            ~merged[canonical].astype(str).str.strip().isin(missing_tokens),
            merged[fallback].astype(str),
        )

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

    matched_rows = 0
    full_df = universe_df.copy()
    if args.fundamentals_csv:
        fundamentals_path = Path(args.fundamentals_csv)
        if not fundamentals_path.exists():
            raise FileNotFoundError(f"Fundamentals CSV not found: {fundamentals_path}")
        full_df, matched_rows = _merge_fundamentals(universe_df, fundamentals_path)

    final_df = _finalize_output(full_df)
    final_df.to_csv(output_csv, index=False)

    if args.fundamentals_csv:
        metric_cols = [c for c in final_df.columns if c not in {"NSE Symbol", "Name", "Macro Sector", "Sector", "Industry", "Basic Industry"}]
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
        "fundamentals_csv": str(args.fundamentals_csv) if args.fundamentals_csv else None,
        "output_csv": str(output_csv),
        "missing_symbols_csv": str(missing_symbols_csv),
        "n_universe_symbols": int(len(universe_df)),
        "n_output_rows": int(len(final_df)),
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
