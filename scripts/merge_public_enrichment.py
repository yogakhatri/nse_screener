#!/usr/bin/env python3
"""
Merge optional public-source enrichment CSVs into the main Screener-style CSV.

The engine treats pledge, governance, and bank/NBFC asset-quality evidence as
critical long-term risk inputs. This script gives those inputs a normalized,
auditable path without forcing one fragile web scraper or paid provider.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Optional

import pandas as pd


SYMBOL_ALIASES = (
    "NSE Symbol",
    "Symbol",
    "Ticker",
    "symbol",
    "nse_symbol",
    "ticker",
)

SHAREHOLDING_COLUMNS: dict[str, tuple[str, ...]] = {
    "Pledged percentage": (
        "Pledged percentage",
        "Promoter Pledge",
        "Promoter Pledge %",
        "pledge_pct",
        "pledged_pct",
        "promoter_pledge_pct",
    ),
    "Promoter Holding %": (
        "Promoter Holding %",
        "Promoter %",
        "Promoter Holdings",
        "promoter_holding_pct",
        "promoter_holding",
    ),
    "Promoter Holding Prev %": (
        "Promoter Holding Prev %",
        "Promoter Prev Quarter %",
        "promoter_holding_prev",
        "promoter_prev_pct",
    ),
    "FII %": (
        "FII %",
        "FII Holding %",
        "FII Holdings",
        "fii_holding_pct",
        "fpi_holding_pct",
    ),
    "DII %": (
        "DII %",
        "DII Holding %",
        "DII Holdings",
        "dii_holding_pct",
    ),
    "MF Holding %": (
        "MF Holding %",
        "Mutual Fund Holding %",
        "MF %",
        "mf_holding_pct",
    ),
}

GOVERNANCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "Governance Events": (
        "Governance Events",
        "Governance Flags",
        "event",
        "event_type",
        "Event Type",
        "Title",
        "Announcement Type",
    ),
    "Governance Risk": (
        "Governance Risk",
        "Governance Event Risk",
        "governance_risk",
    ),
}

FINANCIAL_RISK_COLUMNS: dict[str, tuple[str, ...]] = {
    "GNPA %": ("GNPA %", "Gross NPA %", "GNPA", "gnpa_pct"),
    "NNPA %": ("NNPA %", "Net NPA %", "NNPA", "nnpa_pct"),
    "CAR %": ("CAR %", "CRAR %", "Capital Adequacy", "car_pct"),
    "PCR %": ("PCR %", "Provision Coverage Ratio", "Provision Coverage", "pcr_pct"),
    "NIM": ("NIM", "Net Interest Margin", "nim"),
    "Credit Cost": ("Credit Cost", "Credit Cost Ratio", "credit_cost", "credit_cost_discipline"),
    "Slippage Ratio": ("Slippage Ratio", "Slippages Stress", "slippage_ratio"),
    "ALM ST %": ("ALM ST %", "Short Term Borrowings %", "ALM Mismatch %", "alm_st_pct"),
    "Advances Growth": ("Advances Growth", "Loan Book Growth", "advances_growth"),
    "Deposit Growth": ("Deposit Growth", "deposit_growth"),
    "NII Growth": ("NII Growth", "Net Interest Income Growth", "nii_growth"),
    "Fee Income Growth": ("Fee Income Growth", "Non Interest Income Growth", "fee_income_growth"),
    "Earnings Growth": ("Earnings Growth", "PAT Growth YoY", "earnings_growth"),
    "AUM Growth": ("AUM Growth", "aum_growth"),
    "Cost to Income": ("Cost to Income", "Cost/Income", "Cost-to-Income", "cost_to_income"),
    "Interest Coverage": ("Interest Coverage", "Interest coverage ratio", "ICR", "interest_coverage"),
    "Debt to equity": ("Debt to equity", "Debt/Equity", "D/E", "debt_to_equity"),
    "Credit Rating Grade": ("Credit Rating Grade", "Rating Grade", "credit_rating_grade"),
}

SOURCE_MARKER_COLUMNS = ("Public Enrichment Source", "Public Enrichment Updated At")


@dataclass
class SourceMergeReport:
    """Per-source audit details for enrichment merging."""

    source_id: str
    path: str
    status: str
    input_rows: int = 0
    matched_symbols: int = 0
    missing_symbols: int = 0
    updated_cells: int = 0
    added_columns: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _normalise_header(value: str) -> str:
    """Return a loose comparison key for headers from different public exports."""
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _normalise_symbol(value: object) -> str:
    """Normalize NSE symbols while tolerating Yahoo/BSE-style suffixes."""
    if value is None:
        return ""
    text = str(value).strip().upper()
    if not text:
        return ""
    for suffix in (".NS", ".BO"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _find_column(df: pd.DataFrame, aliases: Iterable[str]) -> Optional[str]:
    """Find the first DataFrame column matching any loose alias."""
    by_key = {_normalise_header(column): column for column in df.columns}
    for alias in aliases:
        column = by_key.get(_normalise_header(alias))
        if column is not None:
            return column
    return None


def _symbol_column(df: pd.DataFrame) -> Optional[str]:
    """Return the symbol column used by an input CSV, if present."""
    return _find_column(df, SYMBOL_ALIASES)


def _is_missing(value: object, *, allow_clean_marker: bool = False) -> bool:
    """Return True for blank placeholder values that should not overwrite evidence."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return True
    lowered = text.lower()
    missing_markers = {"-", "--", "na", "n/a", "nan", "null"}
    if not allow_clean_marker:
        missing_markers.add("none")
    return lowered in missing_markers


def _clean_value(value: object) -> str:
    """Convert a source value into a compact CSV-safe string."""
    text = str(value).strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    return text


def _read_optional_csv(path: Optional[Path], source_id: str) -> tuple[Optional[pd.DataFrame], SourceMergeReport]:
    """Read an optional enrichment file and return a non-fatal source report."""
    if path is None:
        return None, SourceMergeReport(source_id=source_id, path="", status="not_configured")
    report = SourceMergeReport(source_id=source_id, path=str(path), status="missing_optional")
    if not path.exists():
        return None, report
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        report.status = "empty"
        report.notes.append("file has no columns")
        return None, report
    except Exception as exc:  # pragma: no cover - message path is tested through caller behavior.
        report.status = "read_error"
        report.notes.append(str(exc))
        return None, report
    report.status = "loaded"
    report.input_rows = int(len(df))
    if df.empty:
        report.status = "empty"
    return df, report


def _records_by_symbol(
    df: pd.DataFrame,
    mappings: Mapping[str, tuple[str, ...]],
    *,
    source_id: str,
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Extract standard enrichment columns keyed by normalized symbol."""
    notes: list[str] = []
    symbol_col = _symbol_column(df)
    if symbol_col is None:
        return {}, ["missing symbol column"]

    available_columns = {
        standard: _find_column(df, aliases)
        for standard, aliases in mappings.items()
    }
    if not any(available_columns.values()):
        return {}, [f"no supported {source_id} columns found"]

    records: dict[str, dict[str, str]] = {}
    for _, row in df.iterrows():
        symbol = _normalise_symbol(row.get(symbol_col))
        if not symbol:
            continue
        record = records.setdefault(symbol, {})
        for standard, source_column in available_columns.items():
            if source_column is None:
                continue
            value = row.get(source_column)
            if _is_missing(value):
                continue
            record[standard] = _clean_value(value)
    return {symbol: values for symbol, values in records.items() if values}, notes


def _governance_records_by_symbol(df: pd.DataFrame) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Extract governance evidence and preserve explicit clean markers."""
    notes: list[str] = []
    symbol_col = _symbol_column(df)
    if symbol_col is None:
        return {}, ["missing symbol column"]

    event_col = _find_column(df, GOVERNANCE_COLUMNS["Governance Events"])
    risk_col = _find_column(df, GOVERNANCE_COLUMNS["Governance Risk"])
    if event_col is None and risk_col is None:
        return {}, ["no supported governance columns found"]

    events_by_symbol: dict[str, set[str]] = {}
    records: dict[str, dict[str, str]] = {}
    for _, row in df.iterrows():
        symbol = _normalise_symbol(row.get(symbol_col))
        if not symbol:
            continue
        if event_col is not None:
            value = row.get(event_col)
            if not _is_missing(value, allow_clean_marker=True):
                event = _clean_value(value)
                if event.lower() in {"clean", "no events", "no_event", "no-events"}:
                    event = "none"
                events_by_symbol.setdefault(symbol, set()).add(event)
        if risk_col is not None:
            value = row.get(risk_col)
            if not _is_missing(value):
                records.setdefault(symbol, {})["Governance Risk"] = _clean_value(value)

    for symbol, events in events_by_symbol.items():
        ordered = sorted(events, key=lambda item: (item.lower() == "none", item.lower()))
        if "none" in {item.lower() for item in ordered} and len(ordered) > 1:
            ordered = [item for item in ordered if item.lower() != "none"]
        records.setdefault(symbol, {})["Governance Events"] = "; ".join(ordered)
    return {symbol: values for symbol, values in records.items() if values}, notes


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> list[str]:
    """Add missing destination columns and return the list that was added."""
    added: list[str] = []
    for column in columns:
        if column not in df.columns:
            df[column] = ""
            added.append(column)
    return added


def _existing_source_text(value: object) -> str:
    """Normalize the source marker cell before appending a new source id."""
    if _is_missing(value):
        return ""
    return str(value).strip()


def _append_source_marker(existing: object, source_id: str) -> str:
    """Append a source id to the row-level enrichment source marker."""
    current = _existing_source_text(existing)
    parts = [part.strip() for part in current.split(";") if part.strip()]
    if source_id not in parts:
        parts.append(source_id)
    return "; ".join(parts)


def _merge_records(
    base_df: pd.DataFrame,
    records: Mapping[str, Mapping[str, str]],
    *,
    symbol_col: str,
    source_id: str,
    overwrite: bool,
) -> SourceMergeReport:
    """Merge extracted enrichment records into the base Screener CSV."""
    report = SourceMergeReport(source_id=source_id, path="", status="merged")
    destination_columns = sorted({column for values in records.values() for column in values})
    report.added_columns.extend(_ensure_columns(base_df, [*destination_columns, *SOURCE_MARKER_COLUMNS]))

    row_lookup = {
        _normalise_symbol(value): idx
        for idx, value in base_df[symbol_col].items()
        if _normalise_symbol(value)
    }

    timestamp = datetime.now().isoformat(timespec="seconds")
    matched_symbols: set[str] = set()
    for symbol, values in records.items():
        idx = row_lookup.get(symbol)
        if idx is None:
            report.missing_symbols += 1
            continue
        matched_symbols.add(symbol)
        row_updated = False
        for column, value in values.items():
            current = base_df.at[idx, column]
            if overwrite or _is_missing(current):
                if str(current).strip() != str(value).strip():
                    base_df.at[idx, column] = value
                    report.updated_cells += 1
                    row_updated = True
        if row_updated:
            base_df.at[idx, "Public Enrichment Source"] = _append_source_marker(
                base_df.at[idx, "Public Enrichment Source"],
                source_id,
            )
            base_df.at[idx, "Public Enrichment Updated At"] = timestamp

    report.matched_symbols = len(matched_symbols)
    return report


def merge_public_enrichment(
    screener_csv: Path,
    *,
    shareholding_csv: Optional[Path] = None,
    governance_csv: Optional[Path] = None,
    financial_risk_csv: Optional[Path] = None,
    output_csv: Optional[Path] = None,
    overwrite: bool = False,
) -> dict:
    """Merge configured enrichment files into a Screener-style CSV and return an audit report."""
    if not screener_csv.exists():
        raise FileNotFoundError(f"Screener CSV not found: {screener_csv}")

    base_df = pd.read_csv(screener_csv, dtype=str).fillna("")
    symbol_col = _symbol_column(base_df)
    if symbol_col is None:
        raise ValueError("Screener CSV must contain one of: NSE Symbol, Symbol, Ticker")

    output_path = output_csv or screener_csv
    source_reports: list[SourceMergeReport] = []

    source_plan = (
        ("shareholding", shareholding_csv, SHAREHOLDING_COLUMNS, _records_by_symbol),
        ("governance", governance_csv, GOVERNANCE_COLUMNS, None),
        ("financial_risk", financial_risk_csv, FINANCIAL_RISK_COLUMNS, _records_by_symbol),
    )

    for source_id, path, mappings, extractor in source_plan:
        df, read_report = _read_optional_csv(path, source_id)
        if df is None:
            source_reports.append(read_report)
            continue
        if source_id == "governance":
            records, notes = _governance_records_by_symbol(df)
        else:
            records, notes = extractor(df, mappings, source_id=source_id)  # type: ignore[misc]
        read_report.notes.extend(notes)
        if not records:
            read_report.status = "no_supported_data"
            source_reports.append(read_report)
            continue
        merge_report = _merge_records(
            base_df,
            records,
            symbol_col=symbol_col,
            source_id=source_id,
            overwrite=overwrite,
        )
        merge_report.path = read_report.path
        merge_report.status = "merged"
        merge_report.input_rows = read_report.input_rows
        merge_report.notes.extend(read_report.notes)
        source_reports.append(merge_report)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_df.to_csv(output_path, index=False)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "screener_csv": str(screener_csv),
        "output_csv": str(output_path),
        "overwrite": overwrite,
        "base_rows": int(len(base_df)),
        "sources": [asdict(item) for item in source_reports],
        "total_updated_cells": sum(item.updated_cells for item in source_reports),
    }
    return report


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for enrichment merging."""
    parser = argparse.ArgumentParser(description="Merge optional public enrichment CSVs into a Screener CSV")
    parser.add_argument("--screener-csv", required=True, help="Base Screener-style CSV to update")
    parser.add_argument("--shareholding-csv", default=None, help="Optional shareholding/pledge CSV")
    parser.add_argument("--governance-csv", default=None, help="Optional governance events CSV")
    parser.add_argument("--financial-risk-csv", default=None, help="Optional bank/NBFC asset-quality CSV")
    parser.add_argument("--output-csv", default=None, help="Output CSV path. Defaults to overwriting --screener-csv")
    parser.add_argument("--report-json", default=None, help="Optional JSON merge report path")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing non-empty destination values")
    return parser.parse_args()


def _optional_path(value: Optional[str]) -> Optional[Path]:
    """Convert an optional CLI string into a Path."""
    return Path(value) if value else None


def main() -> None:
    """Run the public enrichment merger from the command line."""
    args = parse_args()
    report = merge_public_enrichment(
        Path(args.screener_csv),
        shareholding_csv=_optional_path(args.shareholding_csv),
        governance_csv=_optional_path(args.governance_csv),
        financial_risk_csv=_optional_path(args.financial_risk_csv),
        output_csv=_optional_path(args.output_csv),
        overwrite=args.overwrite,
    )
    if args.report_json:
        report_path = Path(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2))
    print(
        f"[Public Enrichment] Updated {report['total_updated_cells']} cells "
        f"-> {report['output_csv']}"
    )
    for source in report["sources"]:
        print(
            f"  {source['source_id']}: {source['status']}, "
            f"matched={source['matched_symbols']}, updated_cells={source['updated_cells']}"
        )


if __name__ == "__main__":
    main()
