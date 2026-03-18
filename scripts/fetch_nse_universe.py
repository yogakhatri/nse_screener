#!/usr/bin/env python3
"""
Fetch daily NSE universe symbols from official archive bhavcopy.

Compliance intent:
- Uses official downloadable archive files (not page scraping).
- Cached locally to avoid repeated downloads for the same date.
- One file per date, low-frequency usage.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import ensure_folders


DEFAULT_ARCHIVE_URL_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip"
)
FALLBACK_ARCHIVE_URL_TEMPLATES = (
    "https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MMM}/cm{DD}{MMM}{YYYY}bhav.csv.zip",
    "https://nsearchives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MMM}/cm{DD}{MMM}{YYYY}bhav.csv.zip",
)
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/zip,application/octet-stream,*/*",
    "Referer": "https://www.nseindia.com/all-reports",
}

SYMBOL_ALIASES = ["SYMBOL", "Symbol", "NSE Symbol", "TckrSymb", "Ticker Symbol"]
SERIES_ALIASES = ["SERIES", "Series", "SctySrs"]
NAME_ALIASES = ["NAME OF COMPANY", "Name of Company", "Name", "Company Name", "FinInstrmNm"]
MACRO_SECTOR_ALIASES = ["Macro Sector", "Macro", "MacroSector"]
SECTOR_ALIASES = ["Sector", "SECTOR"]
INDUSTRY_ALIASES = ["Industry", "INDUSTRY"]
BASIC_INDUSTRY_ALIASES = ["Basic Industry", "BasicIndustry", "BASIC INDUSTRY"]


def _norm(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _find_col(columns: Iterable[str], aliases: Iterable[str]) -> str | None:
    normalized = {_norm(c): c for c in columns}
    for alias in aliases:
        col = normalized.get(_norm(alias))
        if col:
            return col
    return None


def _clean_symbol(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "-"}:
        return ""
    return text.replace(".NS", "")


def build_archive_url(run_date: dt.date, template: str) -> str:
    return template.format(
        YYYY=run_date.strftime("%Y"),
        MMM=run_date.strftime("%b").upper(),
        DD=run_date.strftime("%d"),
        YYYYMMDD=run_date.strftime("%Y%m%d"),
    )


def legacy_zip_path(run_date: dt.date) -> Path:
    name = f"cm{run_date.strftime('%d%b%Y').upper()}bhav.csv.zip"
    return Path("data/raw/prices/bhavcopy") / name


def default_zip_path(run_date: dt.date) -> Path:
    name = f"BhavCopy_NSE_CM_0_0_0_{run_date.strftime('%Y%m%d')}_F_0000.csv.zip"
    return Path("data/raw/prices/bhavcopy") / name


def default_output_csv(run_date: dt.date) -> Path:
    return Path("data/raw/universe") / f"nse_symbols_{run_date.isoformat()}.csv"


def default_report_path(run_date: dt.date) -> Path:
    return Path("data/processed/universe") / f"universe_fetch_{run_date.isoformat()}.json"


def default_classification_csv() -> Path:
    return Path("data/raw/classification/nse_symbol_classification_master.csv")


def default_missing_classification_csv(run_date: dt.date) -> Path:
    return Path("data/processed/universe") / f"missing_classification_{run_date.isoformat()}.csv"


def _candidate_dates(run_date: dt.date, max_lookback_days: int) -> list[dt.date]:
    safe_lookback = max(0, max_lookback_days)
    return [run_date - dt.timedelta(days=offset) for offset in range(safe_lookback + 1)]


def _candidate_templates(primary_template: str) -> list[str]:
    candidates: list[str] = []
    for template in (primary_template, *FALLBACK_ARCHIVE_URL_TEMPLATES):
        template = str(template or "").strip()
        if not template or template in candidates:
            continue
        candidates.append(template)
    return candidates


def _existing_cached_zip(run_date: dt.date) -> Path | None:
    for candidate in (default_zip_path(run_date), legacy_zip_path(run_date)):
        if candidate.exists():
            return candidate
    return None


def ensure_bhavcopy_zip(
    run_date: dt.date,
    zip_path: Path,
    archive_url_template: str,
    skip_download: bool,
    force_download: bool,
    timeout_sec: int,
    max_lookback_days: int,
) -> tuple[Path, str, dt.date, str]:
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    for candidate_date in _candidate_dates(run_date, max_lookback_days):
        if zip_path.exists() and not force_download:
            return zip_path, "cache", run_date, "local-cache"

        cached = _existing_cached_zip(candidate_date)
        if cached and not force_download:
            return cached, "cache", candidate_date, "local-cache"

    if skip_download:
        raise FileNotFoundError(
            "\n".join(
                [
                    f"Bhavcopy ZIP missing for {run_date.isoformat()} and lookback {max_lookback_days} day(s).",
                    f"Checked explicit path: {zip_path}",
                    "Provide --bhavcopy-zip with a local file, or allow download.",
                ]
            )
        )

    attempted_urls: list[str] = []
    templates = _candidate_templates(archive_url_template)
    for candidate_date in _candidate_dates(run_date, max_lookback_days):
        for template in templates:
            url = build_archive_url(candidate_date, template)
            try:
                response = requests.get(url, headers=HTTP_HEADERS, timeout=timeout_sec)
            except Exception as exc:
                attempted_urls.append(f"{candidate_date.isoformat()} | request_error | {url} | {exc}")
                continue

            if response.status_code != 200:
                attempted_urls.append(f"{candidate_date.isoformat()} | {response.status_code} | {url}")
                continue

            content = response.content or b""
            if not content.startswith(b"PK"):
                attempted_urls.append(f"{candidate_date.isoformat()} | non_zip_response | {url}")
                continue

            save_path = zip_path if zip_path.name else default_zip_path(candidate_date)
            if save_path == zip_path and zip_path == default_zip_path(run_date) and candidate_date != run_date:
                save_path = default_zip_path(candidate_date)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(content)
            return save_path, "download", candidate_date, url

    attempt_preview = "\n".join(attempted_urls[:8])
    if len(attempted_urls) > 8:
        attempt_preview += "\n..."
    raise RuntimeError(
        "\n".join(
            [
                "Unable to download bhavcopy ZIP from official NSE sources.",
                f"Requested date: {run_date.isoformat()} | lookback days: {max_lookback_days}",
                "Attempted URLs (first attempts):",
                attempt_preview or "<none>",
                f"Manual fallback: place a ZIP at {zip_path} and re-run with --skip-download.",
            ]
        )
    )


def read_bhavcopy_zip(zip_path: Path) -> pd.DataFrame:
    if not zip_path.exists():
        raise FileNotFoundError(f"Bhavcopy ZIP not found: {zip_path}")

    with zipfile.ZipFile(zip_path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(f"No CSV found inside ZIP: {zip_path}")
        with zf.open(members[0]) as fh:
            return pd.read_csv(fh, dtype=str).fillna("")


def build_universe_from_bhavcopy(df: pd.DataFrame, include_non_eq: bool) -> pd.DataFrame:
    symbol_col = _find_col(df.columns, SYMBOL_ALIASES)
    if not symbol_col:
        raise RuntimeError("Bhavcopy CSV missing SYMBOL column.")

    series_col = _find_col(df.columns, SERIES_ALIASES)
    name_col = _find_col(df.columns, NAME_ALIASES)

    out = pd.DataFrame()
    out["NSE Symbol"] = df[symbol_col].map(_clean_symbol)
    if series_col:
        out["SERIES"] = df[series_col].astype(str).str.strip().str.upper()
    else:
        out["SERIES"] = ""
    out["Name"] = df[name_col].astype(str).str.strip() if name_col else out["NSE Symbol"]
    out["Macro Sector"] = ""
    out["Sector"] = ""
    out["Industry"] = ""
    out["Basic Industry"] = ""

    out = out[out["NSE Symbol"] != ""]
    if not include_non_eq:
        out = out[out["SERIES"] == "EQ"]
    out = out.drop_duplicates(subset=["NSE Symbol"], keep="first")
    out = out.sort_values("NSE Symbol").reset_index(drop=True)
    return out


def load_classification_master(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    symbol_col = _find_col(df.columns, SYMBOL_ALIASES)
    if not symbol_col:
        raise RuntimeError(
            f"Classification CSV missing symbol column ({path}). "
            f"Expected one of: {', '.join(SYMBOL_ALIASES)}"
        )

    out = pd.DataFrame()
    out["NSE Symbol"] = df[symbol_col].map(_clean_symbol)
    out = out[out["NSE Symbol"] != ""]
    out = out.drop_duplicates(subset=["NSE Symbol"], keep="first")

    def _col(aliases: list[str]) -> pd.Series:
        hit = _find_col(df.columns, aliases)
        return df[hit].astype(str).str.strip() if hit else pd.Series([""] * len(df))

    out["class__Name"] = _col(NAME_ALIASES)
    out["class__Macro Sector"] = _col(MACRO_SECTOR_ALIASES)
    out["class__Sector"] = _col(SECTOR_ALIASES)
    out["class__Industry"] = _col(INDUSTRY_ALIASES)
    out["class__Basic Industry"] = _col(BASIC_INDUSTRY_ALIASES)
    return out


def apply_classification(universe_df: pd.DataFrame, class_df: pd.DataFrame) -> pd.DataFrame:
    merged = universe_df.merge(class_df, how="left", on="NSE Symbol")
    for col in ["Name", "Macro Sector", "Sector", "Industry", "Basic Industry"]:
        class_col = f"class__{col}"
        if class_col not in merged.columns:
            continue
        fallback = merged[class_col].fillna("").astype(str).str.strip()
        merged[col] = merged[col].where(
            merged[col].astype(str).str.strip() != "",
            fallback,
        )
    drop_cols = [c for c in merged.columns if c.startswith("class__")]
    return merged.drop(columns=drop_cols)


def classification_stats(universe_df: pd.DataFrame) -> dict:
    missing_tokens = {"", "nan", "none", "-"}

    def pct_non_empty(column: str) -> float:
        if column not in universe_df.columns or len(universe_df) == 0:
            return 0.0
        series = universe_df[column].fillna("").astype(str).str.strip().str.lower()
        return round((~series.isin(missing_tokens)).sum() / len(series) * 100.0, 2)

    return {
        "macro_sector_pct": pct_non_empty("Macro Sector"),
        "sector_pct": pct_non_empty("Sector"),
        "industry_pct": pct_non_empty("Industry"),
        "basic_industry_pct": pct_non_empty("Basic Industry"),
    }


def write_missing_classification(universe_df: pd.DataFrame, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if universe_df.empty:
        pd.DataFrame(columns=["NSE Symbol", "Name", "Macro Sector", "Sector", "Industry", "Basic Industry"]).to_csv(
            out_path, index=False
        )
        return 0
    missing_tokens = {"", "nan", "none", "-"}
    sector = universe_df["Sector"].fillna("").astype(str).str.strip().str.lower()
    industry = universe_df["Industry"].fillna("").astype(str).str.strip().str.lower()
    basic = universe_df["Basic Industry"].fillna("").astype(str).str.strip().str.lower()
    missing = universe_df[
        sector.isin(missing_tokens)
        | industry.isin(missing_tokens)
        | basic.isin(missing_tokens)
    ][["NSE Symbol", "Name", "Macro Sector", "Sector", "Industry", "Basic Industry"]]
    missing.to_csv(out_path, index=False)
    return int(len(missing))


def parse_args() -> argparse.Namespace:
    today = dt.date.today().isoformat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=today, help="Run date YYYY-MM-DD")
    parser.add_argument("--bhavcopy-zip", default=None, help="Local bhavcopy ZIP path (optional)")
    parser.add_argument("--output-csv", default=None, help="Output universe CSV path")
    parser.add_argument("--report-json", default=None, help="Output fetch diagnostics JSON")
    parser.add_argument(
        "--classification-csv",
        default=None,
        help="Symbol classification master CSV (default: data/raw/classification/nse_symbol_classification_master.csv)",
    )
    parser.add_argument(
        "--missing-classification-csv",
        default=None,
        help="Path to write symbols missing taxonomy",
    )
    parser.add_argument(
        "--archive-url-template",
        default=DEFAULT_ARCHIVE_URL_TEMPLATE,
        help="Official archive URL template",
    )
    parser.add_argument(
        "--max-lookback-days",
        type=int,
        default=7,
        help="If date file is unavailable (holiday/weekend), try earlier dates up to this many days back",
    )
    parser.add_argument("--skip-download", action="store_true", help="Do not download; use local ZIP only")
    parser.add_argument("--force-download", action="store_true", help="Re-download even if local ZIP exists")
    parser.add_argument("--timeout-sec", type=int, default=25, help="HTTP timeout in seconds")
    parser.add_argument("--include-non-eq", action="store_true", help="Include non-EQ series")
    parser.add_argument(
        "--require-classification",
        action="store_true",
        help="Fail if classification CSV is missing or if taxonomy coverage is incomplete",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite output/report if present")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = dt.date.fromisoformat(args.date)

    ensure_folders()
    zip_path = Path(args.bhavcopy_zip) if args.bhavcopy_zip else default_zip_path(run_date)
    output_csv = Path(args.output_csv) if args.output_csv else default_output_csv(run_date)
    report_json = Path(args.report_json) if args.report_json else default_report_path(run_date)
    classification_csv = Path(args.classification_csv) if args.classification_csv else default_classification_csv()
    missing_classification_csv = (
        Path(args.missing_classification_csv)
        if args.missing_classification_csv
        else default_missing_classification_csv(run_date)
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    missing_classification_csv.parent.mkdir(parents=True, exist_ok=True)

    if output_csv.exists() and not args.force:
        raise RuntimeError(f"Output exists: {output_csv}. Use --force to overwrite.")
    if report_json.exists() and not args.force:
        raise RuntimeError(f"Report exists: {report_json}. Use --force to overwrite.")

    zip_file, source_mode, data_date, resolved_url = ensure_bhavcopy_zip(
        run_date=run_date,
        zip_path=zip_path,
        archive_url_template=args.archive_url_template,
        skip_download=args.skip_download,
        force_download=args.force_download,
        timeout_sec=args.timeout_sec,
        max_lookback_days=args.max_lookback_days,
    )
    bhavcopy_df = read_bhavcopy_zip(zip_file)
    universe_df = build_universe_from_bhavcopy(bhavcopy_df, include_non_eq=args.include_non_eq)

    classification_source = None
    if classification_csv.exists():
        class_df = load_classification_master(classification_csv)
        universe_df = apply_classification(universe_df, class_df)
        classification_source = str(classification_csv)
    elif args.require_classification:
        raise FileNotFoundError(
            "\n".join(
                [
                    f"Classification master not found: {classification_csv}",
                    "Create this file with columns:",
                    "NSE Symbol,Name,Macro Sector,Sector,Industry,Basic Industry",
                ]
            )
        )

    class_stats = classification_stats(universe_df)
    missing_count = write_missing_classification(universe_df, missing_classification_csv)
    if args.require_classification and missing_count > 0:
        raise RuntimeError(
            "\n".join(
                [
                    f"Classification incomplete for {missing_count} symbols.",
                    f"See: {missing_classification_csv}",
                    "Fill Sector/Industry/Basic Industry in classification master and rerun.",
                ]
            )
        )

    universe_df.to_csv(output_csv, index=False)

    report = {
        "run_date": run_date.isoformat(),
        "resolved_bhavcopy_date": data_date.isoformat(),
        "bhavcopy_zip": str(zip_file),
        "source_mode": source_mode,
        "archive_url": resolved_url,
        "output_csv": str(output_csv),
        "rows_in_bhavcopy": int(len(bhavcopy_df)),
        "symbols_output": int(len(universe_df)),
        "include_non_eq": bool(args.include_non_eq),
        "classification_source": classification_source,
        "missing_classification_csv": str(missing_classification_csv),
        "missing_classification_count": missing_count,
        "classification_coverage_pct": class_stats,
        "require_classification": bool(args.require_classification),
    }
    with open(report_json, "w") as f:
        json.dump(report, f, indent=2)

    print("Universe fetch complete.")
    print(f"Bhavcopy ZIP: {zip_file}")
    print(f"Bhavcopy date used: {data_date.isoformat()}")
    print(f"Output CSV: {output_csv}")
    print(f"Report: {report_json}")
    print(f"Symbols: {len(universe_df)}")
    print(
        "Classification coverage: "
        f"sector={class_stats['sector_pct']}%, "
        f"industry={class_stats['industry_pct']}%, "
        f"basic_industry={class_stats['basic_industry_pct']}%"
    )


if __name__ == "__main__":
    main()
