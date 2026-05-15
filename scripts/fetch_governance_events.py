#!/usr/bin/env python3
"""
Fetch and normalize public governance-risk announcements.

The script focuses on exchange-announcement evidence that can materially affect
long-term investment research: auditor exits, regulatory action, default,
insolvency, fraud, forensic audits, pledged-share invocation, and similar risks.
It intentionally does not mark companies as clean when no event is found.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.local_storage import FOLDER_MAP
from scripts.merge_public_enrichment import merge_public_enrichment


GOVERNANCE_DIR = FOLDER_MAP["governance"]

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
}

GOVERNANCE_RULES: tuple[tuple[int, str, tuple[str, ...]], ...] = (
    (95, "Fraud / forensic / investigation event", ("fraud", "forensic", "investigation", "sfio")),
    (92, "Insolvency / bankruptcy / liquidation event", ("insolvency", "bankruptcy", "liquidation", "winding up", "cirp", "nclt")),
    (90, "Default / credit rating stress event", ("default", "downgrade", "rating watch negative", "delay in payment")),
    (88, "Regulatory enforcement / SEBI event", ("sebi", "enforcement", "adjudication order", "show cause", "debarred")),
    (84, "Auditor resignation / qualified audit event", ("auditor resignation", "resignation of statutory auditor", "qualified opinion", "adverse opinion")),
    (80, "Pledge invocation / encumbrance event", ("pledge invocation", "invocation of pledge", "encumbrance", "pledged shares invoked")),
    (72, "Material penalty / tax demand event", ("penalty", "tax demand", "income tax demand", "gst demand")),
    (65, "Key management resignation event", ("resignation of chief financial officer", "resignation of cfo", "resignation of compliance officer")),
)

OUTPUT_COLUMNS = (
    "NSE Symbol",
    "Governance Events",
    "Governance Risk",
    "Source URL",
    "As Of Date",
    "Announcement Date",
    "Raw Headline",
)


def _first_text(item: dict, keys: Iterable[str]) -> str:
    """Return the first non-empty string value from a dict."""
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _normalise_symbol(value: object) -> str:
    """Normalize exchange/Yahoo-style symbols to plain NSE symbols."""
    text = str(value or "").strip().upper()
    for suffix in (".NS", ".BO"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _payload_rows(payload: object) -> list[dict]:
    """Extract announcement rows from common NSE/BSE JSON shapes."""
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "announcements", "rows", "Table", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def classify_governance_event(text: str) -> tuple[str, int] | None:
    """Classify a headline into a governance event label and risk score."""
    haystack = text.lower()
    matches = [
        (risk, label)
        for risk, label, keywords in GOVERNANCE_RULES
        if any(keyword in haystack for keyword in keywords)
    ]
    if not matches:
        return None
    risk, label = max(matches, key=lambda item: item[0])
    return label, risk


def parse_nse_announcements(payload: object, *, as_of: date) -> list[dict]:
    """Parse raw announcement JSON into the governance enrichment CSV contract."""
    rows: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for item in _payload_rows(payload):
        symbol = _normalise_symbol(
            _first_text(item, ("symbol", "SYMBOL", "sm_symbol", "companySymbol", "ticker"))
        )
        headline = _first_text(
            item,
            (
                "desc",
                "headline",
                "title",
                "subject",
                "attchmntText",
                "announcement",
                "details",
            ),
        )
        extra_text = _first_text(item, ("sm_name", "companyName", "CompanyName"))
        combined = " ".join(part for part in (headline, extra_text) if part)
        if not symbol or not combined:
            continue
        classification = classify_governance_event(combined)
        if classification is None:
            continue
        event, risk = classification
        announcement_date = _first_text(
            item,
            ("an_dt", "announcementDate", "disseminationTime", "sort_date", "date"),
        )
        source_url = _first_text(item, ("attchmntFile", "attachment", "fileUrl", "url"))
        if source_url.startswith("/"):
            source_url = f"https://www.nseindia.com{source_url}"
        dedupe_key = (symbol, event, announcement_date or combined[:120])
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append(
            {
                "NSE Symbol": symbol,
                "Governance Events": event,
                "Governance Risk": risk,
                "Source URL": source_url,
                "As Of Date": as_of.isoformat(),
                "Announcement Date": announcement_date,
                "Raw Headline": combined,
            }
        )
    return rows


def _get_nse_session():
    """Create an NSE session and warm cookies before hitting API endpoints."""
    import requests

    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        session.get("https://www.nseindia.com/", timeout=10)
        time.sleep(0.75)
    except Exception:
        pass
    return session


def fetch_nse_governance_events(run_date: date, *, lookback_days: int, session=None) -> list[dict]:
    """Fetch exchange announcements from NSE and return classified governance rows."""
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    if session is None:
        session = _get_nse_session()
    from_date = (run_date - timedelta(days=lookback_days - 1)).strftime("%d-%m-%Y")
    to_date = run_date.strftime("%d-%m-%Y")
    url = (
        "https://www.nseindia.com/api/corporate-announcements"
        f"?index=equities&from_date={from_date}&to_date={to_date}"
    )
    response = session.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"NSE governance fetch failed with HTTP {response.status_code}: {url}")
    return parse_nse_announcements(response.json(), as_of=run_date)


def save_governance_events(rows: list[dict], run_date: date, output_csv: Optional[Path] = None) -> Path:
    """Save governance enrichment rows to the standard dated CSV path."""
    output_path = output_csv or GOVERNANCE_DIR / f"governance_events_{run_date.isoformat()}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def load_rows_from_json(path: Path, *, as_of: date) -> list[dict]:
    """Load a downloaded public-announcement JSON file and parse it offline."""
    payload = json.loads(path.read_text())
    return parse_nse_announcements(payload, as_of=as_of)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Fetch public governance-risk events from NSE announcements")
    parser.add_argument("--date", default=date.today().isoformat(), help="Run date YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=120, help="Announcement lookback window")
    parser.add_argument("--input-json", default=None, help="Offline JSON payload to parse instead of network fetch")
    parser.add_argument("--output-csv", default=None, help="Output governance CSV path")
    parser.add_argument("--merge-csv", default=None, help="Merge output into a Screener-style CSV")
    return parser.parse_args()


def main() -> None:
    """Fetch or parse governance events and optionally merge them into the Screener CSV."""
    args = parse_args()
    run_date = date.fromisoformat(args.date)
    if args.input_json:
        rows = load_rows_from_json(Path(args.input_json), as_of=run_date)
    else:
        rows = fetch_nse_governance_events(run_date, lookback_days=args.lookback_days)

    output_path = save_governance_events(
        rows,
        run_date,
        Path(args.output_csv) if args.output_csv else None,
    )
    print(f"[Governance] Saved {len(rows)} classified event rows -> {output_path}")

    if args.merge_csv:
        report = merge_public_enrichment(Path(args.merge_csv), governance_csv=output_path)
        print(f"[Governance] Merged {report['total_updated_cells']} cells into {args.merge_csv}")


if __name__ == "__main__":
    main()
