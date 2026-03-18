#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from scripts.fetch_nse_universe import (
    DEFAULT_ARCHIVE_URL_TEMPLATE,
    default_zip_path,
    ensure_bhavcopy_zip,
)
from scripts.local_storage import ensure_folders


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill local NSE bhavcopy history for raw price-derived metrics."
    )
    parser.add_argument("--end-date", default=dt.date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--sessions", type=int, default=260, help="Number of trading sessions to backfill")
    parser.add_argument(
        "--max-calendar-days",
        type=int,
        default=520,
        help="Maximum calendar days to scan backwards while collecting sessions",
    )
    parser.add_argument("--timeout-sec", type=int, default=25, help="HTTP timeout for each archive request")
    parser.add_argument("--force-download", action="store_true", help="Re-download even when cache exists")
    parser.add_argument("--skip-download", action="store_true", help="Use cache only; do not download")
    parser.add_argument(
        "--report-json",
        default=None,
        help="Optional report path (default: data/processed/universe/price_history_backfill_<date>.json)",
    )
    return parser.parse_args()


def default_report_path(end_date: dt.date) -> Path:
    return Path("data/processed/universe") / f"price_history_backfill_{end_date.isoformat()}.json"


def main() -> None:
    args = parse_args()
    ensure_folders()
    end_date = dt.date.fromisoformat(args.end_date)
    report_path = Path(args.report_json) if args.report_json else default_report_path(end_date)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    fetched: list[dict] = []
    seen_dates: set[str] = set()
    errors: list[dict] = []

    for offset in range(max(0, args.max_calendar_days)):
        if len(fetched) >= args.sessions:
            break
        candidate = end_date - dt.timedelta(days=offset)
        zip_path = default_zip_path(candidate)
        try:
            cached_path, mode, resolved_date, resolved_url = ensure_bhavcopy_zip(
                run_date=candidate,
                zip_path=zip_path,
                archive_url_template=DEFAULT_ARCHIVE_URL_TEMPLATE,
                skip_download=args.skip_download,
                force_download=args.force_download,
                timeout_sec=args.timeout_sec,
                max_lookback_days=0,
            )
        except Exception as exc:
            errors.append({"candidate_date": candidate.isoformat(), "error": str(exc)})
            continue

        resolved_date_str = resolved_date.isoformat()
        if resolved_date_str in seen_dates:
            continue
        seen_dates.add(resolved_date_str)
        fetched.append(
            {
                "candidate_date": candidate.isoformat(),
                "resolved_date": resolved_date_str,
                "mode": mode,
                "zip_path": str(cached_path),
                "resolved_url": resolved_url,
            }
        )

    payload = {
        "end_date": end_date.isoformat(),
        "requested_sessions": args.sessions,
        "fetched_sessions": len(fetched),
        "max_calendar_days": args.max_calendar_days,
        "files": fetched,
        "errors": errors,
    }
    report_path.write_text(json.dumps(payload, indent=2))

    print(f"Backfill complete: {len(fetched)} session(s)")
    print(f"Report: {report_path}")
    if fetched:
        print(f"Latest cached session: {fetched[0]['resolved_date']}")
        print(f"Oldest cached session: {fetched[-1]['resolved_date']}")


if __name__ == "__main__":
    main()
