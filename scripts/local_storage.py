"""
NSE Rating Engine — Local Storage Model & Run Logger
======================================================
Folder contract:
  /data/raw/prices/bhavcopy/          ← NSE UDiFF daily ZIPs (one per trading day)
  /data/raw/prices/delivery/          ← NSE delivery position ZIPs
  /data/raw/prices/indices/           ← NSE index EOD CSVs
  /data/raw/prices/yfinance_cache/    ← yfinance fallback parquet cache
  /data/raw/fundamentals/xbrl/        ← Raw XBRL XMLs by {symbol}/{quarter}/
  /data/raw/fundamentals/screener/    ← Screener.in manual export CSVs
  /data/raw/fundamentals/trendlyne/   ← Trendlyne API JSON responses
  /data/raw/classification/           ← NSE industry classification PDFs + parsed CSV
  /data/raw/redflags/asm/             ← ASM list downloads (dated)
  /data/raw/redflags/gsm/             ← GSM list downloads (dated)
  /data/raw/redflags/shareholding/    ← BSE SHP XBRL by {symbol}/{quarter}/
  /data/processed/                    ← Cleaned parquet/CSV files ready for engine
  /runs/{YYYY-MM-DD}/                 ← One folder per engine run date
    inputs_manifest.json              ← Every file used + its hash + freshness
    scores_raw.csv                    ← All 6 card scores per stock (pre-cap)
    scores_final.csv                  ← Opportunity scores + investability
    leaderboard.csv                   ← Sorted final leaderboard
    stock_{TICKER}.json               ← Full canonical JSON for each rated stock
    run_log.json                      ← Metadata: timing, counts, errors
  /logs/                              ← Persistent cross-run audit trail
    data_gaps.csv                     ← Stocks with missing sub-metrics per run
    label_changes.csv                 ← Stocks whose investability changed vs prior run
    errors.log                        ← Exception traces
"""
from __future__ import annotations
import os, json, hashlib, csv
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).parent.parent

FOLDER_MAP = {
    "bhavcopy":        PROJECT_ROOT / "data/raw/prices/bhavcopy",
    "delivery":        PROJECT_ROOT / "data/raw/prices/delivery",
    "indices":         PROJECT_ROOT / "data/raw/prices/indices",
    "yfinance":        PROJECT_ROOT / "data/raw/prices/yfinance_cache",
    "xbrl":            PROJECT_ROOT / "data/raw/fundamentals/xbrl",
    "screener":        PROJECT_ROOT / "data/raw/fundamentals/screener",
    "trendlyne":       PROJECT_ROOT / "data/raw/fundamentals/trendlyne",
    "classification":  PROJECT_ROOT / "data/raw/classification",
    "asm":             PROJECT_ROOT / "data/raw/redflags/asm",
    "gsm":             PROJECT_ROOT / "data/raw/redflags/gsm",
    "shareholding":    PROJECT_ROOT / "data/raw/redflags/shareholding",
    "processed":       PROJECT_ROOT / "data/processed",
    "logs":            PROJECT_ROOT / "logs",
}

def ensure_folders():
    """Create all required directories if they don't exist."""
    for path in FOLDER_MAP.values():
        path.mkdir(parents=True, exist_ok=True)
    print("✅ All data directories verified.")

def run_folder(run_date: Optional[date] = None) -> Path:
    d = run_date or date.today()
    p = PROJECT_ROOT / f"runs/{d.isoformat()}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def file_hash(path: Path) -> str:
    """SHA-256 of a file for manifest integrity."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:12]

class RunLogger:
    """
    Logs every run with: inputs manifest, scores, and label changes vs prior run.
    Call .start() at beginning, .log_input() for each data file used,
    .log_scores() with the leaderboard, .finish() at end.
    """
    def __init__(self, run_date: Optional[date] = None):
        self.run_date  = run_date or date.today()
        self.folder    = run_folder(self.run_date)
        self.start_ts  = datetime.now().isoformat()
        self.inputs:   List[dict] = []
        self.errors:   List[str] = []
        self.n_rated   = 0
        self.n_excluded = 0

    def start(self):
        ensure_folders()
        print(f"[RunLogger] Run started: {self.run_date} → {self.folder}")

    def log_input(self, source_id: str, file_path: Path, freshness_ts: str):
        """Record each data file consumed in this run."""
        entry = {
            "source_id":    source_id,
            "file":         str(file_path),
            "hash":         file_hash(file_path) if file_path.exists() else "MISSING",
            "freshness_ts": freshness_ts,
            "logged_at":    datetime.now().isoformat(),
        }
        self.inputs.append(entry)

    def log_error(self, ticker: str, message: str):
        self.errors.append({"ticker": ticker, "error": message, "ts": datetime.now().isoformat()})

    def log_scores(self, leaderboard: List[dict]):
        """Write raw scores CSV and leaderboard CSV."""
        self.n_rated = len(leaderboard)
        if not leaderboard:
            return
        keys = list(leaderboard[0].keys())
        lb_path = self.folder / "leaderboard.csv"
        with open(lb_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(leaderboard)
        print(f"[RunLogger] Leaderboard saved: {lb_path} ({self.n_rated} stocks)")

    def compare_with_prior(self, current_leaderboard: List[dict]):
        """
        Detect investability label changes vs. most recent prior run.
        Writes label_changes.csv to /logs/.
        """
        prior_runs = sorted(
            [d for d in (PROJECT_ROOT/"runs").iterdir() if d.is_dir() and d.name != self.run_date.isoformat()],
            reverse=True
        )
        if not prior_runs:
            return

        prior_lb_path = prior_runs[0] / "leaderboard.csv"
        if not prior_lb_path.exists():
            return

        prior_map = {}
        prior_scores = {}
        with open(prior_lb_path) as f:
            for row in csv.DictReader(f):
                prior_map[row["ticker"]] = row.get("investability_status","")
                prior_scores[row["ticker"]] = row.get("opportunity_score","")

        changes = []
        for row in current_leaderboard:
            t = row["ticker"]
            old = prior_map.get(t, "New")
            new = row.get("investability_status","")
            if old != new:
                changes.append({"ticker": t, "name": row.get("name",""), "run_date": self.run_date.isoformat(),
                                 "prior_status": old, "new_status": new,
                                 "prior_score": prior_scores.get(t,""),
                                 "new_score": row.get("opportunity_score","")})
        if changes:
            changes_path = FOLDER_MAP["logs"] / "label_changes.csv"
            write_header = not changes_path.exists()
            with open(changes_path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(changes[0].keys()))
                if write_header: w.writeheader()
                w.writerows(changes)
            print(f"[RunLogger] {len(changes)} label changes detected → {changes_path}")

    def finish(self, leaderboard: Optional[List[dict]] = None):
        """Write inputs manifest and run_log.json."""
        # Inputs manifest
        with open(self.folder / "inputs_manifest.json","w") as f:
            json.dump(self.inputs, f, indent=2)

        # Run log
        run_log = {
            "run_date":    self.run_date.isoformat(),
            "start_ts":    self.start_ts,
            "end_ts":      datetime.now().isoformat(),
            "n_stocks_rated": self.n_rated,
            "n_excluded":  self.n_excluded,
            "n_errors":    len(self.errors),
            "errors":      self.errors,
            "input_files_used": len(self.inputs),
        }
        with open(self.folder / "run_log.json","w") as f:
            json.dump(run_log, f, indent=2)

        # Append to persistent error log
        if self.errors:
            with open(FOLDER_MAP["logs"] / "errors.log","a") as f:
                for e in self.errors:
                    f.write(f"{e['ts']} | {e['ticker']} | {e['error']}\n")

        if leaderboard:
            self.compare_with_prior(leaderboard)

        print(f"[RunLogger] Run complete. Rated: {self.n_rated}. Errors: {len(self.errors)}. Folder: {self.folder}")
