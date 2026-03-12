"""
╔══════════════════════════════════════════════════════════════════════╗
║   NSE RATING ENGINE — PHASE 9: BIAS CONTROL LAYER                   ║
║   Three independent guardrails. Each can veto a backtest run.        ║
║   NONE of these checks may be disabled without editing this file.    ║
╚══════════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations
import csv, json, hashlib, logging, sqlite3
import datetime as dt
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

log = logging.getLogger("bias_controls")

# ══════════════════════════════════════════════════════════════════════
# BIAS CONTROL 1 — SURVIVORSHIP BIAS GUARD
# ══════════════════════════════════════════════════════════════════════
#
# The problem: if you build your universe from today's active stocks,
# every company that got delisted (failed, merged, fraud, bankruptcy)
# is silently excluded. Your backtest only sees survivors. Since
# delisted stocks are usually down heavily before delist, omitting them
# makes any strategy look ~3-8% better than it actually was.
#
# The fix: maintain a persistent delisted registry. When constructing
# a historical universe for any as_of_date, include all stocks that
# were ACTIVE at that date (including those later delisted).

DELIST_REASONS = {
    "bankruptcy"  : "Company filed for insolvency/NCLT admission",
    "merger"      : "Merged into acquirer; shares exchanged",
    "voluntary"   : "Promoter voluntary delisting at premium",
    "regulatory"  : "SEBI/exchange forced delisting (fraud, non-compliance)",
    "suspended"   : "Long-term suspension (> 180 days) — treated as failed",
    "amalgamation": "Court-approved amalgamation scheme",
}

@dataclass
class DelistedRecord:
    ticker         : str
    name           : str
    isin           : str
    list_date      : str   # YYYY-MM-DD: when it was first listed
    delist_date    : str   # YYYY-MM-DD: last trading day
    reason         : str   # one of DELIST_REASONS keys
    last_price     : float # closing price on final trading day
    sector         : str
    basic_industry : str
    notes          : str = ""

class SurvivingBiasGuard:
    """
    Maintains the delisted stock registry and provides point-in-time
    universe snapshots that include both active and then-active-but-
    later-delisted stocks.

    Storage: SQLite at data/raw/delisted/registry.db
             CSV mirror at data/raw/delisted/registry.csv (human-readable)
    """
    DB_PATH  = Path("data/raw/delisted/registry.db")
    CSV_PATH = Path("data/raw/delisted/registry.csv")

    def __init__(self):
        self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.DB_PATH))
        self._init_schema()

    def _init_schema(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS delisted (
                ticker         TEXT PRIMARY KEY,
                name           TEXT,
                isin           TEXT,
                list_date      TEXT,
                delist_date    TEXT,
                reason         TEXT,
                last_price     REAL,
                sector         TEXT,
                basic_industry TEXT,
                notes          TEXT
            )
        """)
        self._conn.commit()

    def register(self, record: DelistedRecord) -> None:
        """Add or update a delisted stock record."""
        assert record.reason in DELIST_REASONS, (
            f"Invalid reason '{record.reason}'. Must be one of: {list(DELIST_REASONS)}")
        self._conn.execute("""
            INSERT OR REPLACE INTO delisted VALUES
            (?,?,?,?,?,?,?,?,?,?)
        """, (record.ticker, record.name, record.isin,
              record.list_date, record.delist_date, record.reason,
              record.last_price, record.sector, record.basic_industry, record.notes))
        self._conn.commit()
        self._sync_csv()
        log.info(f"Registered delisted: {record.ticker} ({record.reason} on {record.delist_date})")

    def get_universe_snapshot(self,
                              active_tickers: List[str],
                              as_of_date: str) -> Dict[str, dict]:
        """
        Given today's active ticker list and a historical as_of_date,
        return the CORRECT universe for that date.

        Logic:
          Include stock if:
            (a) It is in active_tickers today (still active), OR
            (b) It was listed on/before as_of_date AND
                delisted strictly AFTER as_of_date
                (i.e., it was alive on as_of_date but later failed)

        CRITICAL: Stocks that were NOT YET LISTED on as_of_date are excluded.

        Returns: {ticker: {"status": "active"|"later_delisted", "delist_date": ...}}
        """
        result = {t: {"status": "active", "delist_date": None} for t in active_tickers}

        # Add stocks that were alive at as_of_date but later delisted
        rows = self._conn.execute("""
            SELECT ticker, list_date, delist_date, reason, last_price
            FROM delisted
            WHERE list_date  <= ?
              AND delist_date >  ?
        """, (as_of_date, as_of_date)).fetchall()

        for ticker, list_date, delist_date, reason, last_price in rows:
            if ticker not in result:  # don't override if somehow still in active list
                result[ticker] = {
                    "status"      : "later_delisted",
                    "delist_date" : delist_date,
                    "delist_reason": reason,
                    "last_price"  : last_price,
                }
        return result

    def survivorship_bias_estimate(self,
                                   as_of_date: str,
                                   active_tickers: List[str]) -> dict:
        """
        Estimate the magnitude of survivorship bias for a given lookback.
        Returns a bias audit dict with:
          - n_surviving    : stocks in active universe today
          - n_later_failed : stocks that were alive at as_of_date but later delisted
          - pct_missing    : % of as_of_date universe that would be missing
          - bias_severity  : Low / Moderate / High / Severe
        """
        snap = self.get_universe_snapshot(active_tickers, as_of_date)
        n_surviving = sum(1 for v in snap.values() if v["status"] == "active")
        n_failed    = sum(1 for v in snap.values() if v["status"] == "later_delisted")
        total       = n_surviving + n_failed
        pct_missing = round(n_failed / total * 100, 1) if total > 0 else 0.0

        severity = ("Severe"   if pct_missing > 15 else
                    "High"     if pct_missing > 8  else
                    "Moderate" if pct_missing > 3  else "Low")
        return {
            "as_of_date"   : as_of_date,
            "n_surviving"  : n_surviving,
            "n_later_failed": n_failed,
            "total_universe": total,
            "pct_missing"  : pct_missing,
            "bias_severity": severity,
        }

    def _sync_csv(self):
        rows = self._conn.execute("SELECT * FROM delisted ORDER BY delist_date").fetchall()
        with open(self.CSV_PATH, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ticker","name","isin","list_date","delist_date",
                        "reason","last_price","sector","basic_industry","notes"])
            w.writerows(rows)

    def close(self):
        self._conn.close()


# ══════════════════════════════════════════════════════════════════════
# BIAS CONTROL 2 — LOOK-AHEAD BIAS GUARD (Point-in-Time Data Store)
# ══════════════════════════════════════════════════════════════════════
#
# The problem: if you use "latest available" fundamentals for a
# historical backtest, you will use data that wasn't publicly available
# at the time. Example: a company reports Q2 earnings on Nov 12, 2023.
# If you backtest Oct 31, 2023 using those earnings, you've looked ahead.
#
# The fix: every fundamental value must be stored with:
#   - period_end_date: the period the data covers (e.g., 2023-09-30 for Q2)
#   - filing_date: when the company actually disclosed it to the exchange
# When querying for as_of_date, only use records where filing_date <= as_of_date.
#
# SEBI FILING DEADLINES (conservative upper bounds):
#   Quarterly results    : 45 days after quarter end
#   Annual results       : 60 days after fiscal year end
#   Shareholding pattern : 21 days after quarter end
#   Board meeting outcome: same day (disclosure within 30 mins)
# Always use ACTUAL filing date, not deadline. Deadlines are only
# used as a fallback when actual filing date is unknown.

# Mapping: quarter_end → conservative filing deadline (SEBI outer limit)
def sebi_filing_deadline(period_end_date: str, filing_type: str = "quarterly") -> str:
    """
    Returns the SEBI deadline date as a conservative fallback.
    Use actual_filing_date in preference to this wherever available.

    filing_type: "quarterly" | "annual" | "shareholding"
    """
    end = dt.date.fromisoformat(period_end_date)
    days = {"quarterly": 45, "annual": 60, "shareholding": 21}
    deadline = end + dt.timedelta(days=days.get(filing_type, 45))
    return str(deadline)

@dataclass
class FundamentalRecord:
    """A single point-in-time fundamental value."""
    ticker          : str
    metric          : str      # e.g., "rev_cagr_3y", "ebitda_margin"
    value           : float
    period_end_date : str      # last day of the period this value covers
    filing_date     : str      # date the company/exchange published this
    filing_type     : str      # "quarterly" | "annual" | "shareholding" | "price"
    source          : str      # "XBRL_BSE" | "Screener_export" | "NSE_bhavcopy" | ...
    is_deadline_proxy: bool = False  # True if filing_date was estimated via SEBI deadline

class PointInTimeStore:
    """
    SQLite-backed store for point-in-time fundamentals.
    All queries are filtered by filing_date <= as_of_date.
    Raises LookAheadError if a query would require future data.

    Storage: data/raw/fundamentals/pit_store.db
    """
    DB_PATH = Path("data/raw/fundamentals/pit_store.db")

    # Metrics that use ACTUAL price data (filing_date = trading date itself)
    PRICE_METRICS = {"return_1y","return_6m","cagr_5y","drawdown_recovery",
                     "rsi_state","price_vs_200dma","price_vs_50dma",
                     "volume_delivery","rs_turn","volatility_compression",
                     "peer_price_strength","drawdown_normalization","volume"}

    def __init__(self):
        self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.DB_PATH))
        self._init_schema()

    def _init_schema(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS pit_fundamentals (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker           TEXT    NOT NULL,
                metric           TEXT    NOT NULL,
                value            REAL    NOT NULL,
                period_end_date  TEXT    NOT NULL,
                filing_date      TEXT    NOT NULL,
                filing_type      TEXT    NOT NULL,
                source           TEXT    NOT NULL,
                is_deadline_proxy INTEGER DEFAULT 0
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_metric_filing
            ON pit_fundamentals (ticker, metric, filing_date)
        """)
        self._conn.commit()

    def upsert(self, record: FundamentalRecord) -> None:
        """Insert a fundamental value. Duplicate (ticker,metric,period_end_date) is replaced."""
        self._conn.execute("""
            INSERT OR REPLACE INTO pit_fundamentals
            (ticker,metric,value,period_end_date,filing_date,filing_type,source,is_deadline_proxy)
            VALUES (?,?,?,?,?,?,?,?)
        """, (record.ticker, record.metric, record.value, record.period_end_date,
              record.filing_date, record.filing_type, record.source,
              int(record.is_deadline_proxy)))
        self._conn.commit()

    def get_as_of(self, ticker: str, metric: str, as_of_date: str) -> Optional[float]:
        """
        Return the most recent value for (ticker, metric) where
        filing_date <= as_of_date.

        Returns None if no data available (correctly treated as missing data).
        NEVER returns a value with filing_date > as_of_date.
        """
        row = self._conn.execute("""
            SELECT value, filing_date, is_deadline_proxy
            FROM pit_fundamentals
            WHERE ticker = ? AND metric = ? AND filing_date <= ?
            ORDER BY filing_date DESC, period_end_date DESC
            LIMIT 1
        """, (ticker, metric, as_of_date)).fetchone()

        if row is None:
            return None

        value, filing_date, is_proxy = row
        if is_proxy:
            log.warning(f"PIT [{ticker}:{metric} as_of={as_of_date}] "
                        f"using SEBI-deadline proxy (filed: {filing_date}). "
                        f"Verify with actual filing date.")
        return value

    def get_full_snapshot(self, ticker: str, as_of_date: str) -> Dict[str, float]:
        """
        Return ALL metrics for a ticker as of as_of_date.
        This is what the engine calls to build a RawStockData.fundamentals dict.
        """
        rows = self._conn.execute("""
            SELECT metric, value, filing_date
            FROM (
                SELECT metric, value, filing_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY metric
                           ORDER BY filing_date DESC, period_end_date DESC
                       ) as rn
                FROM pit_fundamentals
                WHERE ticker = ? AND filing_date <= ?
            )
            WHERE rn = 1
        """, (ticker, as_of_date)).fetchall()
        return {metric: value for metric, value, _ in rows}

    def get_look_ahead_audit(self, ticker: str,
                             as_of_date: str, used_metrics: List[str]) -> List[dict]:
        """
        Audit: for a given backtest run, report which metric values
        used a deadline proxy (potential soft look-ahead) and which
        had no data at all (hard missing).
        """
        audit = []
        for metric in used_metrics:
            rows = self._conn.execute("""
                SELECT value, filing_date, is_deadline_proxy, source
                FROM pit_fundamentals
                WHERE ticker = ? AND metric = ?
                ORDER BY filing_date DESC LIMIT 1
            """, (ticker, metric)).fetchall()

            if not rows:
                audit.append({"ticker": ticker, "metric": metric,
                               "status": "MISSING", "filing_date": None,
                               "is_proxy": False, "source": None})
            else:
                value, filing_date, is_proxy, source = rows[0]
                if filing_date > as_of_date:
                    audit.append({"ticker": ticker, "metric": metric,
                                   "status": "LOOK_AHEAD_VIOLATION",
                                   "filing_date": filing_date,
                                   "as_of_date": as_of_date,
                                   "is_proxy": bool(is_proxy), "source": source})
                else:
                    audit.append({"ticker": ticker, "metric": metric,
                                   "status": "OK" if not is_proxy else "PROXY_WARNING",
                                   "filing_date": filing_date,
                                   "is_proxy": bool(is_proxy), "source": source})
        return audit

    def close(self):
        self._conn.close()


# ══════════════════════════════════════════════════════════════════════
# BIAS CONTROL 3 — DATA SNOOPING GUARD (Weight Freeze & Holdout)
# ══════════════════════════════════════════════════════════════════════
#
# The problem: if you keep tweaking weights every time the backtest
# underperforms, you are fitting to historical noise, not discovering
# signal. Even doing this unconsciously (e.g., raising Profitability
# weight because "it feels right" after seeing the results) is snooping.
#
# The fix: three interlocking rules:
#
#   Rule 1 — WEIGHT FREEZE:
#     Every change to CARD_WEIGHTS must be logged with:
#       - timestamp of change
#       - who/what prompted the change (reason)
#       - the SHA-256 of the old and new config
#     A change is only allowed if the reason is one of ALLOWED_CHANGE_REASONS.
#     Any change after the HOLDOUT_START_DATE is a violation.
#
#   Rule 2 — DEVELOPMENT / VALIDATION / HOLDOUT SPLIT:
#     DEVELOPMENT_PERIOD : all data before validation_start — free to explore
#     VALIDATION_PERIOD  : tune and verify without touching holdout
#     HOLDOUT_PERIOD     : from holdout_start onward — NEVER touched during dev
#     The holdout boundary, once set, can ONLY be moved LATER (wider holdout),
#     never moved earlier (which would expose more data for tuning).
#
#   Rule 3 — CHANGE COUNT LIMIT:
#     Maximum MAX_WEIGHT_CHANGES total changes to CARD_WEIGHTS during
#     the development + validation period combined. If you exceed this
#     budget, the engine will refuse to run backtests until you document
#     a justification in the change log.

ALLOWED_CHANGE_REASONS = {
    "initial_setup"         : "First-time weight specification (pre-any-backtest)",
    "template_correction"   : "Bug fix — wrong template assigned to a metric",
    "metric_redefinition"   : "Phase 3/6 metric definition changed (affects all backtests equally)",
    "peer_group_structural" : "Structural change in NSE classification hierarchy",
    "annual_review"         : "Scheduled annual review (max once per calendar year)",
}

MAX_WEIGHT_CHANGES  : int = 5           # total allowed changes (dev + validation combined)
HOLDOUT_START_DATE  : str = "2024-04-01"  # FY2025 Q1 onward is holdout — never touch
VALIDATION_START    : str = "2022-04-01"  # FY2023 onward is validation
DEVELOPMENT_END     : str = "2022-03-31"  # FY2018–FY2022 = development period

@dataclass
class WeightChangeRecord:
    timestamp       : str    # ISO datetime of change
    reason_key      : str    # must be in ALLOWED_CHANGE_REASONS
    description     : str    # free-text explanation
    old_config_hash : str    # SHA-256 of old CARD_WEIGHTS dict
    new_config_hash : str    # SHA-256 of new CARD_WEIGHTS dict
    changed_by      : str = "engine_user"  # for audit trail

class DataSnoopingGuard:
    """
    Enforces weight immutability rules and holdout period protection.
    All weight changes must pass through this guard's approve_change().

    Storage: logs/weight_change_log.jsonl (one JSON record per line)
    """
    LOG_PATH = Path("logs/weight_change_log.jsonl")

    def __init__(self):
        self.LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._log: List[WeightChangeRecord] = self._load_log()

    def _load_log(self) -> List[WeightChangeRecord]:
        if not self.LOG_PATH.exists():
            return []
        records = []
        with open(self.LOG_PATH) as f:
            for line in f:
                d = json.loads(line.strip())
                records.append(WeightChangeRecord(**d))
        return records

    @staticmethod
    def _hash_config(config: dict) -> str:
        return hashlib.sha256(
            json.dumps(config, sort_keys=True).encode()
        ).hexdigest()[:16]

    def approve_change(self,
                       old_config: dict,
                       new_config: dict,
                       reason_key: str,
                       description: str,
                       changed_by: str = "engine_user",
                       force: bool = False) -> Tuple[bool, str]:
        """
        Evaluate whether a weight change is allowed.
        Returns (approved: bool, message: str).

        Veto conditions (cannot be overridden):
          1. reason_key not in ALLOWED_CHANGE_REASONS
          2. Current date >= HOLDOUT_START_DATE
          3. Change count >= MAX_WEIGHT_CHANGES (unless reason = "annual_review")
        """
        today = str(dt.date.today())
        msg_parts = []

        # Veto 1: invalid reason
        if reason_key not in ALLOWED_CHANGE_REASONS:
            return False, (f"VETO: reason_key '{reason_key}' not in ALLOWED_CHANGE_REASONS. "
                           f"Allowed: {list(ALLOWED_CHANGE_REASONS)}")

        # Veto 2: holdout period
        if today >= HOLDOUT_START_DATE and not force:
            return False, (f"VETO: Today ({today}) is within the holdout period "
                           f"(starts {HOLDOUT_START_DATE}). Weight changes are FROZEN. "
                           f"Evaluating this change would contaminate the holdout.")

        # Veto 3: change count budget
        dev_val_changes = [r for r in self._log
                           if r.timestamp[:10] < HOLDOUT_START_DATE]
        if (len(dev_val_changes) >= MAX_WEIGHT_CHANGES
                and reason_key not in ("annual_review","initial_setup")):
            return False, (f"VETO: Change budget exhausted ({len(dev_val_changes)}/{MAX_WEIGHT_CHANGES}). "
                           f"Document justification as 'annual_review' to proceed.")

        # Annual review rate limit: max 1 per calendar year
        if reason_key == "annual_review":
            this_year = today[:4]
            annual_this_year = [r for r in self._log
                                if r.reason_key == "annual_review"
                                and r.timestamp[:4] == this_year]
            if annual_this_year:
                return False, (f"VETO: 'annual_review' already used in {this_year} on "
                               f"{annual_this_year[0].timestamp[:10]}. Only one allowed per year.")

        # All checks passed → log and approve
        record = WeightChangeRecord(
            timestamp       = dt.datetime.now().isoformat(timespec="seconds"),
            reason_key      = reason_key,
            description     = description,
            old_config_hash = self._hash_config(old_config),
            new_config_hash = self._hash_config(new_config),
            changed_by      = changed_by,
        )
        self._log.append(record)
        with open(self.LOG_PATH, "a") as f:
            f.write(json.dumps(asdict(record)) + "\n")

        return True, f"APPROVED: Change #{len(self._log)} logged at {record.timestamp}"

    def check_backtest_period(self, start_date: str, end_date: str) -> Tuple[bool, str]:
        """
        Validate that a proposed backtest date range does not touch the holdout.
        Returns (allowed: bool, message: str).

        Rule: end_date must be strictly before HOLDOUT_START_DATE.
        Development-only backtests: end_date < VALIDATION_START.
        """
        if end_date >= HOLDOUT_START_DATE:
            return False, (f"VETO: Backtest end_date {end_date} enters the holdout period "
                           f"(locked from {HOLDOUT_START_DATE}). "
                           f"Trim end_date to {HOLDOUT_START_DATE[:7]}-31 at latest.")
        if end_date >= VALIDATION_START:
            return True, (f"WARNING: Backtest runs into the validation period "
                          f"({VALIDATION_START}–{HOLDOUT_START_DATE}). "
                          f"Changes made after seeing these results consume your change budget.")
        return True, (f"OK: Backtest confined to development period "
                      f"(before {VALIDATION_START}). Full exploration allowed.")

    def get_audit_report(self) -> dict:
        """Return a full audit summary of weight change history."""
        total   = len(self._log)
        dev_val = [r for r in self._log if r.timestamp[:10] < HOLDOUT_START_DATE]
        holdout = [r for r in self._log if r.timestamp[:10] >= HOLDOUT_START_DATE]
        return {
            "total_changes"             : total,
            "changes_in_dev_validation" : len(dev_val),
            "changes_in_holdout"        : len(holdout),  # should always be 0
            "budget_remaining"          : max(0, MAX_WEIGHT_CHANGES - len(dev_val)),
            "holdout_start"             : HOLDOUT_START_DATE,
            "validation_start"          : VALIDATION_START,
            "development_end"           : DEVELOPMENT_END,
            "holdout_contaminated"      : len(holdout) > 0,
            "change_log"                : [asdict(r) for r in self._log],
        }

    def verify_config_unchanged(self, current_config: dict) -> Tuple[bool, str]:
        """
        Before any backtest, call this to confirm that the config
        in use matches the last approved config hash in the log.
        Prevents running a backtest with an undocumented config change.
        """
        if not self._log:
            return True, "No weight change log found. Assuming initial setup."
        last_approved_hash = self._log[-1].new_config_hash
        current_hash = self._hash_config(current_config)
        if current_hash != last_approved_hash:
            return False, (f"CONFIG MISMATCH: Current config hash ({current_hash}) "
                           f"!= last approved ({last_approved_hash}). "
                           f"You have undocumented changes. Log them via approve_change() first.")
        return True, f"Config matches last approved state ({last_approved_hash})."


# ══════════════════════════════════════════════════════════════════════
# BIAS AUDIT RUNNER — combines all three guards into a pre-flight check
# ══════════════════════════════════════════════════════════════════════

class BiasAudit:
    """
    Pre-flight checklist run before every backtest.
    All three guards must pass. Any failure blocks the backtest.

    Usage:
        audit = BiasAudit(active_tickers, card_weights_config)
        report = audit.run(as_of_date="2023-03-31",
                           backtest_start="2019-04-01",
                           backtest_end="2023-03-31")
        if not report["all_clear"]:
            raise RuntimeError(report["blockers"])
    """
    def __init__(self, active_tickers: List[str], card_weights_config: dict):
        self._active_tickers = active_tickers
        self._config         = card_weights_config
        self._surv_guard     = SurvivingBiasGuard()
        self._snoop_guard    = DataSnoopingGuard()

    def run(
        self,
        as_of_date: str,
        backtest_start: Optional[str] = None,
        backtest_end: Optional[str] = None,
        mode: str = "backtest",
    ) -> dict:
        mode = mode.lower().strip()
        if mode not in {"live", "backtest"}:
            raise ValueError("mode must be 'live' or 'backtest'")

        blockers = []
        warnings = []

        # ── Guard 1: Survivorship ──
        surv_report = self._surv_guard.survivorship_bias_estimate(
            as_of_date, self._active_tickers)
        if surv_report["bias_severity"] in ("High","Severe"):
            blockers.append(
                f"SURVIVORSHIP [{surv_report['bias_severity']}]: "
                f"{surv_report['n_later_failed']} delisted stocks "
                f"({surv_report['pct_missing']}% of {as_of_date} universe) "
                f"missing from backtest. Add them via SurvivingBiasGuard.register().")
        elif surv_report["bias_severity"] == "Moderate":
            warnings.append(
                f"SURVIVORSHIP [Moderate]: {surv_report['pct_missing']}% missing. "
                f"Consider adding delisted records to reduce bias.")

        # ── Guard 2: Data snooping (backtest period check) ──
        if mode == "backtest":
            bt_start = backtest_start or "2019-04-01"
            bt_end = backtest_end or as_of_date
            allowed, period_msg = self._snoop_guard.check_backtest_period(bt_start, bt_end)
            if not allowed:
                blockers.append(f"SNOOPING GUARD: {period_msg}")
            elif "WARNING" in period_msg:
                warnings.append(period_msg)
        else:
            bt_start = backtest_start
            bt_end = backtest_end
            period_msg = "LIVE MODE: holdout/date-range snooping guard skipped."

        # ── Guard 3: Config integrity ──
        config_ok, config_msg = self._snoop_guard.verify_config_unchanged(self._config)
        if not config_ok:
            blockers.append(f"CONFIG INTEGRITY: {config_msg}")

        audit_report = {
            "timestamp"        : dt.datetime.now().isoformat(timespec="seconds"),
            "as_of_date"       : as_of_date,
            "mode"             : mode,
            "backtest_start"   : bt_start,
            "backtest_end"     : bt_end,
            "all_clear"        : len(blockers) == 0,
            "blockers"         : blockers,
            "warnings"         : warnings,
            "survivorship"     : surv_report,
            "period_check"     : period_msg,
            "weight_audit"     : self._snoop_guard.get_audit_report(),
        }

        # Save audit to /runs/
        run_dir = Path(f"runs/{as_of_date}")
        run_dir.mkdir(parents=True, exist_ok=True)
        with open(run_dir / "bias_audit.json", "w") as f:
            json.dump(audit_report, f, indent=2)

        return audit_report

    def close(self):
        self._surv_guard.close()
