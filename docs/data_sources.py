"""
NSE Rating Engine — Data Sources & Acquisition Rules
======================================================
COMPLIANCE NOTE:
  NSE Terms of Use explicitly prohibit systematic/automated data collection
  (scraping, data mining, extraction) from nseindia.com or its app.
  All acquisition patterns below are compliant: they use either
  (a) official downloadable archive files (batch, not page-scraping),
  (b) licensed third-party redistributors, or
  (c) low-frequency manual downloads cached locally.
  Do NOT build a real-time crawler against nseindia.com.
"""
from dataclasses import dataclass, field
from typing import List

# ─── Source Registry ───────────────────────────────────────────────
@dataclass
class DataSource:
    source_id:   str
    name:        str
    source_type: str          # "official_archive" | "official_filing" | "licensed_api" | "computed"
    base_url:    str
    format:      str          # "zip/csv" | "json" | "pdf" | "xbrl/xml" | "manual_export"
    refresh_sla: str          # how often to pull
    cache_ttl_hours: int      # local cache lifetime before stale
    rate_limit_note: str
    local_path:  str          # relative to project root /data/raw/

SOURCES = {
    # ── PRICE / VOLUME / DELIVERY ──────────────────────────────────
    "nse_bhavcopy": DataSource(
        source_id    = "nse_bhavcopy",
        name         = "NSE UDiFF CM Bhavcopy (Daily ZIP)",
        source_type  = "official_archive",
        base_url     = "https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MMM}/cm{DD}{MMM}{YYYY}bhav.csv.zip",
        format       = "zip/csv",
        refresh_sla  = "Daily (after 18:30 IST on trading days)",
        cache_ttl_hours = 20,   # re-download next trading day
        rate_limit_note = "Manual download or low-frequency script (<10 req/day). "
                          "Do NOT loop over all dates in one session. "
                          "Download at end-of-day, cache locally, never re-download cached dates.",
        local_path   = "data/raw/prices/bhavcopy/",
    ),

    "nse_delivery": DataSource(
        source_id    = "nse_delivery",
        name         = "NSE Security-wise Delivery Position (daily)",
        source_type  = "official_archive",
        base_url     = "https://archives.nseindia.com/products/content/sec_del_eq_{DDMMYYYY}.zip",
        format       = "zip/csv",
        refresh_sla  = "Daily (after 18:30 IST)",
        cache_ttl_hours = 20,
        rate_limit_note = "Same as bhavcopy — one file per trading day, cache immediately.",
        local_path   = "data/raw/prices/delivery/",
    ),

    "nse_index_eod": DataSource(
        source_id    = "nse_index_eod",
        name         = "NSE Index EOD Closing Values (Nifty 500 + all indices)",
        source_type  = "official_archive",
        base_url     = "https://archives.nseindia.com/content/indices/ind_close_all_{DD}{MMM}{YYYY}.csv",
        format       = "csv",
        refresh_sla  = "Daily",
        cache_ttl_hours = 20,
        rate_limit_note = "One file per day. Cache locally. Used for Nifty 500 RS computation.",
        local_path   = "data/raw/prices/indices/",
    ),

    "yfinance_fallback": DataSource(
        source_id    = "yfinance_fallback",
        name         = "yfinance (Yahoo Finance redistribution) — FALLBACK ONLY",
        source_type  = "licensed_api",  # Yahoo redistributes NSE data
        base_url     = "https://query1.finance.yahoo.com/v8/finance/chart/{TICKER}.NS",
        format       = "json",
        refresh_sla  = "On-demand (fallback when NSE archive unavailable)",
        cache_ttl_hours = 24,
        rate_limit_note = "Use only when NSE archive files are missing or corrupt. "
                          "Do not use as primary source. Rate limit: < 2000 calls/hour.",
        local_path   = "data/raw/prices/yfinance_cache/",
    ),

    # ── FUNDAMENTALS ───────────────────────────────────────────────
    "nse_xbrl_filings": DataSource(
        source_id    = "nse_xbrl_filings",
        name         = "NSE Corporate Financial Results (XBRL/XML)",
        source_type  = "official_filing",
        base_url     = "https://www.nseindia.com/companies-listing/corporate-filings-financial-results",
        format       = "xbrl/xml",
        refresh_sla  = "Quarterly (within 45 days of quarter end per SEBI)",
        cache_ttl_hours = 90 * 24,  # re-check quarterly
        rate_limit_note = "Download result XML files from NSE/BSE portals manually or via "
                          "licensed XBRL aggregators. Do NOT bulk-scrape the NSE results page.",
        local_path   = "data/raw/fundamentals/xbrl/",
    ),

    "bse_filings": DataSource(
        source_id    = "bse_filings",
        name         = "BSE Corporate Filings (Financial Results + Shareholding Pattern)",
        source_type  = "official_filing",
        base_url     = "https://www.bseindia.com/corporates/Comp_Resultsnew.aspx",
        format       = "xbrl/xml",
        refresh_sla  = "Quarterly",
        cache_ttl_hours = 90 * 24,
        rate_limit_note = "Same as NSE XBRL. Use BSE as backup for missing NSE filings.",
        local_path   = "data/raw/fundamentals/bse_xbrl/",
    ),

    "screener_export": DataSource(
        source_id    = "screener_export",
        name         = "Screener.in Manual CSV Export (fundamentals + ratios)",
        source_type  = "licensed_api",
        base_url     = "https://www.screener.in/company/{SYMBOL}/",
        format       = "manual_export",
        refresh_sla  = "Quarterly (after each results season; export fresh CSV)",
        cache_ttl_hours = 90 * 24,
        rate_limit_note = "Manual export from Screener.in Pro account. "
                          "Do NOT automate scraping of Screener pages. "
                          "The export CSV covers: P&L, Balance Sheet, Cash Flow, Ratios, Shareholding.",
        local_path   = "data/raw/fundamentals/screener/",
    ),

    "trendlyne_api": DataSource(
        source_id    = "trendlyne_api",
        name         = "Trendlyne Premium API (forward estimates + NIM/NPA/PCR for banks)",
        source_type  = "licensed_api",
        base_url     = "https://trendlyne.com/developers/",
        format       = "json",
        refresh_sla  = "Quarterly (or as estimates are revised)",
        cache_ttl_hours = 7 * 24,
        rate_limit_note = "Per Trendlyne API fair-use policy. Cache responses; avoid re-hitting for unchanged stocks.",
        local_path   = "data/raw/fundamentals/trendlyne/",
    ),

    # ── RED FLAG INPUTS ────────────────────────────────────────────
    "nse_asm_list": DataSource(
        source_id    = "nse_asm_list",
        name         = "NSE Additional Surveillance Measure (ASM) List",
        source_type  = "official_archive",
        base_url     = "https://www.nseindia.com/static/regulations/additional-surveillance-measure",
        format       = "pdf/csv",
        refresh_sla  = "Before EVERY engine run (ASM can change overnight)",
        cache_ttl_hours = 12,  # stale after 12 hrs — must re-download on run day
        rate_limit_note = "One download per run session. Cache for same-day re-runs only.",
        local_path   = "data/raw/redflags/asm/",
    ),

    "nse_gsm_list": DataSource(
        source_id    = "nse_gsm_list",
        name         = "NSE Graded Surveillance Measure (GSM) List",
        source_type  = "official_archive",
        base_url     = "https://www.nseindia.com/regulations/graded-surveillance-measure",
        format       = "pdf/csv",
        refresh_sla  = "Before EVERY engine run",
        cache_ttl_hours = 12,
        rate_limit_note = "Same as ASM list.",
        local_path   = "data/raw/redflags/gsm/",
    ),

    "bse_shareholding": DataSource(
        source_id    = "bse_shareholding",
        name         = "BSE Shareholding Pattern (promoter pledge data)",
        source_type  = "official_filing",
        base_url     = "https://www.bseindia.com/corporates/shpSummary.html",
        format       = "xbrl/xml",
        refresh_sla  = "Quarterly (within 21 days of quarter end per SEBI regulation)",
        cache_ttl_hours = 90 * 24,
        rate_limit_note = "Download quarterly; cache immediately.",
        local_path   = "data/raw/redflags/shareholding/",
    ),

    # ── CLASSIFICATION ─────────────────────────────────────────────
    "nse_industry_classification": DataSource(
        source_id    = "nse_industry_classification",
        name         = "NSE Indices Industry Classification Structure (4-tier)",
        source_type  = "official_archive",
        base_url     = "https://nsearchives.nseindia.com/web/sites/default/files/inline-files/nse-indices_industry-classification-structure-2023-07.pdf",
        format       = "pdf",
        refresh_sla  = "Annual (NSE revises periodically; check for new version each April)",
        cache_ttl_hours = 365 * 24,
        rate_limit_note = "One-time download; update annually.",
        local_path   = "data/raw/classification/",
    ),
}

# ─── Data Freshness SLAs ──────────────────────────────────────────
FRESHNESS_SLAS = {
    "prices":           "Daily — download bhavcopy before engine run on any trading day",
    "delivery_data":    "Daily — same session as bhavcopy",
    "index_eod":        "Daily — same session as bhavcopy",
    "fundamentals":     "Quarterly — refresh within 7 days of each quarter's results season close (typically Jan/Apr/Jul/Oct)",
    "shareholding":     "Quarterly — refresh within 5 days of BSE SHP filing deadline",
    "asm_gsm":          "Every run — must be < 12 hours old at time of scoring",
    "forward_estimates":"Quarterly — refresh when Trendlyne/Tickertape consensus updates",
    "classification":   "Annual — check for NSE revision each April",
    "credit_ratings":   "Event-driven — scan BSE announcements monthly for downgrades",
}

# ─── Survivorship Bias Rules ──────────────────────────────────────
SURVIVORSHIP_RULES = {
    "current_ranking": (
        "EXCLUDE delisted and suspended stocks from live scoring. "
        "Use NSE active equity list (EQ series, normal trading) as universe filter. "
        "Stocks in T2T, BE, BZ, IL series are included but flagged in liquidity risk."
    ),
    "backtesting": (
        "INCLUDE delisted/suspended names in historical data for backtesting. "
        "Maintain a separate delisted_stocks.csv with delist date and reason. "
        "Any backtest that starts before a stock's delist date must include it "
        "up to that date. Ignoring delisted names creates survivorship bias."
    ),
    "suspended_treatment": (
        "Stocks suspended for < 30 days: flag as data gap, retain in universe. "
        "Stocks suspended > 30 days: exclude from current ranking run; move to watchlist."
    ),
}

