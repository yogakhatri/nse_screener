# NSE Sector-Wise Investment Engine (V2)

Purpose: find high-quality Indian stocks that are currently trading below their potential, then produce actionable outputs (`Buy Candidate`, `Watchlist`, `Avoid`) with risk-aware entry plans.

This project is not a clone of any product. It is built as a transparent, configurable, sector-aware investment research engine.

## What This Project Does

The engine evaluates each stock on six major cards:
1. `Performance`
2. `Valuation`
3. `Growth`
4. `Profitability`
5. `Entry Point`
6. `Red Flags`

Then it adds an advanced decision layer:
1. Bear/neutral/bull market mode handling
2. Sector regime analysis
3. Drawdown resilience estimation
4. Valuation confidence score
5. Expected upside vs downside
6. Risk-reward score
7. Investability hard gate
8. Staged entry plan
9. Portfolio cap controls
10. Post-call outcome monitoring

## Core Design

### 1) Scoring
- Peer-relative percentile scoring by sector hierarchy:
  - Basic Industry -> Industry -> Sector fallback
- Winsorized peer distributions to reduce outlier impact
- Coverage-aware card rankability

### 2) Decisioning
- `Opportunity Score` from 5 cards (red flags applied as penalty/cap)
- `Potential Score` for long-term compounding quality
- `Valuation Gap Score` for discount-to-potential context
- `Selection Score` for final ranking using reward/risk balance

### 3) Actionability
- Recommendation per stock:
  - `Buy Candidate`
  - `Watchlist`
  - `Avoid`
- Each stock gets:
  - confidence level
  - gate pass/fail reasons
  - staged entry plan
  - risk budget hints

## Repo Layout

```text
engine/                   # Scoring engine, models, cards, aggregation, advanced overlays
scripts/                  # Data loading, run orchestration, storage/logging
docs/                     # Data dictionary and source registry
data/                     # Raw + processed datasets (local)
runs/<YYYY-MM-DD>/        # Run artifacts and reports
logs/                     # Cross-run history and monitoring logs
tests/                    # Unit tests
app.py                    # Streamlit dashboard
```

## New Modules (V2.1)

### Data Pipeline Enhancements

| Script | Purpose | Make Target |
|--------|---------|-------------|
| `scripts/enrich_fundamentals.py` | Compute missing Growth/Profitability metrics from available data | `make enrich-fundamentals` |
| `scripts/fetch_asm_gsm.py` | Fetch ASM/GSM surveillance lists from NSE | (manual) |
| `scripts/fetch_shareholding.py` | Fetch promoter holding & pledge data from BSE | `make fetch-shareholding` |
| `scripts/fetch_delivery_data.py` | Download NSE delivery position files, compute volume_delivery score | `make fetch-delivery` |
| `scripts/fetch_index_data.py` | Download Nifty index EOD CSVs for RS/peer metrics | `make fetch-indices` |

### Advanced Scoring & Analysis

| Script | Purpose | Make Target |
|--------|---------|-------------|
| `scripts/momentum_scoring.py` | Dual momentum, trend strength, breakout score, mean reversion risk | `make momentum-scoring` |
| `scripts/institutional_tracking.py` | MF/FII/DII holding changes, bulk deals, fresh MF entry | `make institutional-tracking` |
| `scripts/earnings_surprise.py` | Detect revenue/profit/margin surprises vs 3Y trend | `make earnings-surprise` |
| `scripts/forward_pe_peg.py` | Estimate forward PE, PEG ratio, GARP score | `make forward-pe-peg` |

### Reporting & Distribution

| Script | Purpose | Make Target |
|--------|---------|-------------|
| `scripts/stock_explainer.py` | Generate per-stock "Why This Stock?" investment theses | `make stock-explainer` |
| `scripts/backtest.py` | Validate past recommendations against actual forward returns | `make backtest` |
| `scripts/telegram_alerts.py` | Send daily picks via Telegram bot | `make telegram-alerts` |
| `scripts/data_freshness.py` | Data staleness and quality report before engine runs | `make data-freshness` |
| `scripts/setup_scheduler.py` | Setup daily cron/launchd scheduling | (one-time setup) |
| `app.py` | Interactive Streamlit dashboard | `make dashboard` |

## Configuration Philosophy

All runtime behavior is configurable in:
- [engine/config.py](/Users/yk/work/nse_screener/engine/config.py)

That file is intentionally heavily commented. It explains:
- what each field controls
- what happens when you raise/lower each threshold
- suggested safe ranges

If you are onboarding new teammates, start with `engine/config.py`.

## Data Inputs

Primary run flow expects a dated Screener-format CSV as the universe source:
- `--screener-csv path/to/file.csv`

Recommended daily build path:
1. Fetch NSE symbol universe CSV for the run date (EQ series) from official archive bhavcopy.
2. Enrich symbols using classification master (Sector/Industry/Basic Industry).
3. Merge fundamentals CSV onto that universe.
4. Run quality-gated engine.

The loader supports alias-based mapping and computes derived fields where possible:
- intrinsic gap
- distress risk
- ASM/GSM risk
- GNPA/CAR/PCR/ALM/liquidity/governance risk

Reference:
- [scripts/load_data.py](/Users/yk/work/nse_screener/scripts/load_data.py)
- [docs/screener_csv_template.csv](/Users/yk/work/nse_screener/docs/screener_csv_template.csv)
- [docs/nse_universe_template.csv](/Users/yk/work/nse_screener/docs/nse_universe_template.csv)
- [docs/nse_symbol_classification_template.csv](/Users/yk/work/nse_screener/docs/nse_symbol_classification_template.csv)

## Start Here (Sequential)

Use this exact sequence on a fresh clone.

## User Checklist (Copy-Paste)

If you just want the engine to run end-to-end:

```bash
make init
export RUN_DATE=2026-03-16
make fetch-universe RUN_DATE=$RUN_DATE
make fetch-price-history RUN_DATE=$RUN_DATE SESSIONS=260
make daily-run RUN_DATE=$RUN_DATE
```

If you have a real fundamentals export and want production-quality recommendations:

```bash
export RUN_DATE=2026-03-12
# Put real file here first:
# data/raw/fundamentals/screener/full_fundamentals_${RUN_DATE}.csv
wc -l data/raw/fundamentals/screener/full_fundamentals_${RUN_DATE}.csv
make daily-run RUN_DATE=$RUN_DATE
```

Expected behavior of `daily-run`:
1. If fundamentals file is valid (header + at least 1 data row), it runs production mode.
2. If missing/empty, it auto-falls back to debug mode so pipeline still completes.
3. After changing `engine/config.py`, run `make check-config` before `make daily-run`.
4. Production mode now blocks if any active template is unsupported.
5. Raw price/technical metrics are computed from local bhavcopy history when available.

### Step 1: Clone and initialize

```bash
git clone <YOUR_GITHUB_REPO_URL> nse_screener
cd nse_screener
make init
```

### Step 2: Choose run date

```bash
export RUN_DATE=2026-03-12
```

### Step 3: Fetch NSE universe

```bash
make fetch-universe RUN_DATE=$RUN_DATE
```

Notes:
1. This fetches from official NSE bhavcopy (UDiFF).
2. It auto-falls back to earlier trading day if needed.
3. Missing taxonomy rows are written to:
   `data/processed/universe/missing_classification_${RUN_DATE}.csv`
4. Strict taxonomy mode (optional):
   `make fetch-universe RUN_DATE=$RUN_DATE REQUIRE_CLASSIFICATION=true`

### Step 4: Scrape fundamentals (free, no account needed)

```bash
# First run with limit=50 to test — takes ~2 min
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_LIMIT=50

# Full scrape of all ~2500 NSE stocks — takes ~2-3 hours
make fetch-screener-data RUN_DATE=$RUN_DATE
```

Notes:
1. Scrapes public pages at `screener.in/company/<SYMBOL>/consolidated/` — **no account, no login**.
2. Respects a 1.5s delay between requests by default. Do not lower below 1.0.
3. Results are cached per symbol in `data/raw/fundamentals/screener/cache/` — interrupted runs resume automatically.
4. Use `SCRAPER_WORKERS=2` for faster runs (keep ≤ 3 to avoid being blocked).
5. Once built, re-run daily only fetches symbols without a cached entry for that date.
6. Override delay: `SCRAPER_DELAY=2.0` for safer rate limiting.

### Step 5: Backfill price history (recommended)

```bash
make fetch-price-history RUN_DATE=$RUN_DATE SESSIONS=260
```

Notes:
1. This backfills local bhavcopy history used for raw price-derived metrics.
2. The engine will prefer local raw history for returns, RSI, DMA distance,
   drawdown recovery, volatility compression, volume ratio, and turnover.
3. If raw history is missing, CSV values are used only as fallback.

### Step 6: Run the engine

```bash
make daily-run RUN_DATE=$RUN_DATE
```

`daily-run` automatically:
1. Checks data freshness
2. Fetches the NSE universe
3. Scrapes fundamentals if the CSV doesn't exist yet
4. Enriches with computed metrics
5. Runs the engine in production mode (or debug if data is insufficient)
6. Generates investment theses for the top picks

**Windows PowerShell:**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

## Quick Start — Complete Daily Flow

```bash
export RUN_DATE=$(date +%F)   # today, e.g. 2026-03-18

# 1. First-time setup (only once)
make init

# 2. Fetch NSE stock universe
make fetch-universe RUN_DATE=$RUN_DATE

# 3. Scrape fundamentals from screener.in (free, ~2-3 hrs for full universe)
#    First time: start with a test of 50 stocks
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_LIMIT=50
#    Then run full (cache means subsequent days are much faster)
make fetch-screener-data RUN_DATE=$RUN_DATE

# 4. Backfill price history once (260 trading days ≈ 1 year)
make fetch-price-history RUN_DATE=$RUN_DATE SESSIONS=260

# 5. Run everything
make daily-run RUN_DATE=$RUN_DATE

# 6. View results
cat runs/$RUN_DATE/buy_candidates.csv
cat runs/$RUN_DATE/leaderboard.csv | head -20
make dashboard   # interactive viewer
```

### Optional: Fetch supplemental data before daily-run

```bash
# Delivery volume data (fills volume_delivery metric)
make fetch-delivery RUN_DATE=$RUN_DATE DELIVERY_SESSIONS=60

# Nifty index data (fills rs_turn, peer_price_strength)
make fetch-indices RUN_DATE=$RUN_DATE INDEX_SESSIONS=260

# Shareholding patterns (promoter pledge, FII/DII holdings)
make fetch-shareholding RUN_DATE=$RUN_DATE
```

### Optional: Post-run analysis

```bash
# Generate investment theses for top picks
make stock-explainer RUN_DATE=$RUN_DATE

# Advanced scoring overlays
make momentum-scoring RUN_DATE=$RUN_DATE
make earnings-surprise RUN_DATE=$RUN_DATE
make forward-pe-peg RUN_DATE=$RUN_DATE

# Launch interactive dashboard
make dashboard

# Send picks via Telegram (set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
make telegram-alerts RUN_DATE=$RUN_DATE DRY_RUN=true

# Backtest past recommendations
make backtest RUN_DATE=$RUN_DATE
```

### Scheduling (one-time setup)

```bash
# macOS: install launchd plist (runs Mon-Fri at 19:00)
python scripts/setup_scheduler.py --install

# Check status
python scripts/setup_scheduler.py --status

# Remove
python scripts/setup_scheduler.py --uninstall
```

## CLI Options

Main runner:
- [scripts/run_engine.py](/Users/yk/work/nse_screener/scripts/run_engine.py)
- [scripts/bootstrap.py](/Users/yk/work/nse_screener/scripts/bootstrap.py)
- [scripts/fetch_nse_universe.py](/Users/yk/work/nse_screener/scripts/fetch_nse_universe.py)
- [Makefile](/Users/yk/work/nse_screener/Makefile)

Recommended command wrappers:
1. `make init` -> first-time setup after clone
2. `make bootstrap` -> regenerate template/folders
3. `make fetch-universe RUN_DATE=YYYY-MM-DD` -> fetch universe from official NSE bhavcopy (UDiFF) with automatic fallback to earlier trading day when needed
4. `make fetch-screener-data RUN_DATE=YYYY-MM-DD [SCRAPER_DELAY=1.5] [SCRAPER_LIMIT=N] [SCRAPER_WORKERS=1]` -> scrape fundamentals from public screener.in pages (no account needed)
5. `make fetch-price-history RUN_DATE=YYYY-MM-DD [SESSIONS=260]` -> backfill bhavcopy history for raw price-derived metrics
6. `make prepare-universe RUN_DATE=YYYY-MM-DD NSE_UNIVERSE_CSV=... FUNDAMENTALS_CSV=...
7. `make daily-run RUN_DATE=YYYY-MM-DD` -> full pipeline: freshness check + universe + scrape + enrich + run
7. `make check-config` -> validate config + loader/scorer compatibility before a run
8. `make prepare-csv RUN_DATE=YYYY-MM-DD` -> fallback manual template path
9. `make run RUN_DATE=YYYY-MM-DD SCREENER_CSV=...` -> live/backtest run
10. `make run-debug RUN_DATE=YYYY-MM-DD SCREENER_CSV=...` -> debug run (skips quality gate)
11. `make auto-run RUN_DATE=YYYY-MM-DD [FUNDAMENTALS_CSV=...]` -> alias of daily-run
12. `make run-backtest RUN_DATE=YYYY-MM-DD SCREENER_CSV=...`
13. `make enrich-fundamentals RUN_DATE=YYYY-MM-DD [SCRAPE=true] [SCRAPE_LIMIT=N]` -> compute missing metrics
14. `make fetch-delivery RUN_DATE=YYYY-MM-DD [DELIVERY_SESSIONS=60]` -> NSE delivery data
15. `make fetch-indices RUN_DATE=YYYY-MM-DD [INDEX_SESSIONS=260]` -> Nifty index data
16. `make fetch-shareholding RUN_DATE=YYYY-MM-DD [SHP_LIMIT=N]` -> BSE shareholding data
17. `make momentum-scoring RUN_DATE=YYYY-MM-DD` -> advanced momentum metrics
18. `make institutional-tracking RUN_DATE=YYYY-MM-DD` -> MF/FII holding changes
19. `make earnings-surprise RUN_DATE=YYYY-MM-DD` -> earnings surprise detection
20. `make forward-pe-peg RUN_DATE=YYYY-MM-DD` -> forward PE, PEG, GARP score
21. `make stock-explainer RUN_DATE=YYYY-MM-DD` -> generate investment theses
22. `make data-freshness RUN_DATE=YYYY-MM-DD` -> data staleness report
23. `make telegram-alerts RUN_DATE=YYYY-MM-DD [DRY_RUN=true]` -> Telegram notifications
24. `make backtest RUN_DATE=YYYY-MM-DD` -> backtest past recommendations
25. `make dashboard` -> launch Streamlit dashboard

Important flags:
1. `--mode live|backtest`
2. `--market-mode auto|bear|neutral|bull`
3. `--tickers TCS,INFY,HDFCBANK` (optional filter)
4. `--strict-freshness` (fail if stale dataset)
5. `--skip-quality-gate` (debug only; bypasses universe/coverage safety checks)
6. `--min-universe-size` / `--min-avg-core-rankable-pct` / `--min-core-cards-with-rankable`
7. `--min-classification-coverage-pct`

Default input quality gate (enabled by default):
1. symbols loaded >= `250`
2. average rankable% across core cards >= `8%`
3. at least `3` core cards with non-zero rankable%
4. non-generic taxonomy coverage >= `90%` (Sector/Industry/Basic Industry)
5. every active template must meet template-level publish support thresholds

## Troubleshooting

1. Error: `Fundamentals CSV not found: data/raw/fundamentals/screener/full_fundamentals_<date>.csv`
- Cause: file is missing.
- Fix:
```bash
ls data/raw/fundamentals/screener/
# easiest: run automatic mode; it will fallback to debug if file is missing
make daily-run RUN_DATE=2026-03-12
```

2. Error: `CSV is empty: ...full_fundamentals_<date>.csv`
- Cause: fundamentals file exists but has no columns/data.
- Fix:
```bash
# Option A: replace with a valid fundamentals export
# Option B: run automatic mode and let it fallback to debug
make daily-run RUN_DATE=2026-03-12
```

3. Error: `INPUT QUALITY GATE FAILED`
- Cause: strict production checks failed due to low fundamentals/taxonomy coverage.
- Fix: either provide complete fundamentals + taxonomy and rerun `make run`, or use `make run-debug` for pipeline verification only.

## Run Outputs

Each run writes to `runs/<YYYY-MM-DD>/`.

Primary files:
1. `leaderboard.csv`  
   Global rank sorted by selection score (reward/risk aware).
2. `action_sheet.csv`  
   Best operational file for daily decisions (recommendation, confidence, staged entry, gate notes).
3. `buy_candidates.csv`  
   Investable buy list.
4. `undervalued_high_potential.csv`  
   Discount-first shortlist.
5. `red_flag_exclusions.csv`  
   Risk-first rejection list.
6. `sector_top_10.csv`  
   Top names per sector.
7. `sector_summary.csv`  
   Sector breadth and buy-candidate density.
8. `portfolio_plan.csv`  
   Suggested picks under sector/single-name caps.
9. `stock_<TICKER>.json`  
   Full detailed stock report.
10. `model_monitoring.json`  
    Post-call outcome metrics and recalibration alerts.
11. `coverage_snapshot.json` / `coverage_by_template_card.csv`  
    Data completeness diagnostics.
12. `bias_audit.json`  
    Survivorship/snooping/config integrity checks.

## Recommendation Logic (High Level)

`Buy Candidate` requires:
1. Strong potential score
2. Strong valuation gap score
3. Investable status
4. Investability gate pass
5. Sufficient confidence

Down-market mode (`bear`) is intentionally stricter:
- higher quality requirements
- tighter red-flag thresholds
- stronger downside penalty

## Portfolio Controls

Configured in [engine/config.py](/Users/yk/work/nse_screener/engine/config.py):
1. max holdings
2. max sector weight
3. max single stock weight
4. minimum confidence for inclusion

The portfolio output is a research suggestion, not execution advice.

## Monitoring and Recalibration

Run history is appended to:
- `logs/recommendation_history.csv`

The engine evaluates matured calls (configurable horizon) and outputs:
- hit rates
- average returns
- recalibration alert flags

If `buy_hit_rate` drops below threshold, review:
1. market-mode thresholds
2. gate strictness
3. valuation confidence handling
4. sector regime weighting

## Development and Testing

Syntax check:
```bash
make check
```

Unit tests:
```bash
make test
```

Config validation:
```bash
make check-config
```

## Important Files

Treat these as core logic files:
1. [engine/config.py](/Users/yk/work/nse_screener/engine/config.py)
2. [engine/metric_definitions.py](/Users/yk/work/nse_screener/engine/metric_definitions.py)
3. [engine/aggregator.py](/Users/yk/work/nse_screener/engine/aggregator.py)
4. [engine/advanced.py](/Users/yk/work/nse_screener/engine/advanced.py)
5. [scripts/run_engine.py](/Users/yk/work/nse_screener/scripts/run_engine.py)

## Streamlit Dashboard

Launch the interactive dashboard:

```bash
make dashboard
```

Five tabs:
1. **Overview** — Score distribution histogram, recommendation breakdown pie chart, top picks table
2. **Leaderboard** — Sortable/filterable full stock ranking with CSV download
3. **Stock Detail** — Per-stock deep dive: card scores, sub-scores, investment thesis, gate status
4. **Sector View** — Sector comparison bar charts, top picks per sector
5. **Run Quality** — Coverage diagnostics, bias audit, data completeness

## Telegram Alerts

Send daily picks to a Telegram channel:

```bash
# Set environment variables
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"

# Dry run (prints to stdout only)
make telegram-alerts RUN_DATE=$RUN_DATE DRY_RUN=true

# Live send
make telegram-alerts RUN_DATE=$RUN_DATE
```

## CI/CD (GitHub Actions)

Three workflows are included in `.github/workflows/`:

| Workflow | Trigger | What it does |
|----------|---------|-------------|
| **CI** (`ci.yml`) | Push/PR to `main` | Syntax check, config validation, unit tests (Python 3.12 + 3.13) |
| **Daily Run** (`daily-run.yml`) | Mon-Fri 13:45 UTC (19:15 IST) + manual | Fetch universe, price history, run engine (debug mode), upload run artifacts |
| **Weekly Backtest** (`weekly-backtest.yml`) | Sunday 10:00 UTC + manual | Run backtester, upload reports |

All workflows support `workflow_dispatch` for manual triggering with optional date override.

### Required GitHub Secrets (for optional Telegram alerts)

| Secret | Purpose |
|--------|---------|
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Target channel/group ID |

To enable Telegram alerts in the daily workflow, uncomment the final step in `daily-run.yml`.

### What GitHub Actions CAN automate

- Syntax/lint checks on every push
- Unit tests on every PR
- Daily universe fetch + engine run (debug mode without external fundamentals)
- Weekly backtesting of past recommendations
- Artifact archival (30-day retention for runs, 90-day for backtests)
- Telegram notifications on successful runs

### What requires manual steps

- **BSE shareholding fetch** — BSE India blocks datacenter IPs; run locally
- **NSE delivery data** — NSE blocks non-browser requests from cloud servers
- **Classification master updates** — manual sector/industry taxonomy curation
- **Streamlit dashboard** — interactive app, run locally with `make dashboard`
- **Scheduler setup** — one-time local setup with `python scripts/setup_scheduler.py --install`

## Notes

- This is a decision-support research engine.
- It does not execute trades.
- Keep data quality and freshness controls strict if using it in live workflows.
- Data files (`data/`), run outputs (`runs/`), and logs are gitignored — only code is versioned.
