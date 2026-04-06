# NSE Sector-Wise Investment Engine

Screens NSE stocks using six scoring cards (Performance, Valuation, Growth, Profitability, Entry Point, Red Flags) and outputs actionable picks: **Buy Candidate**, **Watchlist**, **Avoid** — with staged entry plans.

## Quick Start

### macOS / Linux

Use this path if you are on macOS or Linux, or on Windows with WSL/Git Bash and GNU `make`.

### 1. Setup (once)

```bash
git clone <repo> nse_screener
cd nse_screener
make init
```

### 2. Set run date

```bash
export RUN_DATE=$(date +%F)   # or e.g. 2026-03-28
```

### 3. Fetch universe

```bash
make fetch-universe RUN_DATE=$RUN_DATE
```

### 4. Scrape fundamentals

```bash
# Test with 50 stocks first (~2 min)
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_LIMIT=50

# Full scrape (~2-3 hrs, resumes on interruption)
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_WORKERS=6 SCRAPER_DELAY=3.0
```

No account needed. Scrapes public screener.in pages. Keep `SCRAPER_WORKERS` <= 3.

### 5. Backfill price history (once)

```bash
make fetch-price-history RUN_DATE=$RUN_DATE SESSIONS=260
```

### 6. Run

```bash
make daily-run RUN_DATE=$RUN_DATE
```

`daily-run` now has two practical modes:

- If you provide a separate full fundamentals file via `FUNDAMENTALS_CSV=...`, it uses production mode and enforces the input quality gate.
- If it is using the scraper-built `SCREENER_CSV` from `fetch-screener-data`, it uses debug mode automatically so the run can complete even when coverage is not production-grade.

Typical local usage with scraper-built data:

```bash
make daily-run RUN_DATE=$RUN_DATE
```

Production-style run with a separate curated fundamentals file:

```bash
make daily-run RUN_DATE=$RUN_DATE FUNDAMENTALS_CSV=data/raw/fundamentals/screener/full_fundamentals_$RUN_DATE.csv
```

### 7. View results

```bash
cat runs/$RUN_DATE/buy_candidates.csv
make dashboard
```

The dashboard must be started with Streamlit, not with plain Python.

### Windows (PowerShell)

Use this path for native Windows PowerShell. The Makefile targets are Unix-oriented, so on native Windows it is simpler to run the Python scripts directly in sequence.

### 1. Setup (once)

```powershell
git clone <repo> nse_screener
cd nse_screener
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python scripts/bootstrap.py --sample-row
```

### 2. Set variables

```powershell
$env:RUN_DATE = Get-Date -Format 'yyyy-MM-dd'
$UNIVERSE = "data/raw/universe/nse_symbols_$env:RUN_DATE.csv"
$SCREENER = "data/raw/fundamentals/screener/screener_export_$env:RUN_DATE.csv"
```

### 3. Fetch universe

```powershell
python scripts/fetch_nse_universe.py `
  --date $env:RUN_DATE `
  --output-csv $UNIVERSE `
  --report-json "data/processed/universe/universe_fetch_$env:RUN_DATE.json" `
  --classification-csv "data/raw/classification/nse_symbol_classification_master.csv" `
  --missing-classification-csv "data/processed/universe/missing_classification_$env:RUN_DATE.csv" `
  --force
```

### 4. Scrape fundamentals

```powershell
# Test with 50 stocks first
python scripts/fetch_fundamentals_screener.py `
  --universe $UNIVERSE `
  --output $SCREENER `
  --date $env:RUN_DATE `
  --limit 50 `
  --workers 1 `
  --delay 1.5

# Full scrape
python scripts/fetch_fundamentals_screener.py `
  --universe $UNIVERSE `
  --output $SCREENER `
  --date $env:RUN_DATE `
  --workers 3 `
  --delay 3.0
```

### 5. Backfill price history (once)

```powershell
python scripts/fetch_price_history.py `
  --end-date $env:RUN_DATE `
  --sessions 260 `
  --max-calendar-days 520
```

### 6. Enrich and run

For scraper-built data, use debug mode:

```powershell
python scripts/enrich_fundamentals.py `
  --input $SCREENER `
  --output $SCREENER `
  --report "data/processed/enrichment_report_$env:RUN_DATE.json"

python scripts/run_engine.py `
  --date $env:RUN_DATE `
  --mode live `
  --market-mode auto `
  --screener-csv $SCREENER `
  --skip-quality-gate
```

For strict production-quality runs with a separate curated fundamentals file, prepare the universe with that file and run without `--skip-quality-gate`:

```powershell
$FULL_FUND = "data/raw/fundamentals/screener/full_fundamentals_$env:RUN_DATE.csv"

python scripts/prepare_universe.py `
  --date $env:RUN_DATE `
  --universe-csv $UNIVERSE `
  --fundamentals-csv $FULL_FUND `
  --output-csv $SCREENER `
  --report-json "data/processed/universe/universe_prep_$env:RUN_DATE.json" `
  --force

python scripts/enrich_fundamentals.py `
  --input $SCREENER `
  --output $SCREENER `
  --report "data/processed/enrichment_report_$env:RUN_DATE.json"

python scripts/run_engine.py `
  --date $env:RUN_DATE `
  --mode live `
  --market-mode auto `
  --screener-csv $SCREENER
```

### 7. View results

```powershell
Get-Content "runs/$env:RUN_DATE/buy_candidates.csv"
streamlit run app.py
```

---

## Supplemental Data (optional, before daily-run)

```bash
make fetch-delivery RUN_DATE=$RUN_DATE DELIVERY_SESSIONS=60    # NSE delivery volume
make fetch-indices RUN_DATE=$RUN_DATE INDEX_SESSIONS=260        # Nifty index data
make fetch-shareholding RUN_DATE=$RUN_DATE                      # BSE shareholding/pledge
```

Note: BSE/NSE block datacenter IPs — run these locally, not in CI.

## Daily Run Behavior

`make daily-run` performs the following:

1. runs a freshness check
2. fetches the NSE universe
3. scrapes fundamentals from screener.in
4. enriches the resulting CSV
5. runs the engine
6. generates post-run explainers

### Mode selection

- Debug mode:
  used when the run is based on the scraper-built `SCREENER_CSV`
  skips the strict production quality gate by calling `make run-debug`

- Production mode:
  used only when you explicitly provide a separate `FUNDAMENTALS_CSV`
  calls `make run` and enforces the quality gate

### Why this matters

Scraper-built data is often good enough for research and dashboard review, but it may still be incomplete for:

- valuation coverage
- classification coverage
- some profitability and growth enrichments

If production mode is triggered on scraper-only data, the run may fail with:

- `INPUT QUALITY GATE FAILED`
- low rankable coverage
- unsupported template coverage

That is expected behavior for production mode and does not necessarily mean the code is broken.

### If `daily-run` fails with `INPUT QUALITY GATE FAILED`

Use one of these approaches:

- For normal local research:
  run `make daily-run RUN_DATE=$RUN_DATE` and let it use debug mode on scraper-built data.

- For strict production-quality runs:
  provide a separate curated fundamentals file:

```bash
make daily-run RUN_DATE=$RUN_DATE FUNDAMENTALS_CSV=data/raw/fundamentals/screener/full_fundamentals_$RUN_DATE.csv
```

- For one-off debugging:

```bash
make run-debug RUN_DATE=$RUN_DATE SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

## Post-Run Analysis (optional)

```bash
make momentum-scoring RUN_DATE=$RUN_DATE
make earnings-surprise RUN_DATE=$RUN_DATE
make forward-pe-peg RUN_DATE=$RUN_DATE
make stock-explainer RUN_DATE=$RUN_DATE
make backtest RUN_DATE=$RUN_DATE
```

## Dashboard Usage

The dashboard reads run artifacts from `runs/<YYYY-MM-DD>/` and lets you inspect:

- overview charts and recommendation mix
- leaderboard and filters
- per-stock detail with card scores and thesis
- sector analysis
- value-hunter view
- run-quality diagnostics

### Start the dashboard

macOS / Linux:

```bash
make dashboard
```

or

```bash
source .venv/bin/activate
streamlit run app.py
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
streamlit run app.py
```

Then open the local URL printed by Streamlit, usually:

```text
http://localhost:8501
```

### Important: do not run `python app.py`

This is a Streamlit app, not a plain Python CLI script. If you run:

```bash
python app.py
```

or

```bash
.venv/bin/python app.py
```

you may see warnings such as:

- `missing ScriptRunContext`
- `bare mode`

That usually means the app was launched with the wrong command.

### Typical dashboard workflow

1. Run the pipeline first.

```bash
make daily-run RUN_DATE=$RUN_DATE
```

   Windows PowerShell users should use the Windows sequence from the Quick Start section instead of `make daily-run`.

2. Start the dashboard.

```bash
make dashboard
```

3. In the sidebar, select the run date you want to inspect.

4. Use filters for minimum score, gate-passed stocks, and recommendation type.

### Troubleshooting

- `No engine runs found`
  Run `make daily-run RUN_DATE=$RUN_DATE` first so the app has data to load.

- `missing ScriptRunContext`
  Start the app with `make dashboard` or `streamlit run app.py`, not `python app.py`.

- `Address already in use` or port `8501` busy
  Start Streamlit on another port:

```bash
source .venv/bin/activate
streamlit run app.py --server.port 8502
```

  Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
streamlit run app.py --server.port 8502
```

- Dashboard opens but latest run has empty scores
  Check `runs/<date>/template_support.json` and `runs/<date>/input_quality.json`. This usually means the run completed with unsupported or incomplete fundamentals data rather than a dashboard bug.

## All Make Targets

| Target | Purpose |
|--------|---------|
| `make init` | First-time setup |
| `make fetch-universe` | Fetch NSE bhavcopy universe |
| `make fetch-screener-data` | Scrape fundamentals from screener.in |
| `make fetch-price-history` | Backfill bhavcopy price history |
| `make daily-run` | Full pipeline end-to-end |
| `make run` | Engine run with explicit CSV path |
| `make run-debug` | Engine run, skips quality gate |
| `make enrich-fundamentals` | Compute missing metrics |
| `make fetch-delivery` | NSE delivery volume data |
| `make fetch-indices` | Nifty index EOD data |
| `make fetch-shareholding` | BSE shareholding/pledge |
| `make momentum-scoring` | Dual momentum overlays |
| `make institutional-tracking` | MF/FII holding changes |
| `make earnings-surprise` | Earnings surprise detection |
| `make forward-pe-peg` | Forward PE, PEG, GARP score |
| `make stock-explainer` | Per-stock investment theses |
| `make backtest` | Validate past recommendations |
| `make telegram-alerts` | Send picks via Telegram |
| `make dashboard` | Launch Streamlit dashboard |
| `make check-config` | Validate config before a run |
| `make data-freshness` | Data staleness report |
| `make check` | Syntax check |
| `make test` | Unit tests |

## Outputs (`runs/<YYYY-MM-DD>/`)

| File | Contents |
|------|---------|
| `leaderboard.csv` | All stocks ranked by selection score |
| `action_sheet.csv` | Rec + confidence + staged entry + gate notes |
| `buy_candidates.csv` | Investable buy list |
| `undervalued_high_potential.csv` | Discount-first shortlist |
| `sector_summary.csv` | Sector breadth, buy-candidate density |
| `portfolio_plan.csv` | Picks under sector/name caps |
| `stock_<TICKER>.json` | Full per-stock detail |
| `bias_audit.json` | Snooping/config integrity checks |
| `coverage_snapshot.json` | Data completeness diagnostics |

## Configuration

`engine/config.py` — all scoring thresholds, portfolio controls, market-mode settings. Heavily commented; start here when onboarding.

## Telegram Alerts

```bash
export TELEGRAM_BOT_TOKEN="..."
export TELEGRAM_CHAT_ID="..."
make telegram-alerts RUN_DATE=$RUN_DATE DRY_RUN=true   # dry run
make telegram-alerts RUN_DATE=$RUN_DATE                 # live
```

## Scheduling (macOS, one-time)

```bash
python scripts/setup_scheduler.py --install    # Mon-Fri at 19:00
python scripts/setup_scheduler.py --status
python scripts/setup_scheduler.py --uninstall
```

## CI/CD

| Workflow | Trigger | What it does |
|----------|---------|-------------|
| CI | Push/PR to main | Syntax check, config validation, unit tests |
| Daily Run | Mon-Fri 19:15 IST | Fetch universe, run engine (debug), upload artifacts |
| Weekly Backtest | Sunday | Backtester, upload reports |

Secrets for Telegram alerts in CI: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

## Repo Layout

```
engine/    # Scoring engine, models, cards, aggregation, advanced overlays
scripts/   # Data pipeline, scraping, enrichment, reporting
app.py     # Streamlit dashboard
tests/     # Unit tests
docs/      # Data dictionary and CSV templates
data/      # Raw + processed datasets (local, gitignored)
runs/      # Run artifacts per date (gitignored)
logs/      # Cross-run history
```

## Notes

- Research engine only — does not execute trades.
- Data files, run outputs, and logs are gitignored; only code is versioned.
