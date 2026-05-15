# NSE Long-Term Stock Research Assistant

## Project Overview
NSE Long-Term Stock Research Assistant is an open-source Indian equity research engine that ranks NSE-listed stocks sector-wise for long-term research horizons such as 6 months, 1 year, 3 years, and 5 years. It helps users shortlist potentially attractive stocks using configurable filters, peer-relative scoring, valuation gap checks, data-quality gates, and explicit red-flag controls.

This project is a research assistant, not an investment adviser. It is designed to surface candidates for further human analysis, not to tell users what to buy blindly.

## Important Investment Disclaimer
This project is for education and research only. It is not financial advice, investment advice, portfolio advice, or a buy/sell recommendation system. Market data and public fundamentals can be incomplete, stale, delayed, wrong, or misinterpreted by code. Users must independently verify every output with official filings, company results, exchange disclosures, and professional advice where appropriate. No part of this project guarantees profit, downside protection, or future performance.

## Who This Project Is For
- Long-term investors who want a repeatable NSE stock research workflow.
- Developers building transparent equity research tooling from public data.
- Analysts who want peer-relative sector ranking, valuation gap checks, and red-flag visibility.
- Beginners who need a structured shortlist but are willing to do manual follow-up research.
- Advanced users who want configurable profiles, custom ranking weights, and auditable CSV/JSON outputs.

## What This Project Does
- Builds a daily NSE equity universe.
- Scrapes public fundamentals from Screener-style public pages and local CSV inputs.
- Uses locally cached NSE bhavcopy history for price-derived metrics where available.
- Classifies stocks by macro sector, sector, industry, and basic industry.
- Routes companies into template-aware logic for general companies, banks, and NBFC/HFC-style financials.
- Scores stocks on six core cards: Performance, Valuation, Growth, Profitability, Entry Point, and Red Flags.
- Adds long-term overlays for intrinsic value/fair value, expected upside/downside, market regime, sector regime, value-trap risk, data quality, and critical risk evidence.
- Produces sector-wise leaderboards, action sheets, daily research lists, unsupported-stock reports, and source/provenance diagnostics.
- Lets users apply safe research profiles for horizon, risk level, sector preference, market-cap preference, valuation filters, growth filters, debt filters, profitability filters, dividend filters, and custom weights.

## What This Project Does Not Do
- It does not provide financial advice.
- It does not guarantee returns or identify sure-shot stocks.
- It does not execute trades.
- It is not an intraday trading, options, futures, scalping, or short-term speculation tool.
- It does not replace manual reading of annual reports, quarterly results, conference calls, exchange filings, and risk disclosures.
- It does not currently guarantee complete public coverage for governance, pledge, auditor, corporate-action, or bank/NBFC asset-quality evidence.
- It does not require paid Tickertape, Trendlyne, broker, or proprietary APIs.

## Tech Stack
- Python 3.11+
- pandas, numpy, requests, lxml
- Streamlit dashboard
- Makefile workflow
- CSV/JSON artifacts under `data/`, `runs/`, and `logs/`
- Public/local data sources: NSE bhavcopy archives, NSE universe files, Screener public pages, optional BSE/NSE public disclosures, and local CSV fallbacks

## Key Features
- Sector-wise stock ranking for long-term research.
- Template-aware scoring for general companies, banks, and NBFC/HFC-style financials.
- Six-card analysis inspired by structured research workflows: Performance, Valuation, Growth, Profitability, Entry Point, and Red Flags.
- Peer-relative percentile scoring with peer-group quality warnings.
- Conservative input quality gate so sparse CSVs do not create misleading live outputs.
- Explicit `Insufficient Data` and `Unsupported` states instead of silently marking unknown stocks as safe.
- Market-regime inference from cached index data where available, with stock-breadth fallback.
- Value-trap detection for cheap-looking but weak businesses.
- Critical-risk gates for pledge, liquidity, governance, debt-service, ASM/GSM, and financial asset-quality fields.
- Separate daily lists for banks/NBFCs and mixed-market caps so financials do not dominate the final shortlist.
- User research profiles with validated filters and custom scoring weights.
- Metric provenance, source registry, data-quality summary, and template-support outputs.
- Unit tests for scoring, filtering, config validation, data loading, and edge cases.

## Architecture Overview
```text
Public/local data sources
  -> scripts/fetch_* and scripts/load_data.py
  -> RawStockData normalization + metric provenance
  -> engine/peer_group.py peer resolution
  -> engine/cards.py six-card scoring
  -> engine/aggregator.py opportunity score
  -> engine/advanced.py long-term overlays, gates, lists, risk controls
  -> engine/preferences.py user profile filtering and re-ranking
  -> scripts/run_engine.py production runner
  -> runs/<RUN_DATE>/ CSV and JSON outputs
```

Primary modules:
- `scripts/fetch_nse_universe.py`: builds the NSE symbol universe from public exchange data.
- `scripts/fetch_fundamentals_screener.py`: scrapes public fundamentals into the engine schema.
- `scripts/fetch_price_history.py`: backfills NSE bhavcopy history for raw price metrics.
- `scripts/fetch_governance_events.py`: fetches or parses public exchange announcements and classifies material governance events.
- `scripts/load_data.py`: normalizes CSV inputs, computes derived metrics, and records provenance.
- `scripts/merge_public_enrichment.py`: merges optional public evidence into the Screener-style input CSV.
- `scripts/source_registry.py`: records source freshness, file counts, row counts, hashes, and quality status.
- `engine/config.py`: heavily commented scoring, gate, template, and risk configuration.
- `engine/cards.py`: card-level metric scoring.
- `engine/aggregator.py`: opportunity-score aggregation and recommendation baseline.
- `engine/advanced.py`: market regime, intrinsic/fair value, safety gates, daily lists, portfolio caps, and research status.
- `engine/preferences.py`: validated user research profiles and profile-specific filtering.
- `scripts/run_engine.py`: end-to-end runner and output writer.
- `app.py`: Streamlit dashboard.

## Folder Structure
```text
.
├── app.py
├── config/
│   ├── research_profile.example.json
│   └── research_profile.strict_quality.json
├── data/
│   ├── raw/
│   └── processed/
├── docs/
│   ├── data_dictionary.csv
│   ├── nse_symbol_classification_template.csv
│   ├── nse_universe_template.csv
│   ├── public_financial_risk_template.csv
│   ├── public_governance_events_template.csv
│   ├── public_shareholding_template.csv
│   ├── public_source_contracts.md
│   └── screener_csv_template.csv
├── engine/
│   ├── advanced.py
│   ├── aggregator.py
│   ├── cards.py
│   ├── config.py
│   ├── engine.py
│   ├── metric_definitions.py
│   ├── models.py
│   ├── output.py
│   ├── peer_group.py
│   ├── preferences.py
│   └── scoring.py
├── scripts/
│   ├── check_config.py
│   ├── fetch_fundamentals_screener.py
│   ├── fetch_governance_events.py
│   ├── fetch_nse_universe.py
│   ├── fetch_price_history.py
│   ├── load_data.py
│   ├── merge_public_enrichment.py
│   └── run_engine.py
├── tests/
├── LICENSE
├── Makefile
├── requirements.txt
└── README.md
```

## Installation Guide
```bash
git clone <repo-url> nse_screener
cd nse_screener
make init
```

`make init` creates the virtual environment, installs dependencies, creates required folders, and generates starter CSV templates.

## Environment Setup
Use Python 3.11 or newer.

```bash
python3 --version
make setup
make bootstrap
```

Set the run date:
```bash
export RUN_DATE=$(date +%F)
```

For reproducible local testing with the bundled sample data:
```bash
export RUN_DATE=2026-04-09
```

## Data Source Requirements
Minimum useful run:
- NSE universe CSV under `data/raw/universe/`.
- Screener-style fundamentals CSV under `data/raw/fundamentals/screener/`.
- Classification master under `data/raw/classification/`.
- NSE bhavcopy history under `data/raw/prices/bhavcopy/` for better price metrics.

Recommended public-source enrichment:
- NSE index EOD data for market-regime evidence.
- NSE delivery data for delivery/volume confirmation.
- BSE/NSE shareholding data for promoter/pledge evidence.
- NSE ASM/GSM lists for surveillance risk.
- Public exchange announcements for governance/auditor/regulatory events.
- Bank/NBFC filings or investor presentations for GNPA, NNPA, PCR, CAR, NIM, credit cost, slippages, and ALM evidence.

If critical evidence is missing, the engine should still run but must downgrade confidence, mark rows as research-only/unsupported, or block buy-candidate output.

Optional normalized enrichment templates are provided under `docs/`:
- `docs/public_shareholding_template.csv`
- `docs/public_governance_events_template.csv`
- `docs/public_financial_risk_template.csv`
- `docs/public_source_contracts.md`

Place dated enrichment files at the default paths below and run `make merge-public-enrichment`:
- `data/raw/redflags/shareholding/shareholding_$RUN_DATE.csv`
- `data/raw/redflags/governance/governance_events_$RUN_DATE.csv`
- `data/raw/fundamentals/financial_risk/financial_risk_$RUN_DATE.csv`

Use `Governance Events=none` only when the symbol was actually checked for the configured lookback window. Blank governance data means unknown, not clean.

The public source contract is intentionally CSV-first. Live endpoints can change, fail, or block requests; the CSV contract keeps the engine maintainable because any developer can inspect and reproduce the exact data that entered the score.

## How To Run The Project
### 1. First-time setup
```bash
make init
```

### 2. Set run date
```bash
export RUN_DATE=$(date +%F)
```

For local sample validation:
```bash
export RUN_DATE=2026-04-09
```

### 3. Fetch NSE universe
```bash
make fetch-universe RUN_DATE=$RUN_DATE
```

Outputs:
- `data/raw/universe/nse_symbols_$RUN_DATE.csv`
- `data/processed/universe/universe_fetch_$RUN_DATE.json`
- `data/processed/universe/missing_classification_$RUN_DATE.csv`

### 4. Scrape public fundamentals
Start small:
```bash
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_LIMIT=50 SCRAPER_WORKERS=1 SCRAPER_DELAY=1.5
```

Full scrape:
```bash
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_WORKERS=3 SCRAPER_DELAY=3.0
```

Outputs:
- `data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv`
- cache files under `data/raw/fundamentals/screener/cache/`

### 5. Refresh classification master
```bash
make build-classification RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

Output:
- `data/raw/classification/nse_symbol_classification_master.csv`

### 6. Fetch price history
```bash
make fetch-price-history RUN_DATE=$RUN_DATE SESSIONS=260
```

Output:
- ZIP files under `data/raw/prices/bhavcopy/`

### 7. Merge optional public enrichment
If you have pledge/shareholding, governance, or bank/NBFC asset-quality evidence, copy the templates from `docs/`, fill them from public sources, then run:

```bash
make merge-public-enrichment RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

Default input paths:
- `SHAREHOLDING_CSV=data/raw/redflags/shareholding/shareholding_$RUN_DATE.csv`
- `GOVERNANCE_CSV=data/raw/redflags/governance/governance_events_$RUN_DATE.csv`
- `FINANCIAL_RISK_CSV=data/raw/fundamentals/financial_risk/financial_risk_$RUN_DATE.csv`

The merge is non-destructive by default. Existing non-empty values are preserved. To intentionally refresh existing enrichment fields:

```bash
make merge-public-enrichment RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv \
  ENRICHMENT_OVERWRITE=true
```

Output:
- Updated Screener CSV with normalized critical evidence columns.
- `data/processed/public_enrichment_report_$RUN_DATE.json`

If optional enrichment files are missing, the command reports them as `missing_optional` and continues. Missing evidence will still reduce research confidence during scoring.

Optional governance fetch/import:
```bash
make fetch-governance-events RUN_DATE=$RUN_DATE GOVERNANCE_LOOKBACK_DAYS=120
```

If NSE blocks network access or the endpoint changes, parse a downloaded JSON payload instead:
```bash
make fetch-governance-events RUN_DATE=$RUN_DATE \
  GOVERNANCE_INPUT_JSON=/path/to/corporate_announcements.json
```

This creates `data/raw/redflags/governance/governance_events_$RUN_DATE.csv` and merges it into `SCREENER_CSV` when that file exists.

### 8. Run the full daily workflow
```bash
make daily-run RUN_DATE=$RUN_DATE
```

`daily-run` automatically calls `merge-public-enrichment` after fundamental enrichment. If the optional enrichment files are not present, the merge step becomes a no-op and the engine will keep those fields as unknown.

Direct run with an existing fundamentals CSV:
```bash
make run RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

Run with the broad example research profile:
```bash
make run RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv \
  PROFILE_CONFIG=config/research_profile.example.json
```

Run with the stricter conservative profile:
```bash
make run RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv \
  PROFILE_CONFIG=config/research_profile.strict_quality.json
```

Use debug mode only for sparse-data diagnosis:
```bash
make run-debug RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

Do not use `run-debug` outputs for investment decisions because it skips production quality gates.

### 9. Validate code, config, and tests
```bash
make check-config
make check
make test
```

### 10. View outputs
```bash
ls runs/$RUN_DATE
cat runs/$RUN_DATE/daily_market_list.csv
make dashboard
```

Profile runs are written under `runs/$RUN_DATE/profiles/<profile_name>/` so comparing profiles does not overwrite the default run.

## Configuration Guide
There are two configuration layers.

### Engine configuration
`engine/config.py` controls model internals such as:
- peer minimums
- card coverage thresholds
- template quality gates
- market-regime behavior
- template routing
- card weights
- recommendation thresholds
- hard safety gates
- daily-list caps
- data-quality gates
- critical-risk fields
- value-trap thresholds
- calibration settings

After changing `engine/config.py`, run:
```bash
make check-config
make test
```

### User research profiles
Research profiles live in `config/*.json` and can be supplied with `PROFILE_CONFIG=...` or `--profile-config`.

Supported profile fields:
- `profile_name`: descriptive profile label.
- `investment_horizon`: one of `6m`, `1y`, `3y`, `5y`.
- `risk_level`: one of `conservative`, `balanced`, `aggressive`.
- `sector_preference`: list or comma-separated allowlist of sectors.
- `market_cap_preference`: one of `all`, `large`, `mid`, `small`, `micro`, `exclude_micro`.
- `min_market_cap_cr`, `max_market_cap_cr`: market-cap range in INR crore when the data is available.
- `max_pe`, `max_pb`: valuation ceilings. Non-positive P/E and P/B are treated as invalid, not cheap.
- `min_fcf_yield`, `min_iv_gap`, `min_expected_upside_pct`: value/discount filters.
- `min_rev_growth_yoy`, `min_eps_growth_yoy`, `min_rev_cagr_3y`: growth filters.
- `max_debt_to_equity`, `min_interest_coverage`: debt-service filters. These are most meaningful for non-financial companies.
- `min_roce`, `min_roe`, `min_cfo_pat_ratio`: profitability and earnings-quality filters.
- `min_dividend_yield`: dividend filter.
- `custom_weights`: profile-specific ranking weights.

Profile filters never relax production safety gates. They only filter and re-rank rows that the engine has already scored.

Command-line overrides:
```bash
make run RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv \
  INVESTMENT_HORIZON=3y \
  RISK_LEVEL=conservative \
  SECTOR_PREFERENCE='Healthcare,Information Technology' \
  MARKET_CAP_PREFERENCE=exclude_micro \
  RUNNER_EXTRA_ARGS='--max-pe 35 --min-roce 15 --min-expected-upside-pct 20'
```

If a profile JSON is supplied, Makefile defaults do not overwrite it. Only explicit variables or `RUNNER_EXTRA_ARGS` override profile fields.

## Developer Maintenance Guide
This project is designed so a developer can maintain it without AI assistance.

### Safe change workflow
1. Make the smallest code/config change needed.
2. Run `make check-config`.
3. Run `make check`.
4. Run `make test`.
5. Run one sample engine command against a known CSV before trusting outputs.
6. Inspect `runs/<RUN_DATE>/data_quality_summary.csv`, `critical_field_coverage.csv`, and `result_tier_summary.csv`.

### Adding or changing a metric
- Add the raw input alias in `scripts/load_data.py` if the metric comes from CSV.
- Add the normalized metric key to the relevant model/config only if it is used for scoring.
- Add scoring direction and weights in `engine/config.py`.
- Add loader and scoring tests.
- Update `docs/data_dictionary.csv` and this README if the metric affects user interpretation.
- Never treat missing values as zero unless zero is a true observed value.

### Adding a public data source
- Keep the normalized output contract in CSV form.
- Add parser logic in a dedicated `scripts/fetch_*.py` or `scripts/import_*.py` file.
- Add representative parser tests using saved payloads or small inline fixtures.
- Add source freshness to `scripts/source_registry.py` and `scripts/data_freshness.py` if it becomes first-class.
- Merge into the Screener CSV through `scripts/merge_public_enrichment.py`.
- Do not bypass quality gates by writing directly into engine internals.

### Changing recommendation logic
- Keep recommendation labels conservative and research-oriented.
- Do not add language that guarantees returns.
- Check behavior on missing data, stale data, weak peer groups, loss-making companies, banks, NBFCs, and micro-caps.
- Verify that `Buy Candidate` cannot appear when critical evidence is missing.
- Verify that `daily_market_list.csv` remains high-confidence only.

### Changing user configuration
- Keep all profile fields validated in `engine/preferences.py`.
- Make invalid config fail early with a clear error.
- Ensure command-line overrides do not silently replace profile JSON values unless explicitly provided.
- Add or update tests under `tests/test_preferences.py`.

### Output contract
Important user-facing files should remain stable unless there is a deliberate migration:
- `leaderboard.csv`
- `user_filtered_leaderboard.csv`
- `action_sheet.csv`
- `daily_market_list.csv`
- `daily_research_queue.csv`
- `daily_data_incomplete.csv`
- `buy_candidates.csv`
- `critical_field_coverage.csv`
- `sector_readiness.csv`
- `metric_provenance.csv`
- `source_registry.json`

If a column is renamed or removed, add a test and document the change.

## Example Configurations
Broad 1-year quality/value profile:
```json
{
  "profile_name": "quality_value_1y",
  "investment_horizon": "1y",
  "risk_level": "balanced",
  "sector_preference": [],
  "market_cap_preference": "all",
  "max_pe": 45,
  "max_pb": 12,
  "min_iv_gap": 0,
  "min_expected_upside_pct": 5,
  "custom_weights": {
    "selection_score": 0.35,
    "potential_score": 0.25,
    "valuation_gap_score": 0.2,
    "risk_reward_score": 0.1,
    "red_flags": 0.1
  }
}
```

Strict conservative quality profile:
```json
{
  "profile_name": "strict_quality_value_1y",
  "investment_horizon": "1y",
  "risk_level": "conservative",
  "market_cap_preference": "exclude_micro",
  "min_market_cap_cr": 500,
  "max_pe": 35,
  "max_pb": 8,
  "min_fcf_yield": 0,
  "min_iv_gap": 10,
  "min_expected_upside_pct": 15,
  "max_debt_to_equity": 2.5,
  "min_interest_coverage": 1.5,
  "min_roce": 10,
  "min_cfo_pat_ratio": 0.5
}
```

The strict profile may produce zero rows when public data is incomplete. That is a safe outcome, not a pipeline failure.

## How Stock Filtering Works
The engine filters in layers:

1. Universe filter: removes non-equity/fund-like instruments where detectable.
2. Classification gate: requires meaningful sector/industry taxonomy for production-quality output.
3. Template gate: routes stocks into general, bank, or NBFC templates and checks template card coverage.
4. Card coverage gate: avoids scoring stocks with too few rankable cards.
5. Peer-quality gate: warns or blocks weak peer groups where percentile scoring is unreliable.
6. Data-quality gate: scores classification, fundamentals, price source, critical risk coverage, and valuation evidence.
7. Critical-risk gate: checks pledge, liquidity, governance, debt-service, ASM/GSM, and bank/NBFC asset-quality evidence.
8. Value-trap gate: flags cheap stocks with poor growth, weak margins, cash-flow issues, leverage, red flags, or sector headwinds.
9. User profile filter: applies horizon, risk, sector, valuation, growth, debt, profitability, dividend, and custom preference filters.
10. Daily-list caps: limits sector concentration and caps banks/NBFCs in the mixed daily list.

## How Stock Scoring Works
Each stock is compared against peers in the most specific available peer group:
- Basic Industry
- Industry
- Sector

Each card uses configured metric weights from `engine/config.py`. Metrics are converted into peer-relative percentile scores. For lower-is-better metrics such as P/E, P/B, debt stress, cost-to-income, and red-flag risk, the direction is inverted.

Core cards:
- Performance: return strength, relative strength, drawdown recovery, and forward view.
- Valuation: P/E, P/B, FCF yield, intrinsic-value gap, fair P/B gap for financials, and dividend yield where applicable.
- Growth: revenue/profit growth, loan/deposit/AUM growth for financials, and growth stability.
- Profitability: ROCE, margins, CFO/PAT, ROA, ROE, NIM, cost-to-income, PCR, and credit-cost discipline depending on template.
- Entry Point: discount to value, RSI state, moving-average distance, delivery/volume confirmation, relative-strength turn, and volatility compression.
- Red Flags: pledge, ASM/GSM, default distress, accounting quality, liquidity/manipulation, governance events, NPA stress, capital adequacy, PCR weakness, slippages, and ALM risk.

Final outputs include:
- `opportunity_score`: composite score from the six-card model.
- `potential_score`: long-term potential estimate from growth, profitability, resilience, and valuation context.
- `valuation_gap_score`: degree and confidence of undervaluation.
- `risk_reward_score`: expected upside/downside balance.
- `selection_score`: practical shortlist score after gates and overlays.
- `user_profile_score`: profile-specific ranking score after user preferences.

## Example Outputs
Main run directory:
```text
runs/<RUN_DATE>/
runs/<RUN_DATE>/profiles/<PROFILE_NAME>/   # non-default profile runs
```

Important files:
| File | Purpose |
|---|---|
| `leaderboard.csv` | Ranked researchable universe after hard exclusions |
| `user_filtered_leaderboard.csv` | Leaderboard after the active user profile filters |
| `action_sheet.csv` | Analyst-facing recommendation, confidence, gates, and reasons |
| `daily_market_list.csv` | High-confidence mixed daily shortlist only; data-incomplete rows are excluded |
| `daily_research_queue.csv` | Capped queue of interesting rows that still need manual verification |
| `daily_data_incomplete.csv` | Profile-passing rows blocked by missing critical evidence |
| `daily_bank_list.csv` | High-confidence bank-only queue |
| `daily_nbfc_list.csv` | High-confidence NBFC/HFC-style queue |
| `daily_bank_research_queue.csv` | Bank research queue including data-incomplete rows |
| `daily_nbfc_research_queue.csv` | NBFC/HFC research queue including data-incomplete rows |
| `buy_candidates.csv` | Stocks that pass buy-candidate logic and all safety gates |
| `undervalued_high_potential.csv` | Value/potential-focused shortlist |
| `red_flag_exclusions.csv` | Rows rejected by risk, gate, or quality issues |
| `unsupported_stocks.csv` | Unsupported templates/stocks that should not be trusted |
| `data_quality_summary.csv` | Source quality and research-readiness summary |
| `result_tier_summary.csv` | Counts by user-facing research tier |
| `critical_field_coverage.csv` | Critical-risk evidence coverage by template and field |
| `sector_readiness.csv` | Sector-level readiness, gate-pass, and data-quality summary |
| `source_registry.json` | Source status, freshness, hashes, and quality metadata |
| `metric_provenance.csv` | Per-ticker, per-metric source/confidence/method audit trail |
| `research_profile.json` | Active user research profile used for the run |
| `stock_<TICKER>.json` | Full scorecard detail for one stock |

Interpretation fields:
- `research_status`: `Actionable`, `Research Candidate`, `Rejected`, or `Unsupported`.
- `research_tier`: `High Confidence Research`, `Qualified Watchlist`, `Data Incomplete`, `Rejected`, or `Unsupported`.
- `recommendation`: `Buy Candidate`, `Watchlist`, `Insufficient Data`, `Avoid`, or `Unsupported`.
- `data_quality_status`: `Actionable Data`, `Research Only Data`, or `Weak Data`.
- `analysis_caveat`: caveat explaining why the row should or should not be trusted.

A zero-row `buy_candidates.csv` can be the correct result when critical risk evidence is incomplete or the market/profile gates are strict.

## Current Status
Implemented:
- Full daily NSE research pipeline with universe fetch, public fundamentals scrape, classification, price-history support, scoring, risk gates, and CSV/JSON outputs.
- Template-aware scoring for general companies, banks, and NBFC/HFC-style financials.
- Banks/NBFCs are included but capped in the mixed daily market list so they do not dominate results.
- Input quality gate, template support gate, source registry, metric provenance, data-quality scoring, and explicit unsupported/insufficient-data states.
- Conservative treatment of missing critical risk evidence.
- Non-positive valuation multiples are ignored instead of being treated as cheap.
- User research profiles with validated filters and custom ranking weights.
- Normalized public enrichment merge path for pledge/shareholding, governance events, and bank/NBFC asset-quality evidence.
- Public governance-event fetch/import script with offline JSON parsing fallback.
- Public source contracts and enrichment templates under `docs/`.
- MIT license file for open-source distribution.
- GitHub Actions CI for config validation, Python compilation, and unit tests.
- Non-default profile runs now write to profile-specific output folders.
- Main `daily_market_list.csv` is now high-confidence only; incomplete ideas are separated into `daily_research_queue.csv` and `daily_data_incomplete.csv`.
- Critical-risk coverage, result-tier summary, and sector-readiness reports are written for every run.
- Example broad and strict profiles under `config/`.
- Config validation checks all repository research profiles.
- Latest local validation: `make check-config`, `make check`, and 71 unit tests passing.

Latest local full-run observations on `RUN_DATE=2026-04-09`:
- Broad example profile output path: `runs/2026-04-09/profiles/quality_value_1y`.
- Broad example profile: 2,133 stocks rated, 55 user-filtered leaderboard rows, 0 high-confidence daily market-list rows, 15 daily research-queue rows, 0 buy candidates.
- Strict profile: 2,133 stocks rated, 0 user-filtered rows because current sample data lacks enough strict-profile evidence.
- Market mode inferred as `bear`.
- Zero high-confidence ideas is caused by critical evidence gaps, not by a runtime failure.

## Risk Factors And Limitations
- Public scraping can fail due to rate limits, HTML changes, blocking, network errors, or stale cache.
- Public pages may omit critical data such as pledge, auditor events, governance flags, or bank/NBFC asset-quality details.
- Intrinsic value and fair P/B models are approximations, not truth.
- Peer-relative percentile scoring can be unstable for small peer groups.
- Sector classification can drift when source labels change.
- Backtest calibration requires enough historical runs and outcome tracking before it should influence production thresholds.
- Market regime inference is only as good as the available index/price history.
- Financial companies need different metrics from industrial companies; avoid applying non-financial debt/ROCE filters blindly to banks/NBFCs.
- Outputs are shortlists for further research, not final investment decisions.

## Testing Guide
Run all validation:
```bash
make check-config
make check
make test
```

Run unit tests directly:
```bash
.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

Run a full sample engine workflow:
```bash
make run RUN_DATE=2026-04-09 \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_2026-04-09.csv \
  MARKET_MODE=auto \
  PROFILE_CONFIG=config/research_profile.example.json
```

Before trusting a new config:
```bash
make check-config
make test
make run RUN_DATE=$RUN_DATE SCREENER_CSV=<your_csv> PROFILE_CONFIG=<your_profile_json>
```

Validate public enrichment on a small local fixture before using a new source parser:
```bash
make merge-public-enrichment RUN_DATE=$RUN_DATE \
  SCREENER_CSV=<small_screener_csv> \
  SHAREHOLDING_CSV=<small_shareholding_csv> \
  GOVERNANCE_CSV=<small_governance_csv> \
  FINANCIAL_RISK_CSV=<small_financial_risk_csv>
```

## Contribution Guidelines
- Keep the tool focused on long-term research, not intraday trading or speculation.
- Do not add language that guarantees profit or implies certainty.
- Prefer official/public sources and document every source path and freshness expectation.
- Add tests for scoring, filters, config validation, data loading, and edge cases.
- Keep config changes explicit and documented.
- Preserve conservative behavior when data is missing.
- Avoid silent fallbacks that make weak data look reliable.
- Run `make check-config`, `make check`, and `make test` before submitting changes.

## Roadmap
Near-term:
- Add stable source-specific parsers for bank/NBFC result fields where public filing formats are consistent enough to maintain.
- Expand sample full-run smoke tests with checked-in tiny fixtures.
- Add more source-specific parser tests as new public endpoints or export formats are accepted.

Medium-term:
- Add better sub-sector valuation models for IT, pharma, FMCG, commodities, real estate, capital markets, insurance, and cyclicals.
- Build historical run storage and calibration from realized 6M/1Y outcomes.
- Add richer dashboard views for source quality, red-flag timelines, valuation breakdown, and profile comparison.
- Add stronger documentation for each metric and public-source freshness requirement.

Long-term:
- Add a reproducible research notebook/workbench for manual analyst review.
- Add portfolio construction simulations with concentration, liquidity, and risk-budget constraints.
- Build enough historical runs to calibrate thresholds from realized 6M/1Y outcomes.

## Pending Tasks
- No release-blocking code/documentation task is currently listed here.
- Ongoing research calibration is data-dependent and should not be faked. Build historical runs over time, then calibrate thresholds only after enough real outcomes exist.
- Bank/NBFC live filing parsers should be added source-by-source only when a stable public format is confirmed.

## Action Items for User
- Review whether public Screener scraping is acceptable for your distribution/use case and comply with source terms.
- Fill or generate the optional enrichment CSVs when critical evidence is available from public sources.
- Review any new live public fetcher before using it at scale, because source formats and terms can change.
- Treat `Buy Candidate` as a research label only; manually verify every candidate before any investment action.
- Do not use `run-debug` output for real investment decisions.

## License
This project is released under the MIT License. See `LICENSE`.
