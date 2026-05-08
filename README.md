# NSE Sector-Wise Investment Engine

## Project Overview
Internal Indian equity research engine that ranks NSE stocks sector-wise using six analysis cards: Performance, Valuation, Growth, Profitability, Entry Point, and Red Flags. The goal is to find fundamentally strong stocks trading below potential, while separating actionable ideas from research-only or unsupported data.

## Tech Stack
- Python 3.11+
- pandas, numpy, requests, lxml
- Streamlit dashboard
- Makefile-based local workflow
- CSV/JSON artifacts under `data/`, `runs/`, and `logs/`
- Public data sources only: NSE archives/bhavcopy, Screener public pages, optional BSE/NSE public endpoints, local CSV fallbacks

## Current Status
Implemented:
- NSE universe fetch from public bhavcopy with classification master enrichment.
- Public Screener fundamentals scraper with local cache and cache-schema invalidation.
- Local classification master builder: `make build-classification`.
- Raw bhavcopy price-history support for price/technical metrics.
- Template-aware scoring for general companies, banks, and NBFC/HFC-style financials.
- Six-card stock scorecard: Performance, Valuation, Growth, Profitability, Entry Point, Red Flags.
- Advanced overlays: market regime, potential score, valuation-gap score, expected upside/downside, risk/reward, staged entry plan, sector regime, portfolio caps.
- Research workflow labels: `Actionable`, `Research Candidate`, `Rejected`, `Unsupported`.
- Data-quality scoring based on classification confidence, fundamentals source, price source, card coverage, and valuation evidence.
- Critical-risk data gating: missing pledge, liquidity, governance, debt-service, and bank/NBFC asset-quality inputs now reduce confidence and block Buy Candidate output.
- Peer-group quality gating: weak peer groups are exposed and blocked from actionable recommendations.
- First-class `Insufficient Data` recommendation state instead of silently converting weak-data rows to `Avoid`.
- Recommendation reason codes, analyst-readable reasons, risk flags, value-trap flags, and model caveats are included in JSON/CSV outputs.
- Value-trap detection for cheap-looking stocks with weak growth, margins, cash conversion, leverage, red flags, or sector headwinds.
- Market-regime inference now records source/confidence and prefers cached NSE index data when available, with stock-breadth fallback.
- Optional backtest-derived calibration profile support via `data/processed/model_calibration.json`.
- Source registry for every run: required/optional source status, freshness, hashes, row counts, file counts, and quality status.
- Field-level metric provenance: each populated normalized metric records source, source field, confidence, and method.
- Major provenance summaries are surfaced in the leaderboard for valuation, price, and risk metrics.
- Sector-aware intrinsic value models: Template A uses conservative EPV/Graham value with sector and quality adjustments; banks/NBFCs use asset-quality adjusted fair P/B.
- Banks/NBFCs are included but capped in the mixed daily market list so they do not dominate results.
- Separate daily lists for banks and NBFCs.
- Input quality gate and template support gate to prevent misleading production outputs.
- Config validation through `make check-config`.
- Unit test coverage for classification, quality gates, data-quality gating, financial caps, and loader behavior.

Latest local validation:
- Command: `make run RUN_DATE=2026-04-09 SCREENER_CSV=data/raw/fundamentals/screener/screener_export_2026-04-09.csv`
- Stocks rated: 2,133
- Leaderboard rows: 1,351
- Buy candidates: 0
- Daily market list: 15 Watchlist / Research Candidate rows
- Market mode: bear
- Reason for zero buys: current local data is missing critical pledge, governance, and/or debt-service evidence for all rows, so Buy Candidate output is correctly blocked.
- Tests: `51` unit tests passing

## Run From Scratch

### 1. Clone and install
```bash
git clone <repo-url> nse_screener
cd nse_screener
make init
```

### 2. Set run date
```bash
export RUN_DATE=$(date +%F)
```

For historical/local testing:
```bash
export RUN_DATE=2026-04-09
```

### 3. Fetch NSE universe
```bash
make fetch-universe RUN_DATE=$RUN_DATE
```

This writes:
- `data/raw/universe/nse_symbols_$RUN_DATE.csv`
- `data/processed/universe/universe_fetch_$RUN_DATE.json`
- `data/processed/universe/missing_classification_$RUN_DATE.csv`

### 4. Scrape fundamentals
Test scrape first:
```bash
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_LIMIT=50 SCRAPER_WORKERS=1 SCRAPER_DELAY=1.5
```

Full scrape:
```bash
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_WORKERS=3 SCRAPER_DELAY=3.0
```

This writes:
- `data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv`
- cache files under `data/raw/fundamentals/screener/cache/`

### 5. Refresh classification master
```bash
make build-classification RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

This writes:
- `data/raw/classification/nse_symbol_classification_master.csv`

### 6. Fetch price history
```bash
make fetch-price-history RUN_DATE=$RUN_DATE SESSIONS=260
```

This writes/uses bhavcopy archives under:
- `data/raw/prices/bhavcopy/`

### 7. Run the full daily workflow
```bash
make daily-run RUN_DATE=$RUN_DATE
```

For strict production mode with a curated full fundamentals file:
```bash
make daily-run RUN_DATE=$RUN_DATE \
  FUNDAMENTALS_CSV=data/raw/fundamentals/screener/full_fundamentals_$RUN_DATE.csv
```

For a direct run using an existing Screener CSV:
```bash
make run RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

Use debug mode only when diagnosing sparse data:
```bash
make run-debug RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv
```

### 8. Validate code/config
```bash
make check
make check-config
make test
```

Optional strict source validation:
```bash
make source-registry RUN_DATE=$RUN_DATE \
  SCREENER_CSV=data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv

python scripts/run_engine.py \
  --date $RUN_DATE \
  --screener-csv data/raw/fundamentals/screener/screener_export_$RUN_DATE.csv \
  --strict-source-registry
```

### 9. View outputs
```bash
ls runs/$RUN_DATE
cat runs/$RUN_DATE/daily_market_list.csv
make dashboard
```

## Main Outputs

| File | Purpose |
|---|---|
| `leaderboard.csv` | Ranked investable/researchable universe after exclusions |
| `action_sheet.csv` | Analyst-facing recommendation, confidence, gates, staged entry plan |
| `daily_market_list.csv` | Mixed daily shortlist with sector and financial caps |
| `daily_bank_list.csv` | Bank-only research queue |
| `daily_nbfc_list.csv` | NBFC/HFC-style financial research queue |
| `buy_candidates.csv` | Stocks that pass Buy Candidate logic |
| `undervalued_high_potential.csv` | Discount/potential-focused shortlist |
| `red_flag_exclusions.csv` | Rejected rows with major red-flag/gate issues |
| `unsupported_stocks.csv` | Stocks/templates that should not be interpreted as supported |
| `data_quality_summary.csv` | Source confidence and research-readiness summary |
| `source_registry.json` | Normalized public-source registry with required/optional source status |
| `source_registry.csv` | Analyst-friendly source registry summary |
| `metric_provenance.csv` | Per-ticker, per-metric source/confidence/method audit trail |
| `coverage_by_template_card.csv` | Rankable coverage by template and card |
| `template_support.csv` | Template support status and blockers |
| `stock_<TICKER>.json` | Full per-stock scorecard detail |

## Key Config Files

### `engine/config.py`
Primary scoring and gate configuration.

Important sections:
- Peer minimums: `PEER_MIN_BASIC_INDUSTRY`, `PEER_MIN_INDUSTRY`
- Card coverage: `CARD_DATA_THRESHOLD`, `MIN_RANKABLE_CARDS`
- Template quality gate: `MIN_TEMPLATE_*`, `QUALITY_GATE_REQUIRE_ALL_CORE_CARDS`
- Unsupported-template behavior: `BLOCK_RUN_ON_UNSUPPORTED_TEMPLATES`
- Market regime: `DEFAULT_MARKET_MODE`, `AUTO_*`
- Market regime data threshold: `MIN_MARKET_MODE_*`, `INDEX_REGIME_*`
- Template routing: `TEMPLATE_BANKS`, `TEMPLATE_NBFC`, `TEMPLATE_BANK_INDUSTRIES`, `TEMPLATE_NBFC_INDUSTRIES`
- Card weights: `CARD_WEIGHTS`
- Recommendation thresholds: `BUY_*`, `WATCH_*`
- Hard gates: `GATE_*`, `BEAR_GATE_*`
- Daily list caps: `DAILY_LIST_*`
- Data-quality gates: `MIN_DATA_QUALITY_SCORE_ACTIONABLE`, `MIN_DATA_QUALITY_SCORE_RESEARCH`, `GATE_MIN_DATA_QUALITY_SCORE`
- Critical-risk gates: `GENERAL_CRITICAL_RISK_FIELDS`, `BANK_CRITICAL_RISK_FIELDS`, `NBFC_CRITICAL_RISK_FIELDS`, `MAX_MISSING_CRITICAL_FIELDS_*`
- Value-trap gates: `VALUE_TRAP_WARN_THRESHOLD`, `VALUE_TRAP_BLOCK_THRESHOLD`
- Calibration: `CALIBRATION_PROFILE_PATH`, `CALIBRATION_*`

After changing config, always run:
```bash
make check-config
make test
```

## Interpretation Rules

Use `research_status`, not only `recommendation`:
- `Actionable`: passed recommendation, gate, template, and data-quality requirements.
- `Research Candidate`: interesting but needs analyst review or more confirmation.
- `Rejected`: does not meet current filters.
- `Unsupported`: template/data coverage is insufficient; do not rely on the score.

Use `recommendation` conservatively:
- `Buy Candidate`: passes score, valuation, data-quality, red-flag, peer, and critical-risk gates.
- `Watchlist`: worth research, but not cleared for immediate action.
- `Insufficient Data`: score is not reliable enough for decision-making.
- `Avoid`: rejected by current valuation, potential, risk, value-trap, or quality filters.
- `Unsupported`: template coverage is incomplete.

Use `data_quality_status` before trusting a row:
- `Actionable Data`: sufficient source quality for shortlist use.
- `Research Only Data`: usable for review, not for direct action.
- `Weak Data`: should not drive decisions.

## Pending Tasks

Next implementation phases:
- Fetch/merge pledge and promoter holding data so Buy Candidate gates can clear when risk evidence is present.
- Fetch/merge governance and corporate-announcement red flags instead of leaving governance risk unknown.
- Fetch/merge bank/NBFC asset-quality fields: GNPA, NNPA, PCR, CAR, NIM, credit cost, and ALM.
- Fetch cached NSE index data through `make fetch-indices RUN_DATE=$RUN_DATE` so market mode uses benchmark/breadth evidence instead of only stock-level fallback.
- Build enough historical runs and run `make backtest RUN_DATE=...` to create a usable `data/processed/model_calibration.json`.
- Expand sub-sector valuation models for insurance, capital markets, real estate, cyclicals, IT, pharma, FMCG, and commodities.
- Add richer analyst workbench views for red-flag timeline, valuation explanation, and config validation UI.

## Action Items for User

- Confirm which public sources are acceptable besides NSE, BSE, Screener public pages, and Yahoo-style public endpoints.
- Before trusting Buy Candidate output, run/share the public-source fetches for pledge/shareholding, ASM/GSM, governance events, financial asset quality, and index data.
- Treat the latest zero-buy result as a safety result, not a failure: the engine found Watchlist ideas but blocked buys because critical risk evidence is incomplete.
- Decide whether full daily scraping should run locally only or also through a scheduler.
- Review `daily_market_list.csv` manually before any investment action; this is an internal research tool, not financial advice.
- Do not use `run-debug` for investment decisions; it exists only for sparse-data troubleshooting.
