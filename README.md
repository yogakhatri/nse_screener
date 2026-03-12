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
```

## Configuration Philosophy

All runtime behavior is configurable in:
- [engine/config.py](/Users/yk/work/nse_screener/engine/config.py)

That file is intentionally heavily commented. It explains:
- what each field controls
- what happens when you raise/lower each threshold
- suggested safe ranges

If you are onboarding new teammates, start with `engine/config.py`.

## Data Inputs

Primary run flow currently expects a Screener CSV as the universe source:
- `--screener-csv path/to/file.csv`

The loader supports alias-based mapping and computes derived fields where possible:
- intrinsic gap
- distress risk
- ASM/GSM risk
- GNPA/CAR/PCR/ALM/liquidity/governance risk

Reference:
- [scripts/load_data.py](/Users/yk/work/nse_screener/scripts/load_data.py)

## Quick Start

```bash
cd /Users/yk/work/nse_screener
source .venv/bin/activate

python scripts/run_engine.py \
  --date 2026-03-10 \
  --mode live \
  --market-mode auto \
  --screener-csv data/raw/fundamentals/screener/screener_export_2026-03-10.csv
```

## CLI Options

Main runner:
- [scripts/run_engine.py](/Users/yk/work/nse_screener/scripts/run_engine.py)

Important flags:
1. `--mode live|backtest`
2. `--market-mode auto|bear|neutral|bull`
3. `--tickers TCS,INFY,HDFCBANK` (optional filter)
4. `--strict-freshness` (fail if stale dataset)

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
.venv/bin/python -m py_compile engine/*.py scripts/*.py tests/test_engine_phase_upgrade.py run_test.py
```

Unit tests:
```bash
.venv/bin/python -m unittest discover -s tests -p "test_*.py"
```

Smoke sample:
```bash
.venv/bin/python run_test.py
```

## Important Files

Treat these as core logic files:
1. [engine/config.py](/Users/yk/work/nse_screener/engine/config.py)
2. [engine/metric_definitions.py](/Users/yk/work/nse_screener/engine/metric_definitions.py)
3. [engine/aggregator.py](/Users/yk/work/nse_screener/engine/aggregator.py)
4. [engine/advanced.py](/Users/yk/work/nse_screener/engine/advanced.py)
5. [scripts/run_engine.py](/Users/yk/work/nse_screener/scripts/run_engine.py)

## Notes

- This is a decision-support research engine.
- It does not execute trades.
- Keep data quality and freshness controls strict if using it in live workflows.
