# How We Pick 2–3 Stocks (Methodology)

This project is a **long-term research assistant**, not investment advice.

## Pipeline

1. **Universe** — NSE-listed equities with classification (sector / industry / basic industry).
2. **Peer-relative scoring** — Six cards (Performance, Valuation, Growth, Profitability, Entry Point, Red Flags) plus contrarian overlay; percentiles vs peers.
3. **Horizon weights** — Opportunity-score card weights shift by `investment_horizon` (6m–10y).
4. **Safety overlays** — Data quality, value-trap detection, investability gates, market regime (bull/bear).
5. **User profile** — Filters and re-rank (sector, cap, PE, ROCE, etc.); never relaxes safety gates.
6. **Shortlist** — `top_picks.csv` (3 primary) + `top_picks_next_tier.csv` (5 follow-ups) via `research_mode` + `return_persona`.

## Research modes

| Mode | Use when |
|------|----------|
| `high_conviction` | You want only gate-passed, high-confidence names (may be 0–3). |
| `research_shortlist` | Default; balanced quality with explicit caveats. |
| `thematic` | Policy-theme sectors; may include data-incomplete names with warnings. |

## Return personas

| Persona | Bias |
|---------|------|
| `compounder` | ROCE, 3Y revenue CAGR, red-flag strength |
| `quality_value` | Valuation gap + selection score |
| `steady_income` | Dividend, low leverage (not guaranteed FD-like returns) |

## Validation (backtest)

Weekly workflow runs `make backtest` on historical `buy_candidates.csv` and `top_picks.csv`. Threshold changes in `engine/config.py` should follow positive out-of-sample evidence only (`CALIBRATION_MIN_SAMPLE_SIZE`).

## What we do not claim

- Guaranteed returns or FD-equivalent yield
- Complete public data on pledge/governance without enrichment
- Intraday or options suitability

Always verify filings, results, and exchange disclosures before investing.
