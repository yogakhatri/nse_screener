# NSE Long-Term Stock Research Assistant

[![CI](https://github.com/YOUR_GITHUB_USER/nse_screener/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_GITHUB_USER/nse_screener/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Open-source **research engine** for NSE-listed equities. It ranks stocks using peer-relative fundamentals, valuation gaps, risk gates, and user profiles — then surfaces up to **three primary ideas** plus a follow-up tier for manual review.

> **Not financial advice.** Outputs are shortlists for your own due diligence (filings, results, governance). The engine prefers **no pick** over a false-positive when data is incomplete.

---

## Table of contents

1. [Who this is for](#who-this-is-for)
2. [Quick start (5 minutes)](#quick-start-5-minutes)
3. [How it works](#how-it-works)
4. [Production daily workflow](#production-daily-workflow)
5. [Understanding outputs](#understanding-outputs)
6. [Configuration](#configuration)
7. [Project layout](#project-layout)
8. [Development](#development)
9. [Direction & limitations](#direction--limitations)
10. [Disclaimer](#disclaimer)

---

## Who this is for

| You are… | Use this to… |
|----------|----------------|
| Long-term investor (1–10y) | Get 2–3 names worth reading further, with caveats |
| Developer | Audit transparent scoring; extend data sources |
| OSS contributor | Run offline demo + tests without scraping |

**Not for:** intraday trading, options, or guaranteed-return promises.

---

## Quick start (5 minutes)

**Requirements:** Python 3.11+, `make`, `git`.

```bash
git clone <repo-url> nse_screener && cd nse_screener
make init
make demo-run          # offline — no NSE/screener network needed
```

Inspect results:

```bash
cat runs/demo/top_picks.csv
cat runs/demo/search_summary.json
make dashboard         # Streamlit UI → open “Top Picks” tab
```

Run tests:

```bash
make check-config && make test
```

---

## How it works

```text
CSV / scrape  →  load_data (normalize + derive metrics)
             →  peer groups (basic industry → industry → sector)
             →  6 cards (percentile vs peers) + contrarian
             →  opportunity score + red-flag caps
             →  advanced overlays (gates, value trap, data quality)
             →  user profile filter + Top 3 shortlist
             →  runs/<date>/ CSV + JSON
```

### Six scoring cards (template A: general companies)

| Card | What it measures |
|------|------------------|
| Performance | Returns, relative strength, recovery |
| Valuation | P/E, P/B, FCF yield, intrinsic-value gap |
| Growth | Revenue/EPS growth, stability |
| Profitability | ROCE, margins, cash conversion |
| Entry point | vs IV, RSI, moving averages, delivery |
| Red flags | Pledge, ASM/GSM, governance, distress |

Banks (template **B**) and NBFC/HFC (**C**) use sector-specific metrics (NIM, GNPA, CAR, etc.).

### Research modes (`research_mode` in profile)

| Mode | When to use | Typical pick count |
|------|-------------|-------------------|
| `research_shortlist` | Default — balanced quality + caveats | 0–3 |
| `high_conviction` | Only gate-passed, high-confidence tier | 0–2 |
| `thematic` | Policy themes (infra, digital, etc.) | 0–3 |

### Return personas (`return_persona`)

- **`quality_value`** — valuation gap + selection score (default)
- **`compounder`** — ROCE, 3Y growth, red-flag strength
- **`steady_income`** — dividend + low leverage (not FD-like guarantees)

Details: [docs/METHODOLOGY.md](docs/METHODOLOGY.md)

---

## Production daily workflow

```bash
export RUN_DATE=$(date +%F)

# 1) Universe + fundamentals + prices (see Makefile for limits/delays)
make daily-run RUN_DATE=$RUN_DATE

# 2) Or step-by-step:
make fetch-universe RUN_DATE=$RUN_DATE
make fetch-screener-data RUN_DATE=$RUN_DATE SCRAPER_LIMIT=100 SCRAPER_DELAY=2.0
make fetch-price-history RUN_DATE=$RUN_DATE SESSIONS=260
make enrich-fundamentals RUN_DATE=$RUN_DATE
make merge-public-enrichment RUN_DATE=$RUN_DATE   # pledge, governance, bank risk CSVs
make run RUN_DATE=$RUN_DATE \
  PROFILE_CONFIG=config/research_profile.example.json
```

**Data quality matters.** Empty `top_picks.csv` usually means missing pledge/governance/bank fields — not a bug. See [docs/MINIMUM_VIABLE_DATA.md](docs/MINIMUM_VIABLE_DATA.md).

### Example profiles (in `config/`)

| File | Intent |
|------|--------|
| `research_profile.example.json` | Broad 1y quality/value |
| `research_profile.strict_quality.json` | Conservative filters |
| `research_profile.compounder_5y.json` | 5y compounder persona |
| `research_profile.steady_income_10y.json` | Conservative income tilt |
| `research_profile.thematic_policy_3y.json` | Policy themes enabled |

---

## Understanding outputs

After `make run`, open `runs/<RUN_DATE>/` (or `runs/<RUN_DATE>/profiles/<name>/` if using a named profile).

| File | Read this for… |
|------|----------------|
| **`top_picks.csv`** | **Primary 3 ideas** for your search |
| `top_picks_next_tier.csv` | Next 5 for manual review |
| `search_summary.json` | Why picks are empty; mode/persona used |
| `action_sheet.csv` | Recommendation, gates, reasons |
| `leaderboard.csv` | Full ranked universe |
| `buy_candidates.csv` | Strict buy label (often empty — by design) |
| `data_quality_summary.csv` | Source readiness |
| `stock_<TICKER>.json` | Full scorecard per symbol |

**Labels**

- `recommendation`: Buy Candidate / Watchlist / Avoid / Insufficient Data
- `research_tier`: High Confidence Research / Qualified Watchlist / Data Incomplete / Rejected
- `Watchlist` ≠ buy — it means “research further”

---

## Configuration

### User profile (JSON)

```json
{
  "profile_name": "my_3y_quality",
  "investment_horizon": "3y",
  "risk_level": "balanced",
  "research_mode": "research_shortlist",
  "return_persona": "quality_value",
  "policy_themes": ["digital_india", "infrastructure"],
  "max_pe": 40,
  "min_roce": 12
}
```

CLI overrides (examples):

```bash
make run RUN_DATE=$RUN_DATE SCREENER_CSV=... \
  RUNNER_EXTRA_ARGS='--research-mode high_conviction --return-persona compounder --min-roce 15'
```

### Engine thresholds

Edit `engine/config.py` (peer minimums, gate thresholds, card weights). Then:

```bash
make check-config && make test
```

---

## Project layout

```text
engine/           # Scoring core (cards, gates, shortlist, horizons)
scripts/          # Fetch, load, enrich, run_engine, backtest
config/           # Research profiles + modes reference
docs/             # Data dictionary, methodology, CSV templates
tests/            # Unit tests + fixtures/demo_screener.csv
app.py            # Streamlit dashboard
runs/             # Generated outputs (gitignored)
data/raw/         # Inputs (gitignored except samples)
```

Key docs:

- [docs/data_dictionary.csv](docs/data_dictionary.csv) — metric definitions
- [docs/METHODOLOGY.md](docs/METHODOLOGY.md) — how picks are built
- [docs/MINIMUM_VIABLE_DATA.md](docs/MINIMUM_VIABLE_DATA.md) — data coverage targets
- [CONTRIBUTING.md](CONTRIBUTING.md) — PR guidelines

---

## Development

```bash
make init
make check-config    # config ↔ loader ↔ profiles
make check           # Python compile
make test            # unit tests
make demo-run        # fixture pipeline (CI uses this)
```

Add a metric: `scripts/load_data.py` → `engine/config.py` → `engine/cards.py` → tests → `docs/data_dictionary.csv`.

---

## Direction & limitations

### What we are building toward

- **Conservative, auditable** long-term shortlists (quality over quantity)
- **Public-data-first** pipeline with optional enrichment CSVs
- **Explicit empty states** when risk evidence is missing
- **Horizon- and theme-aware** ranking without bypassing safety gates

### Known gaps (honest)

| Gap | Impact | Mitigation |
|-----|--------|------------|
| Scrape-only fundamentals | Missing pledge/governance | `merge-public-enrichment`, BSE/NSE fetchers |
| Sparse scrape → empty growth/profitability cards | Few rankable cards | `make enrich-fundamentals` |
| No live sell-side consensus | `forward_view` often empty | Optional future plugin |
| Backtest not yet driving thresholds | Config is rule-based | Accumulate runs; use `make backtest` |
| Demo ≠ live market | Fixture is IT-only sample | Run full `daily-run` for real universe |

### Validating picks externally

Always cross-check engine output with:

- Company quarterly results and investor presentations
- [NSE](https://www.nseindia.com/) / [BSE](https://www.bseindia.com/) filings
- Broker consensus (Moneycontrol, ET Markets, etc.)

Example: if the engine flags a name **Watchlist** with **value-trap warnings** while brokers say **Neutral/Hold** (e.g. Wipro during weak IT services growth), treat that as a signal to **read the caveat**, not to ignore it.

---

## Disclaimer

This software is for **education and research**. It does not provide investment, legal, or tax advice. Past scores do not guarantee future returns. You are responsible for your own decisions.

MIT License — see [LICENSE](LICENSE).
