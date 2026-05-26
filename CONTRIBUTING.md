# Contributing to NSE Screener

Thank you for helping improve this open-source long-term research engine.

## Setup

```bash
make init
make demo-run    # offline fixture run → runs/demo/
make test
```

## Before opening a PR

1. `make check-config`
2. `make check`
3. `make test`
4. If you change scoring or gates: `make demo-run` and inspect `runs/demo/top_picks.csv`

## Code guidelines

- Keep the tool **long-term research** focused (not intraday trading).
- Never imply guaranteed returns.
- **Missing data ≠ safe**: do not treat unknown pledge/governance as zero.
- Add tests for scoring, filters, and config validation.
- Document new metrics in `docs/data_dictionary.csv`.

## Data sources

- Prefer public/official sources with CSV contracts under `docs/`.
- Document scraping limitations; provide a bring-your-own-CSV path.
- Do not commit large scraped datasets or API secrets.

## Research modes & profiles

- New profiles: `config/research_profile.*.json` — run `make check-config`.
- Modes/personas: `engine/research_modes.py`, `engine/shortlist.py`.

## Questions

Open a GitHub issue with run date, profile used, and `search_summary.json` from the run.
