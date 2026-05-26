# Minimum Viable Data for Top Picks

The engine can rate thousands of symbols from a Screener CSV alone, but **2–3 high-confidence picks** need critical risk evidence.

## Required for every production run

| Source | Path pattern | Purpose |
|--------|----------------|---------|
| NSE universe | `data/raw/universe/nse_symbols_<date>.csv` | Symbol master |
| Fundamentals | `data/raw/fundamentals/screener/screener_export_<date>.csv` | Core ratios |
| Classification | `data/raw/classification/nse_symbol_classification_master.csv` | Peer groups |

## Strongly recommended (unlocks gates & tiers)

| Field group | Enrichment path | Target coverage |
|-------------|-----------------|-----------------|
| Promoter pledge / shareholding | `fetch-shareholding` → `merge-public-enrichment` | ≥60% of non-financial names |
| Governance events | `fetch-governance-events` → merge | Checked per symbol (use `none` if clean) |
| Bank/NBFC asset quality | `financial_risk_<date>.csv` template | 100% of template B/C names you rank |

## Coverage targets before trusting `top_picks.csv`

- **High conviction mode:** ≥80% critical fields on shortlisted names; `research_tier` = High Confidence Research.
- **Research shortlist mode (default):** ≥50% critical fields universe-wide; top 3 may be Qualified Watchlist.
- **Thematic mode:** Policy themes configured; verify sector match manually.

## If `top_picks.csv` is empty

1. Run `make fetch-shareholding` and `make fetch-governance-events` (best-effort).
2. Run `make merge-public-enrichment`.
3. Re-run `make run` with `PROFILE_CONFIG=config/research_profile.example.json`.
4. Read `runs/<date>/search_summary.json` → `empty_reason`.

See `docs/public_source_contracts.md` for CSV column contracts.
