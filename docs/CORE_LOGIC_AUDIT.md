# Core Logic Audit (internal)

Last reviewed: 2026-05-26

## Pipeline integrity ✅

| Stage | Module | Status |
|-------|--------|--------|
| Load + derive | `scripts/load_data.py` | OK — 100+ aliases, provenance |
| Peer groups | `engine/peer_group.py` | OK — fallback chain |
| Card scores | `engine/cards.py` + `scoring.py` | OK — direction map validated |
| Opportunity | `engine/aggregator.py` | OK — horizon weights wired |
| Overlays | `engine/advanced.py` | OK — gates, tiers, value trap |
| Profile | `engine/preferences.py` | OK — never relaxes gates |
| Shortlist | `engine/shortlist.py` | Tightened — no Unsupported tier |

## Fixes applied in this audit

1. **Shortlist** no longer promotes `research_tier=Unsupported` or unsupported templates in `research_shortlist` mode.
2. **Demo pipeline** runs `enrich-fundamentals` so growth/profitability cards can reach rankable coverage.
3. **Top picks CSV** strips internal keys (`_policy_themes`) before write.
4. **`enrich_fundamentals.py`** now derives YoY growth from 3Y CAGR when quarterly YoY is missing (was documented but not implemented).
5. **Enrichment heuristics** for CFO/PAT, margin trend, FCF consistency, and growth stability when only screener-style columns exist.

## Remaining intentional behaviors

- **Empty `buy_candidates.csv`** when critical risk fields missing — correct.
- **`run-debug`** skips production quality gate — must not be used for real decisions.
- **Peer percentile** unstable when peer group &lt; 8 names — `peer_group_quality` warns.
- **IV / fair value** uses fixed WACC/COE — documented approximation.

## Recommended before trusting live picks

1. `make enrich-fundamentals` on every screener CSV
2. `make merge-public-enrichment` with shareholding + governance + financial_risk
3. Use `research_mode=high_conviction` only when enrichment coverage ≥ 60%
4. Read `search_summary.json` when `top_picks.csv` has &lt; 3 rows

## Optional scripts (not in core cards)

`momentum_scoring.py`, `forward_pe_peg.py`, `institutional_tracking.py`, `earnings_surprise.py` — run via Makefile; merge into CSV for future card integration.
