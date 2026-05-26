# Analyst Worksheet (manual step after engine run)

The engine produces **quant + risk** scores. Use this template for each name in `analyst_research_queue.csv` or `top_picks.csv` before treating a Watchlist as a real buy.

## Per-stock checklist

| # | Question | Your notes |
|---|----------|------------|
| 1 | Business model — what do they sell? Who pays? | |
| 2 | Moat — why will returns persist 5y? | |
| 3 | Management — capital allocation, promises vs delivery? | |
| 4 | Catalysts — next 12–36 months? | |
| 5 | Risks — top 3 ways the thesis breaks? | |
| 6 | Valuation — base / bull / bear vs price? | |
| 7 | Macro — rates, oil, policy, geopolitics for this sector? | |
| 8 | Promotion — what fills `missing_critical_fields` or gate failures? | |

## Engine fields to read first

- `gate_fail_reasons`, `missing_critical_fields` in `action_sheet.csv`
- `research_tier`, `value_trap_flags` in `stock_<TICKER>.json`
- `macro_context.json` in the run folder (regime + manual shock checklist)

## Promotion rules (unchanged by design)

**Watchlist → Buy Candidate** only when the **next engine run** shows:

- `recommendation` = Buy Candidate
- `gate_passed` = true
- `research_tier` = High Confidence Research (for highest conviction lists)
- You agree qualitatively with rows 1–7 above

Do not override gates in the spreadsheet alone.
