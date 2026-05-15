# Public Source Contracts

This project accepts public/local data through explicit CSV contracts. The engine
must never infer that a missing value is safe. Missing means unknown.

## Shareholding / Pledge

Default path:

```text
data/raw/redflags/shareholding/shareholding_<RUN_DATE>.csv
```

Required join column:

```text
NSE Symbol
```

Supported aliases:

```text
symbol, ticker, NSE Symbol
```

Supported evidence columns:

```text
Pledged percentage
Promoter Holding %
Promoter Holding Prev %
FII %
DII %
MF Holding %
```

## Governance Events

Default path:

```text
data/raw/redflags/governance/governance_events_<RUN_DATE>.csv
```

Supported evidence columns:

```text
Governance Events
Governance Risk
Source URL
As Of Date
Announcement Date
Raw Headline
```

Use `Governance Events=none` only when the symbol was actually checked for the
configured lookback window. Leave the cell blank when governance evidence is
unknown.

## Bank / NBFC Financial Risk

Default path:

```text
data/raw/fundamentals/financial_risk/financial_risk_<RUN_DATE>.csv
```

Supported evidence columns:

```text
GNPA %
NNPA %
CAR %
PCR %
NIM
Credit Cost
Slippage Ratio
ALM ST %
Advances Growth
Deposit Growth
NII Growth
Fee Income Growth
Earnings Growth
AUM Growth
Cost to Income
Interest Coverage
Debt to equity
Credit Rating Grade
```

## Merge Rules

- `make merge-public-enrichment` preserves existing non-empty values by default.
- Use `ENRICHMENT_OVERWRITE=true` only for an intentional refresh.
- Every merge writes `data/processed/public_enrichment_report_<RUN_DATE>.json`.
- Missing optional enrichment files are reported, not treated as runtime errors.
- Critical evidence gaps still reduce confidence and can block high-confidence lists.

## Maintainer Checklist For New Sources

- Add parser tests with representative source payloads.
- Preserve the CSV contract above or add aliases in `scripts/merge_public_enrichment.py`.
- Do not mark unknown fields as `0`.
- Do not mark governance as `none` unless the source query proves the lookback window was checked.
- Add source freshness to `scripts/source_registry.py` or `scripts/data_freshness.py` if the source becomes first-class.
