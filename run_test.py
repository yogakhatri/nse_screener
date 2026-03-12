import sys
sys.path.insert(0, ".")
from engine import NSERatingEngine, RawStockData, NSEClassification

# Build 3 sample stocks manually
def make(ticker, name, bi, sector, **overrides):
    base = dict(
        return_1y=20.0, return_6m=10.0, cagr_5y=15.0,
        peer_price_strength=60.0, drawdown_recovery=55.0, forward_view=12.0,
        pe_percentile=25.0, pb_percentile=3.5, p_cfo_percentile=22.0,
        ev_ebitda_percentile=18.0, hist_val_band=70.0, fcf_yield=3.5, iv_gap=10.0,
        rev_cagr_3y=0.15, eps_cagr_3y=0.18, rev_growth_yoy=0.14,
        eps_growth_yoy=0.17, peer_growth_rank=62.0, growth_stability=70.0,
        roce_3y_median=22.0, ebitda_margin=26.0, cfo_pat_ratio=1.2,
        margin_trend=1.0, roa=14.0, fcf_consistency=80.0,
        discount_to_iv=12.0, rsi_state=35.0, price_vs_200dma=-4.0,
        price_vs_50dma=-2.0, volume_delivery=58.0, rs_turn=50.0,
        volatility_compression=60.0,
        promoter_pledge=5.0, asm_gsm_risk=0.0, default_distress=5.0,
        accounting_quality=4.0, liquidity_manipulation=5.0, governance_event=0.0,
    )
    base.update(overrides)
    return RawStockData(
        ticker=ticker, name=name,
        classification=NSEClassification(
            macro_sector="Technology", sector=sector,
            industry="IT", basic_industry=bi),
        fundamentals=base)

universe = {
    "INFY":  make("INFY",  "Infosys",          "Computers - Software & Consulting", "Technology",
                  pe_percentile=28.0, rev_cagr_3y=0.14, promoter_pledge=0.0),
    "TCS":   make("TCS",   "TCS",               "Computers - Software & Consulting", "Technology",
                  pe_percentile=32.0, rev_cagr_3y=0.12, promoter_pledge=0.0),
    "WIPRO": make("WIPRO",  "Wipro",             "Computers - Software & Consulting", "Technology",
                  pe_percentile=22.0, rev_cagr_3y=0.10, promoter_pledge=0.0),
    "HCLT":  make("HCLT",  "HCL Tech",          "Computers - Software & Consulting", "Technology",
                  pe_percentile=20.0, rev_cagr_3y=0.16, promoter_pledge=0.0),
    "LTIM":  make("LTIM",  "LTIMindtree",        "Computers - Software & Consulting", "Technology",
                  pe_percentile=30.0, rev_cagr_3y=0.20, promoter_pledge=2.0),
}

engine = NSERatingEngine(universe)
ratings = engine.rate_universe()
leaderboard = engine.to_leaderboard(ratings)

print("\n" + "═"*65)
print(f"  {'RANK':<5} {'TICKER':<8} {'OPP':>5}  {'VALUATION':>9}  "
      f"{'GROWTH':>6}  {'RED FLAGS':>9}  {'STATUS'}")
print("═"*65)
for i, row in enumerate(leaderboard, 1):
    print(f"  #{i:<4} {row['ticker']:<8} {row['opportunity_score']:>5.1f}  "
          f"{row['valuation']:>9.1f}  {row['growth']:>6.1f}  "
          f"{row['red_flags']:>9.1f}  {row['investability_status']}")

print("═"*65)
print("\nFull detail for #1 stock:")
top = leaderboard[0]['ticker']
r   = ratings[top]
print(f"  {r.name} ({top})")
print(f"  Template : {r.template.value}")
print(f"  Performance  : {r.performance.score:.1f}  — {r.performance.reason}")
print(f"  Valuation    : {r.valuation.score:.1f}  — {r.valuation.reason}")
print(f"  Growth       : {r.growth.score:.1f}  — {r.growth.reason}")
print(f"  Profitability: {r.profitability.score:.1f}  — {r.profitability.reason}")
print(f"  Entry Point  : {r.entry_point.score:.1f}  — {r.entry_point.reason}")
print(f"  Red Flags    : {r.red_flags.score:.1f}  — {r.red_flags.reason}")
print()
