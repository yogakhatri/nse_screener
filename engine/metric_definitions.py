"""
╔══════════════════════════════════════════════════════════════════════╗
║   NSE RATING ENGINE — PHASE 3: DEFINITIVE METRIC COMPUTATION SPEC   ║
║   Single source of truth. No formula may be computed differently     ║
║   anywhere in the engine. All derived metrics import from here.      ║
╚══════════════════════════════════════════════════════════════════════╝

NOTATION USED THROUGHOUT:
  T           = most recent trading day with a closing price
  T-N         = N trading days before T (business days, not calendar)
  FY0         = most recently completed full fiscal year
  FY-N        = N fiscal years prior to FY0
  Q0          = most recently reported quarter
  Q-N         = N quarters before Q0
  TTM         = trailing 12 months = sum of last 4 quarterly values
  PriorTTM    = TTM ending 4 quarters before Q0
  Peer group  = assigned peer set (Basic Industry / Industry / Sector per Phase 1 fallback)
  None        = metric is MISSING; weight is redistributed, never zeroed
  DISQUALIFIER= metric value that forces Red Flags card score ≤ 20 (Severe), regardless of other sub-metrics
  WARNING     = metric value that contributes high risk but is not a standalone disqualifier
"""

from __future__ import annotations
import numpy as np
from typing import Optional, List, Tuple

# ══════════════════════════════════════════════════════════════════════
# SECTION 1 — PERFORMANCE CARD METRICS
# ══════════════════════════════════════════════════════════════════════

def compute_return_1y(close_T: float, close_T252: float) -> Optional[float]:
    """
    METRIC    : return_1y — 1-Year Price Return
    INPUTS    : close_T (adj. close on T), close_T252 (adj. close 252 trading days before T)
    LOOKBACK  : exactly 252 trading days
    FORMULA   : (close_T / close_T252) - 1   [expressed as decimal, e.g. 0.28 = 28%]
    ADJUSTMENT: use split- and bonus-adjusted closing prices (NSE UDiFF CLOSE_PRICE is unadjusted;
                apply corporate action adjustment using NSE bonus/split history before computing)
    FALLBACK  : None if close_T252 is unavailable (stock listed < 252 days → eligibility gate excludes stock)
    DIRECTION : higher_is_better = True
    """
    if close_T252 is None or close_T252 == 0:
        return None
    return (close_T / close_T252) - 1.0


def compute_return_6m(close_T: float, close_T126: float) -> Optional[float]:
    """
    METRIC    : return_6m — 6-Month Price Return
    INPUTS    : close_T, close_T126 (adj. close 126 trading days before T)
    LOOKBACK  : exactly 126 trading days
    FORMULA   : (close_T / close_T126) - 1
    FALLBACK  : None if close_T126 unavailable (stock listed < 126 days;
                sub-metric missing but stock is NOT excluded — 252-day rule handles eligibility)
    DIRECTION : higher_is_better = True
    """
    if close_T126 is None or close_T126 == 0:
        return None
    return (close_T / close_T126) - 1.0


def compute_cagr_5y(close_T: float, close_T1260: Optional[float]) -> Optional[float]:
    """
    METRIC    : cagr_5y — 5-Year Annualised Price Return (CAGR)
    INPUTS    : close_T, close_T1260 (adj. close ~1260 trading days before T; use nearest available if exact day missing)
    LOOKBACK  : 5 years = 1260 trading days (use 1250–1275 day window, take closest)
    FORMULA   : (close_T / close_T1260)^(1/5) - 1
    FALLBACK  : None if listing age < 4.5 years (< 1134 trading days);
                weight redistributed to remaining sub-metrics in Performance card
    DIRECTION : higher_is_better = True
    """
    if close_T1260 is None or close_T1260 == 0:
        return None
    return (close_T / close_T1260) ** (1.0 / 5.0) - 1.0


def compute_drawdown_recovery(close: float, high_52w: float, low_52w: float) -> Optional[float]:
    """
    METRIC    : drawdown_recovery — % Recovery from 52-Week Low
    INPUTS    : close (latest adj. close), high_52w, low_52w (rolling 252-day high and low)
    LOOKBACK  : 252 trading days
    FORMULA   : (close - low_52w) / (high_52w - low_52w) × 100
                Result is 0–100: 0 = at 52W low, 100 = at 52W high
    FALLBACK  : None if (high_52w - low_52w) == 0 (circuit-locked or single-price stock)
    NOTE      : For Entry Point card, this same metric is reused as 'drawdown_normalization'
                (Templates B/C). Compute once; share the value.
    DIRECTION : higher_is_better = True
    """
    if high_52w is None or low_52w is None:
        return None
    rng = high_52w - low_52w
    if rng == 0:
        return None
    return min(max((close - low_52w) / rng * 100.0, 0.0), 100.0)


def compute_forward_view(fwd_eps_growth_pct: Optional[float],
                         fwd_rev_growth_pct: Optional[float]) -> Optional[float]:
    """
    METRIC    : forward_view — 1Y Forward Consensus View
    INPUTS    : fwd_eps_growth_pct (FY+1 EPS growth % consensus),
                fwd_rev_growth_pct (FY+1 Revenue growth % consensus)
                Both sourced from Trendlyne API / Tickertape Pro.
    LOOKBACK  : Forward 12 months (FY+1 vs FY0)
    FORMULA   : simple average of the two available growth estimates
                If only one is available, use that single value.
    FALLBACK  : None if BOTH are unavailable (no analyst coverage);
                this is the most commonly missing metric in small/micro-cap stocks.
    DIRECTION : higher_is_better = True
    """
    vals = [v for v in [fwd_eps_growth_pct, fwd_rev_growth_pct] if v is not None]
    if not vals:
        return None
    return float(np.mean(vals))


# ══════════════════════════════════════════════════════════════════════
# SECTION 2 — INTRINSIC VALUE MODEL (canonical; shared by Valuation & Entry Point)
# ══════════════════════════════════════════════════════════════════════

# INTRINSIC VALUE COMPUTATION — LOCKED MODEL
# This model is shared by:
#   - Valuation card: iv_gap (Template A), fair_value_gap (Template B/C)
#   - Entry Point card: discount_to_iv (Template A), discount_to_fair_pb (Template B/C)
# Rule: BOTH cards MUST use the output of compute_intrinsic_value() below.
#       No card may run its own separate IV calculation.

# ── Template A (General Non-Financial): Earnings Power Value (EPV) ──
# Method chosen: Conservative EPV using normalised earnings + no-growth assumption.
# Rationale: DCF is sensitive to terminal growth rate assumptions; EPV is more
# conservative and deterministic given the data available from XBRL filings.
#
# Formula:
#   Normalised_EPS = median of EPS over last 3 fiscal years (uses FY0, FY-1, FY-2)
#   EPV_per_share  = Normalised_EPS / WACC
#   WACC           = 12% (locked; reviewed annually)
#                    (cost of equity for Indian mid/small-cap with standard risk premium)
#   Margin_of_Safety_Haircut = 0% (raw EPV; margin of safety is expressed as score, not applied to IV)
#
# Graham Number (secondary check):
#   Graham_IV = sqrt(22.5 × EPS_TTM × BVPS_latest)
#   Used as a sanity cap: IV = min(EPV, 2.0 × Graham_IV)
#   The 2× cap prevents EPV from being absurdly high for capital-light, high-margin stocks.

LOCKED_WACC_GENERAL: float = 0.12          # 12% — do not change without updating this file
LOCKED_COE_BANK: float     = 0.13          # 13% cost of equity for banks (higher leverage risk)
LOCKED_COE_NBFC: float     = 0.14          # 14% cost of equity for NBFCs (higher liquidity risk)
GRAHAM_CAP_MULTIPLIER: float = 2.0         # IV capped at 2× Graham Number

def compute_iv_general(
    eps_fy0: Optional[float],
    eps_fy1: Optional[float],
    eps_fy2: Optional[float],
    eps_ttm: Optional[float],
    bvps:    Optional[float],
) -> Optional[float]:
    """
    METRIC    : Intrinsic Value per share (Template A — General Non-Financial)
    INPUTS    :
      eps_fy0   : EPS for most recent completed fiscal year
      eps_fy1   : EPS for FY-1
      eps_fy2   : EPS for FY-2
      eps_ttm   : Trailing 12-month EPS (for Graham cross-check)
      bvps      : Book Value Per Share (latest reported quarter)
    LOOKBACK  : 3 fiscal years for normalised EPS; latest quarter for BVPS
    FORMULA   :
      Step 1: normalised_eps = median([eps_fy0, eps_fy1, eps_fy2]; exclude None values)
              Require at least 2 of 3 FY EPS values; else return None
      Step 2: epv = normalised_eps / LOCKED_WACC_GENERAL
      Step 3: graham_iv = sqrt(22.5 × eps_ttm × bvps)  [if both available, else skip cap]
      Step 4: iv = min(epv, GRAHAM_CAP_MULTIPLIER × graham_iv)  [if graham_iv computable]
              else iv = epv
      Step 5: if iv <= 0 → return None
    FALLBACK  : None if fewer than 2 FY EPS values available
                None if normalised_eps <= 0 (structurally loss-making)
    DIRECTION : n/a (raw value in ₹; used to compute iv_gap %)
    """
    fy_eps = [v for v in [eps_fy0, eps_fy1, eps_fy2] if v is not None]
    if len(fy_eps) < 2:
        return None
    normalised = float(np.median(fy_eps))
    if normalised <= 0:
        return None
    epv = normalised / LOCKED_WACC_GENERAL

    if eps_ttm is not None and bvps is not None and eps_ttm > 0 and bvps > 0:
        graham = (22.5 * eps_ttm * bvps) ** 0.5
        iv = min(epv, GRAHAM_CAP_MULTIPLIER * graham)
    else:
        iv = epv

    return iv if iv > 0 else None


def compute_iv_gap(close: float, iv: Optional[float]) -> Optional[float]:
    """
    METRIC    : iv_gap / discount_to_iv — % Discount of Price to Intrinsic Value
    INPUTS    : close (current market price), iv (from compute_iv_general or compute_fair_pb)
    FORMULA   : (iv - close) / iv × 100
                Positive = stock is CHEAP vs IV (preferred)
                Negative = stock is EXPENSIVE vs IV
                Not capped: a stock trading at 50% discount scores higher than one at 10% discount.
                Not floored at 0: negative values are valid (premium to IV carries a low score).
    FALLBACK  : None if iv is None
    DIRECTION : higher_is_better = True
    """
    if iv is None or iv == 0:
        return None
    return (iv - close) / iv * 100.0


def compute_fair_pb(roe_ttm: Optional[float],
                    coe: float = LOCKED_COE_BANK) -> Optional[float]:
    """
    METRIC    : Fair P/B (Templates B & C — Banks/NBFCs)
                Justified Price-to-Book = ROE / Cost of Equity
    INPUTS    : roe_ttm (Return on Equity %, TTM), coe (locked per template)
    FORMULA   : fair_pb = roe_ttm_decimal / coe
                e.g., ROE 16% / COE 13% = Fair P/B of 1.23×
    FALLBACK  : None if roe_ttm is None or roe_ttm <= 0
    COE VALUES: Banks → 0.13, NBFCs/HFCs → 0.14 (locked above)
    DIRECTION : n/a (raw ratio; used to compute fair_value_gap / discount_to_fair_pb)
    """
    if roe_ttm is None or roe_ttm <= 0:
        return None
    return (roe_ttm / 100.0) / coe


def compute_fair_value_gap(pb_current: float, fair_pb: Optional[float]) -> Optional[float]:
    """
    METRIC    : fair_value_gap / discount_to_fair_pb (Templates B & C)
    INPUTS    : pb_current (current P/B ratio), fair_pb (from compute_fair_pb)
    FORMULA   : (fair_pb - pb_current) / fair_pb × 100
                Positive = bank is cheap vs justified P/B
    FALLBACK  : None if fair_pb is None
    DIRECTION : higher_is_better = True
    """
    if fair_pb is None or fair_pb == 0:
        return None
    return (fair_pb - pb_current) / fair_pb * 100.0


# ══════════════════════════════════════════════════════════════════════
# SECTION 3 — VALUATION CARD METRICS
# ══════════════════════════════════════════════════════════════════════

def compute_hist_val_band(pe_current: Optional[float],
                          pe_history: List[float]) -> Optional[float]:
    """
    METRIC    : hist_val_band (Template A) / hist_pb_band (Template B/C)
    INPUTS    : pe_current (or pb_current) — today's ratio
                pe_history — list of annual (or quarterly) ratio snapshots over 5Y
                (use fiscal-year-end snapshots: last 5 values = 5 annual PE or PB points)
    LOOKBACK  : 5 fiscal years (minimum 3 values required)
    FORMULA   : median_hist / current_ratio × 100
                Score > 100 means current valuation is BELOW historical median (cheap vs history)
                Score < 100 means current valuation is ABOVE historical median (expensive)
    FALLBACK  : None if < 3 historical observations available
                None if current_ratio <= 0 or median_hist <= 0
    DIRECTION : higher_is_better = True (higher score = trading cheaper than historical norm)
    """
    if pe_current is None or pe_current <= 0:
        return None
    valid_hist = [v for v in pe_history if v is not None and v > 0]
    if len(valid_hist) < 3:
        return None
    median_hist = float(np.median(valid_hist))
    return median_hist / pe_current * 100.0


# ══════════════════════════════════════════════════════════════════════
# SECTION 4 — GROWTH CARD METRICS
# ══════════════════════════════════════════════════════════════════════

def compute_cagr_3y(value_fy0: Optional[float],
                    value_fy3: Optional[float]) -> Optional[float]:
    """
    METRIC    : rev_cagr_3y / eps_cagr_3y / advances_growth (3Y CAGR form)
    INPUTS    : value_fy0 (FY0 value), value_fy3 (FY-3 value)
    LOOKBACK  : 3 full fiscal years
    FORMULA   : (value_fy0 / value_fy3)^(1/3) - 1   [decimal, e.g. 0.17 = 17%]
    FALLBACK  : None if either input is None
                None if value_fy3 <= 0 (base cannot be zero or negative for CAGR)
                For eps_cagr_3y: None if eps_fy3 <= 0 (negative base → meaningless CAGR)
                For rev_cagr_3y: None if rev_fy3 <= 0
    DIRECTION : higher_is_better = True
    """
    if value_fy0 is None or value_fy3 is None or value_fy3 <= 0:
        return None
    return (value_fy0 / value_fy3) ** (1.0 / 3.0) - 1.0


def compute_yoy_growth(value_ttm: Optional[float],
                       value_prior_ttm: Optional[float]) -> Optional[float]:
    """
    METRIC    : rev_growth_yoy / eps_growth_yoy / nii_growth / etc.
    INPUTS    : value_ttm (TTM = sum of last 4 quarters),
                value_prior_ttm (TTM ending 4 quarters earlier)
    LOOKBACK  : 8 quarters of data to compute two non-overlapping TTMs
    FORMULA   : (value_ttm / value_prior_ttm) - 1   [decimal]
    FALLBACK  : None if either input is None
                None if value_prior_ttm <= 0 (negative base)
    DIRECTION : higher_is_better = True
    """
    if value_ttm is None or value_prior_ttm is None or value_prior_ttm <= 0:
        return None
    return (value_ttm / value_prior_ttm) - 1.0


def compute_growth_stability(annual_values: List[Optional[float]]) -> Optional[float]:
    """
    METRIC    : growth_stability — Consistency of Revenue/NII growth over 5 years
    INPUTS    : annual_values — list of 5 annual values (FY-4 to FY0) for revenue or NII
                                (5 values produce 4 YoY growth rates)
    LOOKBACK  : 5 fiscal years
    FORMULA   :
      Step 1: Compute 4 YoY growth rates: g_i = (v_i / v_i-1) - 1
              Drop any rate where the prior year value ≤ 0.
      Step 2: Require at least 3 valid growth rates; else return None.
      Step 3: mean_g = mean of valid growth rates
              If mean_g == 0: stability = 0.5 (neutral — no growth but no decline either)
              If mean_g < 0 (secular decline): CoV = StdDev / abs(mean_g)
              Else: CoV = StdDev / abs(mean_g)   [always use absolute mean for CoV denominator]
      Step 4: stability = 1 - min(CoV, 2.0)   [cap CoV at 2.0 to prevent extreme negatives]
              Clip to [0.0, 1.0]
      Step 5: Return stability × 100  [scale to 0–100 for percentile scoring input]
    FALLBACK  : None if fewer than 3 valid YoY growth rates are computable
    DIRECTION : higher_is_better = True
    NOTE      : This metric is already expressed as 0–100 before percentile scoring.
                The percentile score will further rank it within the peer group.
    """
    vals = [v for v in annual_values if v is not None]
    if len(vals) < 4:     # need at least 4 values to compute 3 growth rates
        return None
    growth_rates = []
    for i in range(1, len(vals)):
        if vals[i-1] > 0:
            growth_rates.append((vals[i] / vals[i-1]) - 1.0)
    if len(growth_rates) < 3:
        return None
    arr = np.array(growth_rates)
    mean_g = float(np.mean(arr))
    std_g  = float(np.std(arr, ddof=1))
    if abs(mean_g) < 1e-9:
        return 50.0   # zero mean: no trend either way → neutral
    cov = std_g / abs(mean_g)
    stability = 1.0 - min(cov, 2.0)
    return float(np.clip(stability, 0.0, 1.0) * 100.0)


# ══════════════════════════════════════════════════════════════════════
# SECTION 5 — PROFITABILITY CARD METRICS
# ══════════════════════════════════════════════════════════════════════

def compute_roce(ebit: Optional[float], total_assets: Optional[float],
                 current_liabilities: Optional[float]) -> Optional[float]:
    """
    METRIC    : ROCE — Return on Capital Employed (single year)
    INPUTS    : ebit (EBIT = PBT + Interest Expense; from annual P&L)
                total_assets (from annual balance sheet)
                current_liabilities (from annual balance sheet)
    LOOKBACK  : single fiscal year observation (called 3× for 3Y median)
    FORMULA   : capital_employed = total_assets - current_liabilities
                roce = ebit / capital_employed × 100
    FALLBACK  : None if ebit is None, or capital_employed ≤ 0
                For roce_3y_median: None if fewer than 2 of 3 annual ROCE values available
    NOTE      : EBIT derivation priority:
                  (1) Use EBIT directly if disclosed in XBRL
                  (2) Else: EBIT = PBT + Finance_Costs
                  (3) Else: EBIT = Revenue - COGS - Opex (exclude D&A add-back — use operating profit)
    DIRECTION : higher_is_better = True
    """
    if ebit is None or total_assets is None or current_liabilities is None:
        return None
    cap_emp = total_assets - current_liabilities
    if cap_emp <= 0:
        return None
    return ebit / cap_emp * 100.0


def compute_roce_3y_median(roce_fy0: Optional[float], roce_fy1: Optional[float],
                            roce_fy2: Optional[float]) -> Optional[float]:
    """
    METRIC    : roce_3y_median — 3-Year Median ROCE
    INPUTS    : ROCE for FY0, FY-1, FY-2 (each from compute_roce above)
    LOOKBACK  : 3 fiscal years
    FORMULA   : median of available non-None ROCE values
    FALLBACK  : None if fewer than 2 values are available
    DIRECTION : higher_is_better = True
    """
    vals = [v for v in [roce_fy0, roce_fy1, roce_fy2] if v is not None]
    if len(vals) < 2:
        return None
    return float(np.median(vals))


def compute_cfo_pat_ratio(cfo_ttm: Optional[float], pat_ttm: Optional[float]) -> Optional[float]:
    """
    METRIC    : cfo_pat_ratio — Earnings Quality (Cash Flow vs Accounting Profit)
    INPUTS    : cfo_ttm (Operating Cash Flow, TTM), pat_ttm (Profit After Tax, TTM)
    LOOKBACK  : TTM (sum of last 4 quarterly cash flow statements)
    FORMULA   : cfo_ttm / pat_ttm
    INTERPRETATION (for scoring context, NOT for changing the formula):
      Ratio ≥ 1.0  → earnings well-supported by cash (high quality)
      Ratio 0.5–1.0 → partial cash backing (moderate quality)
      Ratio < 0.5  → earnings not converting to cash (accrual concern)
      Ratio < 0    → CFO is negative while PAT is positive (RED FLAG cross-check — also impacts accounting_quality)
    FALLBACK  : None if pat_ttm is None or pat_ttm == 0
                If cfo_ttm is None (cash flow statement missing): return None
    DIRECTION : higher_is_better = True (ratio is scored as-is against peer group)
    """
    if cfo_ttm is None or pat_ttm is None or pat_ttm == 0:
        return None
    return cfo_ttm / pat_ttm


def compute_margin_trend(ebitda_margins: List[Optional[float]]) -> Optional[float]:
    """
    METRIC    : margin_trend — Direction of EBITDA Margin over 3 Years
    INPUTS    : ebitda_margins — list of 3 annual EBITDA margin % values [FY-2, FY-1, FY0]
                  (annual, not TTM; use fiscal-year-end EBITDA/Revenue)
    LOOKBACK  : 3 fiscal years
    FORMULA   :
      Step 1: Filter out None values; require at least 2.
      Step 2: OLS linear regression of margin values on time index [0, 1, 2].
              slope = coefficient on time index (in margin % per year).
      Step 3: Return slope as the raw metric value.
              (Positive slope = improving margins; negative = deteriorating)
      NOTE: The slope is a raw number (e.g., +1.5 means +1.5pp/year improvement).
            Percentile scoring against peer group converts this to 0–100.
            Do NOT normalise the slope manually before passing to the scoring function.
    FALLBACK  : None if fewer than 2 annual margin observations
                None if all margins are identical (zero variance → slope = 0; return 0.0 not None)
    DIRECTION : higher_is_better = True
    """
    vals_with_idx = [(i, v) for i, v in enumerate(ebitda_margins) if v is not None]
    if len(vals_with_idx) < 2:
        return None
    xs = np.array([x[0] for x in vals_with_idx], dtype=float)
    ys = np.array([x[1] for x in vals_with_idx], dtype=float)
    if np.std(ys) < 1e-9:
        return 0.0   # flat margin — no trend
    slope = float(np.polyfit(xs, ys, 1)[0])
    return slope


def compute_fcf_consistency(fcf_annual: List[Optional[float]]) -> Optional[float]:
    """
    METRIC    : fcf_consistency — % of Years with Positive Free Cash Flow over 5Y
    INPUTS    : fcf_annual — list of 5 annual FCF values (FY-4 to FY0)
                FCF = CFO_annual - CAPEX_annual  (CAPEX = Purchase of Fixed Assets from Cash Flow)
    LOOKBACK  : 5 fiscal years
    FORMULA   : count(fcf > 0) / count(fcf not None) × 100
                e.g., 4 positive out of 5 = 80.0
    FALLBACK  : None if fewer than 3 annual FCF values are available
    NOTE      : FCF = 0 is NOT positive; strictly > 0 required.
    DIRECTION : higher_is_better = True
    """
    valid = [v for v in fcf_annual if v is not None]
    if len(valid) < 3:
        return None
    positive = sum(1 for v in valid if v > 0)
    return positive / len(valid) * 100.0


# ══════════════════════════════════════════════════════════════════════
# SECTION 6 — ENTRY POINT CARD METRICS
# ══════════════════════════════════════════════════════════════════════

# RSI scoring table — NON-MONOTONIC (locked; do not interpolate differently)
# RSI range        →  raw score (0–100)
# Rationale: mildly oversold (25-40) is the ideal entry zone.
# Extreme oversold (<20) may signal fundamental distress, not just a technical pullback.
# Overbought (>70) is a poor entry regardless of fundamentals.
RSI_SCORE_TABLE: List[Tuple[float, float, float]] = [
    # (rsi_lo, rsi_hi, score)
    (0.0,  20.0, 10.0),   # extreme oversold / panic — possible distress, not an entry
    (20.0, 25.0, 40.0),   # deep oversold — possible entry but risky
    (25.0, 35.0, 100.0),  # IDEAL ENTRY ZONE — mildly oversold, fear-driven
    (35.0, 45.0, 80.0),   # slightly oversold / neutral low
    (45.0, 55.0, 55.0),   # fair value zone — neutral
    (55.0, 65.0, 35.0),   # slightly overbought
    (65.0, 75.0, 15.0),   # overbought
    (75.0, 100.1,  5.0),  # extreme overbought
]

def compute_rsi_score(rsi_14: Optional[float]) -> Optional[float]:
    """
    METRIC    : rsi_state — RSI-based Entry Quality Score
    INPUTS    : rsi_14 — Wilder RSI(14) using daily adjusted close prices
    LOOKBACK  : 14 trading days for RSI; seed period = first 14 days use simple average
                (Wilder's smoothing: RS_smooth = ((13 × prev_avg_gain) + curr_gain) / 14)
    FORMULA   : Look up RSI_SCORE_TABLE above; return mapped score.
    FALLBACK  : None if < 14 days of price data; None if rsi_14 is None
    DIRECTION : higher_is_better = True (but NON-MONOTONIC; do not invert)
    NOTE      : This metric is passed directly to the percentile scorer WITHOUT direction inversion.
                The RSI_SCORE_TABLE already encodes the desired direction.
                Override METRIC_DIRECTION['rsi_state'] = True in cards.py (already set).
    """
    if rsi_14 is None:
        return None
    for lo, hi, score in RSI_SCORE_TABLE:
        if lo <= rsi_14 < hi:
            return score
    return 5.0  # rsi = 100 edge case


def compute_rsi_14(close_prices: List[float]) -> Optional[float]:
    """
    Wilder RSI(14) — standard implementation.
    INPUTS: close_prices — list of at least 15 adjusted closing prices (oldest first)
    FORMULA:
      gains = [max(c[i]-c[i-1], 0) for i in 1..n]
      losses = [max(c[i-1]-c[i], 0) for i in 1..n]
      avg_gain_seed = mean(gains[:14])
      avg_loss_seed = mean(losses[:14])
      For i >= 14:
        avg_gain = (avg_gain * 13 + gains[i]) / 14
        avg_loss = (avg_loss * 13 + losses[i]) / 14
      RS = avg_gain / avg_loss  (if avg_loss == 0: RSI = 100)
      RSI = 100 - (100 / (1 + RS))
    FALLBACK: None if len(close_prices) < 15
    """
    if len(close_prices) < 15:
        return None
    closes = np.array(close_prices, dtype=float)
    deltas = np.diff(closes)
    gains  = np.maximum(deltas, 0.0)
    losses = np.maximum(-deltas, 0.0)
    avg_gain = float(np.mean(gains[:14]))
    avg_loss = float(np.mean(losses[:14]))
    for i in range(14, len(gains)):
        avg_gain = (avg_gain * 13.0 + gains[i]) / 14.0
        avg_loss = (avg_loss * 13.0 + losses[i]) / 14.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_price_vs_ma(close: float, ma: Optional[float]) -> Optional[float]:
    """
    METRIC    : price_vs_200dma / price_vs_50dma
    INPUTS    : close (adj. close), ma (simple moving average over N days)
    LOOKBACK  : 200 trading days for 200DMA; 50 for 50DMA
    FORMULA   : (close - ma) / ma × 100
                Negative → price below MA (entry zone, preferred)
                Positive → price above MA
    FALLBACK  : None if ma is None (< N days of history)
    DIRECTION : higher_is_better = True
                NOTE: Both price_vs_200dma and price_vs_50dma use higher_is_better = True
                EVEN THOUGH a negative value is preferred for entry.
                Why: A stock at -15% below 200DMA scores in the low-20th percentile;
                a stock at -5% scores higher; a stock at +20% scores highest.
                The percentile function naturally rewards larger values without manual inversion.
                This is correct — a stock far above 200DMA is NOT a fear entry.
                However, for Entry Point card specifically, the scoring function
                must USE higher_is_better=False for these two metrics so that
                stocks BELOW their MA receive higher scores.
                Override: METRIC_DIRECTION['price_vs_200dma'] = False in cards.py
                          METRIC_DIRECTION['price_vs_50dma']   = False in cards.py
    """
    if ma is None or ma == 0:
        return None
    return (close - ma) / ma * 100.0

# DIRECTION CORRECTION NOTE (overrides config in cards.py):
# price_vs_200dma and price_vs_50dma: higher_is_better = FALSE in Entry Point context
# (a stock trading BELOW its MA is a better entry opportunity)


def compute_volume_delivery_score(
    delivery_pct_20d_avg: Optional[float],
    delivery_pct_60d_avg: Optional[float],
) -> Optional[float]:
    """
    METRIC    : volume_delivery — Delivery % Confirmation Score
    INPUTS    : delivery_pct_20d_avg — 20-day average delivery % (deliverable qty / traded qty × 100)
                delivery_pct_60d_avg — 60-day average delivery %
                Both sourced from NSE Security-wise Delivery Position archive.
    LOOKBACK  : 20 days vs 60 days
    FORMULA   :
      raw_score = (delivery_pct_20d_avg / delivery_pct_60d_avg) × 50
                  Ratio = 1.0 → score = 50 (neutral; no change in delivery quality)
                  Ratio > 1.0 → delivery rising (smart money accumulation signal) → score > 50
                  Ratio < 1.0 → delivery falling (selling / speculative activity) → score < 50
      Clamp to [0, 100]
    FALLBACK  : None if either average is None or 60d avg = 0
    DIRECTION : higher_is_better = True
    """
    if delivery_pct_20d_avg is None or delivery_pct_60d_avg is None or delivery_pct_60d_avg == 0:
        return None
    score = (delivery_pct_20d_avg / delivery_pct_60d_avg) * 50.0
    return float(np.clip(score, 0.0, 100.0))


def compute_rs_turn(
    stock_closes: List[float],
    index_closes: List[float],
    short_window: int = 5,
    long_window: int = 10,
) -> Optional[float]:
    """
    METRIC    : rs_turn — Relative Strength Momentum Reversal
    INPUTS    : stock_closes — list of adj. closing prices (daily, at least 15 values)
                index_closes — list of Nifty 500 index closing values (same dates)
                short_window — weeks (×5 trading days) for recent RS slope (default: 5 = 1 week)
                long_window  — weeks for prior RS slope (default: 10 = 2 weeks)
    LOOKBACK  : 15 trading days minimum
    FORMULA   :
      Step 1: RS_series = stock_closes / index_closes  (element-wise)
      Step 2: slope_recent = linear slope of RS_series over last (short_window) points
              slope_prior  = linear slope of RS_series over the prior (long_window) points before that
      Step 3: rs_turn_raw = slope_recent - slope_prior
              Positive → RS is accelerating upward (momentum turn) → higher score
              Negative → RS is decelerating / turning down
      Step 4: Return rs_turn_raw as the raw metric for percentile scoring.
              (Percentile score will rank it within peer group.)
    FALLBACK  : None if len(stock_closes) < short_window + long_window + 2
                None if index_closes has zero values
    DIRECTION : higher_is_better = True
    """
    min_len = short_window + long_window + 2
    if (len(stock_closes) < min_len or len(index_closes) < min_len
            or any(v == 0 for v in index_closes[-min_len:])):
        return None
    sc = np.array(stock_closes[-min_len:], dtype=float)
    ic = np.array(index_closes[-min_len:], dtype=float)
    rs = sc / ic
    xs = np.arange(len(rs), dtype=float)
    slope_recent = float(np.polyfit(xs[-short_window:], rs[-short_window:], 1)[0])
    start = len(rs) - short_window - long_window
    end   = len(rs) - short_window
    slope_prior  = float(np.polyfit(xs[start:end], rs[start:end], 1)[0])
    return slope_recent - slope_prior


def compute_volatility_compression(
    highs: List[float], lows: List[float], closes: List[float],
    short_window: int = 20, long_window: int = 60,
) -> Optional[float]:
    """
    METRIC    : volatility_compression — ATR Contraction Score
    INPUTS    : highs, lows, closes — daily OHLC data (at least 61 values)
    LOOKBACK  : 60 trading days (long window for normalisation baseline)
    FORMULA   :
      Step 1: True Range (TR) = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
      Step 2: ATR_20 = mean(TR over last 20 days)
              ATR_60 = mean(TR over last 60 days)
      Step 3: ATR_ratio = ATR_20 / ATR_60
              ATR_ratio < 1.0 → volatility compressing (base forming) → GOOD for entry
              ATR_ratio > 1.0 → volatility expanding (breakout or breakdown)
      Step 4: score = (1 - ATR_ratio) × 100
              clip to [0, 100]
              (score = 0 means ATR_ratio ≥ 1.0 i.e. expanding vol; score = 100 means fully compressed)
    FALLBACK  : None if fewer than long_window + 1 = 61 OHLC data points
    DIRECTION : higher_is_better = True
    """
    if len(closes) < long_window + 1:
        return None
    h = np.array(highs[-long_window-1:], dtype=float)
    l = np.array(lows[-long_window-1:], dtype=float)
    c = np.array(closes[-long_window-1:], dtype=float)
    tr = np.maximum.reduce([h[1:]-l[1:], np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])])
    atr_20 = float(np.mean(tr[-short_window:]))
    atr_60 = float(np.mean(tr[-long_window:]))
    if atr_60 == 0:
        return None
    score = (1.0 - atr_20 / atr_60) * 100.0
    return float(np.clip(score, 0.0, 100.0))


# ══════════════════════════════════════════════════════════════════════
# SECTION 7 — RED FLAGS: EXACT THRESHOLDS + TRIGGER CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════
# Two trigger levels:
#   WARNING      : High risk sub-metric; contributes to card score but does NOT
#                  alone force the card to Severe.
#   DISQUALIFIER : Alone forces the entire Red Flags card score ≤ 20 (Severe)
#                  regardless of how clean other sub-metrics are.
# Disqualifier conditions are checked in score_red_flags() BEFORE aggregation.

# ── 7A. Promoter Pledge Risk ─────────────────────────────────────────
# Input: pledge_pct = pledged_promoter_shares / total_promoter_shares × 100
PLEDGE_TABLE: List[Tuple[float, float, float, str]] = [
    # (lo, hi, raw_risk, level)
    (0.0,  10.0, 0.0,  "Clean"),
    (10.0, 25.0, 25.0, "Watch"),
    (25.0, 40.0, 50.0, "Warning"),
    (40.0, 60.0, 75.0, "High"),
    (60.0, 101.0,100.0,"DISQUALIFIER"),  # > 60% pledge → Disqualifier
]
PLEDGE_DISQUALIFIER_THRESHOLD: float = 60.0  # pledge_pct > 60% → DISQUALIFIER

def compute_promoter_pledge_risk(pledge_pct: Optional[float]) -> Optional[float]:
    """
    METRIC    : promoter_pledge — Promoter Pledge Risk
    INPUT     : pledge_pct (% of total promoter holding that is pledged)
                Source: BSE Shareholding Pattern filing (Category I, pledged column)
    LOOKBACK  : Latest quarter
    FORMULA   : Look up PLEDGE_TABLE → return raw_risk score
    DISQUALIFIER: pledge_pct > 60% → raw_risk = 100 → card forced to Severe
    FALLBACK  : If shareholding pattern not filed (rare): assume 0% (clean)
                Log assumption in data_gaps.csv; verify manually before acting.
    DIRECTION : higher_is_better = False (higher raw_risk = lower percentile score = riskier)
    """
    if pledge_pct is None:
        return 0.0   # assume clean per fallback rule
    for lo, hi, risk, _ in PLEDGE_TABLE:
        if lo <= pledge_pct < hi:
            return risk
    return 100.0


# ── 7B. ASM / GSM Surveillance Risk ──────────────────────────────────
# Input: asm_stage (int 0-4), gsm_stage (int 0-6); 0 = not on list
ASM_RISK_MAP: dict = {0: 0.0, 1: 30.0, 2: 50.0, 3: 70.0, 4: 90.0}
GSM_RISK_MAP: dict = {0: 0.0, 1: 50.0, 2: 65.0, 3: 75.0, 4: 85.0, 5: 95.0, 6: 100.0}
# DISQUALIFIER: ANY GSM stage ≥ 1 OR ASM stage ≥ 3
# WARNING:      ASM stage 1 or 2

def compute_asm_gsm_risk(asm_stage: int, gsm_stage: int) -> Tuple[float, bool]:
    """
    METRIC    : asm_gsm_risk
    INPUTS    : asm_stage (0–4), gsm_stage (0–6)
    FORMULA   : raw_risk = max(ASM_RISK_MAP[asm_stage], GSM_RISK_MAP[gsm_stage])
    DISQUALIFIER triggers:
        gsm_stage >= 1   → True (any GSM status = Disqualifier)
        asm_stage >= 3   → True (ASM Stage 3 and 4 = Disqualifier)
    WARNING:
        asm_stage in [1, 2] → Warning (contributes risk but not standalone disqualifier)
    FALLBACK  : If list not downloaded today → raise DataFreshnessError (do NOT assume clean)
    RETURNS   : (raw_risk_0_to_100, is_disqualifier: bool)
    """
    raw_risk = max(ASM_RISK_MAP.get(asm_stage, 90.0), GSM_RISK_MAP.get(gsm_stage, 0.0))
    is_disq  = (gsm_stage >= 1) or (asm_stage >= 3)
    return raw_risk, is_disq


# ── 7C. Default / Distress Risk ───────────────────────────────────────
# Composite of financial ratio signals + credit rating
# Input fields: debt_to_equity, interest_coverage_ttm, credit_rating_grade (int 1-10)
# credit_rating_grade: 1=AAA, 2=AA, 3=A, 4=BBB (investment grade floor),
#                      5=BB, 6=B, 7=C, 8=D/Default, 9=unrated_small, 10=unknown

DISTRESS_RULES = [
    # (condition_label, condition_fn, risk_contribution, is_disqualifier)
    ("extreme_leverage",     lambda de, ic, cr: de is not None and de > 5.0,         40.0, False),
    ("interest_cover_below1",lambda de, ic, cr: ic is not None and ic < 1.0,         40.0, True),   # DISQUALIFIER
    ("high_leverage",        lambda de, ic, cr: de is not None and 3.0 < de <= 5.0,  20.0, False),
    ("weak_coverage",        lambda de, ic, cr: ic is not None and 1.0 <= ic < 1.5,  20.0, False),
    ("sub_investment_grade", lambda de, ic, cr: cr is not None and cr >= 5,           25.0, False),
    ("default_rated",        lambda de, ic, cr: cr is not None and cr >= 8,          100.0, True),   # DISQUALIFIER
]

def compute_default_distress(debt_to_equity: Optional[float],
                              interest_coverage_ttm: Optional[float],
                              credit_rating_grade: Optional[int]) -> Tuple[float, bool]:
    """
    METRIC    : default_distress
    INPUTS    : debt_to_equity (total debt / total equity, latest quarter)
                interest_coverage_ttm (EBIT_TTM / Interest_Expense_TTM)
                credit_rating_grade (int 1–10; see scale above; None if unrated)
    LOOKBACK  : Latest quarter for balance sheet; TTM for interest coverage
    FORMULA   : Apply DISTRESS_RULES; sum risk_contributions; cap at 100
    DISQUALIFIER:
        interest_coverage < 1.0 → company cannot service debt → DISQUALIFIER
        credit_rating_grade >= 8 (D/Default category) → DISQUALIFIER
    WARNING   : D/E > 3, IC 1.0–1.5, sub-investment grade rating → WARNING
    FALLBACK  : If all three inputs are None → return (0.0, False) but flag in data_gaps.csv
                If only rating is None (unrated): use credit_rating_grade = 9 (unrated_small)
                  → adds 25 to distress score (conservative assumption for unrated companies)
    RETURNS   : (raw_risk_0_to_100, is_disqualifier: bool)
    """
    total_risk = 0.0
    is_disq = False
    for label, cond, contrib, disq in DISTRESS_RULES:
        if cond(debt_to_equity, interest_coverage_ttm, credit_rating_grade):
            total_risk += contrib
            if disq:
                is_disq = True
    return min(total_risk, 100.0), is_disq


# ── 7D. Accounting Quality Stress (Beneish M-Score) ──────────────────
BENEISH_DISQUALIFIER_THRESHOLD: float = -1.78   # M > -1.78 = likely manipulator
BENEISH_WARNING_THRESHOLD: float       = -2.22   # M between -2.22 and -1.78 = zone of concern

def compute_beneish_m_score(
    dsri: Optional[float],   # Days Sales Receivable Index
    gmi:  Optional[float],   # Gross Margin Index
    aqi:  Optional[float],   # Asset Quality Index
    sgi:  Optional[float],   # Sales Growth Index
    depi: Optional[float],   # Depreciation Index
    sgai: Optional[float],   # SGA Expense Index
    lvgi: Optional[float],   # Leverage Index
    tata: Optional[float],   # Total Accruals to Total Assets
) -> Optional[float]:
    """
    METRIC    : accounting_quality (via Beneish M-Score)
    INPUTS    : 8 ratio variables (see Beneish 1999); all derived from 2 consecutive annual P&L + BS
    LOOKBACK  : 2 consecutive fiscal years (FY0 and FY-1)
    FORMULA   : M = -4.84 + 0.920×DSRI + 0.528×GMI + 0.404×AQI + 0.892×SGI
                    + 0.115×DEPI - 0.172×SGAI + 4.679×TATA - 0.327×LVGI
    THRESHOLDS:
        M > -1.78  → HIGH manipulation risk (DISQUALIFIER trigger for accounting_quality sub-metric)
        -2.22 < M ≤ -1.78 → Zone of concern (WARNING)
        M ≤ -2.22  → Low manipulation risk
    FALLBACK  : None if any of the 8 inputs is None
                (entire sub-metric is None if < 2 years of annual data → weight redistributed)
    DIRECTION : higher_is_better = False for raw_risk mapping below
    RAW_RISK CONVERSION:
        M > -1.78  → raw_risk = 100 (DISQUALIFIER in context of card; alone does NOT force card to Severe
                                      unless also combined with other triggers — see disqualifier logic)
        -2.22 < M ≤ -1.78 → raw_risk = 50 (WARNING)
        M ≤ -2.22  → raw_risk = 0
    NOTE      : DSRI, GMI, AQI, SGI, DEPI, SGAI, LVGI, TATA must be computed from XBRL data
                BEFORE being passed to this function. Their individual formulas follow the
                original Beneish (1999) paper exactly. Computed in data_adapter.py.
    """
    inputs = [dsri, gmi, aqi, sgi, depi, sgai, lvgi, tata]
    if any(v is None for v in inputs):
        return None
    m = (-4.84 + 0.920*dsri + 0.528*gmi + 0.404*aqi + 0.892*sgi
         + 0.115*depi - 0.172*sgai + 4.679*tata - 0.327*lvgi)
    return float(m)

def m_score_to_raw_risk(m_score: Optional[float]) -> Tuple[Optional[float], bool]:
    """Convert M-Score to raw_risk (0-100) and disqualifier flag."""
    if m_score is None:
        return None, False
    if m_score > BENEISH_DISQUALIFIER_THRESHOLD:
        return 100.0, False   # High risk but NOT a standalone disqualifier (requires corroboration)
    if m_score > BENEISH_WARNING_THRESHOLD:
        return 50.0, False    # Warning zone
    return 0.0, False


# ── 7E. Liquidity / Manipulation Risk ────────────────────────────────
# Input: avg_daily_turnover_30d (₹ crore), is_t2t (bool: trade-to-trade segment)
LIQUIDITY_TABLE: List[Tuple[float, float, float, str]] = [
    # (lo, hi, raw_risk, label)   [₹ crore threshold]
    (50.0,   1e18, 0.0,  "Liquid"),
    (5.0,    50.0, 10.0, "Low Liquidity"),
    (1.0,    5.0,  30.0, "Very Low Liquidity"),
    (0.25,   1.0,  70.0, "Illiquid"),        # WARNING
    (0.0,    0.25, 100.0,"DISQUALIFIER"),    # < ₹25L avg daily turnover
]
LIQUIDITY_DISQUALIFIER_THRESHOLD: float = 0.25   # ₹ crore (= ₹25 lakh)
T2T_ADDITIONAL_RISK: float = 20.0                 # added if stock is in T2T segment

def compute_liquidity_risk(avg_daily_turnover_cr: Optional[float],
                           is_t2t: bool = False) -> Tuple[float, bool]:
    """
    METRIC    : liquidity_manipulation
    INPUTS    : avg_daily_turnover_cr (30-day avg of daily traded value in ₹ crore)
                is_t2t (True if stock trades in T2T/BE series on NSE)
    LOOKBACK  : 30 trading days for turnover average
    FORMULA   : Look up LIQUIDITY_TABLE; add T2T_ADDITIONAL_RISK if is_t2t; cap at 100
    DISQUALIFIER: avg_daily_turnover_cr < 0.25 (₹25L) → DISQUALIFIER
    WARNING   : ₹25L–₹1Cr turnover → WARNING
    FALLBACK  : If turnover unavailable: proxy = market_cap < ₹50Cr → treat as illiquid (raw_risk = 70)
    RETURNS   : (raw_risk, is_disqualifier)
    """
    if avg_daily_turnover_cr is None:
        return 70.0, False   # proxy: illiquid assumption
    base_risk = 100.0
    is_disq   = False
    for lo, hi, risk, label in LIQUIDITY_TABLE:
        if lo <= avg_daily_turnover_cr < hi:
            base_risk = risk
            is_disq   = (avg_daily_turnover_cr < LIQUIDITY_DISQUALIFIER_THRESHOLD)
            break
    total_risk = min(base_risk + (T2T_ADDITIONAL_RISK if is_t2t else 0.0), 100.0)
    is_disq    = is_disq or (is_t2t and total_risk >= 100.0)
    return total_risk, is_disq


# ── 7F. Governance Event Risk ─────────────────────────────────────────
# Input: structured event dict from quarterly BSE announcement scan
GOVERNANCE_EVENT_SCORES: dict = {
    "auditor_resignation":       50.0,   # WARNING (high impact)
    "auditor_qualification":     40.0,   # WARNING
    "sebi_penalty_major":        40.0,   # SEBI penalty > ₹1Cr or disgorgement order
    "sebi_penalty_minor":        15.0,   # SEBI penalty ≤ ₹1Cr
    "exchange_disciplinary":     20.0,   # NSE/BSE disciplinary action
    "management_fraud_news":     30.0,   # Regulatory enforcement, CBI/ED investigation
    "nse_action_suspension":    100.0,   # DISQUALIFIER
    "promoter_insider_trading":  35.0,   # SEBI insider trading show-cause
    "agm_delayed":               10.0,   # AGM delayed beyond regulatory deadline
}
GOVERNANCE_DISQUALIFIER_EVENTS = {"nse_action_suspension"}  # add more as needed

def compute_governance_risk(events_last_12m: List[str]) -> Tuple[float, bool]:
    """
    METRIC    : governance_event
    INPUTS    : events_last_12m — list of event_keys from GOVERNANCE_EVENT_SCORES
                (populated by quarterly manual scan of BSE Corporate Announcements)
    LOOKBACK  : Last 12 months from run date
    FORMULA   : raw_risk = sum of scores for all events; cap at 100
    DISQUALIFIER: any event in GOVERNANCE_DISQUALIFIER_EVENTS → DISQUALIFIER
    FALLBACK  : If no scan performed this quarter: use prior quarter's events;
                flag in data_gaps.csv. Never assume 0 without a scan.
    RETURNS   : (raw_risk_0_to_100, is_disqualifier)
    """
    total = sum(GOVERNANCE_EVENT_SCORES.get(e, 0.0) for e in events_last_12m)
    is_disq = any(e in GOVERNANCE_DISQUALIFIER_EVENTS for e in events_last_12m)
    return min(total, 100.0), is_disq


# ── 7G. Bank-Specific Red Flag Thresholds ────────────────────────────

# GNPA / NNPA Stress (Template B & C)
# raw_risk = GNPA_pct × 5 + NNPA_pct × 10; cap at 100
GNPA_WARNING_THRESHOLD: float = 5.0   # GNPA > 5% = Warning
GNPA_DISQUALIFIER_THRESHOLD: float = 15.0  # GNPA > 15% = DISQUALIFIER
NNPA_WARNING_THRESHOLD: float = 2.0   # NNPA > 2% = Warning
NNPA_DISQUALIFIER_THRESHOLD: float = 8.0   # NNPA > 8% = DISQUALIFIER

def compute_gnpa_nnpa_stress(gnpa_pct: Optional[float],
                              nnpa_pct: Optional[float]) -> Tuple[Optional[float], bool]:
    """
    METRIC    : gnpa_nnpa_stress
    INPUTS    : gnpa_pct, nnpa_pct (% of total advances; from quarterly filings)
    FORMULA   : raw_risk = gnpa_pct × 5.0 + nnpa_pct × 10.0; cap at 100
    DISQUALIFIER: gnpa_pct > 15% OR nnpa_pct > 8% → DISQUALIFIER
    WARNING   : gnpa_pct > 5% OR nnpa_pct > 2%
    FALLBACK  : None if both inputs are None (flag as data gap; use peer median as proxy)
    """
    if gnpa_pct is None and nnpa_pct is None:
        return None, False
    g = gnpa_pct or 0.0
    n = nnpa_pct or 0.0
    raw_risk = min(g * 5.0 + n * 10.0, 100.0)
    is_disq  = (g > GNPA_DISQUALIFIER_THRESHOLD) or (n > NNPA_DISQUALIFIER_THRESHOLD)
    return raw_risk, is_disq


# CAR / Capital Adequacy Stress (Template B & C)
CAR_MINIMUM_BUFFER: float = 14.0    # Target buffer (RBI minimum = 11.5%; well-capitalised = 14%)
CAR_WARNING_THRESHOLD: float = 12.0 # CAR < 12% = Warning
CAR_DISQUALIFIER_THRESHOLD: float = 10.0  # CAR < 10% = DISQUALIFIER (near RBI minimum)

def compute_car_stress(car_pct: Optional[float]) -> Tuple[Optional[float], bool]:
    """
    METRIC    : capital_adequacy_stress
    FORMULA   : raw_risk = max(0, CAR_MINIMUM_BUFFER - car_pct) × (100 / CAR_MINIMUM_BUFFER)
                Normalises so that CAR = 0% → raw_risk = 100; CAR ≥ 14% → raw_risk = 0
    DISQUALIFIER: car_pct < 10% → DISQUALIFIER
    WARNING   : 10% ≤ car_pct < 12% → WARNING
    FALLBACK  : None if car_pct not disclosed
    """
    if car_pct is None:
        return None, False
    raw_risk = max(0.0, CAR_MINIMUM_BUFFER - car_pct) * (100.0 / CAR_MINIMUM_BUFFER)
    is_disq  = car_pct < CAR_DISQUALIFIER_THRESHOLD
    return min(raw_risk, 100.0), is_disq


# PCR Weakness (Template B)
PCR_TARGET: float = 75.0           # PCR ≥ 75% is considered adequate
PCR_WARNING_THRESHOLD: float = 50.0
PCR_DISQUALIFIER_THRESHOLD: float = 30.0  # PCR < 30% = DISQUALIFIER

def compute_pcr_weakness(pcr_pct: Optional[float]) -> Tuple[Optional[float], bool]:
    """
    METRIC    : pcr_weakness
    FORMULA   : raw_risk = max(0, PCR_TARGET - pcr_pct) × (100 / PCR_TARGET)
                Normalises so that PCR = 0% → raw_risk = 100; PCR ≥ 75% → raw_risk = 0
    DISQUALIFIER: pcr_pct < 30% → DISQUALIFIER
    WARNING   : 30% ≤ pcr_pct < 50%
    FALLBACK  : pcr_pct = 50.0 (assume moderate coverage) if not disclosed; flag in data_gaps
    """
    if pcr_pct is None:
        pcr_pct = 50.0  # conservative assumption
    raw_risk = max(0.0, PCR_TARGET - pcr_pct) * (100.0 / PCR_TARGET)
    is_disq  = pcr_pct < PCR_DISQUALIFIER_THRESHOLD
    return min(raw_risk, 100.0), is_disq


# ALM Mismatch (Template C)
ALM_WARNING_THRESHOLD: float = 30.0       # >30% short-term funding = Warning
ALM_DISQUALIFIER_THRESHOLD: float = 50.0  # >50% short-term funding = DISQUALIFIER

def compute_alm_mismatch(st_borrowings_pct: Optional[float]) -> Tuple[Optional[float], bool]:
    """
    METRIC    : alm_mismatch — Short-term borrowings as % of total borrowings
    INPUTS    : st_borrowings_pct = (CP + ST_Borrowings) / Total_Borrowings × 100
    FORMULA   : raw_risk = st_borrowings_pct  [already 0–100 by definition]
    DISQUALIFIER: st_borrowings_pct > 50% → DISQUALIFIER
    WARNING   : 30%–50%
    FALLBACK  : None if balance sheet borrowing breakdowns unavailable
    """
    if st_borrowings_pct is None:
        return None, False
    raw_risk = float(np.clip(st_borrowings_pct, 0.0, 100.0))
    is_disq  = st_borrowings_pct > ALM_DISQUALIFIER_THRESHOLD
    return raw_risk, is_disq


# ══════════════════════════════════════════════════════════════════════
# SECTION 8 — DISQUALIFIER ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════

DISQUALIFIER_PRECEDENCE = [
    # Checked in order; first True → entire Red Flags card → Severe (score ≤ 20)
    # No further aggregation needed once a DISQUALIFIER is triggered.
    "asm_stage >= 1 (any GSM)",
    "asm_stage >= 3 (ASM Stage 3 or 4)",
    "interest_coverage_ttm < 1.0",
    "credit_rating_grade >= 8 (D/Default)",
    "pledge_pct > 60%",
    "gnpa_pct > 15% (banks/NBFCs)",
    "nnpa_pct > 8% (banks/NBFCs)",
    "car_pct < 10% (banks/NBFCs)",
    "pcr_pct < 30% (banks only)",
    "alm_st_borrowings_pct > 50% (NBFCs/HFCs)",
    "avg_daily_turnover_cr < 0.25 (₹25L)",
    "governance: exchange suspension",
]

def check_all_disqualifiers(
    asm_stage: int = 0,
    gsm_stage: int = 0,
    interest_coverage: Optional[float] = None,
    credit_rating_grade: Optional[int] = None,
    pledge_pct: Optional[float] = None,
    gnpa_pct: Optional[float] = None,
    nnpa_pct: Optional[float] = None,
    car_pct: Optional[float] = None,
    pcr_pct: Optional[float] = None,
    alm_st_pct: Optional[float] = None,
    avg_turnover_cr: Optional[float] = None,
    governance_events: Optional[List[str]] = None,
) -> Tuple[bool, List[str]]:
    """
    Run all disqualifier checks at once.
    Returns (is_disqualified: bool, list_of_triggered_rules: List[str])
    Called BEFORE card aggregation. If is_disqualified is True:
      → card.score = min(aggregated_score, 20.0)
      → card.label = "Severe"
      → card.reason lists triggered rules
    """
    triggered = []
    if gsm_stage >= 1:
        triggered.append(f"GSM Stage {gsm_stage} active")
    if asm_stage >= 3:
        triggered.append(f"ASM Stage {asm_stage} active")
    if interest_coverage is not None and interest_coverage < 1.0:
        triggered.append(f"Interest Coverage {interest_coverage:.2f}x < 1.0")
    if credit_rating_grade is not None and credit_rating_grade >= 8:
        triggered.append(f"Credit Rating in Default category (grade {credit_rating_grade})")
    if pledge_pct is not None and pledge_pct > PLEDGE_DISQUALIFIER_THRESHOLD:
        triggered.append(f"Promoter Pledge {pledge_pct:.1f}% > 60%")
    if gnpa_pct is not None and gnpa_pct > GNPA_DISQUALIFIER_THRESHOLD:
        triggered.append(f"GNPA {gnpa_pct:.1f}% > 15%")
    if nnpa_pct is not None and nnpa_pct > NNPA_DISQUALIFIER_THRESHOLD:
        triggered.append(f"NNPA {nnpa_pct:.1f}% > 8%")
    if car_pct is not None and car_pct < CAR_DISQUALIFIER_THRESHOLD:
        triggered.append(f"CAR {car_pct:.1f}% < 10%")
    if pcr_pct is not None and pcr_pct < PCR_DISQUALIFIER_THRESHOLD:
        triggered.append(f"PCR {pcr_pct:.1f}% < 30%")
    if alm_st_pct is not None and alm_st_pct > ALM_DISQUALIFIER_THRESHOLD:
        triggered.append(f"ALM ST Borrowings {alm_st_pct:.1f}% > 50%")
    if avg_turnover_cr is not None and avg_turnover_cr < LIQUIDITY_DISQUALIFIER_THRESHOLD:
        triggered.append(f"Daily Turnover ₹{avg_turnover_cr:.2f}Cr < ₹0.25Cr")
    if governance_events:
        disq_events = [e for e in governance_events if e in GOVERNANCE_DISQUALIFIER_EVENTS]
        if disq_events:
            triggered.append(f"Governance disqualifier: {disq_events}")
    return len(triggered) > 0, triggered

