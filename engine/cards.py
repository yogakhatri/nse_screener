"""
NSE Rating Engine – Card Scorers (v2 — full disqualifier integration)
"""
from __future__ import annotations
from typing import List
from .models import RawStockData, CardResult, Template
from .config import CARD_WEIGHTS, CARD_LABELS, CARD_DATA_THRESHOLD
from .scoring import score_metric, weighted_card_score
from .metric_definitions import (
    check_all_disqualifiers,
)

METRIC_DIRECTION = {
    "return_1y":True,"return_6m":True,"cagr_5y":True,
    "peer_price_strength":True,"drawdown_recovery":True,"forward_view":True,
    "pe_percentile":False,"pb_percentile":False,"p_cfo_percentile":False,
    "ev_ebitda_percentile":False,"hist_val_band":True,
    "fcf_yield":True,"iv_gap":True,"roe_adj_pb":False,
    "hist_pb_band":True,"fair_value_gap":True,
    "rev_cagr_3y":True,"eps_cagr_3y":True,"rev_growth_yoy":True,
    "eps_growth_yoy":True,"peer_growth_rank":True,"growth_stability":True,
    "advances_growth":True,"deposit_growth":True,"nii_growth":True,
    "fee_income_growth":True,"earnings_growth":True,"aum_growth":True,
    "roce_3y_median":True,"ebitda_margin":True,"cfo_pat_ratio":True,
    "margin_trend":True,"roa":True,"fcf_consistency":True,
    "roe":True,"nim":True,"cost_to_income":False,
    "provision_coverage":True,"credit_cost_discipline":False,
    # Entry Point — price_vs MAs: False so below-MA = higher score
    "discount_to_iv":True,"rsi_state":True,
    "price_vs_200dma":False,"price_vs_50dma":False,
    "volume_delivery":True,"rs_turn":True,"volatility_compression":True,
    "discount_to_fair_pb":True,"volume":True,"drawdown_normalization":True,
    # Red Flags — ALL False (lower raw risk value = higher score = safer)
    "promoter_pledge":False,"asm_gsm_risk":False,"default_distress":False,
    "accounting_quality":False,"liquidity_manipulation":False,
    "governance_event":False,"gnpa_nnpa_stress":False,"pcr_weakness":False,
    "capital_adequacy_stress":False,"slippages_stress":False,
    "governance_promoter":False,"surveillance_default":False,
    "alm_mismatch":False,
    # Contrarian / Deep Value — ALL True (higher = more attractive)
    "piotroski_f_score":True,"earnings_yield":True,"dividend_yield_score":True,
    "promoter_buying":True,"operating_leverage_score":True,"margin_expansion":True,
}


def validate_metric_direction_map() -> None:
    configured_metrics = {
        metric
        for cards in CARD_WEIGHTS.values()
        for weights in cards.values()
        for metric in weights
    }
    missing = sorted(metric for metric in configured_metrics if metric not in METRIC_DIRECTION)
    if missing:
        raise ValueError(
            "METRIC_DIRECTION missing configured metrics: "
            f"{missing}. Add explicit direction entries before running."
        )

def _get_peer_vals(metric, peer_list):
    return [p.fundamentals.get(metric) for p in peer_list]

def _score_card(card_name, stock, peers, template):
    weights = CARD_WEIGHTS[template.value][card_name]
    sub_scores = {}
    for metric, w in weights.items():
        sv = stock.fundamentals.get(metric)
        pv = _get_peer_vals(metric, peers)
        direction = METRIC_DIRECTION.get(metric, True)
        sub_scores[metric] = score_metric(sv, pv, direction)
    card_score, coverage, is_rankable = weighted_card_score(
        sub_scores, weights, CARD_DATA_THRESHOLD)
    label   = _label(card_name, card_score)
    reason  = _auto_reason(card_name, sub_scores, weights, card_score)
    return CardResult(card_name=card_name, score=card_score, label=label,
                      sub_scores=sub_scores, reason=reason,
                      is_rankable=is_rankable, data_coverage=round(coverage, 3))

def _label(card_name, score):
    if score is None: return "Unrankable"
    for lo, hi, lbl in CARD_LABELS[card_name]:
        if lo <= score < hi or (hi == 100 and score == 100): return lbl
    return None

def _auto_reason(card_name, sub_scores, weights, card_score):
    if card_score is None: return "Insufficient data to score this card."
    scored = {m:(sub_scores[m], weights[m]) for m in sub_scores if sub_scores[m] is not None}
    if not scored: return "No sub-metrics available."
    top = max(scored, key=lambda m: scored[m][0] * scored[m][1])
    top_score = round(scored[top][0], 1)
    direction_word = "strong" if top_score >= 60 else "weak"
    return (f"Primary driver: {top.replace('_',' ').title()} scored {top_score}/100 "
            f"({direction_word} relative to peers).")

def score_performance(stock, peers, template):
    return _score_card("performance", stock, peers, template)

def score_valuation(stock, peers, template):
    return _score_card("valuation", stock, peers, template)

def score_growth(stock, peers, template):
    return _score_card("growth", stock, peers, template)

def score_profitability(stock, peers, template):
    return _score_card("profitability", stock, peers, template)

def score_entry_point(stock, peers, template):
    return _score_card("entry_point", stock, peers, template)

def score_contrarian(stock, peers, template):
    return _score_card("contrarian", stock, peers, template)

def score_red_flags(stock, peers, template):
    """
    Red flags scoring with FULL disqualifier chain from metric_definitions.
    Step 1: Run check_all_disqualifiers() with raw fundamental values.
    Step 2: Score sub-metrics via percentile (same as other cards).
    Step 3: If any disqualifier triggered → cap card score to ≤ 20 (Severe).
    """
    card = _score_card("red_flags", stock, peers, template)
    f = stock.fundamentals

    # ── Raw disqualifier inputs ──
    # These values should be raw levels (not percentile-scored values).
    pledge_pct = f.get("pledge_pct") if f.get("pledge_pct") is not None else f.get("promoter_pledge")
    gnpa_pct = f.get("gnpa_pct") if f.get("gnpa_pct") is not None else f.get("gnpa_nnpa_stress_raw")
    nnpa_pct = f.get("nnpa_pct")
    car_pct = f.get("car_pct") if f.get("car_pct") is not None else f.get("capital_adequacy_stress_raw")
    pcr_pct = f.get("pcr_pct") if f.get("pcr_pct") is not None else f.get("pcr_weakness_raw")
    alm_st_pct = f.get("alm_st_pct") if f.get("alm_st_pct") is not None else f.get("alm_mismatch_raw")
    avg_turnover_cr = f.get("avg_daily_turnover_cr")
    interest_coverage = f.get("interest_coverage")
    credit_rating_grade = f.get("credit_rating_grade")
    gov_events = f.get("governance_events") or []
    if not isinstance(gov_events, list):
        gov_events = []

    asm_stage = int(f.get("asm_stage", 1 if stock.on_asm else 0) or 0)
    gsm_stage = int(f.get("gsm_stage", 1 if stock.on_gsm else 0) or 0)

    is_disq, triggered = check_all_disqualifiers(
        asm_stage   = asm_stage,
        gsm_stage   = gsm_stage,
        interest_coverage = interest_coverage,
        credit_rating_grade = int(credit_rating_grade) if credit_rating_grade is not None else None,
        pledge_pct  = pledge_pct,
        gnpa_pct    = gnpa_pct,
        nnpa_pct    = nnpa_pct,
        car_pct     = car_pct,
        pcr_pct     = pcr_pct,
        alm_st_pct  = alm_st_pct,
        avg_turnover_cr = avg_turnover_cr,
        governance_events = gov_events,
    )

    if is_disq:
        cap_score = 15.0  # <20 → Uninvestable via RED_FLAG_CAPS; 20.0 boundary falls into Avoid
        triggers_str = "; ".join(triggered) if triggered else "ASM/GSM surveillance flag"
        card.score  = cap_score
        card.label  = "Severe"
        card.reason = f"DISQUALIFIER triggered: {triggers_str}"
    return card
