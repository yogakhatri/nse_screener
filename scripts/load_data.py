import csv
import re
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

sys.path.insert(0, ".")

from engine import RawStockData, NSEClassification
from engine.config import (
    CARD_WEIGHTS,
    CARD_DATA_THRESHOLD,
    ENABLE_RAW_PRICE_METRICS,
    PRICE_HISTORY_LOOKBACK_SESSIONS,
    RAW_PRICE_METRIC_FALLBACK_TO_CSV,
    infer_template_code,
    validate_runtime_config,
)
from engine.metric_definitions import (
    compute_asm_gsm_risk,
    compute_default_distress,
    compute_fair_pb,
    compute_fair_value_gap,
    compute_forward_view,
    compute_gnpa_nnpa_stress,
    compute_iv_general,
    compute_iv_gap,
    compute_liquidity_risk,
    compute_car_stress,
    compute_pcr_weakness,
    compute_alm_mismatch,
    compute_promoter_pledge_risk,
    compute_governance_risk,
    m_score_to_raw_risk,
    compute_piotroski_f_score,
    compute_earnings_yield,
    compute_dividend_yield_score,
    compute_promoter_buying,
    compute_operating_leverage_score,
    compute_margin_expansion,
    LOCKED_COE_BANK,
    LOCKED_COE_NBFC,
)
from scripts.price_history import compute_price_metrics_from_history, load_local_price_history

METRIC_KEYS = sorted(
    {
        metric
        for tpl in CARD_WEIGHTS.values()
        for card in tpl.values()
        for metric in card
    }
)

DIRECT_ALIASES = {
    "return_1y": ["1 Year Return", "Return 1yr", "1Y Return", "Price Return 1Y"],
    "return_6m": ["6 Month Return", "Return 6m", "6M Return", "Price Return 6M"],
    "cagr_5y": ["5 Year CAGR", "Price CAGR 5Y", "Stock CAGR 5Years"],
    "peer_price_strength": ["Relative Strength", "Peer Price Strength"],
    "drawdown_recovery": ["Drawdown Recovery", "52W Recovery", "Price from 52W Low"],
    "forward_view": ["Forward Growth", "Consensus Growth", "EPS Growth Next Year"],
    "pe_percentile": ["P/E", "PE", "Price to Earning"],
    "pb_percentile": ["Price to Book value", "P/B", "PB", "Price to Book"],
    "p_cfo_percentile": ["Price to Cash Flow", "P/CFO", "Price/CFO"],
    "ev_ebitda_percentile": ["EV / EBITDA", "EV/EBITDA", "EV EBITDA"],
    "hist_val_band": ["Historical PE Band", "PE vs 5Y Median", "Hist Val Band"],
    "fcf_yield": ["FCF Yield", "Free Cash Flow Yield"],
    "iv_gap": ["IV Gap", "Intrinsic Value Gap", "Discount to IV"],
    "roe_adj_pb": ["ROE Adjusted PB", "PB/ROE", "ROE Adjusted P/B"],
    "hist_pb_band": ["Historical PB Band", "PB vs 5Y Median", "Hist PB Band"],
    "fair_value_gap": ["Fair Value Gap", "Discount to Fair PB", "Fair PB Gap"],
    "rev_cagr_3y": ["Sales growth 3Years", "Revenue CAGR 3Y", "Sales CAGR 3Y"],
    "eps_cagr_3y": ["Profit growth 3Years", "EPS CAGR 3Y", "PAT CAGR 3Y"],
    "rev_growth_yoy": ["Sales growth", "Revenue Growth YoY", "Sales Growth YoY"],
    "eps_growth_yoy": ["Profit growth", "EPS Growth YoY", "PAT Growth YoY"],
    "peer_growth_rank": ["Peer Growth Rank", "Relative Growth Rank"],
    "growth_stability": ["Growth Stability", "Revenue Stability", "Growth Consistency"],
    "advances_growth": ["Advances Growth", "Loan Book Growth"],
    "deposit_growth": ["Deposit Growth"],
    "nii_growth": ["NII Growth", "Net Interest Income Growth"],
    "fee_income_growth": ["Fee Income Growth", "Non Interest Income Growth"],
    "earnings_growth": ["Earnings Growth", "PAT Growth YoY"],
    "aum_growth": ["AUM Growth"],
    "roce_3y_median": ["ROCE 3Years", "ROCE 3Y", "Median ROCE 3Y"],
    "ebitda_margin": ["OPM", "EBITDA Margin", "Operating Margin"],
    "cfo_pat_ratio": ["CFO/PAT", "Cash Conversion", "CFO PAT Ratio"],
    "margin_trend": ["Margin Trend", "EBITDA Margin Trend"],
    "roa": ["ROA", "Return on Assets"],
    "fcf_consistency": ["FCF Consistency", "Positive FCF %"],
    "roe": ["ROE", "Return on Equity"],
    "nim": ["NIM", "Net Interest Margin"],
    "cost_to_income": ["Cost to Income", "Cost/Income", "Cost-to-Income"],
    "provision_coverage": ["Provision Coverage", "PCR", "PCR %"],
    "credit_cost_discipline": ["Credit Cost", "Credit Cost Ratio"],
    "discount_to_iv": ["Discount to IV", "IV Gap"],
    "rsi_state": ["RSI", "RSI 14", "RSI(14)"],
    "price_vs_200dma": ["Price vs 200 DMA", "Price vs 200DMA", "Distance from 200 DMA"],
    "price_vs_50dma": ["Price vs 50 DMA", "Price vs 50DMA", "Distance from 50 DMA"],
    "volume_delivery": ["Delivery Score", "Delivery Ratio", "Volume Delivery Score"],
    "rs_turn": ["RS Turn", "Relative Strength Turn"],
    "volatility_compression": ["Volatility Compression", "ATR Compression"],
    "discount_to_fair_pb": ["Discount to Fair PB", "Fair PB Gap"],
    "volume": ["Volume Score", "Volume Confirmation"],
    "drawdown_normalization": ["Drawdown Normalization", "Drawdown Recovery"],
    "promoter_pledge": ["Pledged percentage", "Promoter Pledge", "Promoter Pledge %"],
    "accounting_quality": ["Accounting Quality Risk", "Beneish Risk"],
    "liquidity_manipulation": ["Liquidity Risk", "Manipulation Risk"],
    "governance_event": ["Governance Risk", "Governance Event Risk"],
    "slippages_stress": ["Slippage Ratio", "Slippages Stress"],
    "governance_promoter": ["Governance Promoter Risk", "Promoter Governance Risk"],
    "surveillance_default": ["Surveillance Default Risk"],
    # Contrarian / Deep Value — CSV pass-through if scraper pre-computed them
    "piotroski_f_score":       ["Piotroski F-Score", "Piotroski Score", "F-Score"],
    "earnings_yield":          ["Earnings Yield Score", "Earnings Yield"],
    "dividend_yield_score":    ["Dividend Yield Score"],
    "promoter_buying":         ["Promoter Buying Signal", "Promoter Buying"],
    "operating_leverage_score":["Operating Leverage Score", "Op Leverage Score"],
    "margin_expansion":        ["Margin Expansion Score", "Margin Expansion"],
}

RAW_ALIASES = {
    "close_price": ["Current Price", "CMP", "Close Price", "Price"],
    "intrinsic_value": ["Intrinsic Value", "Estimated Intrinsic Value", "Fair Value"],
    "pledge_pct": ["Pledged percentage", "Promoter Pledge", "Promoter Pledge %"],
    "book_value_per_share": ["Book Value Per Share", "Book Value", "BVPS"],
    "eps_ttm": ["EPS TTM", "EPS (TTM)", "TTM EPS"],
    "eps_fy0": ["EPS FY0", "EPS Latest FY", "EPS FY"],
    "eps_fy1": ["EPS FY1", "EPS FY-1"],
    "eps_fy2": ["EPS FY2", "EPS FY-2"],
    "debt_to_equity": ["Debt to equity", "Debt/Equity", "D/E"],
    "interest_coverage": ["Interest Coverage", "Interest coverage ratio", "ICR"],
    "credit_rating_grade": ["Credit Rating Grade", "Rating Grade"],
    "gnpa_pct": ["GNPA %", "Gross NPA %", "GNPA"],
    "nnpa_pct": ["NNPA %", "Net NPA %", "NNPA"],
    "car_pct": ["CAR %", "CRAR %", "Capital Adequacy"],
    "pcr_pct": ["PCR %", "Provision Coverage Ratio"],
    "alm_st_pct": ["ALM ST %", "Short Term Borrowings %", "ALM Mismatch %"],
    "avg_daily_turnover_cr": ["Avg Daily Turnover Cr", "Turnover 30D Cr", "Daily Turnover Cr"],
    "asm_stage": ["ASM Stage", "ASM"],
    "gsm_stage": ["GSM Stage", "GSM"],
    "is_t2t": ["T2T", "Trade to Trade", "Is T2T"],
    "governance_events": ["Governance Events", "Governance Flags"],
    "beneish_m_score": ["Beneish M Score", "Beneish M-Score"],
    "roe_ttm": ["ROE", "ROE TTM"],
    # Deep Value / Contrarian raw inputs (populated by scraper)
    "current_ratio":          ["Current Ratio", "CR"],
    "current_ratio_fy1":      ["Current Ratio Prev Year", "Current Ratio FY1"],
    "dividend_yield":         ["Dividend Yield", "Div Yield %", "Yield %"],
    "promoter_holding_pct":   ["Promoter Holding %", "Promoter %", "Promoter Holdings"],
    "promoter_holding_prev":  ["Promoter Holding Prev %", "Promoter Prev Quarter %"],
    "dii_holding_pct":        ["DII %", "DII Holding %", "DII Holdings"],
    "gross_block_fy0":        ["Gross Block", "Fixed Assets Gross", "Gross Block FY0"],
    "gross_block_fy3":        ["Gross Block 3Y Ago", "Gross Block FY3"],
    "asset_turnover":         ["Asset Turnover", "Assets Turnover"],
    "asset_turnover_fy1":     ["Asset Turnover Prev Year", "Asset Turnover FY1"],
    "opm_fy1":                ["OPM FY1", "OPM Prev Year", "Operating Margin FY1"],
    "opm_fy2":                ["OPM FY2", "OPM 2Y Ago", "Operating Margin FY2"],
    "roa_fy0":                ["ROA", "Return on Assets"],
    "roa_fy1":                ["ROA Prev Year", "ROA FY1"],
    "de_fy1":                 ["D/E Prev Year", "Debt to Equity FY1", "D/E FY1"],
    "cfo_annual":             ["Cash from Operations", "CFO Annual", "Operating Cash Flow Annual"],
}

CLASSIFICATION_ALIASES = {
    "macro_sector": ["Macro Sector", "Macro"],
    "sector": ["Sector"],
    "industry": ["Industry"],
    "basic_industry": ["Basic Industry", "BasicIndustry"],
}

TICKER_ALIASES = ["NSE Symbol", "Symbol", "Ticker"]
NAME_ALIASES = ["Name", "Company Name"]
DERIVED_METRICS = {
    "iv_gap",
    "discount_to_iv",
    "fair_value_gap",
    "discount_to_fair_pb",
    "default_distress",
    "asm_gsm_risk",
    "gnpa_nnpa_stress",
    "capital_adequacy_stress",
    "pcr_weakness",
    "alm_mismatch",
    "liquidity_manipulation",
    "governance_event",
    "governance_promoter",
    "surveillance_default",
    "accounting_quality",
    # Contrarian / Deep Value — computed in _fill_derived_metrics
    "piotroski_f_score",
    "earnings_yield",
    "dividend_yield_score",
    "promoter_buying",
    "operating_leverage_score",
    "margin_expansion",
}
RAW_PRICE_METRICS = {
    "close_price",
    "return_1y",
    "return_6m",
    "cagr_5y",
    "drawdown_recovery",
    "drawdown_normalization",
    "rsi_state",
    "price_vs_200dma",
    "price_vs_50dma",
    "volatility_compression",
    "volume",
    "avg_daily_turnover_cr",
}

FUND_LIKE_NAME_PATTERN = re.compile(
    r"\b(etf|etn|bees|index fund|liquid etf|gold etf|silver etf|exchange traded fund)\b",
    re.IGNORECASE,
)


def _norm(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _is_fund_like_security(name: str) -> bool:
    return bool(FUND_LIKE_NAME_PATTERN.search(str(name or "").strip()))


def _first_present(row: pd.Series, aliases: Iterable[str]) -> Optional[object]:
    normalized = {_norm(c): c for c in row.index}
    for alias in aliases:
        for candidate in (alias, f"fund__{alias}"):
            key = normalized.get(_norm(candidate))
            if key is None:
                continue
            value = row.get(key)
            if pd.isna(value):
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return None


def _as_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        text = str(value).strip()
        if not text or text.lower() in {"na", "nan", "none", "-"}:
            return None
        text = text.replace(",", "").replace("%", "")
        return float(text)
    except Exception:
        return None


def _as_bool(value: object) -> bool:
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def _as_events(value: object) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    if ";" in text:
        items = text.split(";")
    elif "," in text:
        items = text.split(",")
    else:
        items = [text]
    return [i.strip() for i in items if i.strip()]


def _get_text(row: pd.Series, aliases: Iterable[str], default: str) -> str:
    value = _first_present(row, aliases)
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _build_raw_inputs(row: pd.Series) -> dict:
    raw = {}
    for key, aliases in RAW_ALIASES.items():
        value = _first_present(row, aliases)
        if key in {"is_t2t"}:
            raw[key] = _as_bool(value)
        elif key in {"governance_events"}:
            raw[key] = _as_events(value)
        elif key in {"asm_stage", "gsm_stage"}:
            numeric = _as_float(value)
            raw[key] = int(numeric) if numeric is not None else 0
        else:
            raw[key] = _as_float(value)
    return raw


def _init_fundamentals() -> dict:
    return {metric: None for metric in METRIC_KEYS}


def _fill_direct_metrics(row: pd.Series, fundamentals: dict) -> None:
    for metric, aliases in DIRECT_ALIASES.items():
        if metric not in fundamentals:
            continue
        value = _as_float(_first_present(row, aliases))
        if value is not None:
            fundamentals[metric] = value


def _apply_price_history_metrics(
    fundamentals: dict,
    raw: dict,
    price_history: Optional[pd.DataFrame],
) -> None:
    computed = compute_price_metrics_from_history(price_history)
    if not computed:
        if not RAW_PRICE_METRIC_FALLBACK_TO_CSV:
            for metric in RAW_PRICE_METRICS:
                fundamentals[metric] = None
        return
    for metric, value in computed.items():
        if value is None:
            continue
        fundamentals[metric] = value
        if metric == "close_price":
            raw["close_price"] = value
        if metric == "avg_daily_turnover_cr":
            raw["avg_daily_turnover_cr"] = value
    if not RAW_PRICE_METRIC_FALLBACK_TO_CSV:
        for metric in RAW_PRICE_METRICS - set(computed):
            fundamentals[metric] = None


def _fill_derived_metrics(fundamentals: dict, raw: dict, template_hint: str) -> tuple[bool, bool]:
    close_price = raw.get("close_price")
    pledge_pct = raw.get("pledge_pct")
    if pledge_pct is not None:
        fundamentals["promoter_pledge"] = compute_promoter_pledge_risk(pledge_pct)

    intrinsic_value = None
    if template_hint == "A":
        intrinsic_value = compute_iv_general(
            eps_fy0=raw.get("eps_fy0"),
            eps_fy1=raw.get("eps_fy1"),
            eps_fy2=raw.get("eps_fy2"),
            eps_ttm=raw.get("eps_ttm"),
            bvps=raw.get("book_value_per_share"),
        )
    if intrinsic_value is None:
        intrinsic_value = raw.get("intrinsic_value")
    if fundamentals.get("iv_gap") is None and close_price is not None and intrinsic_value is not None:
        fundamentals["iv_gap"] = compute_iv_gap(close_price, intrinsic_value)
    if fundamentals.get("discount_to_iv") is None and fundamentals.get("iv_gap") is not None:
        fundamentals["discount_to_iv"] = fundamentals["iv_gap"]

    if fundamentals.get("forward_view") is None:
        eps_forward_growth = None
        eps_fy0 = raw.get("eps_fy0")
        eps_fy1 = raw.get("eps_fy1")
        eps_ttm = raw.get("eps_ttm")
        if eps_fy1 is not None and eps_fy0 is not None and eps_fy0 != 0:
            eps_forward_growth = ((eps_fy1 / eps_fy0) - 1.0) * 100.0
        elif eps_fy0 is not None and eps_ttm is not None and eps_ttm != 0:
            eps_forward_growth = ((eps_fy0 / eps_ttm) - 1.0) * 100.0
        rev_forward_growth = fundamentals.get("earnings_growth")
        if rev_forward_growth is None:
            rev_forward_growth = fundamentals.get("eps_growth_yoy")
        forward_view = compute_forward_view(eps_forward_growth, rev_forward_growth)
        if forward_view is not None:
            fundamentals["forward_view"] = round(forward_view, 2)

    roe_ttm = raw.get("roe_ttm")
    pb_now = fundamentals.get("pb_percentile")
    if pb_now is not None and roe_ttm is not None and roe_ttm > 0:
        if fundamentals.get("roe_adj_pb") is None:
            fundamentals["roe_adj_pb"] = pb_now / roe_ttm
    if template_hint in {"B", "C"} and pb_now is not None and roe_ttm is not None and roe_ttm > 0:
        coe = LOCKED_COE_NBFC if template_hint == "C" else LOCKED_COE_BANK
        fair_pb = compute_fair_pb(roe_ttm, coe=coe)
        fair_gap = compute_fair_value_gap(pb_now, fair_pb)
        if fundamentals.get("fair_value_gap") is None:
            fundamentals["fair_value_gap"] = fair_gap
        if fundamentals.get("discount_to_fair_pb") is None:
            fundamentals["discount_to_fair_pb"] = fair_gap

    distress_risk, _ = compute_default_distress(
        debt_to_equity=raw.get("debt_to_equity"),
        interest_coverage_ttm=raw.get("interest_coverage"),
        credit_rating_grade=int(raw["credit_rating_grade"]) if raw.get("credit_rating_grade") is not None else None,
    )
    if fundamentals.get("default_distress") is None:
        fundamentals["default_distress"] = distress_risk

    asm_risk, _ = compute_asm_gsm_risk(raw.get("asm_stage", 0), raw.get("gsm_stage", 0))
    if fundamentals.get("asm_gsm_risk") is None:
        fundamentals["asm_gsm_risk"] = asm_risk

    gnpa_risk, _ = compute_gnpa_nnpa_stress(raw.get("gnpa_pct"), raw.get("nnpa_pct"))
    if fundamentals.get("gnpa_nnpa_stress") is None:
        fundamentals["gnpa_nnpa_stress"] = gnpa_risk

    car_risk, _ = compute_car_stress(raw.get("car_pct"))
    if fundamentals.get("capital_adequacy_stress") is None:
        fundamentals["capital_adequacy_stress"] = car_risk

    pcr_risk, _ = compute_pcr_weakness(raw.get("pcr_pct"))
    if fundamentals.get("pcr_weakness") is None:
        fundamentals["pcr_weakness"] = pcr_risk

    alm_risk, _ = compute_alm_mismatch(raw.get("alm_st_pct"))
    if fundamentals.get("alm_mismatch") is None:
        fundamentals["alm_mismatch"] = alm_risk

    liq_risk, _ = compute_liquidity_risk(
        avg_daily_turnover_cr=raw.get("avg_daily_turnover_cr"),
        is_t2t=raw.get("is_t2t", False),
    )
    if fundamentals.get("liquidity_manipulation") is None:
        fundamentals["liquidity_manipulation"] = liq_risk

    gov_risk, _ = compute_governance_risk(raw.get("governance_events", []))
    if fundamentals.get("governance_event") is None:
        fundamentals["governance_event"] = gov_risk
    if fundamentals.get("governance_promoter") is None:
        fundamentals["governance_promoter"] = gov_risk
    if fundamentals.get("surveillance_default") is None:
        fundamentals["surveillance_default"] = max(asm_risk, distress_risk)

    m_score = raw.get("beneish_m_score")
    if fundamentals.get("accounting_quality") is None and m_score is not None:
        acc_risk, _ = m_score_to_raw_risk(m_score)
        fundamentals["accounting_quality"] = acc_risk

    # ── Contrarian / Deep Value metrics ──────────────────────────────────────
    if fundamentals.get("piotroski_f_score") is None:
        fundamentals["piotroski_f_score"] = compute_piotroski_f_score(
            roa_fy0=fundamentals.get("roa"),
            cfo=raw.get("cfo_annual"),
            roa_fy1=raw.get("roa_fy1"),
            cfo_pat_ratio=fundamentals.get("cfo_pat_ratio"),
            debt_to_equity_fy0=raw.get("debt_to_equity"),
            debt_to_equity_fy1=raw.get("de_fy1"),
            current_ratio_fy0=raw.get("current_ratio"),
            current_ratio_fy1=raw.get("current_ratio_fy1"),
            gross_margin_fy0=fundamentals.get("ebitda_margin"),
            gross_margin_fy1=raw.get("opm_fy1"),
            asset_turnover_fy0=raw.get("asset_turnover"),
            asset_turnover_fy1=raw.get("asset_turnover_fy1"),
        )

    if fundamentals.get("earnings_yield") is None:
        fundamentals["earnings_yield"] = compute_earnings_yield(
            pe_ratio=fundamentals.get("pe_percentile"),
        )

    if fundamentals.get("dividend_yield_score") is None:
        fundamentals["dividend_yield_score"] = compute_dividend_yield_score(
            dividend_yield_pct=raw.get("dividend_yield"),
        )

    if fundamentals.get("promoter_buying") is None:
        fundamentals["promoter_buying"] = compute_promoter_buying(
            promoter_holding_pct=raw.get("promoter_holding_pct"),
            promoter_holding_prev_pct=raw.get("promoter_holding_prev"),
        )

    if fundamentals.get("operating_leverage_score") is None:
        fundamentals["operating_leverage_score"] = compute_operating_leverage_score(
            gross_block_fy0=raw.get("gross_block_fy0"),
            gross_block_fy3=raw.get("gross_block_fy3"),
            asset_turnover=raw.get("asset_turnover"),
        )

    if fundamentals.get("margin_expansion") is None:
        fundamentals["margin_expansion"] = compute_margin_expansion(
            opm_fy0=fundamentals.get("ebitda_margin"),
            opm_fy1=raw.get("opm_fy1"),
            opm_fy2=raw.get("opm_fy2"),
            rev_growth_yoy=fundamentals.get("rev_growth_yoy"),
        )

    on_asm = raw.get("asm_stage", 0) > 0
    on_gsm = raw.get("gsm_stage", 0) > 0
    return on_asm, on_gsm


def _template_hint(classification: NSEClassification) -> str:
    return infer_template_code(
        macro_sector=classification.macro_sector,
        sector=classification.sector,
        industry=classification.industry,
        basic_industry=classification.basic_industry,
    )


def validate_loader_support() -> None:
    validate_runtime_config()
    configured_metrics = {
        metric
        for cards in CARD_WEIGHTS.values()
        for weights in cards.values()
        for metric in weights
    }
    supported_metrics = set(DIRECT_ALIASES) | DERIVED_METRICS
    missing = sorted(metric for metric in configured_metrics if metric not in supported_metrics)
    if missing:
        raise ValueError(
            "Loader aliases/derivations missing configured metrics: "
            f"{missing}. Add aliases or derivation support before running."
        )


def load_from_screener(
    csv_path: str,
    run_date: Optional[date] = None,
    price_history_map: Optional[Dict[str, pd.DataFrame]] = None,
) -> dict:
    """
    Read a Screener export CSV and return {ticker: RawStockData}.
    Supports multiple alias headers and derives missing risk metrics when possible.
    """
    validate_loader_support()
    df = pd.read_csv(csv_path)
    universe: Dict[str, RawStockData] = {}
    if ENABLE_RAW_PRICE_METRICS:
        if price_history_map is None and run_date is not None:
            tickers = {
                _get_text(row, TICKER_ALIASES, "").upper()
                for _, row in df.iterrows()
                if _get_text(row, TICKER_ALIASES, "")
            }
            price_history_map = load_local_price_history(
                run_date=run_date,
                tickers=tickers,
                lookback_sessions=PRICE_HISTORY_LOOKBACK_SESSIONS,
            )
    price_history_map = price_history_map or {}

    for _, row in df.iterrows():
        ticker = _get_text(row, TICKER_ALIASES, "").upper()
        if not ticker:
            continue
        name = _get_text(row, NAME_ALIASES, ticker)
        if _is_fund_like_security(name):
            continue

        industry = _get_text(row, CLASSIFICATION_ALIASES["industry"], "Diversified")
        basic_industry = _get_text(row, CLASSIFICATION_ALIASES["basic_industry"], "")
        if not basic_industry or basic_industry.strip().lower() == "diversified":
            basic_industry = industry

        classification = NSEClassification(
            macro_sector=_get_text(row, CLASSIFICATION_ALIASES["macro_sector"], "Diversified"),
            sector=_get_text(row, CLASSIFICATION_ALIASES["sector"], "Diversified"),
            industry=industry,
            basic_industry=basic_industry,
        )

        fundamentals = _init_fundamentals()
        _fill_direct_metrics(row, fundamentals)
        raw = _build_raw_inputs(row)
        price_history = price_history_map.get(ticker)
        if ENABLE_RAW_PRICE_METRICS:
            _apply_price_history_metrics(fundamentals, raw, price_history)

        # Persist raw disqualifier inputs for strict red-flag checks.
        fundamentals["gnpa_pct"] = raw.get("gnpa_pct")
        fundamentals["nnpa_pct"] = raw.get("nnpa_pct")
        fundamentals["car_pct"] = raw.get("car_pct")
        fundamentals["pcr_pct"] = raw.get("pcr_pct")
        fundamentals["pledge_pct"] = raw.get("pledge_pct")
        fundamentals["alm_st_pct"] = raw.get("alm_st_pct")
        fundamentals["avg_daily_turnover_cr"] = raw.get("avg_daily_turnover_cr")
        fundamentals["interest_coverage"] = raw.get("interest_coverage")
        fundamentals["credit_rating_grade"] = raw.get("credit_rating_grade")
        fundamentals["governance_events"] = raw.get("governance_events")
        fundamentals["close_price"] = raw.get("close_price")
        fundamentals["asm_stage"] = raw.get("asm_stage")
        fundamentals["gsm_stage"] = raw.get("gsm_stage")

        on_asm, on_gsm = _fill_derived_metrics(
            fundamentals=fundamentals,
            raw=raw,
            template_hint=_template_hint(classification),
        )

        universe[ticker] = RawStockData(
            ticker=ticker,
            name=name,
            classification=classification,
            price_history=price_history,
            fundamentals=fundamentals,
            on_asm=on_asm,
            on_gsm=on_gsm,
        )
    return universe


def metric_coverage(universe: dict) -> dict:
    """
    Coverage report by template/card for quick run diagnostics.
    """
    report: dict = {}
    by_template: Dict[str, list] = {template: [] for template in CARD_WEIGHTS}
    for stock in universe.values():
        template = infer_template_code(
            macro_sector=stock.classification.macro_sector,
            sector=stock.classification.sector,
            industry=stock.classification.industry,
            basic_industry=stock.classification.basic_industry,
        )
        by_template.setdefault(template, []).append(stock)

    for template, cards in CARD_WEIGHTS.items():
        report[template] = {}
        stocks_for_template = by_template.get(template, [])
        for card, weights in cards.items():
            covered = []
            total_weight = sum(weights.values())
            for stock in stocks_for_template:
                seen = 0.0
                for metric, weight in weights.items():
                    if stock.fundamentals.get(metric) is not None:
                        seen += weight
                covered.append(round(seen / total_weight, 3) if total_weight else 0.0)
            n_stocks = len(covered)
            n_rankable = sum(1 for c in covered if c >= CARD_DATA_THRESHOLD)
            avg_cov = round(sum(covered) / n_stocks, 3) if covered else 0.0
            rankable_rate = round((n_rankable / n_stocks) * 100.0, 2) if covered else 0.0
            report[template][card] = {
                "avg_coverage": avg_cov,
                "rankable_pct": rankable_rate,
                "n_stocks": n_stocks,
                "n_rankable": n_rankable,
            }
    return report


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else (
        "data/raw/fundamentals/screener/screener_export_2026-03-10.csv"
    )
    from engine import NSERatingEngine
    import datetime as dt
    import json
    import os

    universe = load_from_screener(path)
    print(f"Loaded {len(universe)} stocks from {path}")
    print("Coverage snapshot:")
    print(json.dumps(metric_coverage(universe), indent=2))

    engine = NSERatingEngine(universe)
    ratings = engine.rate_universe()
    lb = engine.to_leaderboard(ratings)

    today = dt.date.today().isoformat()
    os.makedirs(f"runs/{today}", exist_ok=True)

    with open(f"runs/{today}/leaderboard.csv", "w", newline="") as f:
        if lb:
            w = csv.DictWriter(f, fieldnames=lb[0].keys())
            w.writeheader()
            w.writerows(lb)

    for ticker, rating in ratings.items():
        with open(f"runs/{today}/stock_{ticker}.json", "w") as f:
            json.dump(rating.to_dict(), f, indent=2)

    print("\nTop 10:")
    for i, row in enumerate(lb[:10], 1):
        print(
            f"  #{i} {row['ticker']:<12} Score: {row['opportunity_score']:.1f}  "
            f"{row['recommendation']} ({row['confidence']})"
        )
