from __future__ import annotations

import re
import zipfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

from engine.metric_definitions import (
    compute_cagr_5y,
    compute_drawdown_recovery,
    compute_price_vs_ma,
    compute_return_1y,
    compute_return_6m,
    compute_rsi_14,
    compute_rsi_score,
    compute_volatility_compression,
)

BHAVCOPY_DIR = Path("data/raw/prices/bhavcopy")

BHAVCOPY_DATE_PATTERNS = (
    re.compile(r"BhavCopy_NSE_CM_0_0_0_(\d{8})_F_0000\.csv\.zip$", re.IGNORECASE),
    re.compile(r"cm(\d{2}[A-Z]{3}\d{4})bhav\.csv\.zip$", re.IGNORECASE),
)

TICKER_ALIASES = ("TckrSymb", "SYMBOL")
SERIES_ALIASES = ("SctySrs", "SERIES")
DATE_ALIASES = ("TradDt", "DATE1")
OPEN_ALIASES = ("OpnPric", "OPEN_PRICE")
HIGH_ALIASES = ("HghPric", "HIGH_PRICE")
LOW_ALIASES = ("LwPric", "LOW_PRICE")
CLOSE_ALIASES = ("ClsPric", "CLOSE_PRICE")
PREV_CLOSE_ALIASES = ("PrvsClsgPric", "PREV_CLOSE")
VOLUME_ALIASES = ("TtlTradgVol", "TOTTRDQTY")
TRADED_VALUE_ALIASES = ("TtlTrfVal", "TOTTRDVAL")


def _norm(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _find_col(columns: Iterable[str], aliases: Iterable[str]) -> Optional[str]:
    normalized = {_norm(col): col for col in columns}
    for alias in aliases:
        hit = normalized.get(_norm(alias))
        if hit is not None:
            return hit
    return None


def _as_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"nan", "na", "none", "-"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_bhavcopy_date(path: Path) -> Optional[date]:
    name = path.name
    for pattern in BHAVCOPY_DATE_PATTERNS:
        match = pattern.search(name)
        if not match:
            continue
        token = match.group(1).upper()
        try:
            if len(token) == 8 and token.isdigit():
                return datetime.strptime(token, "%Y%m%d").date()
            return datetime.strptime(token, "%d%b%Y").date()
        except ValueError:
            return None
    return None


def _read_bhavcopy(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(f"No CSV inside bhavcopy zip: {zip_path}")
        with zf.open(members[0]) as fh:
            return pd.read_csv(fh, dtype=str).fillna("")


def load_local_price_history(
    run_date: date,
    tickers: Optional[Iterable[str]] = None,
    prices_dir: Path = BHAVCOPY_DIR,
    lookback_sessions: int = 1300,
) -> Dict[str, pd.DataFrame]:
    if not prices_dir.exists():
        return {}

    wanted = {str(t).strip().upper() for t in tickers or [] if str(t).strip()}
    has_filter = bool(wanted)

    dated_files = []
    for zip_path in sorted(prices_dir.glob("*.zip")):
        data_date = parse_bhavcopy_date(zip_path)
        if data_date is None or data_date > run_date:
            continue
        dated_files.append((data_date, zip_path))

    if not dated_files:
        return {}

    dated_files.sort(key=lambda item: item[0])
    selected = dated_files[-lookback_sessions:]
    rows_by_ticker: dict[str, list[dict]] = defaultdict(list)

    for _, zip_path in selected:
        df = _read_bhavcopy(zip_path)
        ticker_col = _find_col(df.columns, TICKER_ALIASES)
        series_col = _find_col(df.columns, SERIES_ALIASES)
        date_col = _find_col(df.columns, DATE_ALIASES)
        close_col = _find_col(df.columns, CLOSE_ALIASES)
        if not ticker_col or not date_col or not close_col:
            continue

        open_col = _find_col(df.columns, OPEN_ALIASES)
        high_col = _find_col(df.columns, HIGH_ALIASES)
        low_col = _find_col(df.columns, LOW_ALIASES)
        prev_close_col = _find_col(df.columns, PREV_CLOSE_ALIASES)
        volume_col = _find_col(df.columns, VOLUME_ALIASES)
        traded_value_col = _find_col(df.columns, TRADED_VALUE_ALIASES)

        if series_col:
            df = df[df[series_col].astype(str).str.strip().str.upper() == "EQ"]
        df = df[df[ticker_col].astype(str).str.strip() != ""]

        if has_filter:
            df = df[df[ticker_col].astype(str).str.strip().str.upper().isin(wanted)]

        if df.empty:
            continue

        for row in df.to_dict("records"):
            ticker = str(row.get(ticker_col, "")).strip().upper()
            if not ticker:
                continue
            trade_date_text = str(row.get(date_col, "")).strip()
            try:
                trade_date = datetime.fromisoformat(trade_date_text).date()
            except ValueError:
                continue
            rows_by_ticker[ticker].append(
                {
                    "date": trade_date,
                    "open": _as_float(row.get(open_col)) if open_col else None,
                    "high": _as_float(row.get(high_col)) if high_col else None,
                    "low": _as_float(row.get(low_col)) if low_col else None,
                    "close": _as_float(row.get(close_col)),
                    "prev_close": _as_float(row.get(prev_close_col)) if prev_close_col else None,
                    "volume": _as_float(row.get(volume_col)) if volume_col else None,
                    "traded_value": _as_float(row.get(traded_value_col)) if traded_value_col else None,
                }
            )

    out: Dict[str, pd.DataFrame] = {}
    for ticker, rows in rows_by_ticker.items():
        hist = pd.DataFrame(rows)
        if hist.empty:
            continue
        hist = hist.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        out[ticker] = hist
    return out


def compute_price_metrics_from_history(price_history: Optional[pd.DataFrame]) -> dict[str, float]:
    if price_history is None or price_history.empty:
        return {}

    hist = price_history.copy()
    for col in ["date", "open", "high", "low", "close", "prev_close", "volume", "traded_value"]:
        if col not in hist.columns:
            hist[col] = None
    hist = hist.sort_values("date").reset_index(drop=True)
    closes = [float(v) for v in hist["close"].tolist() if v is not None and pd.notna(v)]
    if not closes:
        return {}

    metrics: dict[str, float] = {
        "close_price": round(float(closes[-1]), 2),
    }

    ret_1y = compute_return_1y(closes[-1], closes[-253]) if len(closes) >= 253 else None
    if ret_1y is not None:
        metrics["return_1y"] = round(ret_1y * 100.0, 2)

    ret_6m = compute_return_6m(closes[-1], closes[-127]) if len(closes) >= 127 else None
    if ret_6m is not None:
        metrics["return_6m"] = round(ret_6m * 100.0, 2)

    cagr_5y = compute_cagr_5y(closes[-1], closes[-1261]) if len(closes) >= 1261 else None
    if cagr_5y is not None:
        metrics["cagr_5y"] = round(cagr_5y * 100.0, 2)

    if len(closes) >= 252:
        highs = [float(v) for v in hist["high"].tail(252).tolist() if v is not None and pd.notna(v)]
        lows = [float(v) for v in hist["low"].tail(252).tolist() if v is not None and pd.notna(v)]
        if highs and lows:
            drawdown = compute_drawdown_recovery(closes[-1], max(highs), min(lows))
            if drawdown is not None:
                metrics["drawdown_recovery"] = round(drawdown, 2)
                metrics["drawdown_normalization"] = round(drawdown, 2)

    rsi_14 = compute_rsi_14(closes)
    rsi_score = compute_rsi_score(rsi_14)
    if rsi_score is not None:
        metrics["rsi_state"] = round(rsi_score, 2)

    ma_50 = sum(closes[-50:]) / 50.0 if len(closes) >= 50 else None
    px_50 = compute_price_vs_ma(closes[-1], ma_50)
    if px_50 is not None:
        metrics["price_vs_50dma"] = round(px_50, 2)

    ma_200 = sum(closes[-200:]) / 200.0 if len(closes) >= 200 else None
    px_200 = compute_price_vs_ma(closes[-1], ma_200)
    if px_200 is not None:
        metrics["price_vs_200dma"] = round(px_200, 2)

    highs_full = [float(v) for v in hist["high"].tolist() if v is not None and pd.notna(v)]
    lows_full = [float(v) for v in hist["low"].tolist() if v is not None and pd.notna(v)]
    if len(highs_full) >= 61 and len(lows_full) >= 61 and len(closes) >= 61:
        vol_comp = compute_volatility_compression(highs_full, lows_full, closes)
        if vol_comp is not None:
            metrics["volatility_compression"] = round(vol_comp, 2)

    volume_series = pd.to_numeric(hist["volume"], errors="coerce").dropna()
    if len(volume_series) >= 60:
        avg_20 = float(volume_series.tail(20).mean())
        avg_60 = float(volume_series.tail(60).mean())
        if avg_60 > 0:
            metrics["volume"] = round(avg_20 / avg_60, 4)

    traded_value_series = pd.to_numeric(hist["traded_value"], errors="coerce").dropna()
    if len(traded_value_series) >= 30:
        metrics["avg_daily_turnover_cr"] = round(float(traded_value_series.tail(30).mean()) / 1e7, 2)

    return metrics
