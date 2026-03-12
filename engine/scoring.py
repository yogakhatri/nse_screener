"""
NSE Rating Engine – Normalization & Percentile Scoring
Winsorizes at peer-group 5th/95th percentile, then converts to 0-100 rank score.
"""
from __future__ import annotations
import numpy as np
from typing import List, Optional
from .config import WINSORIZE_LOWER, WINSORIZE_UPPER

def winsorize_peer(values: np.ndarray) -> np.ndarray:
    """Cap values at 5th and 95th percentile of the peer distribution."""
    lo = np.nanpercentile(values, WINSORIZE_LOWER * 100)
    hi = np.nanpercentile(values, WINSORIZE_UPPER * 100)
    return np.clip(values, lo, hi)

def percentile_score(stock_value: float, peer_values: np.ndarray,
                     higher_is_better: bool = True) -> float:
    """
    Return a 0-100 percentile score for stock_value within peer_values.
    All peer_values must already be winsorized.
    higher_is_better=False inverts (e.g., PE, debt ratios).
    """
    peer_clean = peer_values[~np.isnan(peer_values)]
    if len(peer_clean) == 0:
        return 50.0  # No peer data → neutral score
    if higher_is_better:
        pct = float(np.mean(peer_clean <= stock_value) * 100)
    else:
        pct = float(np.mean(peer_clean >= stock_value) * 100)
    return round(min(max(pct, 0.0), 100.0), 2)

def score_metric(
    stock_value: Optional[float],
    peer_series: List[Optional[float]],
    higher_is_better: bool = True,
) -> Optional[float]:
    """
    Full pipeline: handle None, build array, winsorize, percentile-score.
    Returns None if stock_value is missing.
    """
    if stock_value is None:
        return None
    arr = np.array([v for v in peer_series if v is not None], dtype=float)
    if len(arr) == 0:
        return None
    arr_w = winsorize_peer(np.append(arr, stock_value))
    stock_w = arr_w[-1]
    peers_w = arr_w[:-1]
    return percentile_score(stock_w, peers_w, higher_is_better)

def weighted_card_score(
    sub_scores: dict[str, Optional[float]],
    weights: dict[str, float],
    threshold: float,
) -> tuple[Optional[float], float, bool]:
    """
    Aggregate sub-scores into a card score.
    Returns (card_score | None, data_coverage, is_rankable).
    """
    total_weight_available = 0.0
    total_weight_with_data = 0.0
    weighted_sum = 0.0

    for metric, weight in weights.items():
        total_weight_available += weight
        val = sub_scores.get(metric)
        if val is not None:
            total_weight_with_data += weight
            weighted_sum += val * weight

    coverage = total_weight_with_data / total_weight_available if total_weight_available > 0 else 0.0
    is_rankable = coverage >= threshold

    if not is_rankable or total_weight_with_data == 0:
        return None, coverage, False

    # Re-normalize weights to available data only
    normalized_score = weighted_sum / total_weight_with_data * 100 / 100
    # weighted_sum is already in 0-100 range since sub_scores are 0-100
    card_score = round(weighted_sum / total_weight_with_data, 2)
    return card_score, coverage, True

