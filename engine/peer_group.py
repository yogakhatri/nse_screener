"""
NSE Rating Engine – Peer Group Resolver
Implements the 3-tier fallback: Basic Industry → Industry → Sector
"""
from __future__ import annotations
from typing import Dict, List, Tuple
from .models import NSEClassification, PeerLevel
from .config import PEER_MIN_BASIC_INDUSTRY, PEER_MIN_INDUSTRY

def resolve_peer_group(
    target_ticker: str,
    all_stocks: Dict[str, NSEClassification],
) -> Tuple[List[str], PeerLevel]:
    """
    Returns (peer_ticker_list_excluding_self, PeerLevel used).
    Implements the fallback cascade:
      Basic Industry (≥8 eligible) → Industry (≥5) → Sector (no floor).
    Macro-Economic Sector is never used.
    """
    target_cls = all_stocks[target_ticker]

    def _collect(level: str) -> List[str]:
        target_val = getattr(target_cls, level.lower().replace(" ", "_"))
        return [
            t for t, cls in all_stocks.items()
            if t != target_ticker
            and getattr(cls, level.lower().replace(" ", "_")) == target_val
        ]

    bi_peers = _collect("basic_industry")
    if len(bi_peers) + 1 >= PEER_MIN_BASIC_INDUSTRY:      # +1 for the stock itself
        return bi_peers, PeerLevel.BASIC_INDUSTRY

    ind_peers = _collect("industry")
    if len(ind_peers) + 1 >= PEER_MIN_INDUSTRY:
        return ind_peers, PeerLevel.INDUSTRY

    sec_peers = _collect("sector")
    return sec_peers, PeerLevel.SECTOR

