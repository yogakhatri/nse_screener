"""
NSE Rating Engine – Data Models
All dataclasses, enums, and type contracts used across the engine.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum

class Template(str, Enum):
    GENERAL = "A"
    BANK    = "B"
    NBFC    = "C"

class PeerLevel(str, Enum):
    BASIC_INDUSTRY = "Basic Industry"
    INDUSTRY       = "Industry"
    SECTOR         = "Sector"

@dataclass
class NSEClassification:
    macro_sector:   str
    sector:         str
    industry:       str
    basic_industry: str

@dataclass
class CardResult:
    card_name:    str
    score:        Optional[float]   # None if Unrankable
    label:        Optional[str]
    sub_scores:   Dict[str, Optional[float]] = field(default_factory=dict)
    reason:       str = ""
    is_rankable:  bool = True
    data_coverage: float = 1.0     # fraction of weighted sub-metrics with data

@dataclass
class StockRating:
    # Identity
    ticker:         str
    name:           str
    classification: NSEClassification
    template:       Template
    peer_group:     List[str]       # tickers of peers
    peer_level:     PeerLevel
    n_peers:        int

    # Card results
    performance:   CardResult = field(default_factory=lambda: CardResult("performance", None, None))
    valuation:     CardResult = field(default_factory=lambda: CardResult("valuation", None, None))
    growth:        CardResult = field(default_factory=lambda: CardResult("growth", None, None))
    profitability: CardResult = field(default_factory=lambda: CardResult("profitability", None, None))
    entry_point:   CardResult = field(default_factory=lambda: CardResult("entry_point", None, None))
    red_flags:     CardResult = field(default_factory=lambda: CardResult("red_flags", None, None))

    # Final composite
    opportunity_score:    Optional[float] = None
    investability_status: str = "Insufficient Data"
    is_eligible:          bool = True   # False if < 4 cards rankable
    strengths:            List[str] = field(default_factory=list)
    weaknesses:           List[str] = field(default_factory=list)
    potential_score:      Optional[float] = None
    valuation_gap_score:  Optional[float] = None
    recommendation:       str = "Undetermined"
    recommendation_confidence: str = "Low"
    entry_signal:         str = "Unknown"
    market_mode:          str = "auto"
    sector_regime_score:  Optional[float] = None
    sector_regime_label:  str = "Unknown"
    drawdown_resilience_score: Optional[float] = None
    valuation_confidence_score: Optional[float] = None
    expected_upside_pct:  Optional[float] = None
    expected_downside_pct: Optional[float] = None
    risk_reward_ratio:    Optional[float] = None
    risk_reward_score:    Optional[float] = None
    selection_score:      Optional[float] = None
    investability_gate_passed: bool = False
    gate_fail_reasons:    List[str] = field(default_factory=list)
    staged_entry_plan:    str = "Not available"
    action_note:          str = ""
    sector_rank:          Optional[int] = None
    sector_percentile:    Optional[float] = None
    basic_industry_rank:  Optional[int] = None
    basic_industry_percentile: Optional[float] = None
    template_supported:   bool = True
    template_support_status: str = "Supported"
    template_support_reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        from .output import to_dict
        return to_dict(self)

    def to_json(self, indent: int = 2) -> str:
        from .output import to_json
        return to_json(self, indent=indent)

@dataclass
class RawStockData:
    """Flat container passed by the data adapter into the engine."""
    ticker: str
    name:   str
    classification: NSEClassification
    # Price metrics (from market data)
    price_history:  Optional[Any]  = None  # pd.Series indexed by date
    # Fundamental metrics — keyed by metric name matching CARD_WEIGHTS keys
    fundamentals:   Dict[str, Optional[float]] = field(default_factory=dict)
    # Surveillance flags (live NSE data)
    on_asm: bool = False
    on_gsm: bool = False
