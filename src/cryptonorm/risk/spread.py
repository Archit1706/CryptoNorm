"""Cross-exchange spread: the difference between the richest and cheapest
venue mid for a symbol. A wide spread is a (paper) arbitrage signal and an
alert trigger.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class SpreadInfo:
    spread_usd: Decimal
    spread_bps: Decimal
    high_exchange: str  # most expensive venue (sell here)
    low_exchange: str  # cheapest venue (buy here)


def compute_spread(mids: dict[str, Decimal]) -> SpreadInfo | None:
    """Spread across venue mids for one symbol; None if fewer than two venues."""
    if len(mids) < 2:
        return None
    high_exchange = max(mids, key=lambda e: mids[e])
    low_exchange = min(mids, key=lambda e: mids[e])
    hi, lo = mids[high_exchange], mids[low_exchange]
    spread = hi - lo
    bps = (spread / lo * Decimal(10000)) if lo > 0 else Decimal(0)
    return SpreadInfo(spread, bps, high_exchange, low_exchange)
