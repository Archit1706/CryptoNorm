"""Cross-exchange spread computation."""

from __future__ import annotations

from decimal import Decimal

from cryptonorm.risk.spread import compute_spread

D = Decimal


def test_spread_across_venues():
    info = compute_spread({"coinbase": D("100"), "binance": D("101"), "kraken": D("100.5")})
    assert info is not None
    assert info.spread_usd == D("1")
    assert info.high_exchange == "binance"  # most expensive -> sell
    assert info.low_exchange == "coinbase"  # cheapest -> buy
    assert info.spread_bps == D("100")  # 1/100 * 10000


def test_single_venue_returns_none():
    assert compute_spread({"coinbase": D("100")}) is None
    assert compute_spread({}) is None
