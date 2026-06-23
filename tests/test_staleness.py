"""Staleness monitor, driven by an injected clock."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from cryptonorm.common.schemas import Exchange
from cryptonorm.risk.staleness import StalenessMonitor


class _Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def test_fresh_feed_is_not_stale():
    clock = _Clock()
    mon = StalenessMonitor(10.0, clock=clock)
    mon.touch(Exchange.COINBASE, "BTC-USD")
    (status,) = mon.statuses()
    assert status.stale is False
    assert status.age_seconds == 0


def test_feed_goes_stale_past_threshold():
    clock = _Clock()
    mon = StalenessMonitor(10.0, clock=clock)
    mon.touch(Exchange.COINBASE, "BTC-USD")
    clock.advance(11)
    (status,) = mon.statuses()
    assert status.stale is True
    assert status.age_seconds == 11


def test_touch_clears_staleness():
    clock = _Clock()
    mon = StalenessMonitor(10.0, clock=clock)
    mon.touch(Exchange.KRAKEN, "ETH-USD")
    clock.advance(11)
    assert mon.statuses()[0].stale is True
    mon.touch(Exchange.KRAKEN, "ETH-USD")  # new message arrives
    assert mon.statuses()[0].stale is False
