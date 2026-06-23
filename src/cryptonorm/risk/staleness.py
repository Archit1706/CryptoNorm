"""Per-feed staleness monitor.

Records the last time each (exchange, symbol) produced an event and flags a
feed STALE when it has been silent past a threshold. The clock is injectable
so the logic is unit-testable without real time. Detecting staleness in the
pipeline (a single Redis writer) also catches total ingest death, not just a
single quiet feed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from cryptonorm.common.schemas import Exchange
from cryptonorm.common.types import utcnow


@dataclass(frozen=True)
class FeedStatus:
    exchange: Exchange
    symbol: str
    last_ts: datetime
    age_seconds: float
    stale: bool


class StalenessMonitor:
    def __init__(self, threshold_seconds: float, clock: Callable[[], datetime] = utcnow):
        self._threshold = threshold_seconds
        self._clock = clock
        self._last: dict[tuple[Exchange, str], datetime] = {}

    def touch(self, exchange: Exchange, symbol: str) -> None:
        self._last[(exchange, symbol)] = self._clock()

    def statuses(self) -> list[FeedStatus]:
        now = self._clock()
        result: list[FeedStatus] = []
        for (exchange, symbol), ts in self._last.items():
            age = (now - ts).total_seconds()
            result.append(FeedStatus(exchange, symbol, ts, age, age > self._threshold))
        return result
