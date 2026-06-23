"""Common interface every exchange adapter implements.

An adapter owns exactly two concerns:
  1. transport — connect to the venue's public WS, subscribe, and yield
     parsed JSON frames (``stream``); and
  2. normalization — turn one raw frame into zero or more
     ``NormalizedEvent`` objects (``normalize``), delegating to the
     matching ``cryptonorm.normalize.<venue>`` module.

Keeping both behind one ABC means adding a 4th venue is a single new file
plus a registry entry — nothing downstream changes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, ClassVar

import structlog

from cryptonorm.common.config import Settings, get_settings
from cryptonorm.common.logging import get_logger
from cryptonorm.common.schemas import Exchange, NormalizedEvent


class ExchangeAdapter(ABC):
    name: ClassVar[Exchange]

    def __init__(
        self,
        symbols: list[str],
        settings: Settings | None = None,
        logger: structlog.stdlib.BoundLogger | None = None,
    ):
        self.symbols = symbols
        self.settings = settings or get_settings()
        self.log = (logger or get_logger("ingest")).bind(exchange=self.name.value)

    @abstractmethod
    def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Connect, subscribe, and yield parsed JSON frames until the socket closes.

        Implemented as an async generator. Raises on connection loss so a
        reconnect wrapper (phase 3) can restart it.
        """
        ...

    @abstractmethod
    def normalize(self, frame: dict[str, Any]) -> list[NormalizedEvent]:
        """Translate one raw frame into normalized events (possibly empty)."""
        ...

    def snapshot_url(self, canonical_symbol: str) -> str | None:
        """REST URL for a fresh book snapshot, used to resync after a gap.

        Returns ``None`` for venues whose WS stream is self-snapshotting
        (e.g. Coinbase sends a snapshot frame on subscribe).
        """
        return None
