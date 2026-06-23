"""Kafka topic names + the rule mapping each event type to its topic.

Topics are versioned (``.v1``) so the wire schema can evolve without
clobbering existing data. Messages are keyed by canonical symbol, so all
updates for one symbol land on the same partition and stay ordered.
"""

from __future__ import annotations

from cryptonorm.common.config import Settings
from cryptonorm.common.schemas import (
    BookDelta,
    BookSnapshot,
    NormalizedEvent,
    PaperFill,
    Trade,
)


def topic_for(event: NormalizedEvent, settings: Settings) -> str:
    """Return the destination topic for a normalized event."""
    if isinstance(event, (BookSnapshot, BookDelta)):
        return settings.topic_book
    if isinstance(event, Trade):
        return settings.topic_trade
    if isinstance(event, PaperFill):
        return settings.topic_fill
    raise TypeError(f"no topic for event type {type(event).__name__}")


def market_data_topics(settings: Settings) -> list[str]:
    """Market-data topics."""
    return [settings.topic_book, settings.topic_trade]


def consumed_topics(settings: Settings) -> list[str]:
    """All topics the pipeline/risk consumer subscribes to (book, trade, fills)."""
    return [settings.topic_book, settings.topic_trade, settings.topic_fill]
