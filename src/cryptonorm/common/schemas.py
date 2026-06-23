"""The normalized event schema — the system's internal contract.

Every exchange adapter produces these models; every downstream consumer
(pipeline, risk, dashboard) reads only these. Prices and sizes are
``Decimal`` and serialize to JSON as strings so they round-trip over Kafka
without float drift.

The wire form is a discriminated union on ``event_type`` (see
``NormalizedEvent``), so a consumer can ``TypeAdapter(NormalizedEvent)``
a raw frame and get back the correct concrete model.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class Exchange(StrEnum):
    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class PriceLevel(BaseModel):
    """One level of a book. ``size == 0`` means "remove this price level"."""

    model_config = ConfigDict(frozen=True)

    price: Decimal
    size: Decimal


class _Envelope(BaseModel):
    """Fields common to every normalized event."""

    model_config = ConfigDict(use_enum_values=False)

    exchange: Exchange
    symbol: str  # canonical, e.g. "BTC-USD"
    exchange_symbol: str  # raw venue symbol, e.g. "BTCUSDT", "XBT/USD"
    exchange_ts: datetime | None  # exchange clock; None if the venue omits it
    ingest_ts: datetime  # our clock, stamped on receipt
    sequence: int | None  # last/only sequence number on this message


class BookSnapshot(_Envelope):
    """Full L2 book at a point in time (start of stream or after a resync)."""

    event_type: Literal["book_snapshot"] = "book_snapshot"
    bids: list[PriceLevel]
    asks: list[PriceLevel]


class BookDelta(_Envelope):
    """Incremental L2 update: only the price levels that changed.

    ``first_sequence`` is the first sequence id covered by this message
    (Binance depth events span a range ``[U, u]``); for single-counter
    venues it equals ``sequence``. Gap detection compares a message's
    ``first_sequence`` against the previously seen ``sequence`` + 1.
    """

    event_type: Literal["book_delta"] = "book_delta"
    first_sequence: int | None
    bids: list[PriceLevel]
    asks: list[PriceLevel]


class Trade(_Envelope):
    event_type: Literal["trade"] = "trade"
    trade_id: str
    price: Decimal
    size: Decimal
    aggressor: Side  # taker side


class PaperFill(_Envelope):
    """A simulated fill from the paper-trading engine (phase 4)."""

    event_type: Literal["fill"] = "fill"
    order_id: str
    side: Side
    price: Decimal
    size: Decimal
    fee: Decimal = Decimal("0")


NormalizedEvent = Annotated[
    BookSnapshot | BookDelta | Trade | PaperFill,
    Field(discriminator="event_type"),
]
"""Discriminated union of every event that flows through the system."""
