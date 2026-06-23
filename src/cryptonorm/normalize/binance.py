"""Binance (binance.us) WS + REST -> normalized events.

Schema verified against live capture (tests/fixtures/binance_*.json):

  REST snapshot (GET /api/v3/depth):
    {"lastUpdateId": <int>, "bids": [["price","qty"], ...], "asks": [...]}
  The adapter wraps this as a frame tagged ``_kind="snapshot"`` with the
  canonical/raw symbol attached.

  WS depthUpdate (diff stream <sym>@depth):
    {"e":"depthUpdate","E":<ms>,"s":"BTCUSD","U":<firstId>,"u":<lastId>,
     "b":[["price","qty"],...],"a":[...]}   qty "0" removes the level.

  WS trade (<sym>@trade):
    {"e":"trade","E":<ms>,"s":"BTCUSD","t":<id>,"p":"price","q":"qty",
     "T":<ms>,"m":<bool isBuyerMaker>}
  Aggressor: m == true means the buyer was the maker, so the taker (the
  aggressor) was the seller -> SELL; m == false -> BUY.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from cryptonorm.common.schemas import (
    BookDelta,
    BookSnapshot,
    Exchange,
    NormalizedEvent,
    PriceLevel,
    Side,
    Trade,
)
from cryptonorm.common.types import from_epoch_millis, to_canonical_symbol, utcnow


def _levels(raw: list[list[str]]) -> list[PriceLevel]:
    return [PriceLevel(price=Decimal(p), size=Decimal(q)) for p, q in raw]


def normalize_frame(frame: dict[str, Any]) -> list[NormalizedEvent]:
    kind = frame.get("_kind")
    if kind == "snapshot":
        return [_snapshot(frame)]
    event = frame.get("e")
    if event == "depthUpdate":
        return [_depth_update(frame)]
    if event == "trade":
        return [_trade(frame)]
    return []  # subscription acks etc.


def _snapshot(frame: dict[str, Any]) -> NormalizedEvent:
    raw_symbol = frame["_symbol"]
    return BookSnapshot(
        exchange=Exchange.BINANCE,
        symbol=to_canonical_symbol(Exchange.BINANCE, raw_symbol),
        exchange_symbol=raw_symbol,
        exchange_ts=None,  # REST snapshot carries no timestamp
        ingest_ts=utcnow(),
        sequence=frame["lastUpdateId"],
        bids=_levels(frame["bids"]),
        asks=_levels(frame["asks"]),
    )


def _depth_update(frame: dict[str, Any]) -> NormalizedEvent:
    return BookDelta(
        exchange=Exchange.BINANCE,
        symbol=to_canonical_symbol(Exchange.BINANCE, frame["s"]),
        exchange_symbol=frame["s"],
        exchange_ts=from_epoch_millis(frame["E"]),
        ingest_ts=utcnow(),
        first_sequence=frame["U"],
        sequence=frame["u"],
        bids=_levels(frame["b"]),
        asks=_levels(frame["a"]),
    )


def _trade(frame: dict[str, Any]) -> NormalizedEvent:
    return Trade(
        exchange=Exchange.BINANCE,
        symbol=to_canonical_symbol(Exchange.BINANCE, frame["s"]),
        exchange_symbol=frame["s"],
        exchange_ts=from_epoch_millis(frame["T"]),
        ingest_ts=utcnow(),
        sequence=frame["t"],
        trade_id=str(frame["t"]),
        price=Decimal(frame["p"]),
        size=Decimal(frame["q"]),
        aggressor=Side.SELL if frame["m"] else Side.BUY,
    )
