"""Coinbase Advanced Trade WS -> normalized events.

Schema verified against live capture (see tests/fixtures/coinbase_*.json):

  level2 (channel "l2_data"):
    {"channel":"l2_data","timestamp":<rfc3339>,"sequence_num":<int>,
     "events":[{"type":"snapshot"|"update","product_id":"BTC-USD",
                "updates":[{"side":"bid"|"offer","event_time":<rfc3339>,
                            "price_level":<str>,"new_quantity":<str>}]}]}

  market_trades (channel "market_trades"):
    {"channel":"market_trades","timestamp":...,"sequence_num":<int>,
     "events":[{"type":"snapshot"|"update",
                "trades":[{"product_id","trade_id","price","size",
                           "time","side":"BUY"|"SELL"}]}]}

Real-venue details: asks are labelled ``"offer"`` (not "ask");
``new_quantity == "0"`` removes the level; ``sequence_num`` is a per-
connection monotonic counter used for gap detection. Timestamp strings
carry nanosecond precision and are coerced to datetime by pydantic.
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
from cryptonorm.common.types import to_canonical_symbol, utcnow

_TRADE_SIDE = {"BUY": Side.BUY, "SELL": Side.SELL}


def _split_levels(updates: list[dict[str, Any]]) -> tuple[list[PriceLevel], list[PriceLevel]]:
    bids: list[PriceLevel] = []
    asks: list[PriceLevel] = []
    for u in updates:
        level = PriceLevel(price=Decimal(u["price_level"]), size=Decimal(u["new_quantity"]))
        (bids if u["side"] == "bid" else asks).append(level)
    return bids, asks


def normalize_frame(frame: dict[str, Any]) -> list[NormalizedEvent]:
    """Translate one Coinbase frame into normalized events (possibly empty)."""
    channel = frame.get("channel")
    if channel == "l2_data":
        return _normalize_l2(frame)
    if channel == "market_trades":
        return _normalize_trades(frame)
    # subscriptions / heartbeats / unknown -> nothing
    return []


def _normalize_l2(frame: dict[str, Any]) -> list[NormalizedEvent]:
    sequence = frame["sequence_num"]
    ts = frame["timestamp"]
    ingest = utcnow()
    out: list[NormalizedEvent] = []
    for ev in frame.get("events", []):
        symbol = to_canonical_symbol(Exchange.COINBASE, ev["product_id"])
        bids, asks = _split_levels(ev.get("updates", []))
        common = {
            "exchange": Exchange.COINBASE,
            "symbol": symbol,
            "exchange_symbol": ev["product_id"],
            "exchange_ts": ts,
            "ingest_ts": ingest,
            "sequence": sequence,
        }
        if ev["type"] == "snapshot":
            out.append(BookSnapshot(bids=bids, asks=asks, **common))
        else:  # "update"
            out.append(BookDelta(first_sequence=sequence, bids=bids, asks=asks, **common))
    return out


def _normalize_trades(frame: dict[str, Any]) -> list[NormalizedEvent]:
    sequence = frame["sequence_num"]
    ingest = utcnow()
    out: list[NormalizedEvent] = []
    for ev in frame.get("events", []):
        for t in ev.get("trades", []):
            out.append(
                Trade(
                    exchange=Exchange.COINBASE,
                    symbol=to_canonical_symbol(Exchange.COINBASE, t["product_id"]),
                    exchange_symbol=t["product_id"],
                    exchange_ts=t["time"],
                    ingest_ts=ingest,
                    sequence=sequence,
                    trade_id=str(t["trade_id"]),
                    price=Decimal(t["price"]),
                    size=Decimal(t["size"]),
                    aggressor=_TRADE_SIDE[t["side"]],
                )
            )
    return out
