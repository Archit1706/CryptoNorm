"""Kraken WS v2 -> normalized events.

Schema verified against live capture (tests/fixtures/kraken_*.json):

  book snapshot/update (channel "book"):
    {"channel":"book","type":"snapshot"|"update","data":[{"symbol":"BTC/USD",
      "bids":[{"price":<num>,"qty":<num>}],"asks":[...],
      "checksum":<uint32>,"timestamp":<rfc3339>}]}
    qty 0 removes the level. Prices/qtys are JSON numbers; the adapter parses
    frames with ``parse_float=Decimal`` to keep them exact.

  trade (channel "trade"):
    {"channel":"trade","type":"update","data":[{"symbol":"BTC/USD",
      "side":"buy"|"sell","price":<num>,"qty":<num>,"trade_id":<int>,
      "timestamp":<rfc3339>}]}   side is the aggressor.

Kraken v2 has no per-message sequence number; book integrity is verified by
the adapter via the CRC32 checksum, so ``sequence`` is left None here.
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

_SIDE = {"buy": Side.BUY, "sell": Side.SELL}


def _dec(x: object) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def _levels(raw: list[dict[str, Any]]) -> list[PriceLevel]:
    return [PriceLevel(price=_dec(lvl["price"]), size=_dec(lvl["qty"])) for lvl in raw]


def normalize_frame(frame: dict[str, Any]) -> list[NormalizedEvent]:
    if frame.get("channel") == "book":
        return _book(frame)
    if frame.get("channel") == "trade" and frame.get("type") == "update":
        return _trades(frame)
    return []  # status / heartbeat / subscribe ack


def _book(frame: dict[str, Any]) -> list[NormalizedEvent]:
    is_snapshot = frame.get("type") == "snapshot"
    ingest = utcnow()
    out: list[NormalizedEvent] = []
    for d in frame.get("data", []):
        common = {
            "exchange": Exchange.KRAKEN,
            "symbol": to_canonical_symbol(Exchange.KRAKEN, d["symbol"]),
            "exchange_symbol": d["symbol"],
            "exchange_ts": d.get("timestamp"),
            "ingest_ts": ingest,
            "sequence": None,
        }
        bids, asks = _levels(d.get("bids", [])), _levels(d.get("asks", []))
        if is_snapshot:
            out.append(BookSnapshot(bids=bids, asks=asks, **common))
        else:
            out.append(BookDelta(first_sequence=None, bids=bids, asks=asks, **common))
    return out


def _trades(frame: dict[str, Any]) -> list[NormalizedEvent]:
    ingest = utcnow()
    out: list[NormalizedEvent] = []
    for t in frame.get("data", []):
        out.append(
            Trade(
                exchange=Exchange.KRAKEN,
                symbol=to_canonical_symbol(Exchange.KRAKEN, t["symbol"]),
                exchange_symbol=t["symbol"],
                exchange_ts=t["timestamp"],
                ingest_ts=ingest,
                sequence=int(t["trade_id"]),
                trade_id=str(t["trade_id"]),
                price=_dec(t["price"]),
                size=_dec(t["qty"]),
                aggressor=_SIDE[t["side"]],
            )
        )
    return out
