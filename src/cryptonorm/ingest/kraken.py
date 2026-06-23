"""Kraken WS v2 public adapter with CRC32 book-checksum validation.

Kraken v2 has no per-message sequence number; instead every book frame
carries a CRC32 ``checksum`` over the top-of-book. We keep a small shadow
book per symbol, recompute the checksum after each frame, and raise
FeedGapError on a mismatch (the reconnect wrapper then re-snapshots).

Checksum algorithm (verified against live frames): for the top `depth`
asks (ascending) then bids (descending), format each price/qty to the
symbol's precision, strip the decimal point and leading zeros, concatenate,
and CRC32 the ASCII bytes.
"""

from __future__ import annotations

import json
import zlib
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, ClassVar

import websockets

from cryptonorm.common.errors import FeedGapError
from cryptonorm.common.schemas import Exchange, NormalizedEvent
from cryptonorm.common.types import to_raw_symbol
from cryptonorm.ingest.base import ExchangeAdapter
from cryptonorm.normalize.kraken import normalize_frame

# (price_precision, qty_precision) per v2 symbol, from Kraken's AssetPairs
# (pair_decimals / lot_decimals). See tests/fixtures/kraken_assetpairs.json.
_PRECISION: dict[str, tuple[int, int]] = {
    "BTC/USD": (1, 8),
    "ETH/USD": (2, 8),
}


def _fmt(val: Decimal, prec: int) -> str:
    s = f"{val:.{prec}f}".replace(".", "").lstrip("0")
    return s or "0"


def kraken_book_checksum(
    asks_top: list[tuple[Decimal, Decimal]],
    bids_top: list[tuple[Decimal, Decimal]],
    price_prec: int,
    qty_prec: int,
) -> int:
    """CRC32 over top asks (ascending) then top bids (descending)."""
    buf: list[str] = []
    for price, qty in asks_top:
        buf.append(_fmt(price, price_prec))
        buf.append(_fmt(qty, qty_prec))
    for price, qty in bids_top:
        buf.append(_fmt(price, price_prec))
        buf.append(_fmt(qty, qty_prec))
    return zlib.crc32("".join(buf).encode("ascii"))


class _ShadowBook:
    """Minimal top-N book used only to validate Kraken checksums."""

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def apply(self, bids: list[dict[str, Any]], asks: list[dict[str, Any]], depth: int) -> None:
        for side, levels in ((self.bids, bids), (self.asks, asks)):
            for lvl in levels:
                price, qty = Decimal(str(lvl["price"])), Decimal(str(lvl["qty"]))
                if qty == 0:
                    side.pop(price, None)
                else:
                    side[price] = qty
        # Kraken keeps a fixed-depth book and pushes levels out of the window
        # WITHOUT sending a removal, so we must trim to depth ourselves or the
        # book accumulates phantom levels and the checksum diverges.
        for stale in sorted(self.bids, reverse=True)[depth:]:
            del self.bids[stale]
        for stale in sorted(self.asks)[depth:]:
            del self.asks[stale]

    def checksum(self, depth: int, price_prec: int, qty_prec: int) -> int:
        asks_top = sorted(self.asks.items())[:depth]
        bids_top = sorted(self.bids.items(), reverse=True)[:depth]
        return kraken_book_checksum(asks_top, bids_top, price_prec, qty_prec)


class KrakenAdapter(ExchangeAdapter):
    name: ClassVar[Exchange] = Exchange.KRAKEN

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        url = self.settings.kraken_ws_url
        depth = self.settings.kraken_book_depth
        raw_symbols = [to_raw_symbol(self.name, s) for s in self.symbols]
        books: dict[str, _ShadowBook] = {s: _ShadowBook() for s in raw_symbols}

        self.log.info("connecting", url=url, symbols=raw_symbols)
        async with websockets.connect(
            url, open_timeout=15, ping_interval=20, ping_timeout=20
        ) as ws:
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "book", "symbol": raw_symbols, "depth": depth},
            }))
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": raw_symbols},
            }))
            self.log.info("subscribed", channels=["book", "trade"], depth=depth)

            async for raw in ws:
                frame = json.loads(raw, parse_float=Decimal)
                if frame.get("channel") == "book":
                    self._validate_book(frame, books, depth)
                yield frame

    def _validate_book(
        self, frame: dict[str, Any], books: dict[str, _ShadowBook], depth: int
    ) -> None:
        is_snapshot = frame.get("type") == "snapshot"
        for d in frame.get("data", []):
            symbol = d["symbol"]
            book = books[symbol]
            if is_snapshot:
                book.reset()
            book.apply(d.get("bids", []), d.get("asks", []), depth)
            prec = _PRECISION.get(symbol)
            if prec is None or "checksum" not in d:
                continue
            computed = book.checksum(depth, prec[0], prec[1])
            if computed != d["checksum"]:
                raise FeedGapError(
                    f"{symbol} checksum mismatch: computed {computed}, got {d['checksum']}"
                )

    def normalize(self, frame: dict[str, Any]) -> list[NormalizedEvent]:
        return normalize_frame(frame)
