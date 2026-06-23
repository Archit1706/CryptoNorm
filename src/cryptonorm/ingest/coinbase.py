"""Coinbase Advanced Trade public WebSocket adapter (no auth required).

Gap detection: Coinbase stamps every frame on a connection with a
monotonic ``sequence_num``. A break in that sequence means dropped frames,
so we raise ``FeedGapError`` to force a reconnect (which re-snapshots).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, ClassVar

import websockets

from cryptonorm.common.errors import FeedGapError
from cryptonorm.common.schemas import Exchange, NormalizedEvent
from cryptonorm.common.types import to_raw_symbol
from cryptonorm.ingest.base import ExchangeAdapter
from cryptonorm.normalize.coinbase import normalize_frame

_CHANNELS = ("level2", "market_trades")
# The initial level2 snapshot frame is ~5 MB, well over the 1 MiB default.
_MAX_FRAME_BYTES = 16 * 1024 * 1024


class CoinbaseAdapter(ExchangeAdapter):
    name: ClassVar[Exchange] = Exchange.COINBASE

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        url = self.settings.coinbase_ws_url
        raw_symbols = [to_raw_symbol(self.name, s) for s in self.symbols]
        last_seq: int | None = None  # connection-level sequence, reset per connect
        self.log.info("connecting", url=url, symbols=raw_symbols)
        async with websockets.connect(
            url,
            max_size=_MAX_FRAME_BYTES,
            open_timeout=15,
            ping_interval=20,
            ping_timeout=20,
        ) as ws:
            for channel in _CHANNELS:
                await ws.send(
                    json.dumps(
                        {"type": "subscribe", "product_ids": raw_symbols, "channel": channel}
                    )
                )
            self.log.info("subscribed", channels=list(_CHANNELS))
            async for raw in ws:
                frame = json.loads(raw)
                seq = frame.get("sequence_num")
                if seq is not None:
                    if last_seq is not None and seq != last_seq + 1:
                        raise FeedGapError(f"sequence gap {last_seq} -> {seq}")
                    last_seq = seq
                yield frame

    def normalize(self, frame: dict[str, Any]) -> list[NormalizedEvent]:
        return normalize_frame(frame)
