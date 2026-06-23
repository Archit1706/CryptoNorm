"""Binance (binance.us) public WebSocket + REST adapter.

Binance's WS only carries depth *diffs*, so the book must be seeded from a
REST snapshot and the diffs synchronized to it (the documented algorithm):

  * fetch REST snapshot -> lastUpdateId; emit it as a BookSnapshot
  * drop diff events fully older than the snapshot (u <= lastUpdateId)
  * the first applied diff must straddle the snapshot (U <= lastUpdateId+1 <= u)
  * thereafter each diff must be contiguous (U == previous u + 1)

Any violation raises FeedGapError -> the reconnect wrapper refetches the
snapshot and resynchronizes.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, ClassVar

import aiohttp
import websockets

from cryptonorm.common.errors import FeedGapError
from cryptonorm.common.schemas import Exchange, NormalizedEvent
from cryptonorm.common.types import to_raw_symbol
from cryptonorm.ingest.base import ExchangeAdapter
from cryptonorm.normalize.binance import normalize_frame


class BinanceAdapter(ExchangeAdapter):
    name: ClassVar[Exchange] = Exchange.BINANCE

    async def _fetch_snapshot(self, raw_symbol: str) -> dict[str, Any]:
        url = f"{self.settings.binance_rest_url}/depth"
        params = {"symbol": raw_symbol, "limit": "1000"}
        async with aiohttp.ClientSession() as s, s.get(url, params=params) as r:
            r.raise_for_status()
            snap: dict[str, Any] = await r.json()
        snap["_kind"] = "snapshot"
        snap["_symbol"] = raw_symbol
        return snap

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        url = self.settings.binance_ws_url
        raw_symbols = [to_raw_symbol(self.name, s) for s in self.symbols]
        params = [f"{r.lower()}@depth" for r in raw_symbols]
        params += [f"{r.lower()}@trade" for r in raw_symbols]

        # per-symbol sync state, reset each connection
        last_update_id: dict[str, int] = {}
        prev_u: dict[str, int | None] = {}

        self.log.info("connecting", url=url, symbols=raw_symbols)
        async with websockets.connect(
            url, open_timeout=15, ping_interval=20, ping_timeout=20
        ) as ws:
            await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))
            self.log.info("subscribed", streams=params)

            # Seed each book from a REST snapshot (WS messages buffer meanwhile).
            for raw_symbol in raw_symbols:
                snap = await self._fetch_snapshot(raw_symbol)
                last_update_id[raw_symbol] = snap["lastUpdateId"]
                prev_u[raw_symbol] = None
                yield snap

            async for raw in ws:
                frame = json.loads(raw)
                if frame.get("e") != "depthUpdate":
                    yield frame  # trades, acks
                    continue
                sym = frame["s"]
                self._check_depth_continuity(sym, frame, last_update_id, prev_u)
                if prev_u[sym] is not None:  # only yield once synced
                    yield frame

    @staticmethod
    def _check_depth_continuity(
        sym: str,
        frame: dict[str, Any],
        last_update_id: dict[str, int],
        prev_u: dict[str, int | None],
    ) -> None:
        """Validate diff ordering; mutate prev_u. Raise FeedGapError on a gap."""
        first, final = frame["U"], frame["u"]
        current = prev_u[sym]
        if current is None:
            # still syncing to the snapshot
            if final <= last_update_id[sym]:
                return  # stale diff, drop (prev_u stays None)
            if first <= last_update_id[sym] + 1 <= final:
                prev_u[sym] = final  # first valid diff
                return
            raise FeedGapError(f"{sym} snapshot stale: U={first} > lastUpdateId+1")
        if first != current + 1:
            raise FeedGapError(f"{sym} depth gap: expected {current + 1}, got {first}")
        prev_u[sym] = final

    def normalize(self, frame: dict[str, Any]) -> list[NormalizedEvent]:
        return normalize_frame(frame)
