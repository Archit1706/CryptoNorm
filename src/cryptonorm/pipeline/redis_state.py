"""Redis-backed current-state cache.

Holds only the *latest* view downstream readers (the dashboard) need: best
bid/offer, a shallow book, and last trade per (exchange, symbol). Pure
``_dumps`` keeps Decimals as strings and is unit-testable without a server.
All keys are namespaced ``cn:`` and a ``cn:feeds`` set tracks live feeds.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import redis.asyncio as aioredis

from cryptonorm.common.schemas import Exchange, PriceLevel, Side

_FEEDS_KEY = "cn:feeds"


def _dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, default=str)  # Decimal -> str


def _levels(levels: list[PriceLevel]) -> list[list[str]]:
    return [[str(lvl.price), str(lvl.size)] for lvl in levels]


class RedisState:
    def __init__(self, url: str):
        self._redis = aioredis.from_url(url, decode_responses=True)

    async def ping(self) -> bool:
        return bool(await self._redis.ping())

    async def close(self) -> None:
        await self._redis.aclose()

    @staticmethod
    def _feed(exchange: Exchange, symbol: str) -> str:
        return f"{exchange.value}:{symbol}"

    async def register_feed(self, exchange: Exchange, symbol: str) -> None:
        await self._redis.sadd(_FEEDS_KEY, self._feed(exchange, symbol))

    async def set_bbo(
        self,
        exchange: Exchange,
        symbol: str,
        bid: PriceLevel | None,
        ask: PriceLevel | None,
        mid: Decimal | None,
        ts: str,
    ) -> None:
        payload = {
            "exchange": exchange.value,
            "symbol": symbol,
            "bid": str(bid.price) if bid else None,
            "bid_size": str(bid.size) if bid else None,
            "ask": str(ask.price) if ask else None,
            "ask_size": str(ask.size) if ask else None,
            "mid": str(mid) if mid is not None else None,
            "ts": ts,
        }
        await self._redis.set(f"cn:bbo:{self._feed(exchange, symbol)}", _dumps(payload))

    async def set_book_top(
        self,
        exchange: Exchange,
        symbol: str,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
        ts: str,
    ) -> None:
        payload = {"bids": _levels(bids), "asks": _levels(asks), "ts": ts}
        await self._redis.set(f"cn:book:{self._feed(exchange, symbol)}", _dumps(payload))

    async def set_last_trade(
        self,
        exchange: Exchange,
        symbol: str,
        price: Decimal,
        size: Decimal,
        side: Side,
        ts: str,
    ) -> None:
        payload = {"price": str(price), "size": str(size), "side": side.value, "ts": ts}
        await self._redis.set(f"cn:trade:{self._feed(exchange, symbol)}", _dumps(payload))

    async def set_feed_status(
        self, exchange: Exchange, symbol: str, state: str, last_ts: str, age_seconds: float
    ) -> None:
        payload = {
            "exchange": exchange.value,
            "symbol": symbol,
            "state": state,  # "OK" | "STALE"
            "last_ts": last_ts,
            "age_seconds": round(age_seconds, 3),
        }
        await self._redis.set(f"cn:status:{self._feed(exchange, symbol)}", _dumps(payload))

    # --- phase 4: ledger (sim source of truth), risk snapshot, recon ---

    async def set_ledger(self, exchange: Exchange, symbol: str, net_qty: Decimal) -> None:
        await self._redis.set(f"cn:ledger:{self._feed(exchange, symbol)}", str(net_qty))

    async def get_ledger(self) -> dict[tuple[Exchange, str], Decimal]:
        out: dict[tuple[Exchange, str], Decimal] = {}
        async for key in self._redis.scan_iter("cn:ledger:*"):
            raw = await self._redis.get(key)
            if raw is None:
                continue
            val = raw.decode() if isinstance(raw, bytes) else raw
            _, _, exch, sym = key.split(":", 3)
            out[(Exchange(exch), sym)] = Decimal(val)
        return out

    async def set_risk_snapshot(self, payload: dict[str, Any]) -> None:
        await self._redis.set("cn:risk:snapshot", _dumps(payload))

    async def set_recon(self, payload: dict[str, Any]) -> None:
        await self._redis.set("cn:recon", _dumps(payload))

    async def get(self, key: str) -> dict[str, Any] | None:
        raw = await self._redis.get(key)
        return json.loads(raw) if raw else None

    async def list_feeds(self) -> list[str]:
        members = await self._redis.smembers(_FEEDS_KEY)
        return sorted(str(m) for m in members)
