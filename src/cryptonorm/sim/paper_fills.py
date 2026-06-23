"""Paper-fill simulator.

Generates simulated fills so the system has positions to track without real
trading. Each step picks a random venue+symbol, reads the current BBO from
Redis, and "crosses the spread" (buy at the ask, sell at the bid) for a
small random size. Every fill is:

  * published to Kafka (paper.fills.v1) for the risk engine to consume, and
  * recorded in the Redis ledger (the reconciliation source of truth).

The ledger is written directly here — the single authoritative writer — so a
later mismatch between it and the Kafka-derived positions reveals pipeline
loss, not simulator error.
"""

from __future__ import annotations

import asyncio
import random
from decimal import Decimal

from cryptonorm.common.config import Settings
from cryptonorm.common.logging import get_logger
from cryptonorm.common.schemas import Exchange, PaperFill, Side
from cryptonorm.common.types import to_raw_symbol, utcnow
from cryptonorm.pipeline.producer import EventProducer
from cryptonorm.pipeline.redis_state import RedisState


class PaperFillSimulator:
    def __init__(
        self,
        settings: Settings,
        state: RedisState,
        producer: EventProducer,
        rng: random.Random | None = None,
    ):
        self._settings = settings
        self._state = state
        self._producer = producer
        self._rng = rng or random.Random()
        self._log = get_logger("sim")
        self._ledger: dict[tuple[Exchange, str], Decimal] = {}
        self._seq = 0

    async def _bbo(self, exchange: Exchange, symbol: str) -> tuple[Decimal, Decimal] | None:
        bbo = await self._state.get(f"cn:bbo:{exchange.value}:{symbol}")
        if not bbo or bbo.get("bid") is None or bbo.get("ask") is None:
            return None
        return Decimal(bbo["bid"]), Decimal(bbo["ask"])

    async def step(self) -> PaperFill | None:
        exchange = self._rng.choice(self._settings.exchanges)
        symbol = self._rng.choice(self._settings.symbols)
        quote = await self._bbo(exchange, symbol)
        if quote is None:
            return None  # no live market yet for this feed
        bid, ask = quote

        side = self._rng.choice([Side.BUY, Side.SELL])
        price = ask if side is Side.BUY else bid  # cross the spread (taker)
        qty = Decimal(str(round(self._rng.uniform(0.001, self._settings.sim_max_qty), 6)))
        fee = (price * qty * Decimal(str(self._settings.sim_fee_rate))).quantize(Decimal("0.01"))

        self._seq += 1
        fill = PaperFill(
            exchange=exchange,
            symbol=symbol,
            exchange_symbol=to_raw_symbol(exchange, symbol),
            exchange_ts=None,
            ingest_ts=utcnow(),
            sequence=self._seq,
            order_id=f"sim-{self._seq}",
            side=side,
            price=price,
            size=qty,
            fee=fee,
        )
        await self._producer.publish(fill)

        signed = qty if side is Side.BUY else -qty
        key = (exchange, symbol)
        self._ledger[key] = self._ledger.get(key, Decimal("0")) + signed
        await self._state.set_ledger(exchange, symbol, self._ledger[key])
        return fill

    async def run(self, stop_event: asyncio.Event) -> None:
        self._log.info("simulator started", mean_interval=self._settings.sim_interval_seconds)
        while not stop_event.is_set():
            try:
                fill = await self.step()
                if fill is not None:
                    self._log.debug(
                        "fill", exchange=fill.exchange.value, symbol=fill.symbol,
                        side=fill.side.value, px=str(fill.price), qty=str(fill.size),
                    )
            except Exception as exc:
                self._log.error("sim step failed", error=repr(exc))
            # randomized interval around the configured mean (exponential-ish)
            await asyncio.sleep(self._rng.uniform(0.2, 2.0) * self._settings.sim_interval_seconds)
