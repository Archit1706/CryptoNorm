"""Pipeline/risk consumer: Kafka -> books + positions -> Redis.

  python -m cryptonorm.services.run_pipeline

Consumes md.book.v1 / md.trade.v1 / paper.fills.v1 and:
  * maintains an L2 book per (exchange, symbol) -> BBO / shallow book / last
    trade in Redis;
  * applies paper fills to the risk engine and periodically marks positions
    to market, writing P&L, exposure, drawdown and VaR to Redis;
  * reconciles computed positions against the simulator ledger;
  * flags feeds that go silent (staleness watchdog).

Shutdown on SIGINT/SIGTERM drains the consumer and closes Redis.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from decimal import Decimal

from cryptonorm.common.config import Settings, get_settings
from cryptonorm.common.logging import configure_logging, get_logger
from cryptonorm.common.schemas import (
    BookDelta,
    BookSnapshot,
    Exchange,
    NormalizedEvent,
    PaperFill,
    Trade,
)
from cryptonorm.common.shutdown import install_signal_handlers
from cryptonorm.pipeline.consumer import EventConsumer
from cryptonorm.pipeline.redis_state import RedisState
from cryptonorm.pipeline.topics import consumed_topics
from cryptonorm.risk.book import L2Book
from cryptonorm.risk.pnl import RiskEngine
from cryptonorm.risk.recon import reconcile
from cryptonorm.risk.staleness import StalenessMonitor

_log = get_logger("pipeline")

Books = dict[tuple[Exchange, str], L2Book]


async def _publish_book(state: RedisState, book: L2Book, settings: Settings, ts: str) -> None:
    bids, asks = book.top(settings.book_top_n)
    await state.set_bbo(
        book.exchange, book.symbol, book.best_bid(), book.best_ask(), book.mid(), ts
    )
    await state.set_book_top(book.exchange, book.symbol, bids, asks, ts)


async def _handle(
    ev: NormalizedEvent,
    books: Books,
    state: RedisState,
    settings: Settings,
    stats: Counter[str],
    monitor: StalenessMonitor,
    engine: RiskEngine,
) -> None:
    stats[ev.event_type] += 1
    monitor.touch(ev.exchange, ev.symbol)
    ts = ev.ingest_ts.isoformat()

    if isinstance(ev, BookSnapshot):
        book = books.setdefault((ev.exchange, ev.symbol), L2Book(ev.exchange, ev.symbol))
        book.apply_snapshot(ev.bids, ev.asks, ev.sequence)
        await state.register_feed(ev.exchange, ev.symbol)
        await _publish_book(state, book, settings, ts)

    elif isinstance(ev, BookDelta):
        cur = books.get((ev.exchange, ev.symbol))
        if cur is None or not cur.ready:
            stats["delta_before_snapshot"] += 1
            return
        cur.apply_delta(ev.bids, ev.asks, ev.first_sequence, ev.sequence)
        await _publish_book(state, cur, settings, ts)

    elif isinstance(ev, Trade):
        await state.register_feed(ev.exchange, ev.symbol)
        await state.set_last_trade(ev.exchange, ev.symbol, ev.price, ev.size, ev.aggressor, ts)

    elif isinstance(ev, PaperFill):
        engine.apply_fill(ev)


def _marks(books: Books) -> dict[tuple[Exchange, str], Decimal]:
    marks: dict[tuple[Exchange, str], Decimal] = {}
    for key, book in books.items():
        mid = book.mid()
        if mid is not None:
            marks[key] = mid
    return marks


async def _consume_loop(
    consumer: EventConsumer, handle: Callable[[NormalizedEvent], Awaitable[None]]
) -> None:
    async for ev in consumer.events():
        await handle(ev)


async def _risk_loop(
    engine: RiskEngine, books: Books, state: RedisState, interval: float
) -> None:
    while True:
        await asyncio.sleep(interval)
        snap = engine.snapshot(_marks(books))
        await state.set_risk_snapshot(asdict(snap))


async def _recon_loop(engine: RiskEngine, state: RedisState, interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        ledger = await state.get_ledger()
        lines = reconcile(engine.book.net_quantities(), ledger)
        mismatches = [line for line in lines if not line.matched]
        await state.set_recon({
            "ok": not mismatches,
            "checked": len(lines),
            "mismatches": [
                {"exchange": m.exchange.value, "symbol": m.symbol,
                 "computed": str(m.computed_qty), "ledger": str(m.ledger_qty)}
                for m in mismatches
            ],
        })
        if mismatches:
            for m in mismatches:
                _log.error("recon mismatch", exchange=m.exchange.value, symbol=m.symbol,
                           computed=str(m.computed_qty), ledger=str(m.ledger_qty))


async def _watchdog(monitor: StalenessMonitor, state: RedisState, interval: float = 1.0) -> None:
    prev: dict[tuple[Exchange, str], bool] = {}
    while True:
        await asyncio.sleep(interval)
        for st in monitor.statuses():
            label = "STALE" if st.stale else "OK"
            await state.set_feed_status(
                st.exchange, st.symbol, label, st.last_ts.isoformat(), st.age_seconds
            )
            key = (st.exchange, st.symbol)
            if prev.get(key) != st.stale:
                prev[key] = st.stale
                logfn = _log.warning if st.stale else _log.info
                logfn("feed " + label, exchange=st.exchange.value, symbol=st.symbol,
                      age_s=round(st.age_seconds, 1))


async def _stats_logger(stats: Counter[str], books: Books, interval: float = 5.0) -> None:
    while True:
        await asyncio.sleep(interval)
        _log.info("consumed (cumulative)", books=len(books), **dict(stats))


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, json=settings.log_json)
    _log.info("starting", redis=settings.redis_url, kafka=settings.kafka_bootstrap)

    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    state = RedisState(settings.redis_url)
    await state.ping()
    consumer = EventConsumer(settings, consumed_topics(settings))
    await consumer.start()

    books: Books = {}
    stats: Counter[str] = Counter()
    monitor = StalenessMonitor(settings.staleness_seconds)
    engine = RiskEngine(var_window=settings.var_window)

    async def handle(ev: NormalizedEvent) -> None:
        await _handle(ev, books, state, settings, stats, monitor, engine)

    tasks = [
        asyncio.create_task(_consume_loop(consumer, handle)),
        asyncio.create_task(_risk_loop(engine, books, state, settings.risk_interval_seconds)),
        asyncio.create_task(_recon_loop(engine, state, settings.recon_interval_seconds)),
        asyncio.create_task(_watchdog(monitor, state)),
        asyncio.create_task(_stats_logger(stats, books)),
    ]

    try:
        await stop_event.wait()
    finally:
        _log.info("draining")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.stop()
        await state.close()
        _log.info("pipeline stopped")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
