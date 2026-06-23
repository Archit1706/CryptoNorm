"""Ingest service: live feeds -> normalize -> produce to Kafka.

  python -m cryptonorm.services.run_ingest

Each adapter runs behind an auto-reconnect wrapper (exponential backoff +
jitter; a detected book desync forces a resync via reconnect). Shutdown on
SIGINT/SIGTERM drains the producer and closes sockets cleanly.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import Counter

from cryptonorm.common.config import Settings, get_settings
from cryptonorm.common.logging import configure_logging, get_logger
from cryptonorm.common.schemas import BookSnapshot
from cryptonorm.common.shutdown import install_signal_handlers
from cryptonorm.ingest.base import ExchangeAdapter
from cryptonorm.ingest.reconnect import reconnecting_stream
from cryptonorm.ingest.registry import build_adapter
from cryptonorm.pipeline.producer import EventProducer


async def _run_adapter(
    adapter: ExchangeAdapter,
    producer: EventProducer,
    stats: Counter[str],
    settings: Settings,
    stop_event: asyncio.Event,
) -> None:
    log = get_logger("ingest").bind(exchange=adapter.name.value)
    stream = reconnecting_stream(
        adapter,
        settings.reconnect_base_seconds,
        settings.reconnect_max_seconds,
        stop_event=stop_event,
    )
    async for frame in stream:
        try:
            for ev in adapter.normalize(frame):
                await producer.publish(ev)
                stats[ev.event_type] += 1
                if isinstance(ev, BookSnapshot):
                    log.info("book_snapshot", symbol=ev.symbol, levels=len(ev.bids) + len(ev.asks))
        except Exception as exc:
            log.error("normalize/publish failed", error=repr(exc))


async def _stats_logger(stats: Counter[str], interval: float = 5.0) -> None:
    log = get_logger("ingest")
    while True:
        await asyncio.sleep(interval)
        log.info("produced (cumulative)", **dict(stats))


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, json=settings.log_json)
    log = get_logger("ingest-svc")
    log.info("starting", exchanges=[e.value for e in settings.exchanges], symbols=settings.symbols)

    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    producer = EventProducer(settings)
    await producer.start()

    stats: Counter[str] = Counter()
    adapters = [build_adapter(exch, settings.symbols, settings) for exch in settings.exchanges]
    tasks = [
        asyncio.create_task(
            _run_adapter(a, producer, stats, settings, stop_event), name=a.name.value
        )
        for a in adapters
    ]
    stats_task = asyncio.create_task(_stats_logger(stats))

    try:
        await stop_event.wait()
    finally:
        log.info("draining")
        for t in (*tasks, stats_task):
            t.cancel()
        await asyncio.gather(*tasks, stats_task, return_exceptions=True)
        await producer.stop()
        log.info("ingest stopped")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
