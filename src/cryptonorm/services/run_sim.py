"""Paper-fill simulator service: emit simulated fills -> Kafka + ledger.

  python -m cryptonorm.services.run_sim

Requires the pipeline running (it reads live BBO from Redis to price fills).
"""

from __future__ import annotations

import asyncio
import contextlib

from cryptonorm.common.config import get_settings
from cryptonorm.common.logging import configure_logging, get_logger
from cryptonorm.common.shutdown import install_signal_handlers
from cryptonorm.pipeline.producer import EventProducer
from cryptonorm.pipeline.redis_state import RedisState
from cryptonorm.sim.paper_fills import PaperFillSimulator


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, json=settings.log_json)
    log = get_logger("sim-svc")
    log.info("starting", exchanges=[e.value for e in settings.exchanges], symbols=settings.symbols)

    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    state = RedisState(settings.redis_url)
    await state.ping()
    producer = EventProducer(settings)
    await producer.start()

    simulator = PaperFillSimulator(settings, state, producer)
    sim_task = asyncio.create_task(simulator.run(stop_event))

    try:
        await stop_event.wait()
    finally:
        log.info("draining")
        sim_task.cancel()
        await asyncio.gather(sim_task, return_exceptions=True)
        await producer.stop()
        await state.close()
        log.info("sim stopped")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
