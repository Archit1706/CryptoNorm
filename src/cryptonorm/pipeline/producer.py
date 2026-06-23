"""Async Kafka producer for normalized events."""

from __future__ import annotations

from types import TracebackType

from aiokafka import AIOKafkaProducer

from cryptonorm.common.config import Settings
from cryptonorm.common.logging import get_logger
from cryptonorm.common.schemas import NormalizedEvent
from cryptonorm.pipeline.serde import key_for, serialize
from cryptonorm.pipeline.topics import topic_for


class EventProducer:
    """Publishes normalized events to their topics, keyed by symbol.

    Uses batched ``send`` (not ``send_and_wait``) for throughput; the broker
    batches and ``stop()`` flushes outstanding messages on shutdown.
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self._log = get_logger("producer")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap,
            acks=1,
            linger_ms=20,
            enable_idempotence=False,
            max_request_size=settings.max_message_bytes,  # full book snapshots
        )

    async def start(self) -> None:
        await self._producer.start()
        self._log.info("producer started", bootstrap=self._settings.kafka_bootstrap)

    async def stop(self) -> None:
        await self._producer.flush()
        await self._producer.stop()
        self._log.info("producer stopped")

    async def publish(self, event: NormalizedEvent) -> None:
        topic = topic_for(event, self._settings)
        await self._producer.send(topic, value=serialize(event), key=key_for(event))

    async def __aenter__(self) -> EventProducer:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()
