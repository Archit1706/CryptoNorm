"""Async Kafka consumer that yields normalized events."""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import TracebackType

from aiokafka import AIOKafkaConsumer

from cryptonorm.common.config import Settings
from cryptonorm.common.logging import get_logger
from cryptonorm.common.schemas import NormalizedEvent
from cryptonorm.pipeline.serde import deserialize


class EventConsumer:
    """Subscribes to topics and yields decoded ``NormalizedEvent`` objects."""

    def __init__(self, settings: Settings, topics: list[str], group_id: str | None = None):
        self._settings = settings
        self._topics = topics
        self._log = get_logger("consumer").bind(topics=topics)
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=group_id or settings.consumer_group,
            # earliest so we pick up the one-time book snapshot a producer
            # emitted at subscribe; phase 3 adds REST resync for mid-stream starts.
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            max_partition_fetch_bytes=settings.max_message_bytes,  # full book snapshots
        )

    async def start(self) -> None:
        await self._consumer.start()
        self._log.info("consumer started", group=self._consumer._group_id)

    async def stop(self) -> None:
        await self._consumer.stop()
        self._log.info("consumer stopped")

    async def events(self) -> AsyncIterator[NormalizedEvent]:
        """Yield decoded events. Frames that fail to decode are logged and skipped."""
        async for msg in self._consumer:
            try:
                yield deserialize(msg.value)
            except Exception as exc:
                self._log.error("decode failed", topic=msg.topic, error=str(exc))

    async def __aenter__(self) -> EventConsumer:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()
