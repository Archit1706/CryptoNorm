"""(De)serialization between normalized events and Kafka bytes.

Kept separate from the Kafka clients so the wire encoding is unit-testable
without a broker. JSON via pydantic: Decimals ride as strings (no float
drift), and the discriminated union reconstructs the correct concrete type.
"""

from __future__ import annotations

from pydantic import TypeAdapter

from cryptonorm.common.schemas import NormalizedEvent

_ADAPTER: TypeAdapter[NormalizedEvent] = TypeAdapter(NormalizedEvent)


def serialize(event: NormalizedEvent) -> bytes:
    return event.model_dump_json().encode("utf-8")


def deserialize(raw: bytes) -> NormalizedEvent:
    return _ADAPTER.validate_json(raw)


def key_for(event: NormalizedEvent) -> bytes:
    """Partition key: canonical symbol keeps a symbol's events ordered."""
    return event.symbol.encode("utf-8")
