"""Kafka (de)serialization + topic routing, no broker required."""

from __future__ import annotations

import pytest

from cryptonorm.common.config import Settings
from cryptonorm.normalize.coinbase import normalize_frame
from cryptonorm.pipeline.serde import deserialize, key_for, serialize
from cryptonorm.pipeline.topics import topic_for

_FIXTURES = [
    "coinbase_l2_snapshot.json",
    "coinbase_l2_update.json",
    "coinbase_trade.json",
]


@pytest.mark.parametrize("fixture", _FIXTURES)
def test_event_survives_kafka_roundtrip(load_fixture, fixture):
    event = normalize_frame(load_fixture(fixture))[0]
    restored = deserialize(serialize(event))
    assert restored == event  # pydantic compares by type + field values
    assert key_for(event) == event.symbol.encode()


def test_topic_routing(load_fixture):
    settings = Settings()
    snap = normalize_frame(load_fixture("coinbase_l2_snapshot.json"))[0]
    delta = normalize_frame(load_fixture("coinbase_l2_update.json"))[0]
    trade = normalize_frame(load_fixture("coinbase_trade.json"))[0]
    assert topic_for(snap, settings) == "md.book.v1"
    assert topic_for(delta, settings) == "md.book.v1"
    assert topic_for(trade, settings) == "md.trade.v1"
