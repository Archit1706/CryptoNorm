"""Normalization tests for Coinbase, driven by recorded live frames."""

from __future__ import annotations

from decimal import Decimal

from pydantic import TypeAdapter

from cryptonorm.common.schemas import (
    BookDelta,
    BookSnapshot,
    Exchange,
    NormalizedEvent,
    Side,
    Trade,
)
from cryptonorm.normalize.coinbase import normalize_frame

_ADAPTER: TypeAdapter[NormalizedEvent] = TypeAdapter(NormalizedEvent)


def test_snapshot_normalizes(load_fixture):
    events = normalize_frame(load_fixture("coinbase_l2_snapshot.json"))
    assert len(events) == 1
    snap = events[0]
    assert isinstance(snap, BookSnapshot)
    assert snap.exchange is Exchange.COINBASE
    assert snap.symbol == "BTC-USD"
    assert snap.exchange_symbol == "BTC-USD"
    assert snap.sequence == 0
    assert len(snap.bids) == 5
    assert len(snap.asks) == 5
    # "offer" levels land in asks, "bid" in bids
    assert snap.bids[0].price == Decimal("62416.02")
    assert snap.asks[0].price == Decimal("62416.03")
    # exact decimal, no float drift
    assert snap.bids[0].size == Decimal("0.11432512")
    assert snap.exchange_ts is not None


def test_update_is_book_delta(load_fixture):
    events = normalize_frame(load_fixture("coinbase_l2_update.json"))
    assert len(events) == 1
    delta = events[0]
    assert isinstance(delta, BookDelta)
    assert delta.sequence == 1
    assert delta.first_sequence == 1
    assert len(delta.bids) == 2
    assert len(delta.asks) == 0
    assert delta.bids[1].price == Decimal("62394.17")


def test_trade_normalizes(load_fixture):
    events = normalize_frame(load_fixture("coinbase_trade.json"))
    assert len(events) == 1
    trade = events[0]
    assert isinstance(trade, Trade)
    assert trade.symbol == "BTC-USD"
    assert trade.trade_id == "1042333849"
    assert trade.price == Decimal("62404.79")
    assert trade.size == Decimal("0.00000001")
    assert trade.aggressor is Side.BUY


def test_unknown_channel_ignored():
    assert normalize_frame({"channel": "subscriptions", "events": []}) == []
    assert normalize_frame({"channel": "heartbeats"}) == []


def test_decimal_survives_kafka_roundtrip(load_fixture):
    """Serializing to JSON (as over Kafka) and back must preserve Decimals."""
    import json

    trade = normalize_frame(load_fixture("coinbase_trade.json"))[0]
    wire = trade.model_dump_json()
    raw = json.loads(wire)
    # prices/sizes ride the wire as JSON *strings*, never floats — no drift.
    assert isinstance(raw["price"], str)
    assert isinstance(raw["size"], str)
    restored = _ADAPTER.validate_json(wire)
    assert isinstance(restored, Trade)
    assert restored.price == Decimal("62404.79")
    assert restored.size == Decimal("0.00000001")
