"""Kraken normalization, driven by recorded live frames."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from cryptonorm.common.schemas import BookDelta, BookSnapshot, Exchange, Side, Trade
from cryptonorm.normalize.kraken import normalize_frame

_FIXTURES = Path(__file__).parent / "fixtures"


def _load_decimal(name: str) -> dict:
    # Adapters parse Kraken frames with parse_float=Decimal; mirror that here.
    return json.loads((_FIXTURES / name).read_text(), parse_float=Decimal)


def test_book_snapshot_normalizes():
    snap = normalize_frame(_load_decimal("kraken_book_snapshot.json"))[0]
    assert isinstance(snap, BookSnapshot)
    assert snap.exchange is Exchange.KRAKEN
    assert snap.symbol == "BTC-USD"
    assert snap.exchange_symbol == "BTC/USD"
    assert len(snap.bids) == 10 and len(snap.asks) == 10
    assert all(isinstance(lvl.price, Decimal) for lvl in snap.bids)


def test_book_update_is_delta():
    delta = normalize_frame(_load_decimal("kraken_book_update.json"))[0]
    assert isinstance(delta, BookDelta)
    assert delta.symbol == "BTC-USD"
    assert delta.sequence is None  # Kraken has no per-message sequence


def test_trades_expand_to_multiple():
    events = normalize_frame(_load_decimal("kraken_trade.json"))
    assert len(events) == 2  # fixture carries two trades
    assert all(isinstance(e, Trade) for e in events)
    first = events[0]
    assert first.aggressor is Side.SELL
    assert first.symbol == "BTC-USD"
