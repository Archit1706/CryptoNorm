"""Kraken CRC32 book-checksum: validated against a recorded live snapshot."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from cryptonorm.common.errors import FeedGapError
from cryptonorm.ingest.kraken import KrakenAdapter, _ShadowBook

_FIXTURES = Path(__file__).parent / "fixtures"


def _snapshot() -> dict:
    return json.loads(
        (_FIXTURES / "kraken_book_snapshot.json").read_text(), parse_float=Decimal
    )


def test_checksum_matches_live_snapshot():
    data = _snapshot()["data"][0]
    book = _ShadowBook()
    book.apply(data["bids"], data["asks"], depth=10)
    # BTC/USD precision: price 1, qty 8 (from Kraken AssetPairs)
    assert book.checksum(10, 1, 8) == data["checksum"]


def test_validate_book_raises_on_bad_checksum():
    frame = _snapshot()
    frame["data"][0]["checksum"] = 1  # corrupt it
    adapter = KrakenAdapter(["BTC-USD"])
    books = {"BTC/USD": _ShadowBook()}
    with pytest.raises(FeedGapError):
        adapter._validate_book(frame, books, depth=10)


def test_validate_book_passes_on_good_checksum():
    frame = _snapshot()
    adapter = KrakenAdapter(["BTC-USD"])
    books = {"BTC/USD": _ShadowBook()}
    adapter._validate_book(frame, books, depth=10)  # must not raise


def test_shadow_book_trims_to_depth():
    """Kraken pushes levels out of the window without removals; we must trim."""
    book = _ShadowBook()
    bids = [{"price": p, "qty": 1} for p in (100, 99, 98)]
    asks = [{"price": p, "qty": 1} for p in (101, 102, 103)]
    book.apply(bids, asks, depth=2)
    assert len(book.bids) == 2 and len(book.asks) == 2
    assert max(book.bids) == 100 and min(book.bids) == 99  # kept best 2 bids
    assert min(book.asks) == 101 and max(book.asks) == 102  # kept best 2 asks
