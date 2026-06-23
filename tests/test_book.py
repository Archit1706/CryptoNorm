"""L2 order-book maintenance, BBO, level removal, and gap detection."""

from __future__ import annotations

from decimal import Decimal

from cryptonorm.common.schemas import Exchange, PriceLevel
from cryptonorm.risk.book import L2Book


def pl(price: str, size: str) -> PriceLevel:
    return PriceLevel(price=Decimal(price), size=Decimal(size))


def _seeded() -> L2Book:
    book = L2Book(Exchange.COINBASE, "BTC-USD")
    book.apply_snapshot(
        bids=[pl("100", "1"), pl("99", "2"), pl("98", "3")],
        asks=[pl("101", "1"), pl("102", "2"), pl("103", "3")],
        sequence=10,
    )
    return book


def test_snapshot_sets_bbo_and_mid():
    book = _seeded()
    assert book.ready
    assert book.best_bid() == pl("100", "1")
    assert book.best_ask() == pl("101", "1")
    assert book.mid() == Decimal("100.5")


def test_delta_updates_size_and_removes_on_zero():
    book = _seeded()
    contiguous = book.apply_delta(
        bids=[pl("100", "0")],  # remove top bid
        asks=[pl("100.5", "5")],  # insert new best ask
        first_sequence=11,
        last_sequence=11,
    )
    assert contiguous is True
    assert book.best_bid() == pl("99", "2")  # 100 gone
    assert book.best_ask() == pl("100.5", "5")  # tighter ask


def test_sequence_gap_detected():
    book = _seeded()  # last_sequence == 10
    # expected next first_sequence is 11; 13 means we missed 11,12
    assert book.apply_delta([], [], first_sequence=13, last_sequence=13) is False


def test_contiguous_sequence_ok():
    book = _seeded()
    assert book.apply_delta([], [], first_sequence=11, last_sequence=11) is True


def test_top_n_ordering():
    book = _seeded()
    bids, asks = book.top(2)
    assert [b.price for b in bids] == [Decimal("100"), Decimal("99")]  # high -> low
    assert [a.price for a in asks] == [Decimal("101"), Decimal("102")]  # low -> high
