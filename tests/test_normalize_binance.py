"""Binance normalization, driven by recorded live frames."""

from __future__ import annotations

from decimal import Decimal

from cryptonorm.common.schemas import BookDelta, BookSnapshot, Exchange, Side, Trade
from cryptonorm.normalize.binance import normalize_frame


def test_rest_snapshot_normalizes(load_fixture):
    frame = load_fixture("binance_rest_snapshot.json")
    frame["_kind"] = "snapshot"  # added by the adapter
    frame["_symbol"] = "BTCUSD"
    events = normalize_frame(frame)
    assert len(events) == 1
    snap = events[0]
    assert isinstance(snap, BookSnapshot)
    assert snap.exchange is Exchange.BINANCE
    assert snap.symbol == "BTC-USD"
    assert snap.sequence == frame["lastUpdateId"]
    assert snap.bids and snap.asks
    assert snap.bids[0].price == Decimal(frame["bids"][0][0])


def test_depth_update_is_delta(load_fixture):
    delta = normalize_frame(load_fixture("binance_depth_update.json"))[0]
    assert isinstance(delta, BookDelta)
    assert delta.symbol == "BTC-USD"
    assert delta.first_sequence is not None and delta.sequence is not None
    assert delta.first_sequence <= delta.sequence  # U <= u


def test_trade_aggressor_from_maker_flag(load_fixture):
    frame = load_fixture("binance_trade.json")
    trade = normalize_frame(frame)[0]
    assert isinstance(trade, Trade)
    # m == false in the fixture -> buyer is taker -> BUY
    assert frame["m"] is False
    assert trade.aggressor is Side.BUY
    assert trade.price == Decimal(frame["p"])


def test_buyer_maker_means_sell_aggressor(load_fixture):
    frame = load_fixture("binance_trade.json")
    frame["m"] = True  # buyer is maker -> seller is the aggressor
    trade = normalize_frame(frame)[0]
    assert trade.aggressor is Side.SELL
