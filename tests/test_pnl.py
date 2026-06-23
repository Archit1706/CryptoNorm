"""Risk engine: P&L aggregation, exposure, drawdown, and VaR."""

from __future__ import annotations

from decimal import Decimal

from cryptonorm.common.schemas import Exchange, PaperFill, Side
from cryptonorm.common.types import utcnow
from cryptonorm.risk.pnl import RiskEngine

D = Decimal


def _fill(exchange: Exchange, symbol: str, side: Side, price: str, size: str, fee: str = "0"):
    return PaperFill(
        exchange=exchange,
        symbol=symbol,
        exchange_symbol=symbol,
        exchange_ts=None,
        ingest_ts=utcnow(),
        sequence=1,
        order_id="t",
        side=side,
        price=D(price),
        size=D(size),
        fee=D(fee),
    )


def test_snapshot_pnl_and_exposure():
    eng = RiskEngine()
    eng.apply_fill(_fill(Exchange.COINBASE, "BTC-USD", Side.BUY, "100", "2", fee="1"))
    snap = eng.snapshot({(Exchange.COINBASE, "BTC-USD"): D("110")})
    assert snap.unrealized_pnl == D("20")  # 2 * (110 - 100)
    assert snap.fees == D("1")
    assert snap.total_pnl == D("19")  # realized(0) + unrealized(20) - fees(1)
    assert snap.gross_notional == D("220")  # 2 * 110
    (exp,) = snap.exposures
    assert exp.symbol == "BTC-USD" and exp.net_qty == D("2")


def test_exposure_nets_across_venues():
    eng = RiskEngine()
    eng.apply_fill(_fill(Exchange.COINBASE, "BTC-USD", Side.BUY, "100", "3"))
    eng.apply_fill(_fill(Exchange.BINANCE, "BTC-USD", Side.SELL, "100", "1"))
    snap = eng.snapshot({
        (Exchange.COINBASE, "BTC-USD"): D("100"),
        (Exchange.BINANCE, "BTC-USD"): D("100"),
    })
    (exp,) = snap.exposures
    assert exp.net_qty == D("2")  # +3 long coinbase, -1 short binance


def test_drawdown_tracks_peak():
    eng = RiskEngine()
    eng.apply_fill(_fill(Exchange.COINBASE, "BTC-USD", Side.BUY, "100", "1"))
    eng.snapshot({(Exchange.COINBASE, "BTC-USD"): D("110")})  # equity +10 (peak)
    snap = eng.snapshot({(Exchange.COINBASE, "BTC-USD"): D("104")})  # equity +4
    assert snap.peak_equity == D("10")
    assert snap.drawdown == D("6")


def test_var_zero_until_enough_samples_then_positive():
    eng = RiskEngine(var_window=50, var_min_samples=5)
    eng.apply_fill(_fill(Exchange.COINBASE, "BTC-USD", Side.BUY, "100", "1"))
    key = (Exchange.COINBASE, "BTC-USD")
    # below the sample floor -> no VaR yet
    for mark in ("101", "102", "103"):
        snap = eng.snapshot({key: D(mark)})
    assert snap.var_95 == 0
    # feed a volatile series; VaR should become a positive loss magnitude
    for mark in ("90", "120", "85", "130", "80", "140"):
        snap = eng.snapshot({key: D(mark)})
    assert snap.var_95 > 0
