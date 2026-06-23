"""Average-cost position keeping and realized/unrealized P&L."""

from __future__ import annotations

from decimal import Decimal

from cryptonorm.common.schemas import Side
from cryptonorm.risk.positions import Position

D = Decimal


def test_open_and_add_reweights_average():
    pos = Position()
    pos.apply(Side.BUY, D("100"), D("10"))
    assert pos.net_qty == D("10") and pos.avg_price == D("100")
    pos.apply(Side.BUY, D("110"), D("10"))
    assert pos.net_qty == D("20")
    assert pos.avg_price == D("105")  # (100*10 + 110*10)/20
    assert pos.realized_pnl == 0


def test_partial_close_realizes_pnl():
    pos = Position()
    pos.apply(Side.BUY, D("100"), D("20"))
    pos.apply(Side.SELL, D("120"), D("5"))
    assert pos.realized_pnl == D("100")  # (120-100)*5
    assert pos.net_qty == D("15")
    assert pos.avg_price == D("100")  # unchanged on a reduce


def test_full_close_flattens():
    pos = Position()
    pos.apply(Side.BUY, D("100"), D("10"))
    pos.apply(Side.SELL, D("90"), D("10"))
    assert pos.net_qty == 0
    assert pos.avg_price == 0
    assert pos.realized_pnl == D("-100")  # (90-100)*10


def test_flip_long_to_short():
    pos = Position()
    pos.apply(Side.BUY, D("100"), D("10"))
    pos.apply(Side.SELL, D("120"), D("25"))  # close 10, open 15 short
    assert pos.realized_pnl == D("200")  # (120-100)*10
    assert pos.net_qty == D("-15")
    assert pos.avg_price == D("120")  # remainder opened at fill price


def test_short_cover_realizes_correctly():
    pos = Position()
    pos.apply(Side.SELL, D("100"), D("10"))  # open short
    assert pos.net_qty == D("-10") and pos.avg_price == D("100")
    pos.apply(Side.BUY, D("90"), D("4"))  # cover 4 at a profit
    assert pos.realized_pnl == D("40")  # short: (100-90)*4
    assert pos.net_qty == D("-6")


def test_unrealized_pnl_sign():
    long_pos = Position()
    long_pos.apply(Side.BUY, D("100"), D("10"))
    assert long_pos.unrealized_pnl(D("105")) == D("50")

    short_pos = Position()
    short_pos.apply(Side.SELL, D("100"), D("10"))
    assert short_pos.unrealized_pnl(D("90")) == D("100")  # short gains as price falls


def test_fees_accumulate():
    pos = Position()
    pos.apply(Side.BUY, D("100"), D("1"), fee=D("0.10"))
    pos.apply(Side.SELL, D("101"), D("1"), fee=D("0.10"))
    assert pos.fees == D("0.20")
