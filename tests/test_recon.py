"""Position reconciliation: computed (Kafka-derived) vs ledger (truth)."""

from __future__ import annotations

from decimal import Decimal

from cryptonorm.common.schemas import Exchange
from cryptonorm.risk.recon import reconcile

D = Decimal
BTC = (Exchange.COINBASE, "BTC-USD")
ETH = (Exchange.BINANCE, "ETH-USD")


def test_all_match():
    lines = reconcile({BTC: D("1.5"), ETH: D("-2")}, {BTC: D("1.5"), ETH: D("-2")})
    assert all(line.matched for line in lines)


def test_quantity_mismatch_flagged():
    lines = reconcile({BTC: D("1.5")}, {BTC: D("1.4")})
    (line,) = lines
    assert line.matched is False
    assert line.diff == D("0.1")


def test_missing_key_treated_as_zero():
    # computed has a position the ledger never recorded -> mismatch
    lines = reconcile({BTC: D("1")}, {})
    (line,) = lines
    assert line.ledger_qty == 0
    assert line.matched is False


def test_within_tolerance_matches():
    lines = reconcile({BTC: D("1.000000001")}, {BTC: D("1.0")}, tolerance=D("0.00001"))
    assert lines[0].matched is True
