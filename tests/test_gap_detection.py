"""Per-exchange order-book gap detection."""

from __future__ import annotations

import pytest

from cryptonorm.common.errors import FeedGapError
from cryptonorm.ingest.binance import BinanceAdapter


def _frame(U: int, u: int) -> dict:
    return {"e": "depthUpdate", "s": "BTCUSD", "U": U, "u": u, "b": [], "a": []}


def test_binance_drops_stale_diffs_then_syncs():
    last = {"BTCUSD": 100}
    prev: dict[str, int | None] = {"BTCUSD": None}
    check = BinanceAdapter._check_depth_continuity

    # diff entirely older than the snapshot -> dropped, still unsynced
    check("BTCUSD", _frame(90, 99), last, prev)
    assert prev["BTCUSD"] is None

    # diff straddling lastUpdateId+1 (101) -> first valid, now synced
    check("BTCUSD", _frame(100, 105), last, prev)
    assert prev["BTCUSD"] == 105


def test_binance_contiguous_after_sync():
    last = {"BTCUSD": 100}
    prev: dict[str, int | None] = {"BTCUSD": 105}
    BinanceAdapter._check_depth_continuity("BTCUSD", _frame(106, 110), last, prev)
    assert prev["BTCUSD"] == 110


def test_binance_gap_after_sync_raises():
    last = {"BTCUSD": 100}
    prev: dict[str, int | None] = {"BTCUSD": 105}
    with pytest.raises(FeedGapError):
        BinanceAdapter._check_depth_continuity("BTCUSD", _frame(108, 112), last, prev)


def test_binance_stale_snapshot_raises():
    last = {"BTCUSD": 100}
    prev: dict[str, int | None] = {"BTCUSD": None}
    # first diff begins past lastUpdateId+1 -> snapshot too old to bridge
    with pytest.raises(FeedGapError):
        BinanceAdapter._check_depth_continuity("BTCUSD", _frame(103, 108), last, prev)
