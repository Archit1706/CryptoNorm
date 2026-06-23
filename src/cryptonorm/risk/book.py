"""In-memory L2 order book maintained from snapshot + delta events.

A ``SortedDict`` per side gives O(log n) level updates and O(1) best-price
peeks, which matters because a busy book takes hundreds of deltas/second.

Sequence tracking lives here too: ``apply_delta`` returns whether the new
message was contiguous with the last one. Phase 3 wires the gap signal to a
re-snapshot; in phase 2 the consumer just records it.
"""

from __future__ import annotations

from decimal import Decimal

from sortedcontainers import SortedDict

from cryptonorm.common.schemas import Exchange, PriceLevel


class L2Book:
    def __init__(self, exchange: Exchange, symbol: str):
        self.exchange = exchange
        self.symbol = symbol
        self._bids: SortedDict = SortedDict()  # price -> size, ascending keys
        self._asks: SortedDict = SortedDict()
        self.last_sequence: int | None = None
        self.ready: bool = False

    def apply_snapshot(
        self, bids: list[PriceLevel], asks: list[PriceLevel], sequence: int | None
    ) -> None:
        self._bids.clear()
        self._asks.clear()
        for lvl in bids:
            if lvl.size > 0:
                self._bids[lvl.price] = lvl.size
        for lvl in asks:
            if lvl.size > 0:
                self._asks[lvl.price] = lvl.size
        self.last_sequence = sequence
        self.ready = True

    def apply_delta(
        self,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
        first_sequence: int | None,
        last_sequence: int | None,
    ) -> bool:
        """Apply a delta; return True if it was contiguous with the prior message.

        A False return means a sequence gap (lost messages) — the caller
        should resync from a fresh snapshot.
        """
        contiguous = self._is_contiguous(first_sequence)
        self._apply_side(self._bids, bids)
        self._apply_side(self._asks, asks)
        self.last_sequence = last_sequence
        return contiguous

    def _is_contiguous(self, first_sequence: int | None) -> bool:
        if self.last_sequence is None or first_sequence is None:
            return True  # can't tell -> assume ok
        return first_sequence == self.last_sequence + 1

    @staticmethod
    def _apply_side(side: SortedDict, levels: list[PriceLevel]) -> None:
        for lvl in levels:
            if lvl.size == 0:
                side.pop(lvl.price, None)
            else:
                side[lvl.price] = lvl.size

    def best_bid(self) -> PriceLevel | None:
        if not self._bids:
            return None
        price, size = self._bids.peekitem(-1)  # highest bid
        return PriceLevel(price=price, size=size)

    def best_ask(self) -> PriceLevel | None:
        if not self._asks:
            return None
        price, size = self._asks.peekitem(0)  # lowest ask
        return PriceLevel(price=price, size=size)

    def mid(self) -> Decimal | None:
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None:
            return None
        return (bid.price + ask.price) / 2

    def top(self, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]:
        """Top ``n`` levels each side: bids high->low, asks low->high."""
        bids = [
            PriceLevel(price=p, size=s)
            for p, s in reversed(self._bids.items()[-n:])
        ]
        asks = [PriceLevel(price=p, size=s) for p, s in self._asks.items()[:n]]
        return bids, asks

    def __len__(self) -> int:
        return len(self._bids) + len(self._asks)
