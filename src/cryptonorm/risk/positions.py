"""Position keeping with average-cost accounting.

A position is tracked per (exchange, symbol) because P&L is marked at the
venue the position was filled on. Applying a fill realizes P&L on any
quantity that reduces or flips the position; the remainder updates the
volume-weighted average entry price.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from cryptonorm.common.schemas import Exchange, PaperFill, Side

_ZERO = Decimal("0")


@dataclass
class Position:
    net_qty: Decimal = _ZERO  # signed: >0 long, <0 short
    avg_price: Decimal = _ZERO  # average entry of the open quantity
    realized_pnl: Decimal = _ZERO
    fees: Decimal = _ZERO

    def apply(self, side: Side, price: Decimal, qty: Decimal, fee: Decimal = _ZERO) -> None:
        """Apply a fill, realizing P&L on any reduced/flipped quantity."""
        self.fees += fee
        signed = qty if side is Side.BUY else -qty

        if self.net_qty == 0 or (self.net_qty > 0) == (signed > 0):
            # opening or adding in the same direction -> reweight average
            total = self.net_qty + signed
            if total != 0:
                self.avg_price = (self.avg_price * self.net_qty + price * signed) / total
            self.net_qty = total
            return

        # opposite direction: realize against the open position
        direction = Decimal(1) if self.net_qty > 0 else Decimal(-1)
        closed = min(abs(signed), abs(self.net_qty))
        self.realized_pnl += (price - self.avg_price) * closed * direction

        remainder = abs(signed) - abs(self.net_qty)
        if remainder > 0:
            # flipped through zero: open the leftover at the fill price
            self.net_qty = (Decimal(1) if signed > 0 else Decimal(-1)) * remainder
            self.avg_price = price
        else:
            self.net_qty += signed
            if self.net_qty == 0:
                self.avg_price = _ZERO

    def unrealized_pnl(self, mark: Decimal) -> Decimal:
        return self.net_qty * (mark - self.avg_price)


@dataclass
class PositionBook:
    """All positions, keyed by (exchange, symbol)."""

    positions: dict[tuple[Exchange, str], Position] = field(default_factory=dict)

    def apply_fill(self, fill: PaperFill) -> None:
        key = (fill.exchange, fill.symbol)
        self.positions.setdefault(key, Position()).apply(
            fill.side, fill.price, fill.size, fill.fee
        )

    def net_quantities(self) -> dict[tuple[Exchange, str], Decimal]:
        return {k: p.net_qty for k, p in self.positions.items()}
