"""Risk engine: marks positions to market and derives P&L and risk.

Positions are marked at the mid of the venue they were filled on (the
design choice from Phase 0). Drawdown and a simple historical VaR are
derived from a rolling window of total-equity (P&L) samples.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal

from cryptonorm.common.schemas import Exchange, PaperFill
from cryptonorm.risk.positions import PositionBook

_ZERO = Decimal("0")

Key = tuple[Exchange, str]


@dataclass(frozen=True)
class PositionView:
    exchange: Exchange
    symbol: str
    net_qty: Decimal
    avg_price: Decimal
    mark: Decimal | None
    realized_pnl: Decimal
    unrealized_pnl: Decimal


@dataclass(frozen=True)
class AssetExposure:
    symbol: str
    net_qty: Decimal
    net_notional: Decimal  # signed, marked


@dataclass(frozen=True)
class RiskSnapshot:
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    fees: Decimal
    total_pnl: Decimal  # realized + unrealized - fees (== equity)
    gross_notional: Decimal
    peak_equity: Decimal
    drawdown: Decimal  # peak_equity - total_pnl, >= 0
    var_95: Decimal  # 1-period 95% historical VaR (positive = loss)
    positions: list[PositionView] = field(default_factory=list)
    exposures: list[AssetExposure] = field(default_factory=list)


def _percentile(sorted_vals: list[Decimal], pct: float) -> Decimal:
    """Nearest-rank percentile of an already-sorted list."""
    if not sorted_vals:
        return _ZERO
    rank = max(0, min(len(sorted_vals) - 1, round(pct / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[rank]


class RiskEngine:
    def __init__(self, var_window: int = 120, var_min_samples: int = 20):
        self.book = PositionBook()
        self._peak_equity: Decimal | None = None
        self._equity: deque[Decimal] = deque(maxlen=var_window)
        self._var_min_samples = var_min_samples

    def apply_fill(self, fill: PaperFill) -> None:
        self.book.apply_fill(fill)

    def _var_95(self) -> Decimal:
        """Magnitude of the 5th-percentile period-over-period P&L change."""
        if len(self._equity) < self._var_min_samples:
            return _ZERO
        eq = list(self._equity)
        changes = sorted(eq[i] - eq[i - 1] for i in range(1, len(eq)))
        worst = _percentile(changes, 5.0)  # 5th percentile (a loss, typically < 0)
        return -worst if worst < 0 else _ZERO

    def snapshot(self, marks: dict[Key, Decimal]) -> RiskSnapshot:
        realized = unrealized = fees = gross = _ZERO
        positions: list[PositionView] = []
        per_asset_qty: dict[str, Decimal] = {}
        per_asset_notional: dict[str, Decimal] = {}

        for (exchange, symbol), pos in self.book.positions.items():
            mark = marks.get((exchange, symbol))
            upnl = pos.unrealized_pnl(mark) if mark is not None else _ZERO
            realized += pos.realized_pnl
            unrealized += upnl
            fees += pos.fees
            if mark is not None:
                notional = pos.net_qty * mark
                gross += abs(notional)
                per_asset_notional[symbol] = per_asset_notional.get(symbol, _ZERO) + notional
            per_asset_qty[symbol] = per_asset_qty.get(symbol, _ZERO) + pos.net_qty
            positions.append(
                PositionView(exchange, symbol, pos.net_qty, pos.avg_price, mark,
                             pos.realized_pnl, upnl)
            )

        total = realized + unrealized - fees
        self._equity.append(total)
        self._peak_equity = total if self._peak_equity is None else max(self._peak_equity, total)
        drawdown = self._peak_equity - total

        exposures = [
            AssetExposure(sym, qty, per_asset_notional.get(sym, _ZERO))
            for sym, qty in sorted(per_asset_qty.items())
        ]
        return RiskSnapshot(
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            fees=fees,
            total_pnl=total,
            gross_notional=gross,
            peak_equity=self._peak_equity,
            drawdown=drawdown,
            var_95=self._var_95(),
            positions=positions,
            exposures=exposures,
        )
