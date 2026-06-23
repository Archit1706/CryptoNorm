"""Position reconciliation.

Cross-checks the risk engine's computed net positions (derived by consuming
the fills topic from Kafka) against the simulator's source-of-truth ledger
(written directly when each fill is created). A mismatch means the pipeline
lost or mis-applied a fill — exactly what a desk's recon job exists to catch.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from cryptonorm.common.schemas import Exchange

Key = tuple[Exchange, str]
_TOL = Decimal("0.00000001")


@dataclass(frozen=True)
class ReconLine:
    exchange: Exchange
    symbol: str
    computed_qty: Decimal
    ledger_qty: Decimal
    matched: bool

    @property
    def diff(self) -> Decimal:
        return self.computed_qty - self.ledger_qty


def reconcile(
    computed: dict[Key, Decimal], ledger: dict[Key, Decimal], tolerance: Decimal = _TOL
) -> list[ReconLine]:
    """Compare net quantities across the union of keys."""
    lines: list[ReconLine] = []
    for key in sorted(set(computed) | set(ledger), key=lambda k: (k[0].value, k[1])):
        c = computed.get(key, Decimal("0"))
        led = ledger.get(key, Decimal("0"))
        lines.append(ReconLine(key[0], key[1], c, led, abs(c - led) <= tolerance))
    return lines
