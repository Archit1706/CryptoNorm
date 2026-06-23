"""Threshold-based alerting.

Pure evaluation over the current state so it is unit-testable and the API can
recompute it on every dashboard push. Thresholds come from config (env vars).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from cryptonorm.common.config import Settings


@dataclass(frozen=True)
class Alert:
    level: str  # "critical" | "warning"
    kind: str  # "drawdown" | "exposure" | "spread" | "staleness"
    message: str


def evaluate_alerts(
    *,
    drawdown: Decimal,
    exposures: list[tuple[str, Decimal]],  # (symbol, net_notional)
    spreads: list[tuple[str, Decimal]],  # (symbol, spread_bps)
    feeds: list[tuple[str, str, str]],  # (exchange, symbol, state)
    settings: Settings,
) -> list[Alert]:
    alerts: list[Alert] = []

    if drawdown > Decimal(str(settings.alert_max_drawdown_usd)):
        alerts.append(Alert(
            "critical", "drawdown",
            f"Drawdown ${drawdown:,.2f} exceeds limit ${settings.alert_max_drawdown_usd:,.0f}",
        ))

    exposure_limit = Decimal(str(settings.alert_max_exposure_usd))
    for symbol, notional in exposures:
        if abs(notional) > exposure_limit:
            msg = (f"{symbol} exposure ${notional:,.0f} exceeds limit "
                   f"${settings.alert_max_exposure_usd:,.0f}")
            alerts.append(Alert("warning", "exposure", msg))

    spread_limit = Decimal(str(settings.alert_spread_bps))
    for symbol, bps in spreads:
        if bps > spread_limit:
            msg = (f"{symbol} cross-exchange spread {bps:.1f} bps exceeds "
                   f"{settings.alert_spread_bps:.0f} bps")
            alerts.append(Alert("warning", "spread", msg))

    for exchange, symbol, state in feeds:
        if state == "STALE":
            alerts.append(Alert("critical", "staleness", f"{exchange} {symbol} feed is STALE"))

    return alerts
