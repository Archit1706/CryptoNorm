"""Threshold-based alert evaluation."""

from __future__ import annotations

from decimal import Decimal

from cryptonorm.common.config import Settings
from cryptonorm.risk.alerts import evaluate_alerts

D = Decimal


def _settings() -> Settings:
    # explicit thresholds so the test is independent of env/.env
    return Settings(
        alert_max_drawdown_usd=5000,
        alert_max_exposure_usd=50000,
        alert_spread_bps=50,
    )


def test_no_alerts_when_within_limits():
    alerts = evaluate_alerts(
        drawdown=D("100"),
        exposures=[("BTC-USD", D("1000"))],
        spreads=[("BTC-USD", D("5"))],
        feeds=[("coinbase", "BTC-USD", "OK")],
        settings=_settings(),
    )
    assert alerts == []


def test_drawdown_breach_is_critical():
    alerts = evaluate_alerts(
        drawdown=D("6000"), exposures=[], spreads=[], feeds=[], settings=_settings()
    )
    assert len(alerts) == 1
    assert alerts[0].level == "critical" and alerts[0].kind == "drawdown"


def test_exposure_and_spread_warn():
    alerts = evaluate_alerts(
        drawdown=D("0"),
        exposures=[("BTC-USD", D("-60000"))],  # abs exceeds 50k
        spreads=[("ETH-USD", D("80"))],  # exceeds 50 bps
        feeds=[],
        settings=_settings(),
    )
    kinds = {a.kind for a in alerts}
    assert kinds == {"exposure", "spread"}
    assert all(a.level == "warning" for a in alerts)


def test_stale_feed_is_critical():
    alerts = evaluate_alerts(
        drawdown=D("0"), exposures=[], spreads=[],
        feeds=[("kraken", "BTC-USD", "STALE"), ("coinbase", "BTC-USD", "OK")],
        settings=_settings(),
    )
    assert len(alerts) == 1
    assert alerts[0].kind == "staleness" and alerts[0].level == "critical"
