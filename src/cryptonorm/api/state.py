"""Assemble the consolidated dashboard state from Redis.

Reads the current-state cache the pipeline maintains, groups BBOs by symbol
across venues, computes cross-exchange spreads, and evaluates alerts. The
result is a JSON-serializable dict pushed to the browser over WebSocket.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from cryptonorm.common.config import Settings
from cryptonorm.common.types import utcnow
from cryptonorm.pipeline.redis_state import RedisState
from cryptonorm.risk.alerts import evaluate_alerts
from cryptonorm.risk.spread import compute_spread


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


async def build_state(state: RedisState, settings: Settings) -> dict[str, Any]:
    feeds = [tuple(f.split(":", 1)) for f in await state.list_feeds()]  # (exchange, symbol)

    venues: dict[str, list[dict[str, Any]]] = {}
    feed_status: list[tuple[str, str, str]] = []
    mids_by_symbol: dict[str, dict[str, Decimal]] = {}

    for exchange, symbol in feeds:
        bbo = await state.get(f"cn:bbo:{exchange}:{symbol}")
        status = await state.get(f"cn:status:{exchange}:{symbol}")
        trade = await state.get(f"cn:trade:{exchange}:{symbol}")
        state_label = (status or {}).get("state", "OK")
        feed_status.append((exchange, symbol, state_label))

        venues.setdefault(symbol, []).append({
            "exchange": exchange,
            "bid": (bbo or {}).get("bid"),
            "ask": (bbo or {}).get("ask"),
            "mid": (bbo or {}).get("mid"),
            "last_trade": (trade or {}).get("price"),
            "state": state_label,
            "age_seconds": (status or {}).get("age_seconds"),
        })
        mid = _dec((bbo or {}).get("mid"))
        if mid is not None:
            mids_by_symbol.setdefault(symbol, {})[exchange] = mid

    spreads: list[dict[str, Any]] = []
    spread_bps_pairs: list[tuple[str, Decimal]] = []
    for symbol, mids in sorted(mids_by_symbol.items()):
        info = compute_spread(mids)
        if info is None:
            continue
        spreads.append({
            "symbol": symbol,
            "spread_usd": str(info.spread_usd),
            "spread_bps": str(info.spread_bps),
            "buy_venue": info.low_exchange,
            "sell_venue": info.high_exchange,
        })
        spread_bps_pairs.append((symbol, info.spread_bps))

    risk = await state.get("cn:risk:snapshot")
    recon = await state.get("cn:recon")

    drawdown = _dec((risk or {}).get("drawdown")) or Decimal(0)
    exposures_payload = (risk or {}).get("exposures", [])
    exposure_pairs = [
        (e["symbol"], _dec(e.get("net_notional")) or Decimal(0)) for e in exposures_payload
    ]

    alerts = evaluate_alerts(
        drawdown=drawdown,
        exposures=exposure_pairs,
        spreads=spread_bps_pairs,
        feeds=feed_status,
        settings=settings,
    )

    return {
        "ts": utcnow().isoformat(),
        "risk": risk,
        "venues": venues,
        "spreads": spreads,
        "feeds": [{"exchange": e, "symbol": s, "state": st} for e, s, st in feed_status],
        "recon": recon,
        "alerts": [{"level": a.level, "kind": a.kind, "message": a.message} for a in alerts],
        "thresholds": {
            "max_drawdown_usd": settings.alert_max_drawdown_usd,
            "max_exposure_usd": settings.alert_max_exposure_usd,
            "spread_bps": settings.alert_spread_bps,
        },
    }
