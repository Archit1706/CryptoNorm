"""Symbol mapping and time helpers shared across adapters.

Canonical symbols are ``BASE-QUOTE`` (e.g. ``BTC-USD``). Each exchange uses
its own raw form; the registry below is the single place that knows the
translation, so adding a venue or pair is a localized change.

Notes on real-venue quirks captured here:
  * binance.com is geoblocked (HTTP 451) in many regions, so we use
    binance.us, which has native USD pairs (``BTCUSD``) — no USDT proxy.
  * Kraken's WS v2 uses ``BTC/USD`` (its older v1 API used ``XBT/USD``).
"""

from __future__ import annotations

from datetime import UTC, datetime

from cryptonorm.common.schemas import Exchange

# canonical -> raw, per exchange
_CANONICAL_TO_RAW: dict[Exchange, dict[str, str]] = {
    Exchange.BINANCE: {"BTC-USD": "BTCUSD", "ETH-USD": "ETHUSD"},
    Exchange.COINBASE: {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"},
    Exchange.KRAKEN: {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"},
}

# raw -> canonical, derived (built once at import)
_RAW_TO_CANONICAL: dict[Exchange, dict[str, str]] = {
    exch: {raw: canon for canon, raw in mapping.items()}
    for exch, mapping in _CANONICAL_TO_RAW.items()
}


def to_raw_symbol(exchange: Exchange, canonical: str) -> str:
    """Map a canonical symbol to the exchange's raw symbol."""
    try:
        return _CANONICAL_TO_RAW[exchange][canonical]
    except KeyError as exc:
        raise KeyError(f"{exchange.value} has no mapping for {canonical!r}") from exc


def to_canonical_symbol(exchange: Exchange, raw: str) -> str:
    """Map an exchange's raw symbol back to canonical form."""
    try:
        return _RAW_TO_CANONICAL[exchange][raw]
    except KeyError as exc:
        raise KeyError(f"{exchange.value} has no canonical for {raw!r}") from exc


def utcnow() -> datetime:
    """Timezone-aware current time (UTC). Used for ingest_ts."""
    return datetime.now(UTC)


def from_epoch_millis(ms: int | float | str) -> datetime:
    """Binance-style millisecond epoch -> aware UTC datetime."""
    return datetime.fromtimestamp(float(ms) / 1000.0, tz=UTC)


def from_epoch_seconds(s: int | float | str) -> datetime:
    """Kraken-style (fractional) second epoch -> aware UTC datetime."""
    return datetime.fromtimestamp(float(s), tz=UTC)
