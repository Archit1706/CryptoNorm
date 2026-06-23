"""Exchange -> adapter class registry.

Adding a new venue means writing one adapter + one normalizer and adding a
single line here; nothing else in the system needs to change.
"""

from __future__ import annotations

from cryptonorm.common.config import Settings, get_settings
from cryptonorm.common.schemas import Exchange
from cryptonorm.ingest.base import ExchangeAdapter
from cryptonorm.ingest.binance import BinanceAdapter
from cryptonorm.ingest.coinbase import CoinbaseAdapter
from cryptonorm.ingest.kraken import KrakenAdapter

ADAPTERS: dict[Exchange, type[ExchangeAdapter]] = {
    Exchange.COINBASE: CoinbaseAdapter,
    Exchange.BINANCE: BinanceAdapter,
    Exchange.KRAKEN: KrakenAdapter,
}


def build_adapter(
    exchange: Exchange, symbols: list[str], settings: Settings | None = None
) -> ExchangeAdapter:
    settings = settings or get_settings()
    try:
        return ADAPTERS[exchange](symbols, settings)
    except KeyError as exc:
        raise KeyError(f"no adapter registered for {exchange.value!r}") from exc
