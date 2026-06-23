"""Configuration loaded from environment variables (and an optional .env).

All settings are prefixed ``CN_`` and have safe defaults; no secrets are
required because every feed is public. See ``.env.example`` for the full set.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from cryptonorm.common.schemas import Exchange


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CN_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # general
    log_level: str = "INFO"
    log_json: bool = True

    # market data
    # NoDecode: let _split_csv handle the raw env string instead of the
    # settings source trying (and failing) to JSON-decode a list field.
    symbols: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["BTC-USD", "ETH-USD"]
    )
    exchanges: Annotated[list[Exchange], NoDecode] = Field(
        default_factory=lambda: [Exchange.COINBASE, Exchange.BINANCE, Exchange.KRAKEN]
    )

    # reliability
    staleness_seconds: float = 10.0
    reconnect_base_seconds: float = 0.5
    reconnect_max_seconds: float = 30.0

    # exchange endpoints (public, no auth)
    coinbase_ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    binance_ws_url: str = "wss://stream.binance.us:9443/ws"
    binance_rest_url: str = "https://api.binance.us/api/v3"
    kraken_ws_url: str = "wss://ws.kraken.com/v2"
    kraken_book_depth: int = 10

    # kafka / redis (phase 2+)
    kafka_bootstrap: str = "localhost:29092"
    redis_url: str = "redis://localhost:6380/0"
    consumer_group: str = "cn-risk"
    topic_book: str = "md.book.v1"
    topic_trade: str = "md.trade.v1"
    topic_fill: str = "paper.fills.v1"
    book_top_n: int = 10
    # Full L2 book snapshots are large (~1.6 MB for BTC-USD), above Kafka's
    # 1 MB default. Raise the client+broker ceiling; deltas stay tiny.
    max_message_bytes: int = 10 * 1024 * 1024

    # paper-fill simulator + risk (phase 4)
    sim_interval_seconds: float = 1.5  # mean seconds between simulated fills
    sim_max_qty: float = 0.05  # max fill size (base units)
    sim_fee_rate: float = 0.001  # 10 bps taker fee
    risk_interval_seconds: float = 1.0  # how often P&L/risk is recomputed
    recon_interval_seconds: float = 5.0  # how often positions are reconciled
    var_window: int = 120  # rolling samples for historical VaR

    # alert thresholds (phase 5)
    alert_max_drawdown_usd: float = 5000.0
    alert_max_exposure_usd: float = 50000.0
    alert_spread_bps: float = 50.0

    # dashboard / api (phase 5)
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    dashboard_interval_seconds: float = 1.0

    @field_validator("symbols", "exchanges", mode="before")
    @classmethod
    def _split_csv(cls, v: object) -> object:
        """Allow comma-separated env values, e.g. CN_SYMBOLS=BTC-USD,ETH-USD."""
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v


@lru_cache
def get_settings() -> Settings:
    """Process-wide settings singleton."""
    return Settings()
