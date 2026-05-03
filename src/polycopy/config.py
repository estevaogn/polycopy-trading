"""Application settings loaded from environment / .env file.

Uses pydantic-settings: lê variáveis do ambiente, com fallback pra `.env`
na raiz do repo. Sem defaults silenciosos para credenciais — falha rápido
se algo faltar.
"""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class Environment(StrEnum):
    DEV = "dev"
    PROD = "prod"


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Settings(BaseSettings):
    """Configuração aplicacional. Imutável após construção."""

    model_config = SettingsConfigDict(
        env_file=_REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    env: Environment = Field(Environment.DEV, alias="ENV")
    log_level: LogLevel = Field(LogLevel.INFO, alias="LOG_LEVEL")

    postgres_user: str = Field(..., alias="POSTGRES_USER")
    postgres_password: SecretStr = Field(..., alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(..., alias="POSTGRES_DB")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")
    postgres_host: str = Field("127.0.0.1", alias="POSTGRES_HOST")

    nats_url: str = Field(..., alias="NATS_URL")
    redis_url: str = Field(..., alias="REDIS_URL")
    prometheus_port: int = Field(9090, alias="PROMETHEUS_PORT")

    # Watcher
    watcher_interval_s: float = Field(5.0, alias="WATCHER_INTERVAL_S")
    watcher_bootstrap_hours: int = Field(24, alias="WATCHER_BOOTSTRAP_HOURS")
    watcher_metrics_port: int = Field(9101, alias="WATCHER_METRICS_PORT")
    watch_wallets: str = Field("", alias="WATCH_WALLETS")
    """CSV de endereços. Usado no esqueleto da Task 3; substituído pelo YAML na Task 4."""

    wallets_seed_path: Path = Field(Path("config/wallets_seed.yaml"), alias="WALLETS_SEED_PATH")

    polymarket_base_url: str = Field("https://data-api.polymarket.com", alias="POLYMARKET_BASE_URL")

    # Notifier
    notifier_metrics_port: int = Field(9102, alias="NOTIFIER_METRICS_PORT")
    telegram_bot_token: SecretStr | None = Field(None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: int | None = Field(None, alias="TELEGRAM_CHAT_ID")

    # Polymarket bases
    gamma_api_base_url: str = Field("https://gamma-api.polymarket.com", alias="GAMMA_API_BASE_URL")
    clob_api_base_url: str = Field("https://clob.polymarket.com", alias="CLOB_API_BASE_URL")

    # Market data agent
    marketdata_metrics_port: int = Field(9103, alias="MARKETDATA_METRICS_PORT")
    marketdata_sync_interval_s: float = Field(300.0, alias="MARKETDATA_SYNC_INTERVAL_SECONDS")
    marketdata_top_n: int = Field(200, alias="MARKETDATA_TOP_N")
    market_cache_ttl_seconds: int = Field(1800, alias="MARKET_CACHE_TTL_SECONDS")

    # Risk agent (Plano 2B)
    risk_metrics_port: int = Field(9104, alias="RISK_METRICS_PORT")
    risk_max_deliver: int = Field(5, alias="RISK_MAX_DELIVER")
    risk_durable_name: str = Field("risk-1", alias="RISK_DURABLE_NAME")
    risk_max_trade_usdc: Decimal = Field(Decimal("100"), alias="RISK_MAX_TRADE_USDC")
    risk_min_price: Decimal = Field(Decimal("0.05"), alias="RISK_MIN_PRICE")
    risk_max_price: Decimal = Field(Decimal("0.95"), alias="RISK_MAX_PRICE")
    risk_min_liquidity_usdc: Decimal = Field(Decimal("1000"), alias="RISK_MIN_LIQUIDITY_USDC")
    risk_gamma_fetch_timeout_s: float = Field(5.0, alias="RISK_GAMMA_FETCH_TIMEOUT_S")

    # Sizing agent (Plano 2C)
    sizing_metrics_port: int = Field(9105, alias="SIZING_METRICS_PORT")
    sizing_max_deliver: int = Field(5, alias="SIZING_MAX_DELIVER")
    sizing_durable_name: str = Field("sizing-1", alias="SIZING_DURABLE_NAME")
    sizing_proportion_ratio: Decimal = Field(Decimal("0.1"), alias="SIZING_PROPORTION_RATIO")
    sizing_max_size_usdc: Decimal = Field(Decimal("50"), alias="SIZING_MAX_SIZE_USDC")
    sizing_min_size_usdc: Decimal = Field(Decimal("1"), alias="SIZING_MIN_SIZE_USDC")

    @property
    def postgres_dsn(self) -> str:
        """DSN sync (psycopg-style)."""
        return (
            f"postgresql://{self.postgres_user}:"
            f"{self.postgres_password.get_secret_value()}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_async_dsn(self) -> str:
        """DSN async (asyncpg-style). Usado pelo SQLAlchemy async engine."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:"
            f"{self.postgres_password.get_secret_value()}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_db}"
        )
