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
    risk_copy_allowlist: str = Field("", alias="RISK_COPY_ALLOWLIST")
    """CSV de endereços a copiar. Vazio = sem filtro (toda wallet do seed passa).

    Quando preenchido, Risk rejeita trades de wallets fora da lista (fail-fast,
    antes de qualquer lookup). Comparação case-insensitive. Útil pra real-mode
    seletivo: watcher continua coletando dados de todas as wallets do seed, mas
    apenas as listadas aqui chegam ao executor.
    """

    # Sizing agent (Plano 2C)
    sizing_metrics_port: int = Field(9105, alias="SIZING_METRICS_PORT")
    sizing_max_deliver: int = Field(5, alias="SIZING_MAX_DELIVER")
    sizing_durable_name: str = Field("sizing-1", alias="SIZING_DURABLE_NAME")
    sizing_proportion_ratio: Decimal = Field(Decimal("0.1"), alias="SIZING_PROPORTION_RATIO")
    sizing_max_size_usdc: Decimal = Field(Decimal("50"), alias="SIZING_MAX_SIZE_USDC")
    sizing_min_size_usdc: Decimal = Field(Decimal("1"), alias="SIZING_MIN_SIZE_USDC")

    # Executor agent (Plano 3 — DRY-RUN MVP)
    executor_metrics_port: int = Field(9106, alias="EXECUTOR_METRICS_PORT")
    executor_max_deliver: int = Field(5, alias="EXECUTOR_MAX_DELIVER")
    executor_durable_name: str = Field("executor-1", alias="EXECUTOR_DURABLE_NAME")
    executor_dry_run: bool = Field(True, alias="EXECUTOR_DRY_RUN")
    """DRY-RUN by default — Fase 3 MVP. Set to false ONLY after Fase 4
    real-mode (Web3CLOBExecutor) is implemented + tested on testnet."""

    # --- Fase 4 — Real on-chain execution (DANGER ZONE) ---
    # Wallet (real-mode only — None default fail-fast)
    wallet_private_key: SecretStr | None = Field(None, alias="WALLET_PRIVATE_KEY")

    # Polygon network
    polygon_rpc_url: str = Field(
        "https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY",
        alias="POLYGON_RPC_URL",
    )
    polygon_chain_id: int = Field(137, alias="POLYGON_CHAIN_ID")

    # Polymarket contracts (Polygon mainnet)
    polymarket_exchange_address: str = Field(
        "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
        alias="POLYMARKET_EXCHANGE_ADDRESS",
    )
    polymarket_clob_api_url: str = Field(
        "https://clob.polymarket.com",
        alias="POLYMARKET_CLOB_API_URL",
    )

    # Executor real-mode safety gate (double opt-in)
    executor_real_mode_confirmed: bool = Field(False, alias="EXECUTOR_REAL_MODE_CONFIRMED")

    # Approval cap (run setup_wallet script once after funding)
    max_approval_usdc: int = Field(100, alias="MAX_APPROVAL_USDC")

    # Kill-switches (5 camadas)
    executor_max_size_usdc: Decimal = Field(Decimal("2"), alias="EXECUTOR_MAX_SIZE_USDC")
    executor_daily_max_usdc: Decimal = Field(Decimal("20"), alias="EXECUTOR_DAILY_MAX_USDC")
    executor_daily_max_trades: int = Field(10, alias="EXECUTOR_DAILY_MAX_TRADES")
    executor_circuit_breaker_failures: int = Field(3, alias="EXECUTOR_CIRCUIT_BREAKER_FAILURES")
    executor_pause_file: Path = Field(
        Path("/tmp/polycopy/executor.pause"),  # noqa: S108 — default sobrescrevível via env
        alias="EXECUTOR_PAUSE_FILE",
    )

    # Resolver agent (Plano 5A)
    resolver_metrics_port: int = Field(9107, alias="RESOLVER_METRICS_PORT")
    resolver_sync_interval_s: float = Field(3600.0, alias="RESOLVER_SYNC_INTERVAL_SECONDS")
    resolver_batch_size: int = Field(100, alias="RESOLVER_BATCH_SIZE")

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
