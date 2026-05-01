"""Application settings loaded from environment / .env file.

Uses pydantic-settings: lê variáveis do ambiente, com fallback pra `.env`
na raiz do repo. Sem defaults silenciosos para credenciais — falha rápido
se algo faltar.
"""

from __future__ import annotations

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

    nats_url: str = Field(..., alias="NATS_URL")
    redis_url: str = Field(..., alias="REDIS_URL")
    prometheus_port: int = Field(9090, alias="PROMETHEUS_PORT")

    @property
    def postgres_dsn(self) -> str:
        """DSN sync (psycopg-style). Async DSN é montado no Plano 1B (Task 7)."""
        return (
            f"postgresql://{self.postgres_user}:"
            f"{self.postgres_password.get_secret_value()}@127.0.0.1:"
            f"{self.postgres_port}/{self.postgres_db}"
        )
