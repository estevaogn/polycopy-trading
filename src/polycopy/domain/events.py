"""Domain events: imutáveis, identificáveis (event_id UUID), timezone-aware."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from polycopy.domain.models import Trade
from polycopy.domain.value_objects import Money


class WalletTradeDetected(BaseModel):
    """Evento publicado quando o watcher detecta um trade de uma wallet observada.

    NATS subject: `wallet.trade.detected`.
    """

    SUBJECT: ClassVar[str] = "wallet.trade.detected"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    trade: Trade

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v


class RejectionReason(StrEnum):
    """Razões pelas quais Risk rejeita um trade."""

    SIZE_EXCEEDED = "size_exceeded"
    MARKET_NOT_CACHED = "market_not_cached"
    MARKET_INACTIVE = "market_inactive"
    PRICE_OUT_OF_RANGE = "price_out_of_range"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"


class OrderApproved(BaseModel):
    """Evento publicado quando Risk aprova um trade.

    NATS subject: `order.approved`. `event_id` é o mesmo do
    `WalletTradeDetected` original (idempotência cross-agent).

    Campos temporais:
    - `occurred_at`: timestamp do `WalletTradeDetected` original (preservado
      pra Sizing/audit medir lag wallet-detect → risk-decide).
    - `decided_at`: timestamp em que Risk efetivamente decidiu (gravado em DB
      junto com a row em `risk_decisions`).
    """

    SUBJECT: ClassVar[str] = "order.approved"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    trade: Trade
    decided_at: datetime

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided_at(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v


class TradeRejected(BaseModel):
    """Evento publicado quando Risk rejeita um trade.

    NATS subject: `trade.rejected`. Inclui `reason` pra audit.

    Campos temporais:
    - `occurred_at`: timestamp do `WalletTradeDetected` original (preservado
      pra Sizing/audit medir lag wallet-detect → risk-decide).
    - `decided_at`: timestamp em que Risk efetivamente decidiu (gravado em DB
      junto com a row em `risk_decisions`).
    """

    SUBJECT: ClassVar[str] = "trade.rejected"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    trade: Trade
    decided_at: datetime
    reason: RejectionReason

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided_at(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v


class SkipReason(StrEnum):
    """Razões pelas quais Sizing pula um trade aprovado."""

    BELOW_MIN_SIZE = "below_min_size"


class OrderSized(BaseModel):
    """Evento publicado quando Sizing escala um trade aprovado.

    NATS subject: `order.sized`. `event_id` é o mesmo do `WalletTradeDetected`
    original. `occurred_at` preserva o timestamp do trade (pra Sizing/Risk
    medir lag); `decided_at` marca quando Sizing efetivamente decidiu.
    """

    SUBJECT: ClassVar[str] = "order.sized"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money
    original_size_usdc: Money

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v


class OrderSkipped(BaseModel):
    """Evento publicado quando Sizing pula um trade aprovado (final_size < min).

    NATS subject: `order.skipped`. Inclui `reason` pra audit.
    """

    SUBJECT: ClassVar[str] = "order.skipped"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    reason: SkipReason

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v


class ExecutionMode(StrEnum):
    """Modo de execução do ExecutorAgent."""

    REAL = "real"
    DRY_RUN = "dry_run"


class FailureReason(StrEnum):
    """Razões pelas quais Executor falha. Aberto pra extensão (Fase 4)."""

    # Fase 3 (existentes)
    INVALID_TRADE_PARAMS = "invalid_trade_params"
    EXECUTOR_DISABLED = "executor_disabled"

    # Fase 4 — kill-switches (5)
    MANUALLY_PAUSED = "manually_paused"
    DAILY_TRADES_EXCEEDED = "daily_trades_exceeded"
    DAILY_USDC_EXCEEDED = "daily_usdc_exceeded"
    CIRCUIT_BREAKER = "circuit_breaker"
    SIZE_EXCEEDS_EXECUTOR_CAP = "size_exceeds_executor_cap"

    # Fase 4 — falhas on-chain via py-clob-client (5)
    INSUFFICIENT_USDC_BALANCE = "insufficient_usdc_balance"
    INSUFFICIENT_USDC_ALLOWANCE = "insufficient_usdc_allowance"
    CLOB_REJECTED_ORDER = "clob_rejected_order"
    RPC_ERROR = "rpc_error"
    SIGNATURE_ERROR = "signature_error"


class OrderExecuted(BaseModel):
    """Evento publicado quando Executor submete trade real on-chain com sucesso.

    NATS subject: `order.executed`. `tx_hash` é a transação on-chain (Polygon).
    """

    SUBJECT: ClassVar[str] = "order.executed"
    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money
    tx_hash: str
    gas_wei: int

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v

    @field_validator("gas_wei", mode="after")
    @classmethod
    def _gas_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("gas_wei must be non-negative")
        return v


class OrderFailed(BaseModel):
    """Evento publicado quando Executor tenta submeter trade real e falha.

    NATS subject: `order.failed`. Inclui `reason` + `error_message` pra audit.
    """

    SUBJECT: ClassVar[str] = "order.failed"
    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money
    reason: FailureReason
    error_message: str

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v


class OrderDryRun(BaseModel):
    """Evento publicado quando Executor simula trade em modo dry-run.

    NATS subject: `order.dry_run`. Sem dados de tx — apenas snapshot do
    que teria sido feito.
    """

    SUBJECT: ClassVar[str] = "order.dry_run"
    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v


class ResolvedOutcome(StrEnum):
    """Outcome final de um market resolvido (Plano 5A)."""

    YES = "YES"
    NO = "NO"
    INVALID = "INVALID"  # disputed, cancelled, ou outcomes 50/50 split
