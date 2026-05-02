"""Domain events: imutáveis, identificáveis (event_id UUID), timezone-aware."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from polycopy.domain.models import Trade


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
