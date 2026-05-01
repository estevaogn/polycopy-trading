"""Domain models: Wallet, Trade, Position e tipos relacionados."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from polycopy.domain.value_objects import (
    Bps,
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class Wallet(BaseModel):
    """Carteira observada (copiada). Imutável dentro de uma sessão."""

    model_config = ConfigDict(frozen=True, strict=True)

    address: WalletAddress
    nickname: str
    enabled: bool
    max_slippage_bps: Bps = Bps(value=200)


class Trade(BaseModel):
    """Trade detectado on-chain ou via Data API. Identidade = (tx_hash, log_index)."""

    model_config = ConfigDict(frozen=True, strict=True)

    tx_hash: str
    log_index: Annotated[int, Field(ge=0)]
    wallet: WalletAddress
    condition_id: ConditionId
    token_id: TokenId
    side: Side
    price: Price
    size_usdc: Money
    occurred_at: datetime

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @property
    def dedup_key(self) -> tuple[str, int]:
        return (self.tx_hash, self.log_index)


class Position(BaseModel):
    """Posição agregada por (wallet, condition, token). Read model."""

    model_config = ConfigDict(frozen=True, strict=True)

    wallet: WalletAddress
    condition_id: ConditionId
    token_id: TokenId
    size_usdc: Money  # capital alocado
    avg_price: Price
