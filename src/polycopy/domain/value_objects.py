"""Domain value objects: tipos primitivos imutáveis com validação."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

_USDC_QUANTUM = Decimal("0.000001")  # USDC tem 6 decimais on-chain
_PRICE_QUANTUM = Decimal("0.0001")  # 4 casas é o que CLOB Polymarket usa
_HEX_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_HEX_CONDITION_ID_RE = re.compile(r"^0x[0-9a-f]{64}$")
_NUMERIC_TOKEN_ID_RE = re.compile(r"^[0-9]+$")


class Money(BaseModel):
    """Valor monetário em USDC. Sempre quantizado para 6 casas decimais."""

    model_config = ConfigDict(frozen=True, strict=True)

    amount: Decimal

    @field_validator("amount", mode="after")
    @classmethod
    def _quantize(cls, v: Decimal) -> Decimal:
        return v.quantize(_USDC_QUANTUM)

    @classmethod
    def zero(cls) -> Money:
        return cls(amount=Decimal("0"))

    @classmethod
    def from_usdc(cls, value: int | str | Decimal) -> Money:
        return cls(amount=Decimal(str(value)))

    def __add__(self, other: Money) -> Money:
        return Money(amount=self.amount + other.amount)

    def __sub__(self, other: Money) -> Money:
        return Money(amount=self.amount - other.amount)

    def __lt__(self, other: Money) -> bool:
        return self.amount < other.amount

    def __le__(self, other: Money) -> bool:
        return self.amount <= other.amount


class Price(BaseModel):
    """Preço de outcome no Polymarket: probabilidade implícita ∈ [0, 1]."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: Annotated[Decimal, Field(ge=0, le=1)]

    @field_validator("value", mode="after")
    @classmethod
    def _quantize(cls, v: Decimal) -> Decimal:
        return v.quantize(_PRICE_QUANTUM)


class Bps(BaseModel):
    """Basis points: 1 bp = 0.01%. Inteiro ∈ [0, 10000]."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: Annotated[int, Field(ge=0, le=10_000)]

    def as_fraction(self) -> Decimal:
        return Decimal(self.value) / Decimal(10_000)


class WalletAddress(BaseModel):
    """Endereço Ethereum/Polygon: 0x + 40 hex, normalizado lowercase."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: str

    @field_validator("value", mode="after")
    @classmethod
    def _validate_and_lower(cls, v: str) -> str:
        normalized = v.lower()
        if not _HEX_ADDRESS_RE.match(normalized):
            raise ValueError(f"invalid wallet address: {v!r} (expected 0x + 40 hex chars)")
        return normalized


class ConditionId(BaseModel):
    """Polymarket condition_id: 0x + 64 hex (32 bytes), normalizado lowercase."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: str

    @field_validator("value", mode="after")
    @classmethod
    def _validate_and_lower(cls, v: str) -> str:
        normalized = v.lower()
        if not _HEX_CONDITION_ID_RE.match(normalized):
            raise ValueError(f"invalid condition_id: {v!r} (expected 0x + 64 hex chars)")
        return normalized


class TokenId(BaseModel):
    """Polymarket token_id: uint256 representado como string decimal."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: str

    @field_validator("value", mode="after")
    @classmethod
    def _validate_numeric(cls, v: str) -> str:
        if not _NUMERIC_TOKEN_ID_RE.match(v):
            raise ValueError(f"invalid token_id: {v!r} (expected non-negative integer string)")
        return v
