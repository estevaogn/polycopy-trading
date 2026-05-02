"""Domain types pra mercados: OrderBook, Market.

Value objects imutáveis. Sem dependência de infra.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from polycopy.domain.value_objects import ConditionId, Money, Price, TokenId


class Market(BaseModel):
    """Metadata de um token de mercado da Polymarket.

    Cada `condition_id` tem 2 tokens (Yes/No); cada token vira um `Market`.
    """

    model_config = ConfigDict(frozen=True, strict=True)

    token_id: TokenId
    condition_id: ConditionId
    question: str
    slug: str | None
    outcome: Annotated[str, Field(pattern=r"^(Yes|No)$")]
    end_date: datetime | None
    is_active: bool
    is_archived: bool
    volume_24h_usdc: Money | None
    liquidity_usdc: Money | None

    @model_validator(mode="after")
    def _check_archived_consistency(self) -> Market:
        if self.is_archived and self.is_active:
            raise ValueError("market cannot be both is_active=True and is_archived=True")
        return self


class OrderBookLevel(BaseModel):
    """Um nível do orderbook: preço e tamanho agregado nesse preço."""

    model_config = ConfigDict(frozen=True, strict=True)

    price: Price
    size: Money

    @field_validator("size", mode="after")
    @classmethod
    def _size_non_negative(cls, v: Money) -> Money:
        if v.amount < 0:
            raise ValueError(f"order book level size must be >= 0, got {v.amount}")
        return v


class OrderBook(BaseModel):
    """Snapshot do orderbook de um token, capturado num momento específico.

    `bids` em ordem decrescente de preço (melhor primeiro).
    `asks` em ordem crescente de preço (melhor primeiro).
    """

    model_config = ConfigDict(frozen=True, strict=True)

    token_id: TokenId
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    captured_at: datetime

    @model_validator(mode="after")
    def _check_ordering(self) -> OrderBook:
        for i in range(1, len(self.bids)):
            if self.bids[i].price.value > self.bids[i - 1].price.value:
                raise ValueError("bids must be in descending price order")
        for i in range(1, len(self.asks)):
            if self.asks[i].price.value < self.asks[i - 1].price.value:
                raise ValueError("asks must be in ascending price order")
        return self

    @property
    def best_bid(self) -> OrderBookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> OrderBookLevel | None:
        return self.asks[0] if self.asks else None
