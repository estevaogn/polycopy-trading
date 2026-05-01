"""Domain events: imutáveis, identificáveis (event_id UUID), timezone-aware."""

from __future__ import annotations

from datetime import datetime
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
