"""OrderSizing: value object interno representando uma decisão de sizing persistida."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from polycopy.domain.events import SkipReason


@dataclass(frozen=True)
class OrderSizing:
    """Snapshot imutável de uma decisão de sizing.

    Persistido em `order_sizings` table; PK = `trade_event_id`.

    Invariante: `decision == "sized"` ↔ `final_size_usdc is not None and reason is None`.
    """

    trade_event_id: UUID
    wallet: str
    condition_id: str
    token_id: str
    original_size_usdc: Decimal
    final_size_usdc: Decimal | None
    decision: Literal["sized", "skipped"]
    reason: SkipReason | None
    decided_at: datetime

    def __post_init__(self) -> None:
        if self.decision == "sized":
            if self.final_size_usdc is None:
                raise ValueError("sized decision must have final_size_usdc")
            if self.reason is not None:
                raise ValueError("sized decision must have reason=None")
            if self.final_size_usdc <= 0:
                raise ValueError("final_size_usdc must be positive")
        if self.decision == "skipped":
            if self.final_size_usdc is not None:
                raise ValueError("skipped decision must have final_size_usdc=None")
            if self.reason is None:
                raise ValueError("skipped decision must have a reason")
        if self.original_size_usdc <= 0:
            raise ValueError("original_size_usdc must be positive")
        if self.decided_at.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
