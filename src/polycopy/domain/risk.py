"""RiskDecision: value object interno representando uma decisão persistida."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from polycopy.domain.events import RejectionReason


@dataclass(frozen=True)
class RiskDecision:
    """Snapshot imutável de uma decisão do RiskAgent.

    Persistido em `risk_decisions` table; PK = `trade_event_id` dá
    idempotência grátis (re-delivery vê duplicate).

    Invariante: `decision == "approved"` ↔ `reason is None`.
    """

    trade_event_id: UUID
    wallet: str
    condition_id: str
    token_id: str
    decision: Literal["approved", "rejected"]
    reason: RejectionReason | None
    decided_at: datetime

    def __post_init__(self) -> None:
        if self.decision == "approved" and self.reason is not None:
            raise ValueError("approved decision must have reason=None")
        if self.decision == "rejected" and self.reason is None:
            raise ValueError("rejected decision must have a reason")
        if self.decided_at.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
