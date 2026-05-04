"""MarketResolution value object + ResolvedMarketDTO mapper-only DTO.

ResolvedMarketDTO carrega campos brutos extras (closed, outcome_prices_raw,
uma_resolution_statuses_raw, yes_token_id, no_token_id, closed_time) que
NÃO pertencem ao Market value object canonical (que é só pra markets ativos).
DTO usado SOMENTE pelo mapper Gamma → ResolverAgent.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from polycopy.domain.events import ResolvedOutcome


@dataclass(frozen=True)
class ResolvedMarketDTO:
    """DTO mapper-only — carrega campos brutos do Gamma pra classificação."""

    condition_id: str
    yes_token_id: str
    no_token_id: str
    closed: bool
    closed_time: datetime | None
    outcome_prices_raw: str  # JSON string original do Gamma
    uma_resolution_statuses_raw: str | None  # JSON string original (pode ser '[]' ou null)


@dataclass(frozen=True)
class MarketResolution:
    """Snapshot imutável de um market resolvido.

    Persistido em market_resolutions; PK = condition_id.

    Invariantes:
    - resolved_outcome ∈ {YES, NO} ↔ winning_token_id is not None
    - resolved_outcome == INVALID ↔ winning_token_id is None
    - resolved_at e closed_time (se presente) precisam ser tz-aware
    """

    condition_id: str
    resolved_outcome: ResolvedOutcome
    winning_token_id: str | None
    closed_time: datetime | None
    resolved_at: datetime
    outcome_prices_raw: str
    uma_resolution_statuses_raw: str | None

    def __post_init__(self) -> None:
        if self.resolved_outcome in (ResolvedOutcome.YES, ResolvedOutcome.NO):
            if self.winning_token_id is None:
                raise ValueError(
                    f"{self.resolved_outcome.value} resolution must have winning_token_id"
                )
        elif self.resolved_outcome == ResolvedOutcome.INVALID and self.winning_token_id is not None:
            raise ValueError("INVALID resolution must have winning_token_id=None")
        if self.resolved_at.tzinfo is None:
            raise ValueError("resolved_at must be timezone-aware")
        if self.closed_time is not None and self.closed_time.tzinfo is None:
            raise ValueError("closed_time must be timezone-aware")
