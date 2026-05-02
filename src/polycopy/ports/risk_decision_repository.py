"""RiskDecisionRepository: contrato de persistência pra decisões do Risk."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.risk import RiskDecision


class RiskDecisionRepository(Protocol):
    """Persistência idempotente de decisões. Plano 2B."""

    async def insert(self, decision: RiskDecision) -> bool:
        """Insere decisão; retorna True se nova, False se já existia.

        Idempotência via PK `trade_event_id`: 2º insert com mesmo
        event_id retorna False sem erro. Caller (RiskAgent) usa esse
        boolean pra decidir publicar evento (skip publish se duplicate).
        """
        ...
