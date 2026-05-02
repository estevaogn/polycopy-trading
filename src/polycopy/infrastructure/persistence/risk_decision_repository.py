"""SqlAlchemyRiskDecisionRepository: persistência idempotente de decisões do Risk."""

from __future__ import annotations

from typing import cast

from sqlalchemy import CursorResult
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.risk import RiskDecision
from polycopy.infrastructure.persistence.models import RiskDecisionRow


class SqlAlchemyRiskDecisionRepository:
    """Persistência de RiskDecision. Idempotente via PK `trade_event_id`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, decision: RiskDecision) -> bool:
        """Insere decisão. True se nova, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(RiskDecisionRow)
            .values(
                trade_event_id=decision.trade_event_id,
                wallet=decision.wallet,
                condition_id=decision.condition_id,
                token_id=decision.token_id,
                decision=decision.decision,
                reason=decision.reason.value if decision.reason is not None else None,
                decided_at=decision.decided_at,
            )
            .on_conflict_do_nothing(index_elements=["trade_event_id"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1
