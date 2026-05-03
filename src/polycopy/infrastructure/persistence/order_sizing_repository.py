"""SqlAlchemyOrderSizingRepository: persistência idempotente de decisões de sizing."""

from __future__ import annotations

from typing import cast

from sqlalchemy import CursorResult
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.sizing import OrderSizing
from polycopy.infrastructure.persistence.models import OrderSizingRow


class SqlAlchemyOrderSizingRepository:
    """Persistência de OrderSizing. Idempotente via PK `trade_event_id`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, sizing: OrderSizing) -> bool:
        """Insere sizing. True se novo, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(OrderSizingRow)
            .values(
                trade_event_id=sizing.trade_event_id,
                wallet=sizing.wallet,
                condition_id=sizing.condition_id,
                token_id=sizing.token_id,
                original_size_usdc=sizing.original_size_usdc,
                final_size_usdc=sizing.final_size_usdc,
                decision=sizing.decision,
                reason=sizing.reason.value if sizing.reason is not None else None,
                decided_at=sizing.decided_at,
            )
            .on_conflict_do_nothing(index_elements=["trade_event_id"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1
