"""SqlAlchemyOrderExecutionRepository: persistência idempotente de execuções."""

from __future__ import annotations

from typing import cast

from sqlalchemy import CursorResult
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.execution import OrderExecution
from polycopy.infrastructure.persistence.models import OrderExecutionRow


class SqlAlchemyOrderExecutionRepository:
    """Persistência de OrderExecution. Idempotente via PK `trade_event_id`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, execution: OrderExecution) -> bool:
        """Insere execution. True se nova, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(OrderExecutionRow)
            .values(
                trade_event_id=execution.trade_event_id,
                wallet=execution.wallet,
                condition_id=execution.condition_id,
                token_id=execution.token_id,
                final_size_usdc=execution.final_size_usdc,
                mode=execution.mode.value,
                result=execution.result,
                tx_hash=execution.tx_hash,
                gas_wei=execution.gas_wei,
                failure_reason=(
                    execution.failure_reason.value if execution.failure_reason is not None else None
                ),
                error_message=execution.error_message,
                expected_avg_price=execution.expected_avg_price,
                decided_at=execution.decided_at,
            )
            .on_conflict_do_nothing(index_elements=["trade_event_id"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1
