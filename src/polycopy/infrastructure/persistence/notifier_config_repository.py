"""SqlAlchemyNotifierConfigRepository: KV pra config dinâmica do notifier."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.infrastructure.persistence.models import NotifierConfigRow

_KEY_MIN_SIZE = "min_size_usdc"


class SqlAlchemyNotifierConfigRepository:
    """Implementa NotifierConfigRepository via tabela KV `notifier_config`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_min_size_usdc(self) -> Decimal:
        result = await self._session.execute(
            select(NotifierConfigRow.value).where(NotifierConfigRow.key == _KEY_MIN_SIZE)
        )
        row = result.scalar_one_or_none()
        return Decimal(row) if row is not None else Decimal(0)

    async def set_min_size_usdc(self, value: Decimal, *, updated_by: str) -> None:
        """Upsert. Caller é responsável por `await session.commit()`."""
        from sqlalchemy.sql import func

        stmt = (
            pg_insert(NotifierConfigRow)
            .values(key=_KEY_MIN_SIZE, value=str(value), updated_by=updated_by)
            .on_conflict_do_update(
                index_elements=["key"],
                set_={"value": str(value), "updated_by": updated_by, "updated_at": func.now()},
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()
