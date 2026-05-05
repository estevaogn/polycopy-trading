"""SqlAlchemyTrackedWalletRepository: upsert pra sincronizar seed YAML em DB."""

from __future__ import annotations

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from polycopy.infrastructure.persistence.models import TrackedWalletRow


class SqlAlchemyTrackedWalletRepository:
    """Implementa TrackedWalletRepository via tabela `tracked_wallets`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, *, address: str, label: str) -> None:
        """ON CONFLICT atualiza label + last_synced_at; caller commita."""
        stmt = (
            pg_insert(TrackedWalletRow)
            .values(address=address, label=label)
            .on_conflict_do_update(
                index_elements=["address"],
                set_={"label": label, "last_synced_at": func.now()},
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()
