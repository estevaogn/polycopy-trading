"""SqlAlchemyWalletTradeRepository: implementação concreta de WalletTradeRepository."""

from __future__ import annotations

from datetime import datetime
from typing import cast

from sqlalchemy import CursorResult, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.models import Trade
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.persistence.models import WalletTradeRow


class SqlAlchemyWalletTradeRepository:
    """Repositório de trades. Idempotente via PK `(tx_hash, log_index)`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, trade: Trade) -> bool:
        """Insere trade. True se inseriu, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(WalletTradeRow)
            .values(
                tx_hash=trade.tx_hash,
                log_index=trade.log_index,
                wallet=trade.wallet.value,
                condition_id=trade.condition_id.value,
                token_id=trade.token_id.value,
                side=trade.side.value,
                price=trade.price.value,
                size_usdc=trade.size_usdc.amount,
                occurred_at=trade.occurred_at,
            )
            .on_conflict_do_nothing(index_elements=["tx_hash", "log_index"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1

    async def latest_occurred_at(self, wallet: WalletAddress) -> datetime | None:
        stmt = select(func.max(WalletTradeRow.occurred_at)).where(
            WalletTradeRow.wallet == wallet.value
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
