"""SqlAlchemyMarketResolutionRepository: persistência idempotente de resoluções."""

from __future__ import annotations

from typing import cast

from sqlalchemy import CursorResult, distinct, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.resolution import MarketResolution
from polycopy.infrastructure.persistence.models import (
    MarketResolutionRow,
    WalletTradeRow,
)


class SqlAlchemyMarketResolutionRepository:
    """Persistência idempotente. PK = condition_id (1 row por mercado).

    market_resolutions é puramente append-only — sem UPDATEs.
    `insert` retorna False se já existe (PK conflict).
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, resolution: MarketResolution) -> bool:
        """Insere; True se nova, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(MarketResolutionRow)
            .values(
                condition_id=resolution.condition_id,
                resolved_outcome=resolution.resolved_outcome.value,
                winning_token_id=resolution.winning_token_id,
                closed_time=resolution.closed_time,
                resolved_at=resolution.resolved_at,
                outcome_prices_raw=resolution.outcome_prices_raw,
                uma_resolution_statuses_raw=resolution.uma_resolution_statuses_raw,
            )
            .on_conflict_do_nothing(index_elements=["condition_id"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        """LEFT JOIN wallet_trades vs market_resolutions WHERE resolution IS NULL."""
        stmt = (
            select(distinct(WalletTradeRow.condition_id))
            .outerjoin(
                MarketResolutionRow,
                MarketResolutionRow.condition_id == WalletTradeRow.condition_id,
            )
            .where(MarketResolutionRow.condition_id.is_(None))
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.all()]
