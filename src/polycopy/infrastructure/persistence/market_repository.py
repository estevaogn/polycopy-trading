"""SqlAlchemyMarketRepository: cache read-through pra metadata de mercados.

Implementa `MarketRepository` (port). TTL configurável via construtor.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.market import Market
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.persistence.models import MarketRow
from polycopy.ports import CachedMarket


@dataclass(frozen=True)
class _CachedMarket(CachedMarket):
    market: Market
    last_synced_at: datetime
    is_stale: bool


class SqlAlchemyMarketRepository:
    """Cache em Postgres pra `Market`. Idempotente via PK `token_id`.

    TTL afeta apenas leitura (flag `is_stale`); `upsert_many` sempre escreve
    `last_synced_at = now()`.
    """

    def __init__(self, session: AsyncSession, *, ttl_seconds: int) -> None:
        self._session = session
        self._ttl = timedelta(seconds=ttl_seconds)

    async def upsert_many(self, markets: list[Market]) -> int:
        if not markets:
            return 0
        now = datetime.now(tz=UTC)
        values = [_market_to_row_dict(m, last_synced_at=now) for m in markets]

        stmt = pg_insert(MarketRow).values(values)
        update_cols = {
            "condition_id": stmt.excluded.condition_id,
            "question": stmt.excluded.question,
            "slug": stmt.excluded.slug,
            "outcome": stmt.excluded.outcome,
            "end_date": stmt.excluded.end_date,
            "is_active": stmt.excluded.is_active,
            "is_archived": stmt.excluded.is_archived,
            "volume_24h_usdc": stmt.excluded.volume_24h_usdc,
            "liquidity_usdc": stmt.excluded.liquidity_usdc,
            "last_synced_at": stmt.excluded.last_synced_at,
            "updated_at": stmt.excluded.last_synced_at,
        }
        stmt = stmt.on_conflict_do_update(index_elements=["token_id"], set_=update_cols)
        await self._session.execute(stmt)
        await self._session.flush()
        return len(values)

    async def get_market(self, token_id: TokenId) -> CachedMarket | None:
        result = await self._session.execute(
            select(MarketRow).where(MarketRow.token_id == token_id.value)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None

        market = _row_to_market(row)
        is_stale = (datetime.now(tz=UTC) - row.last_synced_at) > self._ttl
        return _CachedMarket(
            market=market,
            last_synced_at=row.last_synced_at,
            is_stale=is_stale,
        )


def _market_to_row_dict(m: Market, *, last_synced_at: datetime) -> dict[str, Any]:
    return {
        "token_id": m.token_id.value,
        "condition_id": m.condition_id.value,
        "question": m.question,
        "slug": m.slug,
        "outcome": m.outcome,
        "end_date": m.end_date,
        "is_active": m.is_active,
        "is_archived": m.is_archived,
        "volume_24h_usdc": (None if m.volume_24h_usdc is None else m.volume_24h_usdc.amount),
        "liquidity_usdc": (None if m.liquidity_usdc is None else m.liquidity_usdc.amount),
        "last_synced_at": last_synced_at,
    }


def _row_to_market(row: MarketRow) -> Market:
    return Market(
        token_id=TokenId(value=row.token_id),
        condition_id=ConditionId(value=row.condition_id),
        question=row.question,
        slug=row.slug,
        outcome=row.outcome,
        end_date=row.end_date,
        is_active=row.is_active,
        is_archived=row.is_archived,
        volume_24h_usdc=(
            Money(amount=row.volume_24h_usdc) if row.volume_24h_usdc is not None else None
        ),
        liquidity_usdc=(
            Money(amount=row.liquidity_usdc) if row.liquidity_usdc is not None else None
        ),
    )
