"""Integration tests do SqlAlchemyMarketRepository — exige Postgres up via docker-compose."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.domain.market import Market
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.persistence.market_repository import (
    SqlAlchemyMarketRepository,
)
from polycopy.infrastructure.persistence.models import MarketRow
from polycopy.ports import MarketRepository

pytestmark = pytest.mark.integration

_DEFAULT_TOKEN_ID = "1"
_DEFAULT_OUTCOME = "Yes"
_VALID_COND = "0x" + "ab" * 32


def _market(
    *,
    token_id: str = _DEFAULT_TOKEN_ID,
    outcome: str = _DEFAULT_OUTCOME,
    is_active: bool = True,
) -> Market:
    return Market(
        token_id=TokenId(value=token_id),
        condition_id=ConditionId(value=_VALID_COND),
        question="Q?",
        slug="q",
        outcome=outcome,
        end_date=datetime.now(tz=UTC) + timedelta(days=14),
        is_active=is_active,
        is_archived=False,
        volume_24h_usdc=Money.from_usdc("100000"),
        liquidity_usdc=Money.from_usdc("5000"),
    )


async def test_upsert_many_inserts_and_updates(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session, ttl_seconds=1800)

        # Inserção inicial
        m1 = _market(token_id="100", outcome="Yes")
        m2 = _market(token_id="101", outcome="No", is_active=True)
        n = await repo.upsert_many([m1, m2])
        await session.commit()
        assert n == 2

        # Update do mesmo token (volume diferente)
        m1_updated = m1.model_copy(update={"volume_24h_usdc": Money.from_usdc("200000")})
        n = await repo.upsert_many([m1_updated])
        await session.commit()
        assert n == 1

        cached = await repo.get_market(TokenId(value="100"))
        assert cached is not None
        assert cached.market.volume_24h_usdc is not None
        assert cached.market.volume_24h_usdc.amount == Decimal("200000.000000")


async def test_get_market_fresh_versus_stale(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session, ttl_seconds=2)

        await repo.upsert_many([_market(token_id="200")])
        await session.commit()

        cached = await repo.get_market(TokenId(value="200"))
        assert cached is not None
        assert cached.is_stale is False

        # Forçar stale ajustando last_synced_at no DB
        await session.execute(
            text(
                "UPDATE markets SET last_synced_at = now() - interval '1 hour' WHERE token_id = :t"
            ),
            {"t": "200"},
        )
        await session.commit()

        # expire_on_commit=False (no factory) mantém row no identity map; sem
        # expire_all() o select retorna o cache stale e ignora o UPDATE acima.
        session.expire_all()

        cached2 = await repo.get_market(TokenId(value="200"))
        assert cached2 is not None
        assert cached2.is_stale is True


async def test_get_market_missing_returns_none(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session, ttl_seconds=1800)
        result = await repo.get_market(TokenId(value="999999"))
        assert result is None


async def test_upsert_many_idempotent_when_called_twice(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session, ttl_seconds=1800)

        markets = [_market(token_id=str(300 + i)) for i in range(5)]
        await repo.upsert_many(markets)
        await session.commit()

        # 2º call não erra e atualiza last_synced_at
        await repo.upsert_many(markets)
        await session.commit()

        result = await session.execute(
            select(MarketRow).where(MarketRow.token_id.in_([str(300 + i) for i in range(5)]))
        )
        rows = result.scalars().all()
        assert len(rows) == 5


async def test_adapter_satisfies_protocol(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Mypy garante que SqlAlchemyMarketRepository satisfaz MarketRepository."""
    async with db_session_factory() as session:
        _: MarketRepository = SqlAlchemyMarketRepository(session, ttl_seconds=60)


async def test_round_trip_with_all_optionals_none(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Round-trip None→DB→None pra opcionais (slug, end_date, volume, liquidity)."""
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session, ttl_seconds=1800)

        m = Market(
            token_id=TokenId(value="500"),
            condition_id=ConditionId(value=_VALID_COND),
            question="Q?",
            slug=None,
            outcome="Yes",
            end_date=None,
            is_active=True,
            is_archived=False,
            volume_24h_usdc=None,
            liquidity_usdc=None,
        )
        await repo.upsert_many([m])
        await session.commit()

        cached = await repo.get_market(TokenId(value="500"))
        assert cached is not None
        assert cached.market.slug is None
        assert cached.market.end_date is None
        assert cached.market.volume_24h_usdc is None
        assert cached.market.liquidity_usdc is None
