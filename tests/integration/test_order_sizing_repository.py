"""Integration tests do SqlAlchemyOrderSizingRepository — exige Postgres up."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.domain.events import SkipReason
from polycopy.domain.sizing import OrderSizing
from polycopy.infrastructure.persistence.models import OrderSizingRow
from polycopy.infrastructure.persistence.order_sizing_repository import (
    SqlAlchemyOrderSizingRepository,
)
from polycopy.ports import OrderSizingRepository

pytestmark = pytest.mark.integration

_VALID_WALLET = "0x" + "1" * 40
_VALID_COND = "0x" + "cd" * 32
_VALID_TOKEN = "42"


def _sizing_sized(
    *,
    original: Decimal = Decimal("100"),
    final: Decimal = Decimal("10"),
) -> OrderSizing:
    return OrderSizing(
        trade_event_id=uuid4(),
        wallet=_VALID_WALLET,
        condition_id=_VALID_COND,
        token_id=_VALID_TOKEN,
        original_size_usdc=original,
        final_size_usdc=final,
        decision="sized",
        reason=None,
        decided_at=datetime.now(tz=UTC),
    )


def _sizing_skipped(
    reason: SkipReason = SkipReason.BELOW_MIN_SIZE,
    *,
    original: Decimal = Decimal("1"),
) -> OrderSizing:
    return OrderSizing(
        trade_event_id=uuid4(),
        wallet=_VALID_WALLET,
        condition_id=_VALID_COND,
        token_id=_VALID_TOKEN,
        original_size_usdc=original,
        final_size_usdc=None,
        decision="skipped",
        reason=reason,
        decided_at=datetime.now(tz=UTC),
    )


async def test_insert_sized_returns_true(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyOrderSizingRepository(session)
        result = await repo.insert(_sizing_sized())
        await session.commit()
        assert result is True


async def test_insert_duplicate_returns_false(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyOrderSizingRepository(session)
        s = _sizing_sized()
        first = await repo.insert(s)
        await session.commit()
        second = await repo.insert(s)
        await session.commit()
        assert first is True
        assert second is False

        # Confirma que só uma row existe
        result = await session.execute(
            select(OrderSizingRow).where(OrderSizingRow.trade_event_id == s.trade_event_id)
        )
        rows = result.scalars().all()
        assert len(rows) == 1


async def test_insert_skipped_persists_reason(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyOrderSizingRepository(session)
        s = _sizing_skipped(SkipReason.BELOW_MIN_SIZE)
        await repo.insert(s)
        await session.commit()

        result = await session.execute(
            select(OrderSizingRow).where(OrderSizingRow.trade_event_id == s.trade_event_id)
        )
        row = result.scalar_one()
        assert row.decision == "skipped"
        assert row.reason == "below_min_size"
        assert row.final_size_usdc is None


async def test_insert_sized_with_reason_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Insere row inconsistente direto via SQL (bypassa o __post_init__ do dataclass).

    Postgres CHECK order_sizings_consistency deve barrar.
    """
    async with db_session_factory() as session:
        from sqlalchemy import text

        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO order_sizings "
                    "(trade_event_id, wallet, condition_id, token_id, "
                    "original_size_usdc, final_size_usdc, decision, reason, decided_at) "
                    "VALUES (:id, :w, :c, :t, 100, 10, 'sized', 'below_min_size', now())"
                ),
                {
                    "id": uuid4(),
                    "w": _VALID_WALLET,
                    "c": _VALID_COND,
                    "t": _VALID_TOKEN,
                },
            )
            await session.commit()


async def test_insert_invalid_decision_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """CHECK decision IN ('sized','skipped') deve barrar valor inválido."""
    async with db_session_factory() as session:
        from sqlalchemy import text

        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO order_sizings "
                    "(trade_event_id, wallet, condition_id, token_id, "
                    "original_size_usdc, final_size_usdc, decision, reason, decided_at) "
                    "VALUES (:id, :w, :c, :t, 100, 10, 'maybe', NULL, now())"
                ),
                {
                    "id": uuid4(),
                    "w": _VALID_WALLET,
                    "c": _VALID_COND,
                    "t": _VALID_TOKEN,
                },
            )
            await session.commit()


async def test_adapter_satisfies_protocol(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Mypy garante que SqlAlchemyOrderSizingRepository satisfaz OrderSizingRepository."""
    async with db_session_factory() as session:
        _: OrderSizingRepository = SqlAlchemyOrderSizingRepository(session)
