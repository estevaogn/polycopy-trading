"""Integration tests do SqlAlchemyRiskDecisionRepository — exige Postgres up."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.domain.events import RejectionReason
from polycopy.domain.risk import RiskDecision
from polycopy.infrastructure.persistence.models import RiskDecisionRow
from polycopy.infrastructure.persistence.risk_decision_repository import (
    SqlAlchemyRiskDecisionRepository,
)
from polycopy.ports import RiskDecisionRepository

pytestmark = pytest.mark.integration

_VALID_WALLET = "0x" + "1" * 40
_VALID_COND = "0x" + "cd" * 32
_VALID_TOKEN = "42"


def _decision_approved() -> RiskDecision:
    return RiskDecision(
        trade_event_id=uuid4(),
        wallet=_VALID_WALLET,
        condition_id=_VALID_COND,
        token_id=_VALID_TOKEN,
        decision="approved",
        reason=None,
        decided_at=datetime.now(tz=UTC),
    )


def _decision_rejected(reason: RejectionReason = RejectionReason.SIZE_EXCEEDED) -> RiskDecision:
    return RiskDecision(
        trade_event_id=uuid4(),
        wallet=_VALID_WALLET,
        condition_id=_VALID_COND,
        token_id=_VALID_TOKEN,
        decision="rejected",
        reason=reason,
        decided_at=datetime.now(tz=UTC),
    )


async def test_insert_new_returns_true(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyRiskDecisionRepository(session)
        result = await repo.insert(_decision_approved())
        await session.commit()
        assert result is True


async def test_insert_duplicate_returns_false(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyRiskDecisionRepository(session)
        d = _decision_approved()
        first = await repo.insert(d)
        await session.commit()
        second = await repo.insert(d)
        await session.commit()
        assert first is True
        assert second is False

        # Confirma que só uma row existe
        result = await session.execute(
            select(RiskDecisionRow).where(RiskDecisionRow.trade_event_id == d.trade_event_id)
        )
        rows = result.scalars().all()
        assert len(rows) == 1


async def test_insert_rejected_persists_reason(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyRiskDecisionRepository(session)
        d = _decision_rejected(RejectionReason.MARKET_INACTIVE)
        await repo.insert(d)
        await session.commit()

        result = await session.execute(
            select(RiskDecisionRow).where(RiskDecisionRow.trade_event_id == d.trade_event_id)
        )
        row = result.scalar_one()
        assert row.decision == "rejected"
        assert row.reason == "market_inactive"


async def test_insert_approved_with_reason_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Insere row inconsistente direto via SQL (bypassa o __post_init__ do dataclass).
    Postgres CHECK reason_consistency deve barrar.
    """
    async with db_session_factory() as session:
        from sqlalchemy import text

        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO risk_decisions "
                    "(trade_event_id, wallet, condition_id, token_id, "
                    "decision, reason, decided_at) "
                    "VALUES (:id, :w, :c, :t, 'approved', 'size_exceeded', now())"
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
    """CHECK decision IN ('approved','rejected') deve barrar valor inválido."""
    async with db_session_factory() as session:
        from sqlalchemy import text

        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO risk_decisions "
                    "(trade_event_id, wallet, condition_id, token_id, decision, decided_at) "
                    "VALUES (:id, :w, :c, :t, 'maybe', now())"
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
    """Mypy garante que SqlAlchemyRiskDecisionRepository satisfaz RiskDecisionRepository."""
    async with db_session_factory() as session:
        _: RiskDecisionRepository = SqlAlchemyRiskDecisionRepository(session)
