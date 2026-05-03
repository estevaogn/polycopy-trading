"""Integration tests do SqlAlchemyOrderExecutionRepository — exige Postgres up."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.domain.events import ExecutionMode, FailureReason
from polycopy.domain.execution import OrderExecution
from polycopy.infrastructure.persistence.models import OrderExecutionRow
from polycopy.infrastructure.persistence.order_execution_repository import (
    SqlAlchemyOrderExecutionRepository,
)
from polycopy.ports import OrderExecutionRepository

pytestmark = pytest.mark.integration

_VALID_WALLET = "0x" + "1" * 40
_VALID_COND = "0x" + "cd" * 32
_VALID_TOKEN = "42"
_VALID_TX = "0x" + "ab" * 32


def _execution_dry_run(
    *,
    final_size: Decimal = Decimal("10"),
) -> OrderExecution:
    return OrderExecution(
        trade_event_id=uuid4(),
        wallet=_VALID_WALLET,
        condition_id=_VALID_COND,
        token_id=_VALID_TOKEN,
        final_size_usdc=final_size,
        mode=ExecutionMode.DRY_RUN,
        result="dry_run",
        tx_hash=None,
        gas_wei=None,
        failure_reason=None,
        error_message=None,
        decided_at=datetime.now(tz=UTC),
    )


def _execution_executed(
    *,
    final_size: Decimal = Decimal("25"),
    tx_hash: str = _VALID_TX,
    gas_wei: int = 21_000_000_000,
) -> OrderExecution:
    return OrderExecution(
        trade_event_id=uuid4(),
        wallet=_VALID_WALLET,
        condition_id=_VALID_COND,
        token_id=_VALID_TOKEN,
        final_size_usdc=final_size,
        mode=ExecutionMode.REAL,
        result="executed",
        tx_hash=tx_hash,
        gas_wei=gas_wei,
        failure_reason=None,
        error_message=None,
        decided_at=datetime.now(tz=UTC),
    )


def _execution_failed(
    *,
    final_size: Decimal = Decimal("15"),
    reason: FailureReason = FailureReason.INVALID_TRADE_PARAMS,
    error_message: str = "trade params invalid",
) -> OrderExecution:
    return OrderExecution(
        trade_event_id=uuid4(),
        wallet=_VALID_WALLET,
        condition_id=_VALID_COND,
        token_id=_VALID_TOKEN,
        final_size_usdc=final_size,
        mode=ExecutionMode.REAL,
        result="failed",
        tx_hash=None,
        gas_wei=None,
        failure_reason=reason,
        error_message=error_message,
        decided_at=datetime.now(tz=UTC),
    )


async def test_insert_dry_run_returns_true(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyOrderExecutionRepository(session)
        result = await repo.insert(_execution_dry_run())
        await session.commit()
        assert result is True


async def test_insert_duplicate_returns_false(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyOrderExecutionRepository(session)
        execution = _execution_dry_run()
        first = await repo.insert(execution)
        await session.commit()
        second = await repo.insert(execution)
        await session.commit()
        assert first is True
        assert second is False

        # Confirma que só uma row existe
        result = await session.execute(
            select(OrderExecutionRow).where(
                OrderExecutionRow.trade_event_id == execution.trade_event_id
            )
        )
        rows = result.scalars().all()
        assert len(rows) == 1


async def test_insert_executed_persists_tx_hash(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyOrderExecutionRepository(session)
        execution = _execution_executed(tx_hash=_VALID_TX, gas_wei=42_000_000_000)
        await repo.insert(execution)
        await session.commit()

        result = await session.execute(
            select(OrderExecutionRow).where(
                OrderExecutionRow.trade_event_id == execution.trade_event_id
            )
        )
        row = result.scalar_one()
        assert row.mode == "real"
        assert row.result == "executed"
        assert row.tx_hash == _VALID_TX
        assert row.gas_wei == Decimal("42000000000")
        assert row.failure_reason is None
        assert row.error_message is None


async def test_insert_failed_persists_reason_and_error(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyOrderExecutionRepository(session)
        execution = _execution_failed(
            reason=FailureReason.INVALID_TRADE_PARAMS,
            error_message="missing token_id",
        )
        await repo.insert(execution)
        await session.commit()

        result = await session.execute(
            select(OrderExecutionRow).where(
                OrderExecutionRow.trade_event_id == execution.trade_event_id
            )
        )
        row = result.scalar_one()
        assert row.mode == "real"
        assert row.result == "failed"
        assert row.tx_hash is None
        assert row.gas_wei is None
        assert row.failure_reason == "invalid_trade_params"
        assert row.error_message == "missing token_id"


async def test_insert_real_with_dry_run_result_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Insere row inconsistente direto via SQL (bypassa __post_init__).

    CHECK order_executions_mode_result_consistency deve barrar
    (mode='real' exige result em ('executed','failed')).
    """
    async with db_session_factory() as session:
        from sqlalchemy import text

        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO order_executions "
                    "(trade_event_id, wallet, condition_id, token_id, "
                    "final_size_usdc, mode, result, tx_hash, gas_wei, "
                    "failure_reason, error_message, decided_at) "
                    "VALUES (:id, :w, :c, :t, 10, 'real', 'dry_run', "
                    "NULL, NULL, NULL, NULL, now())"
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
    """Mypy garante que SqlAlchemyOrderExecutionRepository satisfaz o Protocol."""
    async with db_session_factory() as session:
        _: OrderExecutionRepository = SqlAlchemyOrderExecutionRepository(session)
