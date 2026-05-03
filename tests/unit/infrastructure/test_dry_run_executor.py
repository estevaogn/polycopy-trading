"""Testes unit do DryRunExecutor."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from polycopy.domain.events import ExecutionMode
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.dry_run_executor import DryRunExecutor
from polycopy.ports.order_executor import OrderExecutor


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


async def test_execute_returns_dry_run_success() -> None:
    executor = DryRunExecutor()
    result = await executor.execute(_trade(), Decimal("10"))
    assert result.mode == ExecutionMode.DRY_RUN
    assert result.success is True
    assert result.tx_hash is None
    assert result.gas_wei is None
    assert result.failure_reason is None
    assert result.error_message is None


async def test_dry_run_executor_satisfies_port() -> None:
    """Mypy garante que DryRunExecutor satisfaz OrderExecutor Protocol."""
    _: OrderExecutor = DryRunExecutor()
