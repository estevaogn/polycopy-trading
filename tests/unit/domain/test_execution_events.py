"""Testes unit dos events, enums e value object do Plano 3 (Executor)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from polycopy.domain.events import (
    ExecutionMode,
    FailureReason,
    OrderDryRun,
    OrderExecuted,
    OrderFailed,
)
from polycopy.domain.execution import ExecutionResult, OrderExecution
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("100"),
        occurred_at=datetime.now(tz=UTC),
    )


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


def test_execution_mode_values() -> None:
    assert ExecutionMode.REAL.value == "real"
    assert ExecutionMode.DRY_RUN.value == "dry_run"


def test_failure_reason_values() -> None:
    assert FailureReason.INVALID_TRADE_PARAMS.value == "invalid_trade_params"
    assert FailureReason.EXECUTOR_DISABLED.value == "executor_disabled"


# ---------------------------------------------------------------------------
# OrderExecuted
# ---------------------------------------------------------------------------


def test_order_executed_subject_constant() -> None:
    assert OrderExecuted.SUBJECT == "order.executed"


def test_order_executed_requires_tzaware_occurred() -> None:
    with pytest.raises(ValidationError):
        OrderExecuted(
            event_id=uuid4(),
            occurred_at=datetime(2026, 1, 1),  # naive
            decided_at=datetime.now(tz=UTC),
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
            tx_hash="0x" + "ee" * 32,
            gas_wei=21_000,
        )


def test_order_executed_requires_tzaware_decided() -> None:
    with pytest.raises(ValidationError):
        OrderExecuted(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            decided_at=datetime(2026, 1, 1),  # naive
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
            tx_hash="0x" + "ee" * 32,
            gas_wei=21_000,
        )


def test_order_executed_gas_non_negative() -> None:
    with pytest.raises(ValidationError):
        OrderExecuted(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            decided_at=datetime.now(tz=UTC),
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
            tx_hash="0x" + "ee" * 32,
            gas_wei=-1,
        )


def test_order_executed_happy_path() -> None:
    ev = OrderExecuted(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        final_size_usdc=Money.from_usdc("10"),
        tx_hash="0x" + "ee" * 32,
        gas_wei=21_000,
    )
    assert ev.tx_hash.startswith("0x")
    assert ev.gas_wei == 21_000


# ---------------------------------------------------------------------------
# OrderFailed
# ---------------------------------------------------------------------------


def test_order_failed_subject_constant() -> None:
    assert OrderFailed.SUBJECT == "order.failed"


def test_order_failed_requires_tzaware_occurred() -> None:
    with pytest.raises(ValidationError):
        OrderFailed(
            event_id=uuid4(),
            occurred_at=datetime(2026, 1, 1),  # naive
            decided_at=datetime.now(tz=UTC),
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
            reason=FailureReason.INVALID_TRADE_PARAMS,
            error_message="boom",
        )


def test_order_failed_requires_tzaware_decided() -> None:
    with pytest.raises(ValidationError):
        OrderFailed(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            decided_at=datetime(2026, 1, 1),  # naive
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
            reason=FailureReason.INVALID_TRADE_PARAMS,
            error_message="boom",
        )


def test_order_failed_happy_path() -> None:
    ev = OrderFailed(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        final_size_usdc=Money.from_usdc("10"),
        reason=FailureReason.EXECUTOR_DISABLED,
        error_message="executor offline",
    )
    assert ev.reason == FailureReason.EXECUTOR_DISABLED
    assert ev.error_message == "executor offline"


# ---------------------------------------------------------------------------
# OrderDryRun
# ---------------------------------------------------------------------------


def test_order_dry_run_subject_constant() -> None:
    assert OrderDryRun.SUBJECT == "order.dry_run"


def test_order_dry_run_requires_tzaware_occurred() -> None:
    with pytest.raises(ValidationError):
        OrderDryRun(
            event_id=uuid4(),
            occurred_at=datetime(2026, 1, 1),  # naive
            decided_at=datetime.now(tz=UTC),
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
        )


def test_order_dry_run_requires_tzaware_decided() -> None:
    with pytest.raises(ValidationError):
        OrderDryRun(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            decided_at=datetime(2026, 1, 1),  # naive
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
        )


def test_order_dry_run_happy_path() -> None:
    ev = OrderDryRun(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        final_size_usdc=Money.from_usdc("10"),
    )
    assert ev.final_size_usdc == Money.from_usdc("10")


# ---------------------------------------------------------------------------
# OrderExecution (value object) — invariantes
# ---------------------------------------------------------------------------


def _execution_dry_run(**overrides: object) -> OrderExecution:
    defaults: dict[str, object] = {
        "trade_event_id": uuid4(),
        "wallet": "0x" + "1" * 40,
        "condition_id": "0x" + "cd" * 32,
        "token_id": "42",
        "final_size_usdc": Decimal("10"),
        "mode": ExecutionMode.DRY_RUN,
        "result": "dry_run",
        "tx_hash": None,
        "gas_wei": None,
        "failure_reason": None,
        "error_message": None,
        "decided_at": datetime.now(tz=UTC),
    }
    defaults.update(overrides)
    return OrderExecution(**defaults)  # type: ignore[arg-type]


def _execution_executed(**overrides: object) -> OrderExecution:
    defaults: dict[str, object] = {
        "trade_event_id": uuid4(),
        "wallet": "0x" + "1" * 40,
        "condition_id": "0x" + "cd" * 32,
        "token_id": "42",
        "final_size_usdc": Decimal("10"),
        "mode": ExecutionMode.REAL,
        "result": "executed",
        "tx_hash": "0x" + "ee" * 32,
        "gas_wei": 21_000,
        "failure_reason": None,
        "error_message": None,
        "decided_at": datetime.now(tz=UTC),
    }
    defaults.update(overrides)
    return OrderExecution(**defaults)  # type: ignore[arg-type]


def _execution_failed(**overrides: object) -> OrderExecution:
    defaults: dict[str, object] = {
        "trade_event_id": uuid4(),
        "wallet": "0x" + "1" * 40,
        "condition_id": "0x" + "cd" * 32,
        "token_id": "42",
        "final_size_usdc": Decimal("10"),
        "mode": ExecutionMode.REAL,
        "result": "failed",
        "tx_hash": None,
        "gas_wei": None,
        "failure_reason": FailureReason.INVALID_TRADE_PARAMS,
        "error_message": "bad params",
        "decided_at": datetime.now(tz=UTC),
    }
    defaults.update(overrides)
    return OrderExecution(**defaults)  # type: ignore[arg-type]


# Invariante 1: mode == REAL ↔ result ∈ {executed, failed}
def test_order_execution_real_dry_run_result_raises() -> None:
    with pytest.raises(ValueError, match="real mode must produce executed or failed"):
        _execution_executed(mode=ExecutionMode.REAL, result="dry_run", tx_hash=None, gas_wei=None)


def test_order_execution_real_executed_ok() -> None:
    ex = _execution_executed()
    assert ex.mode == ExecutionMode.REAL
    assert ex.result == "executed"


# Invariante 2: mode == DRY_RUN ↔ result IN {"dry_run", "failed"}
def test_order_execution_dry_run_executed_result_raises() -> None:
    with pytest.raises(ValueError, match=r"dry_run mode must produce result='dry_run' or 'failed'"):
        _execution_dry_run(result="executed", tx_hash="0x" + "ee" * 32)


def test_order_execution_dry_run_ok() -> None:
    ex = _execution_dry_run()
    assert ex.mode == ExecutionMode.DRY_RUN
    assert ex.result == "dry_run"


def test_order_execution_dry_run_with_failed_is_valid() -> None:
    """C-1 fix: dry_run mode + result='failed' agora é válido (executor stub raise)."""
    ex = _execution_dry_run(
        result="failed",
        failure_reason=FailureReason.EXECUTOR_DISABLED,
        error_message="executor raised",
    )
    assert ex.mode == ExecutionMode.DRY_RUN
    assert ex.result == "failed"
    assert ex.failure_reason == FailureReason.EXECUTOR_DISABLED
    assert ex.error_message == "executor raised"


# Invariante 3: result == "executed" → tx_hash IS NOT NULL
def test_order_execution_executed_without_tx_hash_raises() -> None:
    with pytest.raises(ValueError, match="executed result must have tx_hash"):
        _execution_executed(tx_hash=None)


def test_order_execution_executed_with_tx_hash_ok() -> None:
    ex = _execution_executed(tx_hash="0x" + "aa" * 32)
    assert ex.tx_hash == "0x" + "aa" * 32


# Invariante 4: result == "failed" → failure_reason IS NOT NULL AND error_message IS NOT NULL
def test_order_execution_failed_without_reason_raises() -> None:
    with pytest.raises(ValueError, match="failed result must have failure_reason"):
        _execution_failed(failure_reason=None)


def test_order_execution_failed_without_error_message_raises() -> None:
    with pytest.raises(ValueError, match="failed result must have error_message"):
        _execution_failed(error_message=None)


def test_order_execution_failed_ok() -> None:
    ex = _execution_failed()
    assert ex.failure_reason == FailureReason.INVALID_TRADE_PARAMS
    assert ex.error_message == "bad params"


# Invariante 5: result == "dry_run" → tx_hash/gas_wei/failure_reason all None
def test_order_execution_dry_run_with_tx_hash_raises() -> None:
    with pytest.raises(ValueError, match="dry_run must have tx_hash=None"):
        _execution_dry_run(tx_hash="0x" + "ee" * 32)


def test_order_execution_dry_run_with_gas_wei_raises() -> None:
    with pytest.raises(ValueError, match="dry_run must have gas_wei=None"):
        _execution_dry_run(gas_wei=21_000)


def test_order_execution_dry_run_with_failure_reason_raises() -> None:
    with pytest.raises(ValueError, match="dry_run must have failure_reason=None"):
        _execution_dry_run(failure_reason=FailureReason.INVALID_TRADE_PARAMS)


# Invariante size positive
def test_order_execution_size_zero_raises() -> None:
    with pytest.raises(ValueError, match="final_size_usdc must be positive"):
        _execution_dry_run(final_size_usdc=Decimal("0"))


def test_order_execution_size_negative_raises() -> None:
    with pytest.raises(ValueError, match="final_size_usdc must be positive"):
        _execution_dry_run(final_size_usdc=Decimal("-1"))


# Invariante gas non-negative
def test_order_execution_gas_negative_raises() -> None:
    with pytest.raises(ValueError, match="gas_wei must be non-negative"):
        _execution_executed(gas_wei=-1)


# Invariante decided_at tz-aware
def test_order_execution_naive_decided_at_raises() -> None:
    with pytest.raises(ValueError, match="decided_at must be timezone-aware"):
        _execution_dry_run(decided_at=datetime(2026, 1, 1))


# ---------------------------------------------------------------------------
# ExecutionResult dataclass (smoke)
# ---------------------------------------------------------------------------


def test_execution_result_dry_run_defaults() -> None:
    r = ExecutionResult(mode=ExecutionMode.DRY_RUN, success=True)
    assert r.mode == ExecutionMode.DRY_RUN
    assert r.success is True
    assert r.tx_hash is None
    assert r.gas_wei is None
    assert r.failure_reason is None
    assert r.error_message is None
