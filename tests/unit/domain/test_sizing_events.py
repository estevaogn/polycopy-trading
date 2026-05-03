"""Testes unit dos events e value object do Plano 2C (Sizing)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from polycopy.domain.events import (
    OrderSized,
    OrderSkipped,
    SkipReason,
)
from polycopy.domain.models import Side, Trade
from polycopy.domain.sizing import OrderSizing
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
# OrderSized
# ---------------------------------------------------------------------------


def test_order_sized_requires_tzaware_occurred() -> None:
    with pytest.raises(ValidationError):
        OrderSized(
            event_id=uuid4(),
            occurred_at=datetime(2026, 1, 1),  # naive
            decided_at=datetime.now(tz=UTC),
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
            original_size_usdc=Money.from_usdc("100"),
        )


def test_order_sized_requires_tzaware_decided() -> None:
    with pytest.raises(ValidationError):
        OrderSized(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            decided_at=datetime(2026, 1, 1),  # naive
            trade=_trade(),
            final_size_usdc=Money.from_usdc("10"),
            original_size_usdc=Money.from_usdc("100"),
        )


def test_order_sized_subject_constant() -> None:
    assert OrderSized.SUBJECT == "order.sized"


# ---------------------------------------------------------------------------
# OrderSkipped
# ---------------------------------------------------------------------------


def test_order_skipped_requires_reason() -> None:
    with pytest.raises(ValidationError):
        OrderSkipped(  # type: ignore[call-arg]
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            decided_at=datetime.now(tz=UTC),
            trade=_trade(),
        )


def test_order_skipped_subject_constant() -> None:
    assert OrderSkipped.SUBJECT == "order.skipped"


# ---------------------------------------------------------------------------
# SkipReason
# ---------------------------------------------------------------------------


def test_skip_reason_values() -> None:
    assert SkipReason.BELOW_MIN_SIZE.value == "below_min_size"


# ---------------------------------------------------------------------------
# OrderSizing (value object)
# ---------------------------------------------------------------------------


def test_order_sizing_sized_with_reason_raises() -> None:
    with pytest.raises(ValueError, match="sized decision must have reason=None"):
        OrderSizing(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            original_size_usdc=Decimal("100"),
            final_size_usdc=Decimal("10"),
            decision="sized",
            reason=SkipReason.BELOW_MIN_SIZE,
            decided_at=datetime.now(tz=UTC),
        )


def test_order_sizing_sized_without_size_raises() -> None:
    with pytest.raises(ValueError, match="sized decision must have final_size_usdc"):
        OrderSizing(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            original_size_usdc=Decimal("100"),
            final_size_usdc=None,
            decision="sized",
            reason=None,
            decided_at=datetime.now(tz=UTC),
        )


def test_order_sizing_skipped_with_size_raises() -> None:
    with pytest.raises(ValueError, match="skipped decision must have final_size_usdc=None"):
        OrderSizing(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            original_size_usdc=Decimal("100"),
            final_size_usdc=Decimal("10"),
            decision="skipped",
            reason=SkipReason.BELOW_MIN_SIZE,
            decided_at=datetime.now(tz=UTC),
        )


def test_order_sizing_skipped_without_reason_raises() -> None:
    with pytest.raises(ValueError, match="skipped decision must have a reason"):
        OrderSizing(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            original_size_usdc=Decimal("100"),
            final_size_usdc=None,
            decision="skipped",
            reason=None,
            decided_at=datetime.now(tz=UTC),
        )


def test_order_sizing_negative_size_raises() -> None:
    with pytest.raises(ValueError, match="original_size_usdc must be positive"):
        OrderSizing(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            original_size_usdc=Decimal("0"),
            final_size_usdc=None,
            decision="skipped",
            reason=SkipReason.BELOW_MIN_SIZE,
            decided_at=datetime.now(tz=UTC),
        )


def test_order_sizing_naive_decided_at_raises() -> None:
    with pytest.raises(ValueError, match="decided_at must be timezone-aware"):
        OrderSizing(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            original_size_usdc=Decimal("100"),
            final_size_usdc=Decimal("10"),
            decision="sized",
            reason=None,
            decided_at=datetime(2026, 1, 1),
        )


def test_order_sizing_valid_sized() -> None:
    s = OrderSizing(
        trade_event_id=uuid4(),
        wallet="0x" + "1" * 40,
        condition_id="0x" + "cd" * 32,
        token_id="42",
        original_size_usdc=Decimal("100"),
        final_size_usdc=Decimal("10"),
        decision="sized",
        reason=None,
        decided_at=datetime.now(tz=UTC),
    )
    assert s.decision == "sized"
    assert s.final_size_usdc == Decimal("10")
    assert s.reason is None


def test_order_sizing_valid_skipped() -> None:
    s = OrderSizing(
        trade_event_id=uuid4(),
        wallet="0x" + "1" * 40,
        condition_id="0x" + "cd" * 32,
        token_id="42",
        original_size_usdc=Decimal("1"),
        final_size_usdc=None,
        decision="skipped",
        reason=SkipReason.BELOW_MIN_SIZE,
        decided_at=datetime.now(tz=UTC),
    )
    assert s.decision == "skipped"
    assert s.final_size_usdc is None
    assert s.reason == SkipReason.BELOW_MIN_SIZE
