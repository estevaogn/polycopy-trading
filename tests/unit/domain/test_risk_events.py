"""Testes unit dos value objects do Plano 2B."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from polycopy.domain.events import (
    OrderApproved,
    RejectionReason,
    TradeRejected,
)
from polycopy.domain.models import Side, Trade
from polycopy.domain.risk import RiskDecision
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
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def test_order_approved_requires_tzaware() -> None:
    with pytest.raises(ValidationError):
        OrderApproved(
            event_id=uuid4(),
            occurred_at=datetime(2026, 1, 1),  # naive
            trade=_trade(),
        )


def test_order_approved_frozen() -> None:
    ev = OrderApproved(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=_trade(),
    )
    with pytest.raises(ValidationError):
        ev.event_id = uuid4()  # type: ignore[misc]


def test_order_approved_subject_constant() -> None:
    assert OrderApproved.SUBJECT == "order.approved"


def test_trade_rejected_requires_reason() -> None:
    with pytest.raises(ValidationError):
        TradeRejected(  # type: ignore[call-arg]
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            trade=_trade(),
        )


def test_trade_rejected_subject_constant() -> None:
    assert TradeRejected.SUBJECT == "trade.rejected"


def test_rejection_reason_values() -> None:
    assert RejectionReason.SIZE_EXCEEDED.value == "size_exceeded"
    assert RejectionReason.MARKET_NOT_CACHED.value == "market_not_cached"
    assert RejectionReason.MARKET_INACTIVE.value == "market_inactive"
    assert RejectionReason.PRICE_OUT_OF_RANGE.value == "price_out_of_range"
    assert RejectionReason.INSUFFICIENT_LIQUIDITY.value == "insufficient_liquidity"


def test_risk_decision_approved_with_reason_raises() -> None:
    with pytest.raises(ValueError, match="approved decision must have reason=None"):
        RiskDecision(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            decision="approved",
            reason=RejectionReason.SIZE_EXCEEDED,
            decided_at=datetime.now(tz=UTC),
        )


def test_risk_decision_rejected_without_reason_raises() -> None:
    with pytest.raises(ValueError, match="rejected decision must have a reason"):
        RiskDecision(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            decision="rejected",
            reason=None,
            decided_at=datetime.now(tz=UTC),
        )


def test_risk_decision_naive_decided_at_raises() -> None:
    with pytest.raises(ValueError, match="decided_at must be timezone-aware"):
        RiskDecision(
            trade_event_id=uuid4(),
            wallet="0x" + "1" * 40,
            condition_id="0x" + "cd" * 32,
            token_id="42",
            decision="approved",
            reason=None,
            decided_at=datetime(2026, 1, 1),
        )


def test_risk_decision_valid_approved() -> None:
    d = RiskDecision(
        trade_event_id=uuid4(),
        wallet="0x" + "1" * 40,
        condition_id="0x" + "cd" * 32,
        token_id="42",
        decision="approved",
        reason=None,
        decided_at=datetime.now(tz=UTC),
    )
    assert d.decision == "approved"


def test_risk_decision_valid_rejected() -> None:
    d = RiskDecision(
        trade_event_id=uuid4(),
        wallet="0x" + "1" * 40,
        condition_id="0x" + "cd" * 32,
        token_id="42",
        decision="rejected",
        reason=RejectionReason.MARKET_INACTIVE,
        decided_at=datetime.now(tz=UTC),
    )
    assert d.reason == RejectionReason.MARKET_INACTIVE
