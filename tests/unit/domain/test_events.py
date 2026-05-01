"""Unit tests for domain events."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"
_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN = "12345"
_VALID_TX = "0x" + "cd" * 32


def _trade() -> Trade:
    return Trade(
        tx_hash=_VALID_TX,
        log_index=0,
        wallet=WalletAddress(value=_VALID_ADDR),
        condition_id=ConditionId(value=_VALID_COND),
        token_id=TokenId(value=_VALID_TOKEN),
        side=Side.BUY,
        price=Price(value=Decimal("0.55")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )


class TestWalletTradeDetected:
    def test_construct(self) -> None:
        ev = WalletTradeDetected(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            trade=_trade(),
        )
        assert isinstance(ev.event_id, UUID)
        assert ev.trade.dedup_key == (_VALID_TX, 0)

    def test_subject(self) -> None:
        assert WalletTradeDetected.SUBJECT == "wallet.trade.detected"

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletTradeDetected(
                event_id=uuid4(),
                occurred_at=datetime(2026, 5, 1, 12, 0),  # naive
                trade=_trade(),
            )

    def test_serialization_roundtrip(self) -> None:
        ev = WalletTradeDetected(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            trade=_trade(),
        )
        payload = ev.model_dump_json()
        restored = WalletTradeDetected.model_validate_json(payload)
        assert restored == ev

    def test_frozen(self) -> None:
        ev = WalletTradeDetected(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=UTC),
            trade=_trade(),
        )
        with pytest.raises(ValidationError):
            ev.event_id = uuid4()  # type: ignore[misc]
