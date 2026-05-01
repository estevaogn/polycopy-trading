"""Unit tests for domain models."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from polycopy.domain.models import Side, Trade, Wallet
from polycopy.domain.value_objects import (
    Bps,
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"
_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN = "12345678901234567890"
_VALID_TX = "0x" + "cd" * 32


class TestWallet:
    def test_construct_minimal(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="alice",
            enabled=True,
        )
        assert w.nickname == "alice"
        assert w.enabled is True
        assert w.max_slippage_bps.value == 200  # default

    def test_disabled_wallet(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="bob",
            enabled=False,
        )
        assert w.enabled is False

    def test_custom_slippage(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="alice",
            enabled=True,
            max_slippage_bps=Bps(value=500),
        )
        assert w.max_slippage_bps.value == 500

    def test_frozen(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="alice",
            enabled=True,
        )
        with pytest.raises(ValidationError):
            w.enabled = False  # type: ignore[misc]


class TestTrade:
    def _trade(self, **overrides: object) -> Trade:
        defaults: dict[str, object] = {
            "tx_hash": _VALID_TX,
            "log_index": 0,
            "wallet": WalletAddress(value=_VALID_ADDR),
            "condition_id": ConditionId(value=_VALID_COND),
            "token_id": TokenId(value=_VALID_TOKEN),
            "side": Side.BUY,
            "price": Price(value=Decimal("0.55")),
            "size_usdc": Money.from_usdc("10"),
            "occurred_at": datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        }
        defaults.update(overrides)
        return Trade(**defaults)  # type: ignore[arg-type]

    def test_construct_buy(self) -> None:
        t = self._trade()
        assert t.side is Side.BUY
        assert t.size_usdc.amount == Decimal("10.000000")

    def test_construct_sell(self) -> None:
        t = self._trade(side=Side.SELL)
        assert t.side is Side.SELL

    def test_dedup_key(self) -> None:
        t = self._trade()
        assert t.dedup_key == (_VALID_TX, 0)

    def test_negative_log_index_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._trade(log_index=-1)

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._trade(occurred_at=datetime(2026, 5, 1, 12, 0))  # sem tzinfo
