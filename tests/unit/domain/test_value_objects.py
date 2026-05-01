"""Unit tests for domain value objects."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from polycopy.domain.value_objects import (
    Bps,
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)


class TestMoney:
    def test_construct_quantizes_to_six_decimals(self) -> None:
        m = Money(amount=Decimal("1.123456789"))
        assert m.amount == Decimal("1.123457")  # ROUND_HALF_EVEN

    def test_zero_factory(self) -> None:
        assert Money.zero().amount == Decimal("0.000000")

    def test_from_usdc_int(self) -> None:
        assert Money.from_usdc(100).amount == Decimal("100.000000")

    def test_from_usdc_str(self) -> None:
        assert Money.from_usdc("1.5").amount == Decimal("1.500000")

    def test_addition(self) -> None:
        a = Money.from_usdc("1.50")
        b = Money.from_usdc("2.25")
        assert (a + b).amount == Decimal("3.750000")

    def test_subtraction(self) -> None:
        a = Money.from_usdc("5.00")
        b = Money.from_usdc("1.50")
        assert (a - b).amount == Decimal("3.500000")

    def test_lt(self) -> None:
        assert Money.from_usdc("1") < Money.from_usdc("2")
        assert not (Money.from_usdc("2") < Money.from_usdc("1"))

    def test_frozen(self) -> None:
        m = Money.from_usdc("1")
        with pytest.raises(ValidationError):
            m.amount = Decimal("99")  # type: ignore[misc]

    def test_negative_allowed(self) -> None:
        # Money pode ser negativo (PnL drawdown). Sem validador de >= 0.
        assert Money.from_usdc("-1.5").amount == Decimal("-1.500000")


class TestPrice:
    def test_zero_and_one_are_valid(self) -> None:
        assert Price(value=Decimal("0")).value == Decimal("0.0000")
        assert Price(value=Decimal("1")).value == Decimal("1.0000")

    def test_quantize_to_four_decimals(self) -> None:
        p = Price(value=Decimal("0.12345678"))
        assert p.value == Decimal("0.1235")

    def test_below_zero_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Price(value=Decimal("-0.0001"))

    def test_above_one_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Price(value=Decimal("1.0001"))


class TestBps:
    def test_construct_from_int(self) -> None:
        assert Bps(value=200).value == 200

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Bps(value=-1)

    def test_above_10000_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Bps(value=10001)

    def test_to_decimal_fraction(self) -> None:
        # 200 bps = 2% = 0.02
        assert Bps(value=200).as_fraction() == Decimal("0.02")


class TestWalletAddress:
    VALID = "0x1234567890abcdef1234567890abcdef12345678"

    def test_valid_lowercase(self) -> None:
        w = WalletAddress(value=self.VALID)
        assert w.value == self.VALID

    def test_normalized_to_lowercase(self) -> None:
        upper = "0x" + "A" * 40
        w = WalletAddress(value=upper)
        assert w.value == upper.lower()

    def test_missing_0x_prefix_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletAddress(value=self.VALID[2:])

    def test_wrong_length_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletAddress(value="0x1234")

    def test_non_hex_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletAddress(value="0x" + "z" * 40)


class TestConditionId:
    VALID = "0x" + "ab" * 32  # 64 hex chars

    def test_valid(self) -> None:
        c = ConditionId(value=self.VALID)
        assert c.value == self.VALID

    def test_normalized_to_lowercase(self) -> None:
        upper = "0x" + "AB" * 32
        c = ConditionId(value=upper)
        assert c.value == upper.lower()

    def test_wrong_length_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ConditionId(value="0xabcd")


class TestTokenId:
    def test_string_form_accepted(self) -> None:
        t = TokenId(value="123456789012345678901234567890")
        assert t.value == "123456789012345678901234567890"

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TokenId(value="-1")

    def test_non_numeric_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TokenId(value="abc")
