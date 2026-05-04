from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from polycopy.domain.discovery import (
    CandidateWallet,
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
    derive_label,
)
from polycopy.domain.value_objects import WalletAddress


class TestEnums:
    def test_time_period_values(self) -> None:
        assert TimePeriod.DAY.value == "DAY"
        assert TimePeriod.WEEK.value == "WEEK"
        assert TimePeriod.MONTH.value == "MONTH"
        assert TimePeriod.ALL.value == "ALL"

    def test_category_overall(self) -> None:
        assert Category.OVERALL.value == "OVERALL"

    def test_category_has_all_ten(self) -> None:
        names = {c.name for c in Category}
        assert names == {
            "OVERALL",
            "POLITICS",
            "SPORTS",
            "CRYPTO",
            "CULTURE",
            "MENTIONS",
            "WEATHER",
            "ECONOMICS",
            "TECH",
            "FINANCE",
        }

    def test_order_by_values(self) -> None:
        assert OrderBy.PNL.value == "PNL"
        assert OrderBy.VOL.value == "VOL"


class TestLeaderboardEntry:
    def test_minimum_fields(self) -> None:
        entry = LeaderboardEntry(
            rank=1,
            address=WalletAddress(value="0x" + "a" * 40),
            user_name="alice",
            volume_usdc=Decimal("1000"),
            pnl_usdc=Decimal("100"),
            verified_badge=True,
        )
        assert entry.rank == 1
        assert entry.user_name == "alice"

    def test_user_name_can_be_none(self) -> None:
        entry = LeaderboardEntry(
            rank=2,
            address=WalletAddress(value="0x" + "b" * 40),
            user_name=None,
            volume_usdc=Decimal("0"),
            pnl_usdc=Decimal("0"),
            verified_badge=False,
        )
        assert entry.user_name is None

    def test_frozen(self) -> None:
        entry = LeaderboardEntry(
            rank=1,
            address=WalletAddress(value="0x" + "c" * 40),
            user_name="x",
            volume_usdc=Decimal("0"),
            pnl_usdc=Decimal("0"),
            verified_badge=False,
        )
        with pytest.raises(FrozenInstanceError):
            entry.rank = 99  # type: ignore[misc]


class TestCandidateWallet:
    def test_minimum_fields(self) -> None:
        cand = CandidateWallet(
            address=WalletAddress(value="0x" + "d" * 40),
            label="alice",
            rank=1,
            volume_usdc=Decimal("1000"),
            pnl_usdc=Decimal("100"),
            verified_badge=True,
        )
        assert cand.label == "alice"


class TestDeriveLabel:
    def _entry(self, addr_hex: str = "a" * 40, user_name: str | None = None) -> LeaderboardEntry:
        return LeaderboardEntry(
            rank=1,
            address=WalletAddress(value="0x" + addr_hex),
            user_name=user_name,
            volume_usdc=Decimal("0"),
            pnl_usdc=Decimal("0"),
            verified_badge=False,
        )

    def test_user_name_present(self) -> None:
        assert derive_label(self._entry(user_name="alice")) == "alice"

    def test_user_name_trimmed(self) -> None:
        assert derive_label(self._entry(user_name="  bob  ")) == "bob"

    def test_user_name_whitespace_replaced_with_underscore(self) -> None:
        assert derive_label(self._entry(user_name="alice smith")) == "alice_smith"

    def test_user_name_internal_multiple_whitespace_collapsed(self) -> None:
        assert derive_label(self._entry(user_name="a   b\tc")) == "a_b_c"

    def test_user_name_non_printable_dropped(self) -> None:
        assert derive_label(self._entry(user_name="al\x00ice")) == "alice"

    def test_user_name_max_32_chars(self) -> None:
        long_name = "x" * 100
        assert len(derive_label(self._entry(user_name=long_name))) == 32

    def test_user_name_none_falls_back_to_address_prefix(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name=None))
        assert result == "0xcafef00d…"

    def test_user_name_empty_string_falls_back(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name=""))
        assert result == "0xcafef00d…"

    def test_user_name_only_whitespace_falls_back(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name="   "))
        assert result == "0xcafef00d…"

    def test_user_name_only_non_printable_falls_back(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name="\x00\x01"))
        assert result == "0xcafef00d…"
