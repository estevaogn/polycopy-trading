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
