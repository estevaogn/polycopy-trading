from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path

import pytest

from polycopy.domain.discovery import (
    CandidateWallet,
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
    derive_label,
    filter_and_rank,
    render_candidates_yaml,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.wallets_seed import load_wallets_seed


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


class TestFilterAndRank:
    def _entry(
        self,
        *,
        rank: int,
        addr_hex: str,
        vol: str,
        pnl: str,
        name: str = "user",
    ) -> LeaderboardEntry:
        return LeaderboardEntry(
            rank=rank,
            address=WalletAddress(value="0x" + addr_hex),
            user_name=name,
            volume_usdc=Decimal(vol),
            pnl_usdc=Decimal(pnl),
            verified_badge=False,
        )

    def test_keeps_order_from_input(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500"),
            self._entry(rank=2, addr_hex="b" * 40, vol="20000", pnl="400"),
            self._entry(rank=3, addr_hex="c" * 40, vol="20000", pnl="300"),
        ]
        result = filter_and_rank(
            entries,
            min_volume_usdc=Decimal("0"),
            exclude=set(),
            top_n=10,
        )
        assert [c.rank for c in result] == [1, 2, 3]

    def test_excludes_seed_addresses(self) -> None:
        excluded = WalletAddress(value="0x" + "a" * 40)
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500"),
            self._entry(rank=2, addr_hex="b" * 40, vol="20000", pnl="400"),
        ]
        result = filter_and_rank(
            entries,
            min_volume_usdc=Decimal("0"),
            exclude={excluded},
            top_n=10,
        )
        assert len(result) == 1
        assert result[0].address.value == "0x" + "b" * 40

    def test_filters_by_min_volume(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="100", pnl="500"),
            self._entry(rank=2, addr_hex="b" * 40, vol="20000", pnl="400"),
        ]
        result = filter_and_rank(
            entries,
            min_volume_usdc=Decimal("1000"),
            exclude=set(),
            top_n=10,
        )
        assert len(result) == 1
        assert result[0].address.value == "0x" + "b" * 40

    def test_top_n_caps_output(self) -> None:
        entries = [
            self._entry(rank=i, addr_hex=f"{i:040x}", vol="20000", pnl="100") for i in range(1, 6)
        ]
        result = filter_and_rank(
            entries,
            min_volume_usdc=Decimal("0"),
            exclude=set(),
            top_n=2,
        )
        assert len(result) == 2

    def test_dedups_by_address(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500"),
            self._entry(rank=2, addr_hex="a" * 40, vol="20000", pnl="500"),
        ]
        result = filter_and_rank(
            entries,
            min_volume_usdc=Decimal("0"),
            exclude=set(),
            top_n=10,
        )
        assert len(result) == 1

    def test_empty_input(self) -> None:
        result = filter_and_rank(
            [],
            min_volume_usdc=Decimal("0"),
            exclude=set(),
            top_n=10,
        )
        assert result == []

    def test_label_derived_in_output(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500", name="alice"),
        ]
        result = filter_and_rank(
            entries,
            min_volume_usdc=Decimal("0"),
            exclude=set(),
            top_n=10,
        )
        assert result[0].label == "alice"


class TestRenderCandidatesYaml:
    def _candidate(self, addr_hex: str, label: str) -> CandidateWallet:
        return CandidateWallet(
            address=WalletAddress(value="0x" + addr_hex),
            label=label,
            rank=1,
            volume_usdc=Decimal("1000"),
            pnl_usdc=Decimal("100"),
            verified_badge=True,
        )

    def test_empty_list(self) -> None:
        assert render_candidates_yaml([]) == "wallets: []\n"

    def test_single_candidate_shape(self) -> None:
        out = render_candidates_yaml([self._candidate("a" * 40, "alice")])
        assert "wallets:" in out
        assert 'address: "0x' + "a" * 40 + '"' in out
        assert 'label: "alice"' in out

    def test_roundtrip_via_load_wallets_seed(self, tmp_path: Path) -> None:
        candidates = [
            self._candidate("a" * 40, "alice"),
            self._candidate("b" * 40, "bob"),
        ]
        yaml_text = render_candidates_yaml(candidates)
        path = tmp_path / "out.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        loaded = load_wallets_seed(path)
        assert len(loaded) == 2
        assert loaded[0].address.value == "0x" + "a" * 40
        assert loaded[0].label == "alice"
        assert loaded[1].label == "bob"

    def test_roundtrip_label_with_quotes_and_backslash(self, tmp_path: Path) -> None:
        candidates = [
            self._candidate("a" * 40, 'al"i\\ce'),
        ]
        yaml_text = render_candidates_yaml(candidates)
        path = tmp_path / "out.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        loaded = load_wallets_seed(path)
        assert len(loaded) == 1
        assert loaded[0].label == 'al"i\\ce'
