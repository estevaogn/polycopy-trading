from __future__ import annotations

import json
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
import structlog

from polycopy.config import Environment, LogLevel
from polycopy.domain.discovery import (
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.observability.logging import configure_logging
from polycopy.scripts.discover_wallets import (
    DiscoverArgs,
    parse_args,
    run_discover,
)

SEED_YAML = """\
wallets:
  - address: "0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a"
    label: "bossoskil1"
"""


class FakeLeaderboard:
    def __init__(self, pages: list[list[LeaderboardEntry]]) -> None:
        self._pages = pages
        self.calls: list[dict[str, Any]] = []

    async def fetch_leaderboard(
        self,
        *,
        time_period: TimePeriod,
        category: Category,
        order_by: OrderBy = OrderBy.PNL,
        limit: int = 50,
        offset: int = 0,
    ) -> list[LeaderboardEntry]:
        self.calls.append({"limit": limit, "offset": offset})
        idx = offset // limit
        return self._pages[idx] if idx < len(self._pages) else []


def _entry(addr_hex: str, vol: str, pnl: str, name: str = "user") -> LeaderboardEntry:
    return LeaderboardEntry(
        rank=1,
        address=WalletAddress(value="0x" + addr_hex),
        user_name=name,
        volume_usdc=Decimal(vol),
        pnl_usdc=Decimal(pnl),
        verified_badge=False,
    )


class TestParseArgs:
    def test_defaults(self) -> None:
        args = parse_args([])
        assert args.time_period == TimePeriod.MONTH
        assert args.category == Category.OVERALL
        assert args.top == 50
        assert args.min_volume_usdc == Decimal("5000")
        assert args.dry_run is False

    def test_top_clamped_with_warning(self, capsys: pytest.CaptureFixture[str]) -> None:
        args = parse_args(["--top", "9999"])
        captured = capsys.readouterr()
        assert args.top == 1050
        assert "clamped" in captured.err.lower()

    def test_invalid_time_period(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["--time-period", "FOO"])


@pytest.mark.asyncio
class TestRunDiscover:
    async def test_writes_outputs(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(
            pages=[
                [
                    _entry("b" * 40, "10000", "500", name="alice"),
                    _entry("c" * 40, "10000", "400", name="bob"),
                ]
            ]
        )

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("5000"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        assert cands_out.exists()
        assert report_out.exists()
        text = cands_out.read_text(encoding="utf-8")
        assert "0x" + "b" * 40 in text
        assert "0x" + "c" * 40 in text

    async def test_excludes_seed_wallet(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        seeded_addr = "a5ea13a81d2b7e8e424b182bdc1db08e756bd96a"
        leaderboard = FakeLeaderboard(
            pages=[
                [
                    _entry(seeded_addr, "10000", "500"),
                    _entry("b" * 40, "10000", "400"),
                ]
            ]
        )

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("0"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        text = cands_out.read_text(encoding="utf-8")
        assert "0x" + seeded_addr not in text
        assert "0x" + "b" * 40 in text

    async def test_dry_run_does_not_write(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(
            pages=[
                [
                    _entry("b" * 40, "10000", "500"),
                ]
            ]
        )

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("0"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=True,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        assert not cands_out.exists()
        assert not report_out.exists()

    async def test_no_candidates_after_filters_exit_2(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(
            pages=[
                [
                    _entry("b" * 40, "100", "500"),
                ]
            ]
        )

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("5000"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 2
        assert not cands_out.exists()
        assert not report_out.exists()

    async def test_paginates_until_top(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text("wallets: []\n", encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        page0 = [_entry(f"{i:040x}", "10000", "100") for i in range(50)]
        page1 = [_entry(f"{i:040x}", "10000", "100") for i in range(50, 75)]
        leaderboard = FakeLeaderboard(pages=[page0, page1])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=70,
            min_volume_usdc=Decimal("0"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        assert len(leaderboard.calls) == 2
        assert leaderboard.calls[0]["offset"] == 0
        assert leaderboard.calls[1]["offset"] == 50

    async def test_seed_path_not_found_exit_1(self, tmp_path: Path) -> None:
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(pages=[])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("0"),
            seed_path=tmp_path / "does_not_exist.yaml",
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 1
        assert not cands_out.exists()
        assert not report_out.exists()

    async def test_api_failure_during_pagination_exit_1(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        import httpx

        class RaisingLeaderboard:
            def __init__(self) -> None:
                self.calls = 0

            async def fetch_leaderboard(
                self,
                *,
                time_period: TimePeriod,
                category: Category,
                order_by: OrderBy = OrderBy.PNL,
                limit: int = 50,
                offset: int = 0,
            ) -> list[LeaderboardEntry]:
                self.calls += 1
                if self.calls == 1:
                    return [_entry(f"{i:040x}", "10000", "100") for i in range(50)]
                request = httpx.Request("GET", "https://example/v1/leaderboard")
                response = httpx.Response(503, request=request, text="busy")
                raise httpx.HTTPStatusError("server error", request=request, response=response)

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=70,
            min_volume_usdc=Decimal("0"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, RaisingLeaderboard())  # type: ignore[arg-type]
        assert exit_code == 1
        assert not cands_out.exists()
        assert not report_out.exists()
        captured = capsys.readouterr()
        assert "503" in captured.err or "server error" in captured.err.lower()


class TestStructuredLogs:
    """Spec §8.2: 4 events emitidos via structlog em json mode."""

    @pytest.fixture
    def log_buf(self) -> StringIO:
        structlog.reset_defaults()
        buf = StringIO()
        configure_logging(env=Environment.PROD, level=LogLevel.INFO, stream=buf)
        return buf

    def _events(self, buf: StringIO) -> list[dict[str, Any]]:
        return [json.loads(line) for line in buf.getvalue().splitlines() if line.strip()]

    async def test_emits_all_four_events_on_happy_path(
        self, tmp_path: Path, log_buf: StringIO
    ) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(
            pages=[
                [
                    _entry("b" * 40, "10000", "500"),
                    _entry("c" * 40, "10000", "400"),
                ]
            ]
        )
        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("5000"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0

        events = {e["event"]: e for e in self._events(log_buf)}

        assert "discover_run_started" in events
        started = events["discover_run_started"]
        assert started["time_period"] == "MONTH"
        assert started["category"] == "OVERALL"
        assert started["top"] == 50
        assert started["min_volume_usdc"] == "5000"
        assert started["seed_size"] == 1

        assert "leaderboard_page_fetched" in events
        page = events["leaderboard_page_fetched"]
        assert page["offset"] == 0
        assert page["count"] == 2

        assert "discover_run_filtered" in events
        filtered = events["discover_run_filtered"]
        assert filtered["total_fetched"] == 2
        assert filtered["excluded_existing"] == 0
        assert filtered["excluded_min_volume"] == 0
        assert filtered["total_candidates"] == 2

        assert "discover_run_completed" in events
        completed = events["discover_run_completed"]
        assert completed["dry_run"] is False
        assert completed["candidates_path"].endswith("candidates.yaml")
        assert completed["report_path"].endswith("report.md")

    async def test_completed_event_signals_dry_run(self, tmp_path: Path, log_buf: StringIO) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        leaderboard = FakeLeaderboard(pages=[[_entry("b" * 40, "10000", "500")]])
        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("0"),
            seed_path=seed_path,
            candidates_out=tmp_path / "candidates.yaml",
            report_out=tmp_path / "report.md",
            dry_run=True,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0

        events = {e["event"]: e for e in self._events(log_buf)}
        completed = events["discover_run_completed"]
        assert completed["dry_run"] is True
        assert "candidates_path" not in completed
        assert "report_path" not in completed

    async def test_emits_one_page_event_per_pagination_call(
        self, tmp_path: Path, log_buf: StringIO
    ) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text("wallets: []\n", encoding="utf-8")

        page0 = [_entry(f"{i:040x}", "10000", "100") for i in range(50)]
        page1 = [_entry(f"{i:040x}", "10000", "100") for i in range(50, 75)]
        leaderboard = FakeLeaderboard(pages=[page0, page1])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=70,
            min_volume_usdc=Decimal("0"),
            seed_path=seed_path,
            candidates_out=tmp_path / "candidates.yaml",
            report_out=tmp_path / "report.md",
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0

        page_events = [e for e in self._events(log_buf) if e["event"] == "leaderboard_page_fetched"]
        assert len(page_events) == 2
        assert page_events[0]["offset"] == 0
        assert page_events[0]["count"] == 50
        assert page_events[1]["offset"] == 50
        assert page_events[1]["count"] == 25
