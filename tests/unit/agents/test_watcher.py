"""Unit tests for WatcherAgent."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
from prometheus_client import CollectorRegistry

from polycopy.agents.watcher import TrackedWallet, WatcherAgent
from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.observability.metrics import make_metrics

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"


def _trade(
    *, tx_hash: str = "0x" + "cd" * 32, log_index: int = 0, occurred_at: datetime | None = None
) -> Trade:
    return Trade(
        tx_hash=tx_hash,
        log_index=log_index,
        wallet=WalletAddress(value=_VALID_ADDR),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=occurred_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )


class _FakeDataClient:
    def __init__(self, responses: list[list[Trade]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[WalletAddress, datetime | None]] = []

    async def fetch_user_activity(
        self,
        wallet: WalletAddress,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[Trade]:
        self.calls.append((wallet, since))
        if not self.responses:
            return []
        return self.responses.pop(0)


class _FakeRepo:
    def __init__(self, *, latest: datetime | None = None) -> None:
        self._latest = latest
        self.inserted: list[Trade] = []
        self.dedup_keys: set[tuple[str, int]] = set()

    async def insert_if_absent(self, trade: Trade) -> bool:
        key = (trade.tx_hash, trade.log_index)
        if key in self.dedup_keys:
            return False
        self.dedup_keys.add(key)
        self.inserted.append(trade)
        return True

    async def latest_occurred_at(self, wallet: WalletAddress) -> datetime | None:
        return self._latest


def _repo_factory(repo: _FakeRepo):
    @asynccontextmanager
    async def _factory() -> AsyncIterator[_FakeRepo]:
        yield repo

    return _factory


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[WalletTradeDetected] = []

    async def publish_wallet_trade_detected(self, event: WalletTradeDetected) -> None:
        self.published.append(event)


def _agent(
    *,
    data_client: _FakeDataClient,
    repo: _FakeRepo,
    bus: _FakeBus,
    wallets: list[TrackedWallet] | None = None,
    bootstrap_hours: int = 24,
) -> tuple[WatcherAgent, asyncio.Event]:
    stopping = asyncio.Event()
    metrics = make_metrics(registry=CollectorRegistry())
    agent = WatcherAgent(
        stopping=stopping,
        interval_s=0.01,
        wallets=wallets or [TrackedWallet(address=WalletAddress(value=_VALID_ADDR), label="W1")],
        data_client=data_client,  # type: ignore[arg-type]
        repo_factory=_repo_factory(repo),
        bus=bus,  # type: ignore[arg-type]
        metrics=metrics,
        bootstrap_hours=bootstrap_hours,
    )
    return agent, stopping


async def test_bootstrap_uses_now_minus_24h_when_repo_returns_none() -> None:
    data = _FakeDataClient(responses=[[]])
    repo = _FakeRepo(latest=None)
    bus = _FakeBus()
    agent, _ = _agent(data_client=data, repo=repo, bus=bus, bootstrap_hours=24)

    await agent.run_once()

    assert len(data.calls) == 1
    _, since = data.calls[0]
    assert since is not None
    delta = datetime.now(tz=UTC) - since
    # 24h ± alguns segundos
    assert timedelta(hours=23, minutes=59) < delta < timedelta(hours=24, minutes=1)


async def test_uses_latest_occurred_at_as_cursor() -> None:
    cursor = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)
    data = _FakeDataClient(responses=[[]])
    repo = _FakeRepo(latest=cursor)
    bus = _FakeBus()
    agent, _ = _agent(data_client=data, repo=repo, bus=bus)

    await agent.run_once()

    _, since = data.calls[0]
    assert since == cursor


async def test_dedup_publishes_only_inserted_trades() -> None:
    t_a = _trade(tx_hash="0x" + "11" * 32, log_index=0)
    t_b = _trade(tx_hash="0x" + "22" * 32, log_index=0)
    data = _FakeDataClient(responses=[[t_a, t_b]])
    repo = _FakeRepo(latest=datetime(2026, 5, 1, 10, 0, tzinfo=UTC))
    repo.dedup_keys.add((t_a.tx_hash, t_a.log_index))  # já existe
    bus = _FakeBus()
    agent, _ = _agent(data_client=data, repo=repo, bus=bus)

    await agent.run_once()

    assert len(repo.inserted) == 1
    assert repo.inserted[0].tx_hash == t_b.tx_hash
    assert len(bus.published) == 1
    assert bus.published[0].trade.tx_hash == t_b.tx_hash


async def test_data_client_error_logs_and_continues() -> None:
    class _RaisingClient(_FakeDataClient):
        async def fetch_user_activity(
            self,
            wallet: WalletAddress,
            since: datetime | None = None,
            limit: int = 100,
        ) -> list[Trade]:
            raise httpx.HTTPStatusError(
                "503",
                request=httpx.Request("GET", "x"),
                response=httpx.Response(503),
            )

    data = _RaisingClient(responses=[])
    repo = _FakeRepo(latest=datetime(2026, 5, 1, 10, 0, tzinfo=UTC))
    bus = _FakeBus()
    agent, _ = _agent(data_client=data, repo=repo, bus=bus)

    # Não deve levantar — W1
    await agent.run_once()
    assert len(bus.published) == 0


async def test_run_loop_stops_on_event() -> None:
    import time as _time

    data = _FakeDataClient(responses=[[]] * 10)
    repo = _FakeRepo(latest=datetime(2026, 5, 1, 10, 0, tzinfo=UTC))
    bus = _FakeBus()
    agent, stopping = _agent(data_client=data, repo=repo, bus=bus)

    task = asyncio.create_task(agent.run())
    deadline = _time.monotonic() + 1.0
    while not data.calls and _time.monotonic() < deadline:
        await asyncio.sleep(0)
    stopping.set()
    await task

    assert len(data.calls) >= 1
