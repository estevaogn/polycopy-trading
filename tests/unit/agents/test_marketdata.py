"""Testes unit do MarketDataAgent — Gamma + repo mockados via Protocol."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.marketdata import MarketDataAgent
from polycopy.domain.market import Market
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.ports import CachedMarket, MarketRepository, PolymarketGammaPort


def _market(token_id: str = "1") -> Market:  # noqa: S107  # token_id é id de mercado, não senha
    return Market(
        token_id=TokenId(value=token_id),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        question="?",
        slug="?",
        outcome="Yes",
        end_date=datetime.now(tz=UTC) + timedelta(days=7),
        is_active=True,
        is_archived=False,
        volume_24h_usdc=Money.from_usdc("100000"),
        liquidity_usdc=Money.from_usdc("5000"),
    )


class _StubGamma:
    def __init__(self, markets: list[Market]) -> None:
        self._markets = markets
        self.calls = 0

    async def get_market(self, token_id: TokenId) -> Market | None:
        return None

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        self.calls += 1
        return self._markets[:limit]


class _StubRepo:
    def __init__(self) -> None:
        self.upserts: list[list[Market]] = []

    async def upsert_many(self, markets: list[Market]) -> int:
        self.upserts.append(list(markets))
        return len(markets)

    async def get_market(self, token_id: TokenId) -> CachedMarket | None:
        return None


def _accepts_gamma(_: PolymarketGammaPort) -> None: ...
def _accepts_repo(_: MarketRepository) -> None: ...


@pytest.fixture
def metrics() -> object:
    return make_metrics(registry=CollectorRegistry())


async def test_run_once_pulls_and_upserts(metrics: object) -> None:
    gamma = _StubGamma([_market("100"), _market("101")])
    repo = _StubRepo()
    _accepts_gamma(gamma)
    _accepts_repo(repo)

    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_repo_factory(repo),
        top_n=2,
        metrics=metrics,
    )

    await agent.run_once()

    assert gamma.calls == 1
    assert len(repo.upserts) == 1
    assert len(repo.upserts[0]) == 2


async def test_loop_stops_on_event(metrics: object) -> None:
    gamma = _StubGamma([_market("200")])
    repo = _StubRepo()
    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_repo_factory(repo),
        top_n=1,
        metrics=metrics,
    )

    async def stopper() -> None:
        await asyncio.sleep(0.15)
        stopping.set()

    await asyncio.gather(agent.run(), stopper())

    assert gamma.calls >= 1


async def test_gamma_failure_logged_metric_continues(metrics: object) -> None:
    class FlakyGamma:
        def __init__(self) -> None:
            self.calls = 0

        async def get_market(self, token_id: TokenId) -> Market | None:
            return None

        async def list_active_markets(self, *, limit: int) -> list[Market]:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("simulated gamma down")
            return [_market("300")]

    gamma = FlakyGamma()
    repo = _StubRepo()
    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_repo_factory(repo),
        top_n=1,
        metrics=metrics,
    )

    async def stopper() -> None:
        await asyncio.sleep(0.30)
        stopping.set()

    await asyncio.gather(agent.run(), stopper())

    # Pelo menos 1 sucesso após falha inicial.
    assert any(len(b) == 1 for b in repo.upserts)


def _repo_factory(repo: _StubRepo):
    """Wrap repo num async context manager pra bater com a assinatura do agente."""

    @asynccontextmanager
    async def _factory() -> AsyncIterator[_StubRepo]:
        yield repo

    return _factory
