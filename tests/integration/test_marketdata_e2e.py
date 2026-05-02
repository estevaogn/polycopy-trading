"""E2E do MarketDataAgent: agente real + Postgres real + Gamma fake (respx)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.marketdata import MarketDataAgent, _make_repo_factory
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.models import MarketRow
from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

pytestmark = pytest.mark.integration

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "polymarket"


@respx.mock
async def test_one_sync_cycle_populates_markets(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payload = json.loads((_FIXTURES / "gamma_market.json").read_text())
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    repo_factory = _make_repo_factory(db_session_factory, ttl_seconds=1800)

    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        top_n=2,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(select(MarketRow))
        rows = result.scalars().all()
    assert len(rows) >= 1


@respx.mock
async def test_two_cycles_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payload = json.loads((_FIXTURES / "gamma_market.json").read_text())
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    repo_factory = _make_repo_factory(db_session_factory, ttl_seconds=1800)

    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        top_n=2,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(select(MarketRow))
        n_after_1 = len(result.scalars().all())

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(select(MarketRow))
        n_after_2 = len(result.scalars().all())

    assert n_after_1 == n_after_2  # idempotente
