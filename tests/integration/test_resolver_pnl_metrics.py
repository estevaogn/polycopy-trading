"""Integration: ResolverAgent popula gauges Prometheus após cada loop."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.resolver import ResolverAgent
from polycopy.domain.resolution import MarketResolution
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.market_resolution_repository import (
    SqlAlchemyMarketResolutionRepository,
)
from polycopy.ports import MarketResolutionRepository

pytestmark = pytest.mark.integration


def _unique_cond() -> str:
    return "0x" + uuid.uuid4().hex.ljust(64, "0")[:64]


class _StubGamma:
    async def get_market(self, token_id):
        return None

    async def list_active_markets(self, *, limit: int):
        return []

    async def list_markets_by_condition_ids_closed(self, *, condition_ids: list[str], limit: int):
        return []


async def test_resolver_metrics_populated_after_loop(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Após run_once, os gauges aparecem com valores plausíveis."""
    cond = _unique_cond()
    tid = uuid.uuid4()

    async with db_session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO order_executions "
                "(trade_event_id, wallet, condition_id, token_id, side, "
                " final_size_usdc, mode, result, decided_at, expected_avg_price) "
                "VALUES (:t, :w, :c, '111', 'BUY', 10, 'dry_run', 'dry_run', "
                "        now(), 0.5)"
            ),
            {"t": tid, "w": "0x" + "1" * 40, "c": cond},
        )
        await session.execute(
            text(
                "INSERT INTO market_resolutions "
                "(condition_id, resolved_outcome, winning_token_id, "
                " resolved_at, outcome_prices_raw) "
                "VALUES (:c, 'YES', '111', now(), '[\"1\",\"0\"]')"
            ),
            {"c": cond},
        )
        await session.commit()

    metrics = make_metrics(registry=CollectorRegistry())

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        async with db_session_factory() as session:
            yield SqlAlchemyMarketResolutionRepository(session)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=_StubGamma(),
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    assert metrics.hypothetical_trades_resolved._value.get() >= 1
    assert metrics.hypothetical_pnl_total_usdc._value.get() != 0


async def test_resolver_metrics_set_after_loop_with_no_pending(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Gauges são setados mesmo quando não há condition_ids pendentes de resolução.

    Não presume banco vazio (testes de integração compartilham dados). Verifica
    apenas que os gauges foram atualizados (valores >= 0) após run_once.
    """
    metrics = make_metrics(registry=CollectorRegistry())
    # Marca sentinela impossível antes do run
    metrics.hypothetical_trades_pending.set(-999)

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        async with db_session_factory() as session:
            yield SqlAlchemyMarketResolutionRepository(session)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=_StubGamma(),
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    # O gauge deve ter sido sobrescrito pelo agente (não mais -999)
    assert metrics.hypothetical_trades_pending._value.get() >= 0
    assert metrics.hypothetical_pnl_total_usdc._value.get() >= 0


async def test_resolver_metrics_query_failure_logs_warning(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Se get_pnl_summary falha, gauges ficam stale (não zerados), log warning."""
    metrics = make_metrics(registry=CollectorRegistry())
    metrics.hypothetical_pnl_total_usdc.set(99.99)

    class _FailingRepo:
        async def insert(self, resolution: MarketResolution) -> bool:
            return True

        async def get_unresolved_condition_ids(self, *, limit: int):
            return []

        async def get_pnl_summary(self):
            raise RuntimeError("simulated DB failure")

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        yield _FailingRepo()

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=_StubGamma(),
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    assert metrics.hypothetical_pnl_total_usdc._value.get() == 99.99
