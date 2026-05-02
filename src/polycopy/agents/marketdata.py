"""MarketDataAgent: sincroniza top N mercados ativos via Gamma para a tabela `markets`.

Rodando local (sem Docker):
    uv run python -m polycopy.agents.marketdata

Settings:
    MARKETDATA_SYNC_INTERVAL_SECONDS  default 300
    MARKETDATA_TOP_N                  default 200
    MARKETDATA_METRICS_PORT           default 9103
    GAMMA_API_BASE_URL                default https://gamma-api.polymarket.com
    MARKET_CACHE_TTL_SECONDS          default 1800 (lido por consumers do repo, não pelo agente)
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MarketRepository, PolymarketGammaPort

RepoFactory = Callable[[], AbstractAsyncContextManager[MarketRepository]]


class MarketDataAgent(AgentBase):
    name = "marketdata"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        sync_interval_s: float,
        gamma: PolymarketGammaPort,
        repo_factory: RepoFactory,
        top_n: int,
        metrics: Metrics,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=sync_interval_s)
        self._gamma = gamma
        self._repo_factory = repo_factory
        self._top_n = top_n
        self._metrics = metrics

    async def run_once(self) -> None:
        start = time.perf_counter()
        try:
            markets = await self._gamma.list_active_markets(limit=self._top_n)
            async with self._repo_factory() as repo:
                inserted = await repo.upsert_many(markets)
            self._metrics.marketdata_sync_total.labels(result="ok").inc()
            self._metrics.marketdata_markets_tracked.set(inserted)
            self._log.info(
                "marketdata_sync_completed",
                markets_synced=inserted,
                top_n=self._top_n,
            )
        except Exception as exc:
            # Continua o loop após falha. Alerta vem por métrica + log.
            # KeyboardInterrupt/SystemExit herdam de BaseException, então propagam.
            self._metrics.marketdata_sync_total.labels(result="fail").inc()
            self._log.warning(
                "marketdata_sync_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
        finally:
            self._metrics.marketdata_sync_duration_seconds.observe(time.perf_counter() - start)


def _make_repo_factory(
    session_factory: async_sessionmaker[AsyncSession], *, ttl_seconds: int
) -> RepoFactory:
    from polycopy.infrastructure.persistence.market_repository import (
        SqlAlchemyMarketRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyMarketRepository(session, ttl_seconds=ttl_seconds)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return _factory


async def main() -> None:
    """Entrypoint: monta dependências, sobe /metrics, registra signal handlers, roda."""
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.marketdata_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(
        session_factory, ttl_seconds=settings.market_cache_ttl_seconds
    )

    gamma = PolymarketGammaClient(
        base_url=settings.gamma_api_base_url,
        metrics=metrics,
    )

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=settings.marketdata_sync_interval_s,
        gamma=gamma,
        repo_factory=repo_factory,
        top_n=settings.marketdata_top_n,
        metrics=metrics,
    )
    try:
        await agent.run()
    finally:
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
