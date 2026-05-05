"""ResolverAgent: detecta quando markets do Polymarket resolvem (YES/NO/INVALID).

Loop polling-driven (não consome JetStream). A cada RESOLVER_SYNC_INTERVAL_SECONDS:
1. Lê condition_ids de wallet_trades sem resolução em market_resolutions.
2. Consulta Gamma com filtro closed=true + condition_ids.
3. Classifica cada market via _classify_resolution (tolerâncias de pricing).
4. Insere idempotentemente em market_resolutions.

Plano 5A — primeira peça da Fase 5 (backtest infra).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution, ResolvedMarketDTO
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MarketResolutionRepository, PolymarketGammaPort

RepoFactory = Callable[[], AbstractAsyncContextManager[MarketResolutionRepository]]


_TOLERANCE_TERMINAL = Decimal("0.01")  # extremos: ≥0.99 / ≤0.01
_TOLERANCE_INVALID_LOW = Decimal("0.45")
_TOLERANCE_INVALID_HIGH = Decimal("0.55")


def _classify_resolution(dto: ResolvedMarketDTO) -> MarketResolution | None:
    """Classifica um ResolvedMarketDTO em MarketResolution ou None (pending).

    Tolerâncias:
    - Terminal (YES/NO): preços ≥0.99 e ≤0.01
    - INVALID: ambos preços ∈ [0.45, 0.55]
    - Senão: pending UMA — retorna None
    """
    if not dto.closed:
        return None

    try:
        prices = json.loads(dto.outcome_prices_raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(prices, list) or len(prices) != 2:
        return None

    try:
        yes_price = Decimal(str(prices[0]))
        no_price = Decimal(str(prices[1]))
    except (ValueError, TypeError):
        return None

    now = datetime.now(tz=UTC)

    # Settled YES
    if yes_price >= (Decimal("1") - _TOLERANCE_TERMINAL) and no_price <= _TOLERANCE_TERMINAL:
        return MarketResolution(
            condition_id=dto.condition_id,
            resolved_outcome=ResolvedOutcome.YES,
            winning_token_id=dto.yes_token_id,
            closed_time=dto.closed_time,
            resolved_at=now,
            outcome_prices_raw=dto.outcome_prices_raw,
            uma_resolution_statuses_raw=dto.uma_resolution_statuses_raw,
        )

    # Settled NO
    if no_price >= (Decimal("1") - _TOLERANCE_TERMINAL) and yes_price <= _TOLERANCE_TERMINAL:
        return MarketResolution(
            condition_id=dto.condition_id,
            resolved_outcome=ResolvedOutcome.NO,
            winning_token_id=dto.no_token_id,
            closed_time=dto.closed_time,
            resolved_at=now,
            outcome_prices_raw=dto.outcome_prices_raw,
            uma_resolution_statuses_raw=dto.uma_resolution_statuses_raw,
        )

    # INVALID (split 50/50 com tolerância)
    if (
        _TOLERANCE_INVALID_LOW <= yes_price <= _TOLERANCE_INVALID_HIGH
        and _TOLERANCE_INVALID_LOW <= no_price <= _TOLERANCE_INVALID_HIGH
    ):
        return MarketResolution(
            condition_id=dto.condition_id,
            resolved_outcome=ResolvedOutcome.INVALID,
            winning_token_id=None,
            closed_time=dto.closed_time,
            resolved_at=now,
            outcome_prices_raw=dto.outcome_prices_raw,
            uma_resolution_statuses_raw=dto.uma_resolution_statuses_raw,
        )

    # Preços não-terminais — UMA ainda processando
    return None


class ResolverAgent(AgentBase):
    """Polling-driven agent — detecta resoluções de markets em wallet_trades.

    Não consome JetStream. Loop a cada sync_interval_s:
    repo.get_unresolved_condition_ids → gamma.list_markets_by_condition_ids_closed →
    classify → repo.insert (idempotente).
    """

    name = "resolver"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        sync_interval_s: float,
        gamma: PolymarketGammaPort,
        repo_factory: RepoFactory,
        batch_size: int,
        metrics: Metrics,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=sync_interval_s)
        self._gamma = gamma
        self._repo_factory = repo_factory
        self._batch_size = batch_size
        self._metrics = metrics

    async def run_once(self) -> None:
        start = time.perf_counter()
        try:
            async with self._repo_factory() as repo:
                unresolved = await repo.get_unresolved_condition_ids(limit=self._batch_size)

            if not unresolved:
                self._metrics.resolver_sync_total.labels(result="ok").inc()
                self._metrics.resolver_unresolved_pending.set(0)
                self._log.info("resolver_sync_no_unresolved")
                await self._compute_pnl_metrics()
                return

            markets = await self._gamma.list_markets_by_condition_ids_closed(
                condition_ids=unresolved,
                limit=len(unresolved),
            )

            resolutions_detected = 0
            outcomes_count = {"yes": 0, "no": 0, "invalid": 0}
            async with self._repo_factory() as repo:
                for market_dto in markets:
                    resolution = _classify_resolution(market_dto)
                    if resolution is None:
                        continue
                    inserted = await repo.insert(resolution)
                    if inserted:
                        resolutions_detected += 1
                        outcome_label = resolution.resolved_outcome.value.lower()
                        outcomes_count[outcome_label] += 1
                        self._metrics.resolver_resolutions_detected_total.labels(
                            outcome=outcome_label
                        ).inc()

            self._metrics.resolver_sync_total.labels(result="ok").inc()
            self._metrics.resolver_unresolved_pending.set(len(unresolved) - resolutions_detected)
            self._log.info(
                "resolver_sync_completed",
                unresolved_checked=len(unresolved),
                resolutions_detected=resolutions_detected,
                yes=outcomes_count["yes"],
                no=outcomes_count["no"],
                invalid=outcomes_count["invalid"],
            )
            await self._compute_pnl_metrics()
        except Exception as exc:  # noqa: BLE001
            self._metrics.resolver_sync_total.labels(result="fail").inc()
            self._log.warning(
                "resolver_sync_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
        finally:
            self._metrics.resolver_sync_duration_seconds.observe(time.perf_counter() - start)

    async def _compute_pnl_metrics(self) -> None:
        """Recomputa e seta gauges Prometheus a partir da view hypothetical_pnl. Best-effort."""
        try:
            async with self._repo_factory() as repo:
                summary = await repo.get_pnl_summary()
            self._metrics.hypothetical_pnl_total_usdc.set(float(summary.total_pnl_usdc))
            self._metrics.hypothetical_pnl_24h_usdc.set(float(summary.pnl_24h_usdc))
            self._metrics.hypothetical_winrate.set(summary.winrate)
            self._metrics.hypothetical_trades_resolved.set(summary.trades_resolved)
            self._metrics.hypothetical_trades_pending.set(summary.trades_pending)
            # NaN sinaliza "indisponível" pra Prometheus (gauge sem valor sensato).
            self._metrics.hypothetical_sharpe.set(
                summary.sharpe if summary.sharpe is not None else float("nan")
            )
            self._metrics.hypothetical_max_drawdown_usdc.set(float(summary.max_drawdown_usdc))
            self._metrics.hypothetical_avg_holding_hours.set(
                summary.avg_holding_hours if summary.avg_holding_hours is not None else float("nan")
            )
        except Exception as exc:  # noqa: BLE001
            self._log.warning(
                "pnl_metrics_compute_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )


def _make_repo_factory(session_factory: async_sessionmaker[AsyncSession]) -> RepoFactory:
    from polycopy.infrastructure.persistence.market_resolution_repository import (
        SqlAlchemyMarketResolutionRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyMarketResolutionRepository(session)
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
    metrics_server, _ = start_metrics_server(settings.resolver_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(session_factory)

    gamma = PolymarketGammaClient(
        base_url=settings.gamma_api_base_url,
        metrics=metrics,
    )

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = ResolverAgent(
        stopping=stopping,
        sync_interval_s=settings.resolver_sync_interval_s,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=settings.resolver_batch_size,
        metrics=metrics,
    )
    try:
        await agent.run()
    finally:
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
