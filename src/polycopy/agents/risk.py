"""RiskAgent: gate de risco entre detecção (1B) e sizing (2C).

Consome `wallet.trade.detected`, aplica 6 regras hardcoded com lazy
fallback via Gamma quando MarketRepository miss/stale, persiste
decisão em `risk_decisions` e publica `order.approved` ou
`trade.rejected`.

Rodando local (sem Docker):
    uv run python -m polycopy.agents.risk

Settings (Plano 2B):
    RISK_METRICS_PORT             default 9104
    RISK_DURABLE_NAME             default "risk-1"
    RISK_MAX_DELIVER              default 5
    RISK_MAX_TRADE_USDC           default 100
    RISK_MIN_PRICE                default 0.05
    RISK_MAX_PRICE                default 0.95
    RISK_MIN_LIQUIDITY_USDC       default 1000
    RISK_GAMMA_FETCH_TIMEOUT_S    default 5.0
    RISK_COPY_ALLOWLIST           default ""  (CSV; vazio = sem filtro)
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import (
    OrderApproved,
    RejectionReason,
    TradeRejected,
    WalletTradeDetected,
)
from polycopy.domain.market import Market
from polycopy.domain.models import Trade
from polycopy.domain.risk import RiskDecision
from polycopy.domain.value_objects import TokenId
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.infrastructure.polymarket.gamma_client import PolymarketUnavailableError
from polycopy.ports import (
    MarketRepository,
    MessagingPort,
    PolymarketGammaPort,
    RiskDecisionRepository,
)

DecisionRepoFactory = Callable[[], AbstractAsyncContextManager[RiskDecisionRepository]]
MarketRepoFactory = Callable[[], AbstractAsyncContextManager[MarketRepository]]


class RiskAgent(AgentBase):
    name = "risk"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        bus: MessagingPort,
        gamma: PolymarketGammaPort,
        decision_repo_factory: DecisionRepoFactory,
        market_repo_factory: MarketRepoFactory,
        max_trade_usdc: Decimal,
        min_price: Decimal,
        max_price: Decimal,
        min_liquidity_usdc: Decimal,
        metrics: Metrics,
        copy_allowlist: frozenset[str] = frozenset(),
        durable_name: str = "risk-1",
        max_deliver: int = 5,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=1.0)
        self._bus = bus
        self._gamma = gamma
        self._decision_repo_factory = decision_repo_factory
        self._market_repo_factory = market_repo_factory
        self._max_trade_usdc = max_trade_usdc
        self._min_price = min_price
        self._max_price = max_price
        self._min_liquidity_usdc = min_liquidity_usdc
        self._metrics = metrics
        self._copy_allowlist = copy_allowlist
        self._durable_name = durable_name
        self._max_deliver = max_deliver

    async def start(self) -> None:
        """Registra durable consumer no JetStream; chamar antes de `run()`."""
        await self._bus.subscribe(
            WalletTradeDetected.SUBJECT,
            self._handle_message,
            durable=self._durable_name,
            max_deliver=self._max_deliver,
        )

    async def run_once(self) -> None:
        # Trabalho real está no callback. AgentBase loop dá heartbeat estruturado.
        await asyncio.sleep(self._interval_s)

    async def _handle_message(self, payload: bytes, num_delivered: int) -> None:
        start = time.perf_counter()
        try:
            try:
                event = WalletTradeDetected.model_validate_json(payload)
            except ValidationError as exc:
                # Poison message: nunca melhora com retry. Ack silencioso.
                self._log.warning(
                    "risk_invalid_payload",
                    num_delivered=num_delivered,
                    payload_preview=payload[:200].decode("utf-8", errors="replace"),
                    error=str(exc),
                )
                self._metrics.risk_decisions_total.labels(
                    result="rejected", reason="invalid_payload"
                ).inc()
                return

            if (
                self._copy_allowlist
                and event.trade.wallet.value.lower() not in self._copy_allowlist
            ):
                # Fail-fast: pula fetch_market (sem I/O) pra wallets descartadas.
                reason: RejectionReason | None = RejectionReason.WALLET_NOT_IN_ALLOWLIST
                cache_result = "skipped_allowlist"
            else:
                market, cache_result = await self._fetch_market(event.trade.token_id)
                self._metrics.market_cache_hits_total.labels(result=cache_result).inc()
                reason = self._evaluate(event.trade, market)

            decision = RiskDecision(
                trade_event_id=event.event_id,
                wallet=event.trade.wallet.value,
                condition_id=event.trade.condition_id.value,
                token_id=event.trade.token_id.value,
                decision="approved" if reason is None else "rejected",
                reason=reason,
                decided_at=datetime.now(tz=UTC),
            )

            async with self._decision_repo_factory() as repo:
                is_new = await repo.insert(decision)

            if not is_new:
                # Re-delivery; já decidida antes. Ack silencioso, NÃO re-publica.
                self._metrics.risk_decisions_total.labels(
                    result="duplicate_skip",
                    reason=reason.value if reason is not None else "none",
                ).inc()
                return

            if reason is None:
                await self._bus.publish_order_approved(
                    OrderApproved(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=decision.decided_at,
                        trade=event.trade,
                    )
                )
            else:
                await self._bus.publish_trade_rejected(
                    TradeRejected(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=decision.decided_at,
                        trade=event.trade,
                        reason=reason,
                    )
                )

            self._metrics.risk_decisions_total.labels(
                result="approved" if reason is None else "rejected",
                reason=reason.value if reason is not None else "none",
            ).inc()
            self._log.info(
                "risk_decision",
                trade_event_id=str(event.event_id),
                wallet=event.trade.wallet.value,
                decision=decision.decision,
                reason=reason.value if reason is not None else None,
                cache_result=cache_result,
            )
        finally:
            self._metrics.risk_decision_duration_seconds.observe(time.perf_counter() - start)

    async def _fetch_market(self, token_id: TokenId) -> tuple[Market | None, str]:
        """Lookup com lazy fallback. Retorna (market | None, cache_result label)."""
        async with self._market_repo_factory() as repo_market:
            cached = await repo_market.get_market(token_id)

            if cached is not None and not cached.is_stale:
                return cached.market, "hit_fresh"

            # Cache stale ou miss → tenta lazy fetch via Gamma
            try:
                fresh = await self._gamma.get_market(token_id)
                self._metrics.risk_lazy_fetch_total.labels(result="success").inc()
                if fresh is not None:
                    await repo_market.upsert_many([fresh])
                    return fresh, ("hit_stale" if cached is not None else "miss")
            except PolymarketUnavailableError:
                self._metrics.risk_lazy_fetch_total.labels(result="fail").inc()

            # Lazy fetch falhou ou retornou None. Fail-safe brando: usa stale se houver.
            if cached is not None:
                return cached.market, "hit_stale"
            return None, "miss"

    def _evaluate(self, trade: Trade, market: Market | None) -> RejectionReason | None:
        """Aplica 6 regras na ordem. Retorna RejectionReason (rejeição) ou None (aprovado).

        Regras avaliadas em ordem; primeira falha é a única reportada.
        Trade pode violar múltiplas — `reason` é a "pior primeira".

        Ordem é fail-fast: allowlist (set lookup, sem I/O) vem ANTES de qualquer
        verificação que dependa de market metadata.
        """
        if self._copy_allowlist and trade.wallet.value.lower() not in self._copy_allowlist:
            return RejectionReason.WALLET_NOT_IN_ALLOWLIST
        if trade.size_usdc.amount > self._max_trade_usdc:
            return RejectionReason.SIZE_EXCEEDED
        if market is None:
            return RejectionReason.MARKET_NOT_CACHED
        if not market.is_active or market.is_archived:
            return RejectionReason.MARKET_INACTIVE
        if not (self._min_price <= trade.price.value <= self._max_price):
            return RejectionReason.PRICE_OUT_OF_RANGE
        if market.liquidity_usdc is None or market.liquidity_usdc.amount < self._min_liquidity_usdc:
            return RejectionReason.INSUFFICIENT_LIQUIDITY
        return None


def _make_decision_repo_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> DecisionRepoFactory:
    from polycopy.infrastructure.persistence.risk_decision_repository import (
        SqlAlchemyRiskDecisionRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[RiskDecisionRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyRiskDecisionRepository(session)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return _factory


def _make_market_repo_factory(
    session_factory: async_sessionmaker[AsyncSession], *, ttl_seconds: int
) -> MarketRepoFactory:
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
    from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.risk_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    decision_repo_factory = _make_decision_repo_factory(session_factory)
    market_repo_factory = _make_market_repo_factory(
        session_factory, ttl_seconds=settings.market_cache_ttl_seconds
    )

    gamma = PolymarketGammaClient(
        base_url=settings.gamma_api_base_url,
        metrics=metrics,
        timeout_s=settings.risk_gamma_fetch_timeout_s,
    )

    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    copy_allowlist = frozenset(
        addr.strip().lower() for addr in settings.risk_copy_allowlist.split(",") if addr.strip()
    )

    agent = RiskAgent(
        stopping=stopping,
        bus=bus,
        gamma=gamma,
        decision_repo_factory=decision_repo_factory,
        market_repo_factory=market_repo_factory,
        max_trade_usdc=settings.risk_max_trade_usdc,
        min_price=settings.risk_min_price,
        max_price=settings.risk_max_price,
        min_liquidity_usdc=settings.risk_min_liquidity_usdc,
        metrics=metrics,
        copy_allowlist=copy_allowlist,
        durable_name=settings.risk_durable_name,
        max_deliver=settings.risk_max_deliver,
    )
    await agent.start()
    try:
        await agent.run()
    finally:
        await bus.close()
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
