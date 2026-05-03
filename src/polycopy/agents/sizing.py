"""SizingAgent: aplica proporcionalidade hardcoded em trades aprovados pelo Risk.

Consome `order.approved`, escala o tamanho original (ratio * size_usdc), aplica
cap (max_size_usdc) e floor (min_size_usdc), persiste decisão em `order_sizings`
e publica `order.sized` ou `order.skipped`.

Rodando local (sem Docker):
    uv run python -m polycopy.agents.sizing

Settings (Plano 2C):
    SIZING_METRICS_PORT          default 9105
    SIZING_DURABLE_NAME          default "sizing-1"
    SIZING_MAX_DELIVER           default 5
    SIZING_PROPORTION_RATIO      default 0.1
    SIZING_MAX_SIZE_USDC         default 50
    SIZING_MIN_SIZE_USDC         default 1
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import OrderApproved, OrderSized, OrderSkipped, SkipReason
from polycopy.domain.models import Trade
from polycopy.domain.sizing import OrderSizing
from polycopy.domain.value_objects import Money
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MessagingPort, OrderSizingRepository

SizingRepoFactory = Callable[[], AbstractAsyncContextManager[OrderSizingRepository]]

_USDC_QUANTUM = Decimal("0.000001")


@dataclass(frozen=True)
class _SizeResult:
    """Resultado interno de `_size`: encapsula decisão + final_size + reason."""

    decision: str  # "sized" | "skipped"
    final_size_usdc: Decimal | None
    reason: SkipReason | None


class SizingAgent(AgentBase):
    """Durable consumer de `order.approved`. Aplica sizing proporcional + cap/floor."""

    name = "sizing"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        bus: MessagingPort,
        repo_factory: SizingRepoFactory,
        proportion_ratio: Decimal,
        max_size_usdc: Decimal,
        min_size_usdc: Decimal,
        metrics: Metrics,
        durable_name: str = "sizing-1",
        max_deliver: int = 5,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=1.0)
        self._bus = bus
        self._repo_factory = repo_factory
        self._proportion_ratio = proportion_ratio
        self._max_size_usdc = max_size_usdc
        self._min_size_usdc = min_size_usdc
        self._metrics = metrics
        self._durable_name = durable_name
        self._max_deliver = max_deliver

    async def start(self) -> None:
        """Registra durable consumer no JetStream; chamar antes de `run()`."""
        await self._bus.subscribe(
            OrderApproved.SUBJECT,
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
                event = OrderApproved.model_validate_json(payload)
            except ValidationError as exc:
                # Poison message: nunca melhora com retry. Ack silencioso.
                self._log.warning(
                    "sizing_invalid_payload",
                    num_delivered=num_delivered,
                    payload_preview=payload[:200].decode("utf-8", errors="replace"),
                    error=str(exc),
                )
                self._metrics.sizing_decisions_total.labels(
                    result="skipped", reason="invalid_payload"
                ).inc()
                return

            result = self._size(event.trade)

            sizing = OrderSizing(
                trade_event_id=event.event_id,
                wallet=event.trade.wallet.value,
                condition_id=event.trade.condition_id.value,
                token_id=event.trade.token_id.value,
                original_size_usdc=event.trade.size_usdc.amount,
                final_size_usdc=result.final_size_usdc,
                decision=result.decision,  # type: ignore[arg-type]
                reason=result.reason,
                decided_at=datetime.now(tz=UTC),
            )

            async with self._repo_factory() as repo:
                is_new = await repo.insert(sizing)

            if not is_new:
                # Re-delivery; já decidida antes. Ack silencioso, NÃO re-publica.
                self._metrics.sizing_decisions_total.labels(
                    result="duplicate_skip",
                    reason=result.reason.value if result.reason is not None else "none",
                ).inc()
                return

            if result.decision == "sized":
                if result.final_size_usdc is None:  # invariante já validado em _size
                    raise RuntimeError("sized result missing final_size_usdc")
                await self._bus.publish_order_sized(
                    OrderSized(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=sizing.decided_at,
                        trade=event.trade,
                        final_size_usdc=Money(amount=result.final_size_usdc),
                        original_size_usdc=event.trade.size_usdc,
                    )
                )
                # Observa razão final/original no histograma (só faz sentido pra sized).
                ratio = result.final_size_usdc / event.trade.size_usdc.amount
                self._metrics.sizing_size_ratio_observed.observe(float(ratio))
            else:
                if result.reason is None:  # invariante já validado em _size
                    raise RuntimeError("skipped result missing reason")
                await self._bus.publish_order_skipped(
                    OrderSkipped(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=sizing.decided_at,
                        trade=event.trade,
                        reason=result.reason,
                    )
                )

            self._metrics.sizing_decisions_total.labels(
                result=result.decision,
                reason=result.reason.value if result.reason is not None else "none",
            ).inc()
            self._log.info(
                "sizing_decision",
                trade_event_id=str(event.event_id),
                wallet=event.trade.wallet.value,
                decision=result.decision,
                original_size_usdc=str(event.trade.size_usdc.amount),
                final_size_usdc=(
                    str(result.final_size_usdc) if result.final_size_usdc is not None else None
                ),
                reason=result.reason.value if result.reason is not None else None,
            )
        finally:
            self._metrics.sizing_decision_duration_seconds.observe(time.perf_counter() - start)

    def _size(self, trade: Trade) -> _SizeResult:
        """Aplica proporcionalidade + cap + floor.

        scaled = original * ratio
        capped = min(scaled, max)
        se capped < min → skipped (BELOW_MIN_SIZE)
        senão → sized=capped (quantizado pra USDC quantum)
        """
        original = trade.size_usdc.amount
        scaled = original * self._proportion_ratio
        capped = min(scaled, self._max_size_usdc)
        if capped < self._min_size_usdc:
            return _SizeResult(
                decision="skipped",
                final_size_usdc=None,
                reason=SkipReason.BELOW_MIN_SIZE,
            )
        return _SizeResult(
            decision="sized",
            final_size_usdc=capped.quantize(_USDC_QUANTUM),
            reason=None,
        )


def _make_repo_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> SizingRepoFactory:
    from polycopy.infrastructure.persistence.order_sizing_repository import (
        SqlAlchemyOrderSizingRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[OrderSizingRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyOrderSizingRepository(session)
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

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.sizing_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(session_factory)

    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = SizingAgent(
        stopping=stopping,
        bus=bus,
        repo_factory=repo_factory,
        proportion_ratio=settings.sizing_proportion_ratio,
        max_size_usdc=settings.sizing_max_size_usdc,
        min_size_usdc=settings.sizing_min_size_usdc,
        metrics=metrics,
        durable_name=settings.sizing_durable_name,
        max_deliver=settings.sizing_max_deliver,
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
