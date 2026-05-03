"""ExecutorAgent: gate de execução entre sizing (2C) e on-chain (Fase 4).

Consome `order.sized`, delega ao `OrderExecutor` (strategy: dry-run no MVP, real
em Fase 4), persiste decisão em `order_executions` e publica
`order.dry_run` | `order.executed` | `order.failed`.

Rodando local (sem Docker):
    uv run python -m polycopy.agents.executor

Settings (Plano 3):
    EXECUTOR_METRICS_PORT     default 9106
    EXECUTOR_DURABLE_NAME     default "executor-1"
    EXECUTOR_MAX_DELIVER      default 5
    EXECUTOR_DRY_RUN          default True  — NUNCA setar False fora da Fase 4
                                              (raise RuntimeError em main())
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from typing import Literal

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import (
    ExecutionMode,
    FailureReason,
    OrderDryRun,
    OrderExecuted,
    OrderFailed,
    OrderSized,
)
from polycopy.domain.execution import ExecutionResult, OrderExecution
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MessagingPort, OrderExecutionRepository, OrderExecutor

ExecutionRepoFactory = Callable[[], AbstractAsyncContextManager[OrderExecutionRepository]]


class ExecutorAgent(AgentBase):
    """Durable consumer de `order.sized`. Delega execução ao OrderExecutor."""

    name = "executor"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        bus: MessagingPort,
        executor: OrderExecutor,
        repo_factory: ExecutionRepoFactory,
        metrics: Metrics,
        dry_run: bool = True,
        durable_name: str = "executor-1",
        max_deliver: int = 5,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=1.0)
        self._bus = bus
        self._executor = executor
        self._repo_factory = repo_factory
        self._metrics = metrics
        self._dry_run = dry_run
        self._durable_name = durable_name
        self._max_deliver = max_deliver

    async def start(self) -> None:
        """Registra durable consumer no JetStream; chamar antes de `run()`."""
        await self._bus.subscribe(
            OrderSized.SUBJECT,
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
                event = OrderSized.model_validate_json(payload)
            except ValidationError as exc:
                # Poison message: nunca melhora com retry. Ack silencioso.
                self._log.warning(
                    "executor_invalid_payload",
                    num_delivered=num_delivered,
                    payload_preview=payload[:200].decode("utf-8", errors="replace"),
                    error=str(exc),
                )
                self._metrics.executor_orders_total.labels(
                    result="failed",
                    mode="dry_run" if self._dry_run else "real",
                    reason="invalid_payload",
                ).inc()
                return

            try:
                exec_result = await self._executor.execute(
                    event.trade, event.final_size_usdc.amount
                )
            except Exception as exc:  # noqa: BLE001 — vira OrderFailed (spec §7 + dry_run/real distinguidos via self._dry_run flag)
                # Em dry-run mode, exceção do executor stub vira mode=DRY_RUN+failed
                # (invariante relaxada na migration 0006). Em real-mode, mode=REAL+failed.
                exec_result = ExecutionResult(
                    mode=ExecutionMode.DRY_RUN if self._dry_run else ExecutionMode.REAL,
                    success=False,
                    failure_reason=FailureReason.EXECUTOR_DISABLED,
                    error_message=str(exc),
                )

            result_label = self._result_label(exec_result)

            execution = OrderExecution(
                trade_event_id=event.event_id,
                wallet=event.trade.wallet.value,
                condition_id=event.trade.condition_id.value,
                token_id=event.trade.token_id.value,
                final_size_usdc=event.final_size_usdc.amount,
                mode=exec_result.mode,
                result=result_label,
                tx_hash=exec_result.tx_hash,
                gas_wei=exec_result.gas_wei,
                failure_reason=exec_result.failure_reason,
                error_message=exec_result.error_message,
                decided_at=datetime.now(tz=UTC),
            )

            async with self._repo_factory() as repo:
                is_new = await repo.insert(execution)

            if not is_new:
                # Re-delivery; já decidida antes. Ack silencioso, NÃO re-publica.
                self._metrics.executor_orders_total.labels(
                    result="duplicate_skip",
                    mode=exec_result.mode.value,
                    reason=(
                        exec_result.failure_reason.value
                        if exec_result.failure_reason is not None
                        else "none"
                    ),
                ).inc()
                return

            if result_label == "dry_run":
                await self._bus.publish_order_dry_run(
                    OrderDryRun(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=execution.decided_at,
                        trade=event.trade,
                        final_size_usdc=event.final_size_usdc,
                    )
                )
            elif result_label == "executed":
                if exec_result.tx_hash is None or exec_result.gas_wei is None:
                    raise RuntimeError("executed result missing tx_hash or gas_wei")
                await self._bus.publish_order_executed(
                    OrderExecuted(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=execution.decided_at,
                        trade=event.trade,
                        final_size_usdc=event.final_size_usdc,
                        tx_hash=exec_result.tx_hash,
                        gas_wei=exec_result.gas_wei,
                    )
                )
                self._metrics.executor_gas_wei.observe(float(exec_result.gas_wei))
            else:  # failed
                if exec_result.failure_reason is None or exec_result.error_message is None:
                    raise RuntimeError("failed result missing reason or error_message")
                await self._bus.publish_order_failed(
                    OrderFailed(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=execution.decided_at,
                        trade=event.trade,
                        final_size_usdc=event.final_size_usdc,
                        reason=exec_result.failure_reason,
                        error_message=exec_result.error_message,
                    )
                )

            self._metrics.executor_orders_total.labels(
                result=result_label,
                mode=exec_result.mode.value,
                reason=(
                    exec_result.failure_reason.value
                    if exec_result.failure_reason is not None
                    else "none"
                ),
            ).inc()
            self._log.info(
                "executor_decision",
                trade_event_id=str(event.event_id),
                wallet=event.trade.wallet.value,
                mode=exec_result.mode.value,
                result=result_label,
                final_size_usdc=str(event.final_size_usdc.amount),
                tx_hash=exec_result.tx_hash,
                gas_wei=exec_result.gas_wei,
                reason=(
                    exec_result.failure_reason.value
                    if exec_result.failure_reason is not None
                    else None
                ),
            )
        finally:
            self._metrics.executor_decision_duration_seconds.observe(time.perf_counter() - start)

    def _result_label(
        self, exec_result: ExecutionResult
    ) -> Literal["executed", "failed", "dry_run"]:
        """Mapeia (mode, success) pra label persistido em order_executions.result.

        Em DRY_RUN mode, success=True → 'dry_run'; success=False → 'failed' (executor
        stub lançou exception; invariante relaxada na migration 0006).
        """
        if exec_result.mode == ExecutionMode.DRY_RUN:
            return "dry_run" if exec_result.success else "failed"
        return "executed" if exec_result.success else "failed"


def _make_repo_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> ExecutionRepoFactory:
    from polycopy.infrastructure.persistence.order_execution_repository import (
        SqlAlchemyOrderExecutionRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[OrderExecutionRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyOrderExecutionRepository(session)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return _factory


async def main() -> None:
    """Entrypoint: monta dependências, sobe /metrics, registra signal handlers, roda."""
    from polycopy.infrastructure.execution.dry_run_executor import DryRunExecutor
    from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.executor_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(session_factory)

    executor: OrderExecutor
    if settings.executor_dry_run:
        executor = DryRunExecutor()
    else:
        raise RuntimeError("Real-mode not yet implemented — Fase 4 required")

    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = ExecutorAgent(
        stopping=stopping,
        bus=bus,
        executor=executor,
        repo_factory=repo_factory,
        metrics=metrics,
        dry_run=settings.executor_dry_run,
        durable_name=settings.executor_durable_name,
        max_deliver=settings.executor_max_deliver,
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
