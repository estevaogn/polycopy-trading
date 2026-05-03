"""E2E do ExecutorAgent: agente real + NATS real + Postgres real (sem APIs externas).

Exige `docker compose up -d postgres nats` antes.

ATENÇÃO operacional: se o container `polycopy-executor` estiver rodando (durable
`executor-1`), ele também consome `order.sized` da stream `SIZING_DECISIONS` e
grava em `order_executions` (mesma tabela). Cada teste já usa durable consumer
com sufixo uuid4 pra isolar consumer state, mas pra evitar interferência via DB
shared (mesmo `event_id` cair em qualquer dos dois agents) recomenda-se parar o
container antes (`docker compose stop executor`) e religar no fim
(`docker compose start executor`).

Cada teste:
- Usa `event_id` (uuid4) único pra evitar conflito de PK em `order_executions`.
- Usa `tx_hash` único pra evitar dedup JetStream cross-test.
- Cria durable consumer com nome único (sufixo uuid4).
- Limpa o consumer durable do stream `SIZING_DECISIONS` no teardown.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import nats as _nats
import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.executor import ExecutorAgent, _make_repo_factory
from polycopy.config import Settings
from polycopy.domain.events import OrderDryRun, OrderSized
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.dry_run_executor import DryRunExecutor
from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.models import OrderExecutionRow

pytestmark = pytest.mark.integration


def _trade(
    *,
    size_usdc: str = "100",
    price: str = "0.5",
    token_id: str = "42",  # noqa: S107
    tx_hash: str | None = None,
) -> Trade:
    """Constrói um Trade com defaults sensatos. `tx_hash` único por chamada
    (uuid4 duplo) evita colisão com dedup window cross-test.
    """
    return Trade(
        tx_hash=tx_hash or ("0x" + uuid4().hex + uuid4().hex),
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value=token_id),
        side=Side.BUY,
        price=Price(value=Decimal(price)),
        size_usdc=Money.from_usdc(size_usdc),
        occurred_at=datetime.now(tz=UTC),
    )


def _order_sized_event(trade: Trade) -> OrderSized:
    """Embrulha um Trade num OrderSized com event_id único (uuid4).

    `final_size_usdc` fixo em 10 USDC (valor representativo pós-sizing); o
    `original_size_usdc` preserva o tamanho do trade original.
    """
    now = datetime.now(tz=UTC)
    return OrderSized(
        event_id=uuid4(),
        occurred_at=now,
        decided_at=now,
        trade=trade,
        final_size_usdc=Money.from_usdc("10"),
        original_size_usdc=trade.size_usdc,
    )


@pytest.fixture
async def bus(settings: Settings) -> AsyncIterator[NatsMessagingBus]:
    """NatsMessagingBus conectado ao broker do `.env`. Drena no teardown."""
    b = NatsMessagingBus(url=settings.nats_url)
    await b.connect()
    try:
        yield b
    finally:
        await b.close()


async def _cleanup_consumer(settings: Settings, durable: str) -> None:
    """Remove o consumer durable do stream SIZING_DECISIONS (idempotente).

    Executor consome `order.sized`, que está no stream SIZING_DECISIONS (ver
    `_SIZING_STREAM_SUBJECTS` em nats_bus.py). Por isso o cleanup é nesse
    stream, não em EXECUTION_RESULTS (que recebe `order.dry_run`/`order.executed`/
    `order.failed`).
    """
    nc = await _nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        with suppress(Exception):
            await js.delete_consumer("SIZING_DECISIONS", durable)
    finally:
        await nc.close()


async def _make_agent(
    *,
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    metrics_registry: CollectorRegistry,
    durable_name: str,
) -> ExecutorAgent:
    """Instancia + start de um ExecutorAgent isolado (registry e durable únicos)."""
    metrics = make_metrics(registry=metrics_registry)
    repo_factory = _make_repo_factory(db_session_factory)

    agent = ExecutorAgent(
        stopping=asyncio.Event(),
        bus=bus,
        executor=DryRunExecutor(),
        repo_factory=repo_factory,
        metrics=metrics,
        dry_run=True,
        durable_name=durable_name,
    )
    await agent.start()
    return agent


async def test_e2e_dry_run_flow(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """OrderSized publicado → DryRunExecutor → DB grava mode='dry_run'/result='dry_run'.

    Bus recebe OrderDryRun com mesmos event_id/occurred_at/final_size_usdc;
    `decided_at` populado pelo agent (now() dentro do handler).
    """
    received_dry_run: list[bytes] = []

    async def dry_run_handler(payload: bytes) -> None:
        received_dry_run.append(payload)

    await bus.subscribe(OrderDryRun.SUBJECT, dry_run_handler)

    durable = f"executor-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        event = _order_sized_event(_trade(size_usdc="100"))
        await bus.publish_order_sized(event)
        await asyncio.sleep(2.0)  # tempo pro JetStream entregar + agent processar + commit

        # DB tem execução dry_run com final_size_usdc=10 e decided_at populado
        async with db_session_factory() as session:
            result = await session.execute(
                select(OrderExecutionRow).where(OrderExecutionRow.trade_event_id == event.event_id)
            )
            row = result.scalar_one()
            assert row.mode == "dry_run"
            assert row.result == "dry_run"
            assert row.tx_hash is None
            assert row.gas_wei is None
            assert row.failure_reason is None
            assert row.error_message is None
            assert row.final_size_usdc == Decimal("10.000000")
            assert row.decided_at is not None
            assert row.decided_at.tzinfo is not None

        # Bus recebeu order.dry_run com os mesmos valores e occurred_at preservado
        assert len(received_dry_run) == 1
        parsed = OrderDryRun.model_validate_json(received_dry_run[0])
        assert parsed.event_id == event.event_id
        assert parsed.occurred_at == event.occurred_at
        assert parsed.decided_at is not None
        assert parsed.decided_at.tzinfo is not None
        assert parsed.final_size_usdc.amount == Decimal("10.000000")
    finally:
        await _cleanup_consumer(settings, durable)


async def test_e2e_redelivery_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """Publica o MESMO OrderSized 2x (mesmo event_id como Nats-Msg-Id).

    NATS dedup por `Nats-Msg-Id = event_id` (window de 5min) garante que o
    agent só recebe a mensagem uma vez: 1 row em `order_executions` e 1 evento
    publicado em `order.dry_run`.

    Mesmo se o NATS dedup falhasse, o PK em `order_executions` (trade_event_id)
    + a checagem `is_new` no agent garantiriam que a 2ª delivery seria skipada
    sem re-publicar.
    """
    received_dry_run: list[bytes] = []

    async def dry_run_handler(payload: bytes) -> None:
        received_dry_run.append(payload)

    await bus.subscribe(OrderDryRun.SUBJECT, dry_run_handler)

    durable = f"executor-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        event = _order_sized_event(_trade(size_usdc="100"))
        await bus.publish_order_sized(event)
        await bus.publish_order_sized(event)  # JetStream dedup pelo event_id
        await asyncio.sleep(2.0)

        # 1 row no DB
        async with db_session_factory() as session:
            result = await session.execute(
                select(OrderExecutionRow).where(OrderExecutionRow.trade_event_id == event.event_id)
            )
            rows = result.scalars().all()
            assert len(rows) == 1

        # 1 evento no bus (NATS dedup garantiu publish único)
        assert len(received_dry_run) == 1
    finally:
        await _cleanup_consumer(settings, durable)
