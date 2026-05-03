"""E2E do SizingAgent: agente real + NATS real + Postgres real (sem APIs externas).

Exige `docker compose up -d postgres nats` antes.

ATENÇÃO operacional: se o container `polycopy-sizing` estiver rodando (durable
`sizing-1`), ele também consome `order.approved` da stream `RISK_DECISIONS` e
grava em `order_sizings` (mesma tabela). Cada teste já usa durable consumer com
sufixo uuid4 pra isolar consumer state, mas pra evitar interferência via DB
shared (mesmo `event_id` cair em qualquer dos dois agents) recomenda-se parar o
container antes (`docker compose stop sizing`) e religar no fim
(`docker compose start sizing`).

Cada teste:
- Usa `event_id` (uuid4) único pra evitar conflito de PK em `order_sizings`.
- Usa `tx_hash` único pra evitar dedup JetStream cross-test.
- Cria durable consumer com nome único (sufixo uuid4).
- Limpa o consumer durable do stream `RISK_DECISIONS` no teardown.
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

from polycopy.agents.sizing import SizingAgent, _make_repo_factory
from polycopy.config import Settings
from polycopy.domain.events import (
    OrderApproved,
    OrderSized,
    OrderSkipped,
    SkipReason,
)
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.models import OrderSizingRow

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


def _order_approved_event(trade: Trade) -> OrderApproved:
    """Embrulha um Trade num OrderApproved com event_id único (uuid4)."""
    now = datetime.now(tz=UTC)
    return OrderApproved(
        event_id=uuid4(),
        occurred_at=now,
        decided_at=now,
        trade=trade,
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
    """Remove o consumer durable do stream RISK_DECISIONS (idempotente).

    Sizing consome `order.approved`, que está no stream RISK_DECISIONS (ver
    `_RISK_STREAM_SUBJECTS` em nats_bus.py). Por isso o cleanup é nesse stream,
    não em SIZING_DECISIONS (que recebe `order.sized`/`order.skipped`).
    """
    nc = await _nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        with suppress(Exception):
            await js.delete_consumer("RISK_DECISIONS", durable)
    finally:
        await nc.close()


async def _make_agent(
    *,
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    metrics_registry: CollectorRegistry,
    durable_name: str,
    proportion_ratio: Decimal = Decimal("0.1"),
    max_size_usdc: Decimal = Decimal("50"),
    min_size_usdc: Decimal = Decimal("1"),
) -> SizingAgent:
    """Instancia + start de um SizingAgent isolado (registry e durable únicos)."""
    metrics = make_metrics(registry=metrics_registry)
    repo_factory = _make_repo_factory(db_session_factory)

    agent = SizingAgent(
        stopping=asyncio.Event(),
        bus=bus,
        repo_factory=repo_factory,
        proportion_ratio=proportion_ratio,
        max_size_usdc=max_size_usdc,
        min_size_usdc=min_size_usdc,
        metrics=metrics,
        durable_name=durable_name,
    )
    await agent.start()
    return agent


async def test_e2e_sized_flow(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """Trade size 100 (ratio 0.1, max 50, min 1) → final 10 USDC.

    DB grava decision='sized' + final_size_usdc=10 + decided_at populado.
    Bus recebe OrderSized com mesmos valores e occurred_at preservado.
    """
    received_sized: list[bytes] = []

    async def sized_handler(payload: bytes) -> None:
        received_sized.append(payload)

    await bus.subscribe(OrderSized.SUBJECT, sized_handler)

    durable = f"sizing-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        event = _order_approved_event(_trade(size_usdc="100"))
        await bus.publish_order_approved(event)
        await asyncio.sleep(2.0)  # tempo pro JetStream entregar + agent processar + commit

        # DB tem decisão sized com final_size_usdc=10
        async with db_session_factory() as session:
            result = await session.execute(
                select(OrderSizingRow).where(OrderSizingRow.trade_event_id == event.event_id)
            )
            row = result.scalar_one()
            assert row.decision == "sized"
            assert row.reason is None
            assert row.final_size_usdc == Decimal("10.000000")
            assert row.original_size_usdc == Decimal("100.000000")
            assert row.decided_at is not None
            assert row.decided_at.tzinfo is not None

        # Bus recebeu order.sized com os mesmos valores e occurred_at preservado
        assert len(received_sized) == 1
        parsed = OrderSized.model_validate_json(received_sized[0])
        assert parsed.event_id == event.event_id
        assert parsed.occurred_at == event.occurred_at
        assert parsed.decided_at is not None
        assert parsed.final_size_usdc.amount == Decimal("10.000000")
        assert parsed.original_size_usdc.amount == Decimal("100")
    finally:
        await _cleanup_consumer(settings, durable)


async def test_e2e_skipped_flow(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """Trade size 1 (ratio 0.1 → scaled 0.1 < min 1) → skipped BELOW_MIN_SIZE.

    DB grava decision='skipped' + final_size_usdc=NULL + reason='below_min_size'.
    Bus recebe OrderSkipped com reason=BELOW_MIN_SIZE.
    """
    received_skipped: list[bytes] = []

    async def skipped_handler(payload: bytes) -> None:
        received_skipped.append(payload)

    await bus.subscribe(OrderSkipped.SUBJECT, skipped_handler)

    durable = f"sizing-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        event = _order_approved_event(_trade(size_usdc="1"))  # scaled 0.1 < min 1
        await bus.publish_order_approved(event)
        await asyncio.sleep(2.0)

        # DB tem decisão skipped com final_size_usdc=NULL e reason populado
        async with db_session_factory() as session:
            result = await session.execute(
                select(OrderSizingRow).where(OrderSizingRow.trade_event_id == event.event_id)
            )
            row = result.scalar_one()
            assert row.decision == "skipped"
            assert row.reason == "below_min_size"
            assert row.final_size_usdc is None
            assert row.original_size_usdc == Decimal("1.000000")
            assert row.decided_at is not None
            assert row.decided_at.tzinfo is not None

        # Bus recebeu order.skipped com reason=BELOW_MIN_SIZE
        assert len(received_skipped) == 1
        parsed = OrderSkipped.model_validate_json(received_skipped[0])
        assert parsed.event_id == event.event_id
        assert parsed.occurred_at == event.occurred_at
        assert parsed.reason == SkipReason.BELOW_MIN_SIZE
    finally:
        await _cleanup_consumer(settings, durable)


async def test_e2e_redelivery_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """Publica o MESMO OrderApproved 2x (mesmo event_id como Nats-Msg-Id).

    NATS dedup por `Nats-Msg-Id = event_id` (window de 5min) garante que o
    agent só recebe a mensagem uma vez: 1 row em `order_sizings` e 1 evento
    publicado em `order.sized`.

    Mesmo se o NATS dedup falhasse, o PK em `order_sizings` (trade_event_id)
    + a checagem `is_new` no agent garantiriam que a 2ª delivery seria skipada
    sem re-publicar.
    """
    received_sized: list[bytes] = []

    async def sized_handler(payload: bytes) -> None:
        received_sized.append(payload)

    await bus.subscribe(OrderSized.SUBJECT, sized_handler)

    durable = f"sizing-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        event = _order_approved_event(_trade(size_usdc="100"))
        await bus.publish_order_approved(event)
        await bus.publish_order_approved(event)  # JetStream dedup pelo event_id
        await asyncio.sleep(2.0)

        # 1 row no DB
        async with db_session_factory() as session:
            result = await session.execute(
                select(OrderSizingRow).where(OrderSizingRow.trade_event_id == event.event_id)
            )
            rows = result.scalars().all()
            assert len(rows) == 1

        # 1 evento no bus (NATS dedup garantiu publish único)
        assert len(received_sized) == 1
    finally:
        await _cleanup_consumer(settings, durable)
