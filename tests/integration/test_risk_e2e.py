"""E2E do RiskAgent: agente real + NATS real + Postgres real + Gamma fake (respx).

Exige `docker compose up -d postgres nats` antes.

ATENÇÃO operacional: se o container `polycopy-risk` estiver rodando (durable
`risk-1`), ele também consome `wallet.trade.detected` da stream e grava em
`risk_decisions` (mesma tabela). Como o cache de markets é dropado pelo
conftest entre runs, o container rejeita o trade do test com
`market_not_cached` ANTES do test agent processar — quando o test agent vê
`is_new=False` ele faz ack silencioso e não publica. Resultado: testes
falham com `decision='rejected'` em vez de `approved`.

Workaround: pare o container antes de rodar (`docker compose stop risk`)
e suba de novo no fim (`docker compose start risk`). Cada teste já usa
durable consumer com sufixo uuid4 pra não conflitar com `risk-1` no nível
do consumer state, mas a interferência via DB shared é inescapável sem
parar o container.

Cada teste:
- Usa `event_id` (uuid4) único pra evitar conflito de PK em `risk_decisions`.
- Usa `tx_hash` único pra evitar dedup JetStream cross-test (msg_id = tx_hash:log_index).
- Cria durable consumer com nome único (sufixo uuid4).
- Limpa o consumer durable no teardown.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import nats as _nats
import pytest
import respx
from prometheus_client import CollectorRegistry
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.risk import (
    RiskAgent,
    _make_decision_repo_factory,
    _make_market_repo_factory,
)
from polycopy.config import Settings
from polycopy.domain.events import (
    OrderApproved,
    RejectionReason,
    TradeRejected,
    WalletTradeDetected,
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
from polycopy.infrastructure.persistence.models import RiskDecisionRow
from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

pytestmark = pytest.mark.integration

# Payload mínimo válido do Gamma /markets pra um mercado ativo passar todas as 5 regras:
# - active=True / archived=False  → MARKET_INACTIVE não dispara
# - end_date > now+14d            → mercado não está expirando (informativo)
# - clobTokenIds=[42,43]          → token "42" usado nos trades resolve aqui
# - outcomes=[Yes,No]             → shape válido pro _row_to_markets do Gamma client
# - liquidity=5000 (>1000)        → INSUFFICIENT_LIQUIDITY não dispara
_GAMMA_OK_PAYLOAD = [
    {
        "conditionId": "0x" + "cd" * 32,
        "question": "Test market",
        "slug": "test-market",
        "active": True,
        "archived": False,
        "endDate": (datetime.now(tz=UTC) + timedelta(days=14)).isoformat().replace("+00:00", "Z"),
        "volume24hr": "100000",
        "liquidity": "5000",
        "clobTokenIds": '["42", "43"]',
        "outcomes": '["Yes", "No"]',
    }
]


def _trade(
    *,
    size_usdc: str = "10",
    price: str = "0.5",
    token_id: str = "42",  # noqa: S107
    tx_hash: str | None = None,
) -> Trade:
    """Constrói um Trade com defaults sensatos. `tx_hash` único por teste evita
    colisão com o dedup window (5min) do stream WALLET_TRADES."""
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


def _wallet_event(trade: Trade) -> WalletTradeDetected:
    """Embrulha um Trade num WalletTradeDetected com event_id único (uuid4)."""
    return WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
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
    """Remove o consumer durable do stream WALLET_TRADES (idempotente)."""
    nc = await _nats.connect(settings.nats_url)
    try:
        js = nc.jetstream()
        with suppress(Exception):
            await js.delete_consumer("WALLET_TRADES", durable)
    finally:
        await nc.close()


async def _make_agent(
    *,
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    metrics_registry: CollectorRegistry,
    durable_name: str,
    max_trade_usdc: Decimal = Decimal("100"),
) -> RiskAgent:
    """Instancia + start de um RiskAgent isolado (registry e durable únicos)."""
    metrics = make_metrics(registry=metrics_registry)
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    decision_factory = _make_decision_repo_factory(db_session_factory)
    market_factory = _make_market_repo_factory(db_session_factory, ttl_seconds=1800)

    agent = RiskAgent(
        stopping=asyncio.Event(),
        bus=bus,
        gamma=gamma,
        decision_repo_factory=decision_factory,
        market_repo_factory=market_factory,
        max_trade_usdc=max_trade_usdc,
        min_price=Decimal("0.05"),
        max_price=Decimal("0.95"),
        min_liquidity_usdc=Decimal("1000"),
        metrics=metrics,
        durable_name=durable_name,
    )
    await agent.start()
    return agent


@respx.mock
async def test_e2e_approved_flow(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """Trade dentro dos limites + market válido via Gamma → DB approved + bus order.approved."""
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=_GAMMA_OK_PAYLOAD),
    )

    received_approved: list[bytes] = []

    async def approved_handler(payload: bytes) -> None:
        received_approved.append(payload)

    await bus.subscribe(OrderApproved.SUBJECT, approved_handler)

    durable = f"risk-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        event = _wallet_event(_trade())
        await bus.publish_wallet_trade_detected(event)
        await asyncio.sleep(2.0)  # tempo pro JetStream entregar + agent processar + commit

        # DB tem decisão approved
        async with db_session_factory() as session:
            result = await session.execute(
                select(RiskDecisionRow).where(RiskDecisionRow.trade_event_id == event.event_id)
            )
            row = result.scalar_one()
            assert row.decision == "approved"
            assert row.reason is None

        # Bus recebeu order.approved (e o evento tem decided_at preenchido)
        assert len(received_approved) == 1
        parsed = OrderApproved.model_validate_json(received_approved[0])
        assert parsed.event_id == event.event_id
        assert parsed.decided_at is not None
    finally:
        await _cleanup_consumer(settings, durable)


@respx.mock
async def test_e2e_rejected_size_exceeded(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """Trade size 500 (> MAX_TRADE_USDC=100) → DB rejected + bus trade.rejected size_exceeded."""
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=_GAMMA_OK_PAYLOAD),
    )

    received_rejected: list[bytes] = []

    async def rejected_handler(payload: bytes) -> None:
        received_rejected.append(payload)

    await bus.subscribe(TradeRejected.SUBJECT, rejected_handler)

    durable = f"risk-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        event = _wallet_event(_trade(size_usdc="500"))  # excede MAX_TRADE_USDC=100
        await bus.publish_wallet_trade_detected(event)
        await asyncio.sleep(2.0)

        async with db_session_factory() as session:
            result = await session.execute(
                select(RiskDecisionRow).where(RiskDecisionRow.trade_event_id == event.event_id)
            )
            row = result.scalar_one()
            assert row.decision == "rejected"
            assert row.reason == "size_exceeded"

        assert len(received_rejected) == 1
        parsed = TradeRejected.model_validate_json(received_rejected[0])
        assert parsed.reason == RejectionReason.SIZE_EXCEEDED
        assert parsed.event_id == event.event_id
        assert parsed.decided_at is not None
    finally:
        await _cleanup_consumer(settings, durable)


@respx.mock
async def test_e2e_redelivery_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
    bus: NatsMessagingBus,
    settings: Settings,
) -> None:
    """Publica o MESMO trade 2x (mesmo tx_hash:log_index e mesmo event_id).

    NATS dedup por `Nats-Msg-Id = tx_hash:log_index` (window de 5min) garante
    que o agent só recebe a mensagem uma vez, então: 1 row em risk_decisions
    e 1 evento publicado em order.approved.

    Mesmo se o NATS dedup falhasse, o PK em `risk_decisions` (trade_event_id)
    + a checagem `is_new` no agent garantiriam que a 2ª delivery seria skipada
    sem re-publicar.
    """
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=_GAMMA_OK_PAYLOAD),
    )

    received_approved: list[bytes] = []

    async def approved_handler(payload: bytes) -> None:
        received_approved.append(payload)

    await bus.subscribe(OrderApproved.SUBJECT, approved_handler)

    durable = f"risk-test-{uuid4().hex[:8]}"
    try:
        await _make_agent(
            db_session_factory=db_session_factory,
            bus=bus,
            metrics_registry=CollectorRegistry(),
            durable_name=durable,
        )

        trade = _trade()
        event = _wallet_event(trade)
        await bus.publish_wallet_trade_detected(event)
        await bus.publish_wallet_trade_detected(event)  # JetStream dedup pelo tx_hash:log_index
        await asyncio.sleep(2.0)

        # 1 row no DB
        async with db_session_factory() as session:
            result = await session.execute(
                select(RiskDecisionRow).where(RiskDecisionRow.trade_event_id == event.event_id)
            )
            rows = result.scalars().all()
            assert len(rows) == 1

        # 1 evento no bus (NATS dedup garantiu publish único)
        assert len(received_approved) == 1
    finally:
        await _cleanup_consumer(settings, durable)
