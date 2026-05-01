"""Integration test E2E for WatcherAgent.

Sobe o agent contra:
- Polymarket Data API mockado (respx)
- Postgres real (db_session do conftest)
- NATS JetStream real

Verifica:
- row em wallet_trades apos iteracao
- mensagem no stream WALLET_TRADES
- metricas incrementadas
- segunda iteracao com mesmo trade nao duplica
"""

from __future__ import annotations

import asyncio
import time as _time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Any

import httpx
import nats as _nats
import pytest
import respx
from prometheus_client import CollectorRegistry
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.agents.watcher import TrackedWallet, WatcherAgent
from polycopy.config import Settings
from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.models import WalletTradeRow
from polycopy.infrastructure.persistence.wallet_trade_repository import (
    SqlAlchemyWalletTradeRepository,
)
from polycopy.infrastructure.polymarket.data_client import PolymarketDataClient

pytestmark = pytest.mark.integration

_BASE = "https://test-data-api.polymarket.com"
_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"


def _row(*, tx: str) -> dict[str, Any]:
    """Schema da Polymarket Data API real (sem logIndex; campo proxyWallet)."""
    return {
        "transactionHash": tx,
        "proxyWallet": _VALID_ADDR,
        "conditionId": "0x" + "ab" * 32,
        "asset": "12345",
        "side": "BUY",
        "price": "0.55",
        "usdcSize": "10",
        "timestamp": int(datetime(2026, 5, 1, 12, 0, tzinfo=UTC).timestamp()),
    }


@respx.mock
async def test_watcher_e2e_persists_publishes_dedups(
    db_session: AsyncSession, settings: Settings
) -> None:
    # Mock da Polymarket Data API: retorna 1 trade na 1a iter, mesma resposta na 2a.
    respx.get(f"{_BASE}/activity").mock(
        return_value=httpx.Response(200, json=[_row(tx="0x" + "aa" * 32)])
    )

    metrics = make_metrics(registry=CollectorRegistry())
    data_client = PolymarketDataClient(base_url=_BASE, metrics=metrics)
    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    @asynccontextmanager
    async def _factory() -> AsyncIterator[SqlAlchemyWalletTradeRepository]:
        # Reusa db_session do conftest (transacao rollbackada no teardown).
        repo = SqlAlchemyWalletTradeRepository(db_session)
        yield repo
        # Sem commit - o conftest faz rollback. Mas precisamos do flush
        # pro insert ser visivel na mesma sessao.
        await db_session.flush()

    received: list[WalletTradeDetected] = []

    async def consumer(payload: bytes, num_delivered: int) -> None:
        received.append(WalletTradeDetected.model_validate_json(payload))

    await bus.subscribe(WalletTradeDetected.SUBJECT, consumer, durable="test-watcher-e2e")

    stopping = asyncio.Event()
    agent = WatcherAgent(
        stopping=stopping,
        interval_s=0.05,
        wallets=[TrackedWallet(address=WalletAddress(value=_VALID_ADDR), label="W")],
        data_client=data_client,
        repo_factory=_factory,
        bus=bus,
        metrics=metrics,
        bootstrap_hours=24,
    )

    async def _cleanup_consumer() -> None:
        nc = await _nats.connect(settings.nats_url)
        try:
            js = nc.jetstream()
            with suppress(Exception):
                await js.delete_consumer("WALLET_TRADES", "test-watcher-e2e")
        finally:
            await nc.close()

    try:
        task = asyncio.create_task(agent.run())
        deadline = _time.monotonic() + 5.0
        # Aguarda ate pelo menos 1 trade chegar ao consumer.
        while _time.monotonic() < deadline:
            if received and len(received) >= 1:
                # Da uma iter extra pra dedup ser exercitada (mesmo mock retorna trade dupe).
                await asyncio.sleep(0.1)
                break
            await asyncio.sleep(0.02)
        stopping.set()
        await task

        # Banco: so 1 row apesar de N iteracoes (dedup PK)
        rows = (await db_session.execute(select(WalletTradeRow))).scalars().all()
        assert len(rows) == 1
        assert rows[0].tx_hash == "0x" + "aa" * 32

        # NATS: aguarda mensagens chegarem ao consumer
        for _ in range(20):
            if received:
                break
            await asyncio.sleep(0.05)

        assert len(received) >= 1
        assert received[0].trade.tx_hash == "0x" + "aa" * 32

        # Metricas: pelo menos 1 ok, >=1 trade inserido
        samples = list(metrics.watcher_iterations_total.collect())[0].samples
        ok_samples = [s for s in samples if s.labels.get("outcome") == "ok"]
        assert any(s.value >= 1 for s in ok_samples)
    finally:
        await bus.close()
        await _cleanup_consumer()
