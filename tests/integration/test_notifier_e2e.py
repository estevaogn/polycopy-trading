"""Integration test E2E for NotifierAgent.

Sobe o agent contra:
- NATS JetStream real
- Telegram Bot mockado (AsyncMock)

Verifica:
- consumer recebe mensagem publicada
- aiogram.Bot.send_message chamado com payload formatado
- métrica `sent` incrementada
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import nats as _nats
import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.notifier import NotifierAgent
from polycopy.config import Settings
from polycopy.domain.events import WalletTradeDetected
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
from polycopy.infrastructure.telegram.notifier_client import TelegramNotifier
from polycopy.infrastructure.wallets_seed import TrackedWallet

pytestmark = pytest.mark.integration

_ADDR = "0x1234567890abcdef1234567890abcdef12345678"


def _event(*, tx_hash: str = "0x" + "ee" * 32) -> WalletTradeDetected:
    trade = Trade(
        tx_hash=tx_hash,
        log_index=0,
        wallet=WalletAddress(value=_ADDR),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        token_id=TokenId(value="12345"),
        side=Side.BUY,
        price=Price(value=Decimal("0.55")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )
    return WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=trade,
    )


async def test_notifier_e2e_consumes_and_sends(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    metrics = make_metrics(registry=CollectorRegistry())
    bot = AsyncMock()
    telegram = TelegramNotifier(bot=bot, chat_id=42)
    wallets_by_addr = {_ADDR: TrackedWallet(address=WalletAddress(value=_ADDR), label="WhaleE2E")}

    stopping = asyncio.Event()
    agent = NotifierAgent(
        stopping=stopping,
        bus=bus,
        telegram=telegram,
        wallets_by_address=wallets_by_addr,
        metrics=metrics,
    )

    async def _cleanup_consumer() -> None:
        nc = await _nats.connect(settings.nats_url)
        try:
            js = nc.jetstream()
            with suppress(Exception):
                await js.delete_consumer("WALLET_TRADES", "notifier-1")
        finally:
            await nc.close()

    try:
        await agent.start()
        task = asyncio.create_task(agent.run())

        # Publica evento
        event = _event(tx_hash="0x" + "f1" * 32)
        await bus.publish_wallet_trade_detected(event)

        # Espera consumer processar
        for _ in range(40):
            if bot.send_message.called:
                break
            await asyncio.sleep(0.05)

        stopping.set()
        await task

        bot.send_message.assert_called_once()
        kwargs = bot.send_message.call_args.kwargs
        assert kwargs["chat_id"] == 42
        assert "WhaleE2E" in kwargs["text"]

        samples = list(metrics.notifier_messages_total.collect())[0].samples
        sent = [s for s in samples if s.labels.get("outcome") == "sent"]
        assert any(s.value >= 1 for s in sent)
    finally:
        await bus.close()
        await _cleanup_consumer()
