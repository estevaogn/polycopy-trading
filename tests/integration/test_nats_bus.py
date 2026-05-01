"""Integration tests for NatsMessagingBus (requer NATS up no docker-compose)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

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
from polycopy.ports import MessagingPort

pytestmark = pytest.mark.integration


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "cd" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def _event() -> WalletTradeDetected:
    return WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=_trade(),
    )


async def test_publish_and_subscribe_roundtrip(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(WalletTradeDetected.SUBJECT, handler)
    await asyncio.sleep(0.05)  # garante que o subscribe está pronto

    event = _event()
    await bus.publish_wallet_trade_detected(event)

    # Polling curto até receber ou timeout
    for _ in range(20):
        if received:
            break
        await asyncio.sleep(0.05)

    await bus.close()
    assert len(received) == 1
    parsed = WalletTradeDetected.model_validate_json(received[0])
    assert parsed.event_id == event.event_id


async def test_close_is_idempotent(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()
    await bus.close()
    await bus.close()  # não deve levantar


async def test_publish_without_connect_raises(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    with pytest.raises(RuntimeError, match="not connected"):
        await bus.publish_wallet_trade_detected(_event())


def _accepts(_: MessagingPort) -> None:
    return


async def test_adapter_satisfies_protocol(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    _accepts(bus)
