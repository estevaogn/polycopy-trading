"""Integration tests for NatsMessagingBus (JetStream) — requer NATS up no docker-compose."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from polycopy.config import Settings
from polycopy.domain.events import (
    FailureReason,
    OrderApproved,
    OrderDryRun,
    OrderExecuted,
    OrderFailed,
    OrderSized,
    OrderSkipped,
    RejectionReason,
    SkipReason,
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
from polycopy.ports import MessagingPort

pytestmark = pytest.mark.integration


def _trade(*, tx_hash: str = "0x" + "cd" * 32, log_index: int = 0) -> Trade:
    return Trade(
        tx_hash=tx_hash,
        log_index=log_index,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def _event(*, tx_hash: str = "0x" + "cd" * 32, log_index: int = 0) -> WalletTradeDetected:
    return WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=_trade(tx_hash=tx_hash, log_index=log_index),
    )


def _order_approved_event() -> OrderApproved:
    return OrderApproved(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
    )


def _trade_rejected_event(
    reason: RejectionReason = RejectionReason.SIZE_EXCEEDED,
) -> TradeRejected:
    return TradeRejected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        reason=reason,
    )


def _order_sized_event() -> OrderSized:
    trade = _trade()
    return OrderSized(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=trade,
        final_size_usdc=Money.from_usdc("10"),
        original_size_usdc=trade.size_usdc,
    )


def _order_skipped_event() -> OrderSkipped:
    return OrderSkipped(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        reason=SkipReason.BELOW_MIN_SIZE,
    )


def _order_executed_event() -> OrderExecuted:
    return OrderExecuted(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        final_size_usdc=Money.from_usdc("10"),
        tx_hash="0x" + "ab" * 32,
        gas_wei=21000,
    )


def _order_failed_event() -> OrderFailed:
    return OrderFailed(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        final_size_usdc=Money.from_usdc("10"),
        reason=FailureReason.EXECUTOR_DISABLED,
        error_message="test failure",
    )


def _order_dry_run_event() -> OrderDryRun:
    return OrderDryRun(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        final_size_usdc=Money.from_usdc("10"),
    )


@pytest.fixture
async def bus(settings: Settings) -> NatsMessagingBus:
    """Bus conectado; testes finalizam com `await bus.close()`."""
    b = NatsMessagingBus(url=settings.nats_url)
    await b.connect()
    return b


async def test_durable_subscribe_receives_published_event(bus: NatsMessagingBus) -> None:
    received: list[tuple[bytes, int]] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append((payload, num_delivered))

    await bus.subscribe(WalletTradeDetected.SUBJECT, handler, durable="test-1")
    await asyncio.sleep(0.05)

    event = _event(tx_hash="0x" + "01" * 32, log_index=0)
    await bus.publish_wallet_trade_detected(event)

    for _ in range(20):
        if received:
            break
        await asyncio.sleep(0.05)

    await bus.close()
    assert len(received) == 1
    payload, num_delivered = received[0]
    assert num_delivered == 1
    parsed = WalletTradeDetected.model_validate_json(payload)
    assert parsed.event_id == event.event_id


async def test_publish_dedup_by_msg_id(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    await bus.subscribe(WalletTradeDetected.SUBJECT, handler, durable="test-2")
    await asyncio.sleep(0.05)

    # Mesmo tx_hash + log_index → mesmo Nats-Msg-Id → JetStream dedupa server-side
    event_a = _event(tx_hash="0x" + "02" * 32, log_index=0)
    event_b = _event(tx_hash="0x" + "02" * 32, log_index=0)
    await bus.publish_wallet_trade_detected(event_a)
    await bus.publish_wallet_trade_detected(event_b)

    await asyncio.sleep(0.3)
    await bus.close()
    assert len(received) == 1


async def test_handler_exception_triggers_redelivery(bus: NatsMessagingBus) -> None:
    attempts: list[int] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        attempts.append(num_delivered)
        if num_delivered < 3:
            raise RuntimeError("simulated failure")

    await bus.subscribe(
        WalletTradeDetected.SUBJECT,
        handler,
        durable="test-3",
        ack_wait_seconds=1,
        max_deliver=5,
    )
    await asyncio.sleep(0.05)

    event = _event(tx_hash="0x" + "03" * 32, log_index=0)
    await bus.publish_wallet_trade_detected(event)

    # Aguarda até 3 entregas (max_deliver=5; ack_wait=1s pra redelivery rápido)
    deadline = asyncio.get_event_loop().time() + 5.0
    while len(attempts) < 3 and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.1)

    await bus.close()
    assert attempts[:3] == [1, 2, 3]


async def test_close_is_idempotent(bus: NatsMessagingBus) -> None:
    await bus.close()
    await bus.close()  # não deve levantar


async def test_publish_without_connect_raises(settings: Settings) -> None:
    fresh = NatsMessagingBus(url=settings.nats_url)
    with pytest.raises(RuntimeError, match="not connected"):
        await fresh.publish_wallet_trade_detected(_event())


def _accepts(_: MessagingPort) -> None:
    return


async def test_adapter_satisfies_protocol(bus: NatsMessagingBus) -> None:
    _accepts(bus)
    await bus.close()


async def test_publish_order_approved_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderApproved.SUBJECT, handler)
    await bus.publish_order_approved(_order_approved_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_trade_rejected_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(TradeRejected.SUBJECT, handler)
    await bus.publish_trade_rejected(_trade_rejected_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_approved_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id -> mesmo Nats-Msg-Id -> JetStream dedupa server-side.

    Usa durable consumer (JetStream) ao invés de ephemeral (NATS core) porque o
    subscriber ephemeral recebe ambos os publishes via core antes do server
    completar a dedup; a dedup é visível apenas via leitura JetStream.
    """
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    durable = f"test-order-dedup-{uuid4().hex[:8]}"
    await bus.subscribe(OrderApproved.SUBJECT, handler, durable=durable)
    await asyncio.sleep(0.05)

    event = _order_approved_event()
    await bus.publish_order_approved(event)
    await bus.publish_order_approved(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_trade_rejected_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id -> mesmo Nats-Msg-Id -> JetStream dedupa server-side."""
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    durable = f"test-trade-dedup-{uuid4().hex[:8]}"
    await bus.subscribe(TradeRejected.SUBJECT, handler, durable=durable)
    await asyncio.sleep(0.05)

    event = _trade_rejected_event()
    await bus.publish_trade_rejected(event)
    await bus.publish_trade_rejected(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_sized_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderSized.SUBJECT, handler)
    await bus.publish_order_sized(_order_sized_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_skipped_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderSkipped.SUBJECT, handler)
    await bus.publish_order_skipped(_order_skipped_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_sized_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id -> mesmo Nats-Msg-Id -> JetStream dedupa server-side.

    Validado via durable subscriber (ephemeral não vê dedup — ver T5/2B).
    """
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    durable = f"sizing-dedup-{uuid4().hex[:8]}"
    await bus.subscribe(OrderSized.SUBJECT, handler, durable=durable)
    await asyncio.sleep(0.05)

    event = _order_sized_event()
    await bus.publish_order_sized(event)
    await bus.publish_order_sized(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_skipped_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id -> mesmo Nats-Msg-Id -> JetStream dedupa server-side."""
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    durable = f"sizing-skip-dedup-{uuid4().hex[:8]}"
    await bus.subscribe(OrderSkipped.SUBJECT, handler, durable=durable)
    await asyncio.sleep(0.05)

    event = _order_skipped_event()
    await bus.publish_order_skipped(event)
    await bus.publish_order_skipped(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_executed_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderExecuted.SUBJECT, handler)
    await bus.publish_order_executed(_order_executed_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_executed_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id -> mesmo Nats-Msg-Id -> JetStream dedupa server-side.

    Validado via durable subscriber (ephemeral não vê dedup — ver T5/2B).
    """
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    durable = f"executor-exec-dedup-{uuid4().hex[:8]}"
    await bus.subscribe(OrderExecuted.SUBJECT, handler, durable=durable)
    await asyncio.sleep(0.05)

    event = _order_executed_event()
    await bus.publish_order_executed(event)
    await bus.publish_order_executed(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_failed_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderFailed.SUBJECT, handler)
    await bus.publish_order_failed(_order_failed_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_failed_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id -> mesmo Nats-Msg-Id -> JetStream dedupa server-side."""
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    durable = f"executor-fail-dedup-{uuid4().hex[:8]}"
    await bus.subscribe(OrderFailed.SUBJECT, handler, durable=durable)
    await asyncio.sleep(0.05)

    event = _order_failed_event()
    await bus.publish_order_failed(event)
    await bus.publish_order_failed(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_dry_run_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderDryRun.SUBJECT, handler)
    await bus.publish_order_dry_run(_order_dry_run_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_dry_run_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id -> mesmo Nats-Msg-Id -> JetStream dedupa server-side."""
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    durable = f"executor-dry-dedup-{uuid4().hex[:8]}"
    await bus.subscribe(OrderDryRun.SUBJECT, handler, durable=durable)
    await asyncio.sleep(0.05)

    event = _order_dry_run_event()
    await bus.publish_order_dry_run(event)
    await bus.publish_order_dry_run(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()
