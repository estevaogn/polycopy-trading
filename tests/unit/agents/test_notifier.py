"""Unit tests for NotifierAgent."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.notifier import NotifierAgent
from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.telegram.notifier_client import TelegramError
from polycopy.infrastructure.wallets_seed import TrackedWallet

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"


def _event() -> WalletTradeDetected:
    trade = Trade(
        tx_hash="0x" + "cd" * 32,
        log_index=0,
        wallet=WalletAddress(value=_VALID_ADDR),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        token_id=TokenId(value="12345"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )
    return WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=trade,
    )


def _agent_with_mocks(
    *,
    max_deliver: int = 5,
    wallet_allowlist: frozenset[str] = frozenset(),
) -> tuple[NotifierAgent, AsyncMock]:
    metrics = make_metrics(registry=CollectorRegistry())
    telegram = AsyncMock()
    wallets_by_addr = {
        _VALID_ADDR: TrackedWallet(address=WalletAddress(value=_VALID_ADDR), label="W1")
    }
    stopping = asyncio.Event()
    agent = NotifierAgent(
        stopping=stopping,
        bus=AsyncMock(),
        telegram=telegram,
        wallets_by_address=wallets_by_addr,
        metrics=metrics,
        max_deliver=max_deliver,
        wallet_allowlist=wallet_allowlist,
    )
    return agent, telegram


async def test_handle_message_sends_via_telegram() -> None:
    agent, telegram = _agent_with_mocks()
    payload = _event().model_dump_json().encode("utf-8")
    await agent._handle_message(payload, 1)
    telegram.send_trade_notification.assert_called_once()
    args, kwargs = telegram.send_trade_notification.call_args
    assert kwargs["label"] == "W1"


async def test_handle_message_unknown_wallet_uses_short_label() -> None:
    agent, telegram = _agent_with_mocks()
    agent._wallets_by_address.clear()
    payload = _event().model_dump_json().encode("utf-8")
    await agent._handle_message(payload, 1)
    args, kwargs = telegram.send_trade_notification.call_args
    assert kwargs["label"].startswith("0x123456")
    assert kwargs["label"].endswith("…")


async def test_handle_message_telegram_error_propagates() -> None:
    agent, telegram = _agent_with_mocks()
    telegram.send_trade_notification.side_effect = TelegramError("boom")
    payload = _event().model_dump_json().encode("utf-8")
    with pytest.raises(TelegramError):
        await agent._handle_message(payload, 1)


async def test_handle_message_at_max_deliver_increments_drop_metric() -> None:
    agent, telegram = _agent_with_mocks(max_deliver=3)
    telegram.send_trade_notification.side_effect = TelegramError("boom")
    payload = _event().model_dump_json().encode("utf-8")
    with pytest.raises(TelegramError):
        await agent._handle_message(payload, 3)

    samples = list(agent._metrics.notifier_messages_total.collect())[0].samples
    drop = [s for s in samples if s.labels.get("outcome") == "dropped_max_deliver"]
    assert any(s.value >= 1 for s in drop)


async def test_handle_message_invalid_payload_increments_metric_and_returns() -> None:
    agent, telegram = _agent_with_mocks()
    # Payload propositalmente inválido — falta campos obrigatórios
    payload = b'{"event_id": "not-a-uuid"}'
    # Não deve levantar: poison message é absorvido
    await agent._handle_message(payload, num_delivered=1)
    # Telegram NÃO foi chamado (parse falhou antes)
    telegram.send_trade_notification.assert_not_called()
    # Métrica invalid_payload registrada
    samples = list(agent._metrics.notifier_messages_total.collect())[0].samples
    invalid = [s for s in samples if s.labels.get("outcome") == "invalid_payload"]
    assert any(s.value >= 1 for s in invalid)


async def test_handle_message_filters_below_min_size() -> None:
    """K1: trade com size < min_size_usdc não envia Telegram, conta filtered_size."""
    agent, telegram = _agent_with_mocks()
    agent._min_size_usdc = Decimal("50")  # threshold acima do size do _event() (10)
    payload = _event().model_dump_json().encode("utf-8")
    await agent._handle_message(payload, num_delivered=1)
    telegram.send_trade_notification.assert_not_called()
    samples = list(agent._metrics.notifier_messages_total.collect())[0].samples
    filtered = [s for s in samples if s.labels.get("outcome") == "filtered_size"]
    assert any(s.value >= 1 for s in filtered)


async def test_handle_message_passes_when_size_at_or_above_min() -> None:
    """K1: size >= threshold passa pra Telegram normalmente."""
    agent, telegram = _agent_with_mocks()
    agent._min_size_usdc = Decimal("10")  # _event() size_usdc = 10 → passa
    payload = _event().model_dump_json().encode("utf-8")
    await agent._handle_message(payload, num_delivered=1)
    telegram.send_trade_notification.assert_called_once()


async def test_handle_message_no_filter_when_min_zero() -> None:
    """Backward-compat: min=0 (default sem config_repo) não filtra nada."""
    agent, telegram = _agent_with_mocks()
    assert agent._min_size_usdc == Decimal(0)
    payload = _event().model_dump_json().encode("utf-8")
    await agent._handle_message(payload, num_delivered=1)
    telegram.send_trade_notification.assert_called_once()


async def test_handle_message_filters_when_wallet_not_in_allowlist() -> None:
    """Wallet fora da allowlist é silenciada (sem Telegram, métrica filtered_wallet)."""
    other = "0x" + "2" * 40
    agent, telegram = _agent_with_mocks(wallet_allowlist=frozenset({other.lower()}))
    payload = _event().model_dump_json().encode("utf-8")
    await agent._handle_message(payload, num_delivered=1)
    telegram.send_trade_notification.assert_not_called()
    samples = list(agent._metrics.notifier_messages_total.collect())[0].samples
    filtered = [s for s in samples if s.labels.get("outcome") == "filtered_wallet"]
    assert any(s.value >= 1 for s in filtered)


async def test_handle_message_passes_when_wallet_in_allowlist() -> None:
    """Wallet listada na allowlist envia normalmente."""
    agent, telegram = _agent_with_mocks(wallet_allowlist=frozenset({_VALID_ADDR.lower()}))
    payload = _event().model_dump_json().encode("utf-8")
    await agent._handle_message(payload, num_delivered=1)
    telegram.send_trade_notification.assert_called_once()
