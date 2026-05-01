"""Tests for TelegramNotifier."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.telegram.notifier_client import (
    TelegramError,
    TelegramNotifier,
    _escape_md,
    _format_trade_message,
)


def _trade(*, side: Side = Side.BUY) -> Trade:
    return Trade(
        tx_hash="0x" + "cd" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x1234567890abcdef1234567890abcdef12345678"),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        token_id=TokenId(value="12345"),
        side=side,
        price=Price(value=Decimal("0.55")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )


def test_escape_md_escapes_special_chars() -> None:
    assert _escape_md("a.b") == r"a\.b"
    assert _escape_md("(x)") == r"\(x\)"
    assert _escape_md("a-b_c*d") == r"a\-b\_c\*d"


def test_format_trade_buy_includes_emoji_and_label() -> None:
    msg = _format_trade_message(_trade(side=Side.BUY), label="Whale 1")
    assert msg.startswith("🟢")
    assert "*BUY*" in msg
    assert "Whale 1" in msg


def test_format_trade_sell_uses_red_emoji() -> None:
    msg = _format_trade_message(_trade(side=Side.SELL), label="W")
    assert msg.startswith("🔴")
    assert "*SELL*" in msg


def test_format_includes_polygonscan_link() -> None:
    trade = _trade()
    msg = _format_trade_message(trade, label="W")
    assert f"https://polygonscan.com/tx/{trade.tx_hash}" in msg


async def test_send_calls_bot_with_markdown_v2() -> None:
    bot = AsyncMock()
    notifier = TelegramNotifier(bot=bot, chat_id=42)
    await notifier.send_trade_notification(_trade(), label="W")
    bot.send_message.assert_called_once()
    kwargs = bot.send_message.call_args.kwargs
    assert kwargs["chat_id"] == 42
    assert kwargs["parse_mode"] == "MarkdownV2"
    assert kwargs["text"].startswith("🟢")


async def test_send_wraps_aiogram_errors() -> None:
    from aiogram.exceptions import TelegramAPIError

    bot = AsyncMock()
    bot.send_message.side_effect = TelegramAPIError(method=AsyncMock(), message="boom")
    notifier = TelegramNotifier(bot=bot, chat_id=42)
    with pytest.raises(TelegramError):
        await notifier.send_trade_notification(_trade(), label="W")
