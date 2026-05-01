"""TelegramNotifier: wrapper fino sobre aiogram.Bot.send_message com format MarkdownV2."""

from __future__ import annotations

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError

from polycopy.domain.models import Side, Trade

_MD_V2_SPECIALS = "_*[]()~`>#+-=|{}.!"


class TelegramError(Exception):
    """Wrapper p/ erros transitórios do Telegram (API/network)."""


def _escape_md(s: str) -> str:
    """Escapa caracteres especiais de MarkdownV2."""
    return "".join(f"\\{c}" if c in _MD_V2_SPECIALS else c for c in s)


def _format_trade_message(trade: Trade, *, label: str) -> str:
    """Mensagem MarkdownV2 padrão pra um trade."""
    emoji = "🟢" if trade.side is Side.BUY else "🔴"
    side = trade.side.value
    price = _escape_md(str(trade.price.value))
    size = _escape_md(f"${trade.size_usdc.amount}")
    token = _escape_md(trade.token_id.value)
    label_e = _escape_md(label)
    occurred = _escape_md(trade.occurred_at.astimezone().strftime("%Y-%m-%d %H:%M:%S UTC"))
    tx = trade.tx_hash
    line1 = f"{emoji} *{side}* — *{label_e}*"
    line2 = f"{size} @ {price} \\(token {token}\\)"
    line3 = occurred
    line4 = f"[tx](https://polygonscan.com/tx/{tx})"
    return "\n".join([line1, line2, line3, line4])


class TelegramNotifier:
    """Cliente Telegram do notifier. Envia notificação de trade pra um chat fixo."""

    def __init__(self, *, bot: Bot, chat_id: int) -> None:
        self._bot = bot
        self._chat_id = chat_id

    async def send_trade_notification(self, trade: Trade, *, label: str) -> None:
        text = _format_trade_message(trade, label=label)
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode="MarkdownV2",
            )
        except (TelegramAPIError, TelegramNetworkError) as exc:
            raise TelegramError(f"telegram send failed: {exc}") from exc
