"""Smoke tests para confirmar que os ports são importáveis e implementáveis.

NÃO testa comportamento (Protocol não tem comportamento). Mypy faz o trabalho
de validar que adapters concretos no Plano 1B implementam os contratos.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.ports import (
    MessagingPort,
    PolymarketDataPort,
    WalletTradeRepository,
)
from polycopy.ports.messaging import EventHandler


class _FakeMessaging:
    """Stub que implementa MessagingPort por duck-typing."""

    def __init__(self) -> None:
        self.published: list[WalletTradeDetected] = []

    async def publish_wallet_trade_detected(self, event: WalletTradeDetected) -> None:
        self.published.append(event)

    async def subscribe(self, subject: str, handler: EventHandler) -> None:
        return None

    async def close(self) -> None:
        return None


def _addr() -> WalletAddress:
    return WalletAddress(value="0x" + "1" * 40)


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=_addr(),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def _accepts_messaging_port(_: MessagingPort) -> None:
    """Helper: mypy falha aqui se o argumento não satisfizer MessagingPort."""


async def test_fake_messaging_satisfies_port() -> None:
    fake = _FakeMessaging()
    _accepts_messaging_port(fake)  # mypy strict garante o contrato

    ev = WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=_trade(),
    )
    await fake.publish_wallet_trade_detected(ev)
    assert fake.published == [ev]


def test_ports_importable() -> None:
    assert MessagingPort is not None
    assert PolymarketDataPort is not None
    assert WalletTradeRepository is not None
