"""MessagingPort: contrato para publicar/assinar eventos no bus (NATS no Plano 1B)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from polycopy.domain.events import WalletTradeDetected

EventHandler = Callable[[bytes], Awaitable[None]]


class MessagingPort(Protocol):
    """Bus de eventos. Implementação concreta: NATS JetStream (Plano 1B)."""

    async def publish_wallet_trade_detected(self, event: WalletTradeDetected) -> None:
        """Publica evento no subject `wallet.trade.detected`."""
        ...

    async def subscribe(self, subject: str, handler: EventHandler) -> None:
        """Assina subject; handler recebe payload bruto (bytes JSON)."""
        ...

    async def close(self) -> None:
        """Fecha conexão com graceful drain."""
        ...
