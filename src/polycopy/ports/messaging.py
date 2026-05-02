"""MessagingPort: contrato para publicar/assinar eventos no bus (NATS no Plano 1B)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from polycopy.domain.events import OrderApproved, TradeRejected, WalletTradeDetected

EventHandler = Callable[[bytes], Awaitable[None]]
"""Handler para subscribe ephemeral (sem ack manual)."""

DurableEventHandler = Callable[[bytes, int], Awaitable[None]]
"""Handler para subscribe durable. Recebe (payload, num_delivered).

`num_delivered` é o número da tentativa atual (1 na primeira entrega, N após
redeliveries). Útil pra detectar mensagens prestes a serem descartadas
(num_delivered == max_deliver) e instrumentar métricas adequadas.
"""


class MessagingPort(Protocol):
    """Bus de eventos. Implementação concreta: NATS JetStream (Plano 1C)."""

    async def publish_wallet_trade_detected(self, event: WalletTradeDetected) -> None:
        """Publica evento no subject `wallet.trade.detected`."""
        ...

    async def publish_order_approved(self, event: OrderApproved) -> None:
        """Publica evento no subject `order.approved`."""
        ...

    async def publish_trade_rejected(self, event: TradeRejected) -> None:
        """Publica evento no subject `trade.rejected`."""
        ...

    async def subscribe(
        self,
        subject: str,
        handler: EventHandler | DurableEventHandler,
        *,
        durable: str | None = None,
        ack_wait_seconds: int = 30,
        max_deliver: int = 5,
    ) -> None:
        """Assina subject. Se `durable` é dado, cria push durable consumer JetStream
        e o handler deve ser `DurableEventHandler` (recebe `num_delivered`).
        Caso contrário, ephemeral subscribe e handler é `EventHandler`."""
        ...

    async def close(self) -> None:
        """Fecha conexão com graceful drain."""
        ...
