"""NatsMessagingBus: adapter de MessagingPort usando nats-py (core pub/sub)."""

from __future__ import annotations

import nats
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from polycopy.domain.events import WalletTradeDetected
from polycopy.ports.messaging import EventHandler


class NatsMessagingBus:
    """Bus core NATS. JetStream entra em fase posterior se precisar de durability."""

    def __init__(self, *, url: str) -> None:
        self._url = url
        self._nc: NatsClient | None = None

    async def connect(self) -> None:
        if self._nc is not None and self._nc.is_connected:
            return
        self._nc = await nats.connect(self._url)

    async def publish_wallet_trade_detected(self, event: WalletTradeDetected) -> None:
        if self._nc is None or not self._nc.is_connected:
            raise RuntimeError("NatsMessagingBus not connected; call connect() first")
        payload = event.model_dump_json().encode("utf-8")
        await self._nc.publish(WalletTradeDetected.SUBJECT, payload)
        await self._nc.flush()

    async def subscribe(self, subject: str, handler: EventHandler) -> None:
        if self._nc is None or not self._nc.is_connected:
            raise RuntimeError("NatsMessagingBus not connected; call connect() first")

        async def _wrapper(msg: Msg) -> None:
            await handler(msg.data)

        await self._nc.subscribe(subject, cb=_wrapper)

    async def close(self) -> None:
        if self._nc is None:
            return
        if self._nc.is_connected:
            await self._nc.drain()
        self._nc = None
