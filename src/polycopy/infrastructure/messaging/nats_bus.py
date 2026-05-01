"""NatsMessagingBus: adapter de MessagingPort usando JetStream.

Stream `WALLET_TRADES`:
- Subject filter: `wallet.trade.>`
- Retention: limits (default)
- Max age: 7 dias
- Storage: file
- Replicas: 1 (single-node na fase 1)

Dedup server-side via `Nats-Msg-Id` header (formato: `tx_hash:log_index`).

Subscribe pode ser:
- Ephemeral (durable=None): handler `EventHandler` (sem ack manual; entrega best-effort)
- Durable (durable=str): handler `DurableEventHandler` (recebe num_delivered;
  ack manual após sucesso, redeliver em exception até `max_deliver`)
"""

from __future__ import annotations

import contextlib
from typing import cast

import nats
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import (
    ConsumerConfig,
    DeliverPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import BadRequestError

from polycopy.domain.events import WalletTradeDetected
from polycopy.infrastructure.observability.logging import get_logger
from polycopy.ports.messaging import DurableEventHandler, EventHandler

_log = get_logger(__name__)

_STREAM_NAME = "WALLET_TRADES"
_STREAM_SUBJECTS = ["wallet.trade.>"]
_STREAM_MAX_AGE_S = 7 * 24 * 3600


class NatsMessagingBus:
    """Adapter JetStream de `MessagingPort`."""

    def __init__(self, *, url: str) -> None:
        self._url = url
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None

    async def connect(self) -> None:
        if self._nc is not None and self._nc.is_connected:
            return
        self._nc = await nats.connect(self._url)
        self._js = self._nc.jetstream()
        await self._ensure_stream()

    def _require_connected(self) -> tuple[NatsClient, JetStreamContext]:
        """Garante que a conexão NATS+JetStream está ativa; retorna (nc, js)."""
        if self._nc is None or not self._nc.is_connected or self._js is None:
            raise RuntimeError("NatsMessagingBus not connected; call connect() first")
        return self._nc, self._js

    async def _ensure_stream(self) -> None:
        _, js = self._require_connected()
        config = StreamConfig(
            name=_STREAM_NAME,
            subjects=_STREAM_SUBJECTS,
            retention=RetentionPolicy.LIMITS,
            max_age=_STREAM_MAX_AGE_S,
            storage=StorageType.FILE,
            num_replicas=1,
            duplicate_window=300,  # 5min: dedup window por Nats-Msg-Id
        )
        # Stream pode já existir com config compatível — BadRequestError é benigno.
        with contextlib.suppress(BadRequestError):
            await js.add_stream(config=config)

    async def publish_wallet_trade_detected(self, event: WalletTradeDetected) -> None:
        _, js = self._require_connected()
        payload = event.model_dump_json().encode("utf-8")
        msg_id = f"{event.trade.tx_hash}:{event.trade.log_index}"
        await js.publish(
            WalletTradeDetected.SUBJECT,
            payload,
            headers={"Nats-Msg-Id": msg_id},
        )

    async def subscribe(
        self,
        subject: str,
        handler: EventHandler | DurableEventHandler,
        *,
        durable: str | None = None,
        ack_wait_seconds: int = 30,
        max_deliver: int = 5,
    ) -> None:
        nc, js = self._require_connected()

        if durable is None:
            ephemeral_handler = cast(EventHandler, handler)

            async def _ephemeral_wrapper(msg: Msg) -> None:
                await ephemeral_handler(msg.data)

            await nc.subscribe(subject, cb=_ephemeral_wrapper)
            return

        durable_handler = cast(DurableEventHandler, handler)

        async def _durable_wrapper(msg: Msg) -> None:
            num_delivered = msg.metadata.num_delivered or 1
            try:
                await durable_handler(msg.data, num_delivered)
            except Exception as exc:
                _log.warning(
                    "durable_handler_failed",
                    subject=subject,
                    durable=durable,
                    num_delivered=num_delivered,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                # Não acka — JetStream redelivera até max_deliver.
                return
            await msg.ack()

        await js.subscribe(
            subject,
            durable=durable,
            cb=_durable_wrapper,
            manual_ack=True,  # ack/nak controlado por _durable_wrapper; sem auto-ack
            config=ConsumerConfig(
                ack_wait=ack_wait_seconds,  # nats-py 2.10+: float em segundos
                max_deliver=max_deliver,
                deliver_policy=DeliverPolicy.NEW,
            ),
        )

    async def close(self) -> None:
        if self._nc is None:
            return
        if self._nc.is_connected:
            await self._nc.drain()
        self._nc = None
        self._js = None
