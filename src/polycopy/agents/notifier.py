"""NotifierAgent: consome WalletTradeDetected do JetStream e manda Telegram.

Rodando local (sem Docker):
    TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=... \\
    uv run python -m polycopy.agents.notifier

Configuração via env (`Settings`):
    TELEGRAM_BOT_TOKEN=...        # token do bot
    TELEGRAM_CHAT_ID=...          # chat id (int)
    NOTIFIER_METRICS_PORT=9102
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager
from decimal import Decimal

from pydantic import ValidationError

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import WalletTradeDetected
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.infrastructure.telegram.notifier_client import (
    TelegramError,
    TelegramNotifier,
)
from polycopy.infrastructure.wallets_seed import TrackedWallet
from polycopy.ports import MessagingPort, NotifierConfigRepository

# Polling do config: 30s entre re-leituras. Hot reload sem restart.
_CONFIG_RELOAD_S = 30.0

# Factory async-context que entrega um repository pronto (gerencia session).
ConfigRepoFactory = Callable[[], AbstractAsyncContextManager[NotifierConfigRepository]]


class NotifierAgent(AgentBase):
    name = "notifier"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        bus: MessagingPort,
        telegram: TelegramNotifier,
        wallets_by_address: dict[str, TrackedWallet],
        metrics: Metrics,
        config_repo_factory: ConfigRepoFactory | None = None,
        wallet_allowlist: frozenset[str] = frozenset(),
        max_deliver: int = 5,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=1.0)
        self._bus = bus
        self._telegram = telegram
        self._wallets_by_address = wallets_by_address
        self._metrics = metrics
        self._max_deliver = max_deliver
        self._config_repo_factory = config_repo_factory
        self._wallet_allowlist = wallet_allowlist
        # Cache do filtro: 0 = sem filtro (default backward-compat).
        self._min_size_usdc: Decimal = Decimal(0)
        self._last_config_load: float = 0.0

    async def start(self) -> None:
        """Registra durable consumer no JetStream; chamar antes de `run()`.

        O trabalho real do agente (consumir mensagens, enviar Telegram) acontece
        no callback `_handle_message` registrado aqui. O loop do `AgentBase.run()`
        apenas mantém o agente vivo e emite heartbeat estruturado periódico.
        """
        await self._reload_config_if_due(force=True)
        await self._bus.subscribe(
            WalletTradeDetected.SUBJECT,
            self._handle_message,
            durable="notifier-1",
            max_deliver=self._max_deliver,
        )

    async def run_once(self) -> None:
        # Trabalho real está no callback. AgentBase loop dá heartbeat estruturado.
        await self._reload_config_if_due(force=False)
        await asyncio.sleep(self._interval_s)

    async def _reload_config_if_due(self, *, force: bool) -> None:
        """Recarrega filtro do DB se passou _CONFIG_RELOAD_S desde último load.

        Best-effort: erros são logados e mantém o último valor conhecido.
        """
        if self._config_repo_factory is None:
            return
        now = time.monotonic()
        if not force and (now - self._last_config_load) < _CONFIG_RELOAD_S:
            return
        try:
            async with self._config_repo_factory() as repo:
                new_value = await repo.get_min_size_usdc()
            if new_value != self._min_size_usdc:
                self._log.info(
                    "notifier_filter_updated",
                    old_min_size_usdc=str(self._min_size_usdc),
                    new_min_size_usdc=str(new_value),
                )
            self._min_size_usdc = new_value
            self._last_config_load = now
        except Exception as exc:  # noqa: BLE001
            self._log.warning(
                "notifier_config_load_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )

    def _label_for(self, address: str) -> str:
        wallet = self._wallets_by_address.get(address)
        if wallet is not None:
            return wallet.label
        # Fallback para wallet desconhecida: prefixo `0x` + 6 hex chars (formato
        # comum de explorers blockchain pra short address).
        return f"{address[:8]}…"

    async def _handle_message(self, payload: bytes, num_delivered: int) -> None:
        start = time.perf_counter()
        try:
            try:
                event = WalletTradeDetected.model_validate_json(payload)
            except ValidationError as exc:
                # Poison message: payload corrompido nunca vai melhorar com retry.
                # Logar com payload truncado, contar métrica, e RETORNAR (acka via
                # _durable_wrapper) para parar o ciclo de redelivery.
                self._log.warning(
                    "notifier_invalid_payload",
                    num_delivered=num_delivered,
                    payload_preview=payload[:200].decode("utf-8", errors="replace"),
                    error=str(exc),
                )
                self._metrics.notifier_messages_total.labels(outcome="invalid_payload").inc()
                return  # acka (no _durable_wrapper) e descarta a mensagem corrompida

            # Wallet allowlist (mesma do RISK_COPY_ALLOWLIST): se preenchida, só
            # notifica trades das wallets em copy. Vazia = notifica todas (default).
            if (
                self._wallet_allowlist
                and event.trade.wallet.value.lower() not in self._wallet_allowlist
            ):
                self._metrics.notifier_messages_total.labels(outcome="filtered_wallet").inc()
                return

            # K1 filter: mensagens com size < min_size_usdc são ackadas sem enviar.
            if event.trade.size_usdc.amount < self._min_size_usdc:
                self._metrics.notifier_messages_total.labels(outcome="filtered_size").inc()
                return

            label = self._label_for(event.trade.wallet.value)
            try:
                await self._telegram.send_trade_notification(event.trade, label=label)
            except TelegramError:
                if num_delivered >= self._max_deliver:
                    self._metrics.notifier_messages_total.labels(
                        outcome="dropped_max_deliver"
                    ).inc()
                else:
                    self._metrics.notifier_messages_total.labels(outcome="telegram_error").inc()
                raise  # propaga pro _durable_wrapper não ackar (redelivery / max_deliver drop)
            self._metrics.notifier_messages_total.labels(outcome="sent").inc()
        finally:
            self._metrics.notifier_send_duration_seconds.observe(time.perf_counter() - start)


async def main() -> None:
    """Entrypoint: monta dependências, sobe /metrics, registra signal handlers, roda."""
    from contextlib import asynccontextmanager

    from aiogram import Bot

    from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.persistence.notifier_config_repository import (
        SqlAlchemyNotifierConfigRepository,
    )
    from polycopy.infrastructure.wallets_seed import load_wallets_seed

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    if settings.telegram_bot_token is None or not settings.telegram_bot_token.get_secret_value():
        raise RuntimeError("TELEGRAM_BOT_TOKEN must be set for notifier")
    if settings.telegram_chat_id is None:
        raise RuntimeError("TELEGRAM_CHAT_ID must be set for notifier")

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.notifier_metrics_port)

    seed = load_wallets_seed(settings.wallets_seed_path)
    wallets_by_address = {w.address.value: w for w in seed}

    bot = Bot(token=settings.telegram_bot_token.get_secret_value())
    telegram = TelegramNotifier(bot=bot, chat_id=settings.telegram_chat_id)

    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)

    @asynccontextmanager
    async def _repo_factory() -> AsyncGenerator[NotifierConfigRepository, None]:
        async with session_factory() as session:
            yield SqlAlchemyNotifierConfigRepository(session)

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    wallet_allowlist = frozenset(
        addr.strip().lower() for addr in settings.risk_copy_allowlist.split(",") if addr.strip()
    )

    agent = NotifierAgent(
        stopping=stopping,
        bus=bus,
        telegram=telegram,
        wallets_by_address=wallets_by_address,
        metrics=metrics,
        config_repo_factory=_repo_factory,
        wallet_allowlist=wallet_allowlist,
    )
    await agent.start()
    try:
        await agent.run()
    finally:
        await bus.close()
        await bot.session.close()
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
