"""WatcherAgent: faz polling da Polymarket Data API por wallet e publica
`WalletTradeDetected` no JetStream após dedup PK no Postgres.

Rodando local (sem Docker):
    uv run python -m polycopy.agents.watcher

Configuração via env (`Settings`):
    WATCH_WALLETS=0xaaa,0xbbb     # CSV de endereços (esqueleto Task 3)
    WATCHER_INTERVAL_S=5          # segundos entre iterações
    WATCHER_BOOTSTRAP_HOURS=24    # bootstrap de cursor pra wallet nova
    WATCHER_METRICS_PORT=9101     # porta do servidor /metrics
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Trade
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MessagingPort, PolymarketDataPort, WalletTradeRepository


@dataclass(frozen=True)
class TrackedWallet:
    address: WalletAddress
    label: str


RepoFactory = Callable[[], AbstractAsyncContextManager[WalletTradeRepository]]


class WatcherAgent(AgentBase):
    name = "watcher"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        interval_s: float,
        wallets: list[TrackedWallet],
        data_client: PolymarketDataPort,
        repo_factory: RepoFactory,
        bus: MessagingPort,
        metrics: Metrics,
        bootstrap_hours: int,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=interval_s)
        self._wallets = wallets
        self._data_client = data_client
        self._repo_factory = repo_factory
        self._bus = bus
        self._metrics = metrics
        self._bootstrap_hours = bootstrap_hours

    async def run_once(self) -> None:
        for wallet in self._wallets:
            await self._poll_wallet(wallet)

    async def _poll_wallet(self, wallet: TrackedWallet) -> None:
        start = time.perf_counter()
        addr = wallet.address.value
        try:
            inserted_trades: list[Trade] = await self._fetch_and_persist(wallet)

            for trade in inserted_trades:
                event = WalletTradeDetected(
                    event_id=uuid4(),
                    occurred_at=datetime.now(tz=UTC),
                    trade=trade,
                )
                await self._bus.publish_wallet_trade_detected(event)

            outcome = "ok" if inserted_trades else "empty"
            self._metrics.watcher_iterations_total.labels(wallet=addr, outcome=outcome).inc()
            if inserted_trades:
                self._metrics.watcher_trades_inserted_total.labels(wallet=addr).inc(
                    len(inserted_trades)
                )
        except Exception as exc:
            # W1: continua o loop após falha. Retries internos do client já esgotaram.
            # Não pegamos BaseException de propósito (KeyboardInterrupt/SystemExit propagam).
            self._metrics.watcher_iterations_total.labels(wallet=addr, outcome="error").inc()
            self._log.warning(
                "watcher_poll_failed",
                wallet=addr,
                error=str(exc),
                error_type=type(exc).__name__,
            )
        finally:
            self._metrics.watcher_iteration_duration_seconds.labels(wallet=addr).observe(
                time.perf_counter() - start
            )

    async def _fetch_and_persist(self, wallet: TrackedWallet) -> list[Trade]:
        """Faz fetch da Data API, dedup via PK e retorna apenas trades novos inseridos."""
        async with self._repo_factory() as repo:
            since = await repo.latest_occurred_at(wallet.address)
            if since is None:
                since = datetime.now(tz=UTC) - timedelta(hours=self._bootstrap_hours)

            trades = await self._data_client.fetch_user_activity(wallet.address, since=since)

            inserted_trades: list[Trade] = []
            for trade in trades:
                if await repo.insert_if_absent(trade):
                    inserted_trades.append(trade)
        return inserted_trades


def _parse_watch_wallets_csv(csv: str) -> list[TrackedWallet]:
    """Parse `WATCH_WALLETS=0xaaa,0xbbb`. Label vira `addr[:8]…`."""
    wallets: list[TrackedWallet] = []
    for idx, item in enumerate(csv.split(",")):
        s = item.strip()
        if not s:
            continue
        try:
            addr = WalletAddress(value=s)
        except ValueError as exc:
            raise ValueError(f"WATCH_WALLETS entry {idx} ({s!r}) inválida: {exc}") from exc
        wallets.append(TrackedWallet(address=addr, label=f"{s[:8]}…"))
    return wallets


def _load_wallets(settings: Settings) -> list[TrackedWallet]:
    """Carrega wallets de WATCH_WALLETS (override dev) ou do YAML (default)."""
    from polycopy.infrastructure.wallets_seed import load_wallets_seed  # lazy

    if settings.watch_wallets.strip():
        return _parse_watch_wallets_csv(settings.watch_wallets)
    seed = load_wallets_seed(settings.wallets_seed_path)
    return [TrackedWallet(address=w.address, label=w.label) for w in seed]


async def _sync_tracked_wallets(
    session_factory: async_sessionmaker[AsyncSession],
    wallets: list[TrackedWallet],
) -> None:
    """Espelha seed em DB pra dashboard. Best-effort: erros logam e seguem."""
    from polycopy.infrastructure.observability.logging import get_logger
    from polycopy.infrastructure.persistence.tracked_wallet_repository import (
        SqlAlchemyTrackedWalletRepository,
    )

    log = get_logger("watcher.sync_tracked_wallets")
    try:
        async with session_factory() as session:
            repo = SqlAlchemyTrackedWalletRepository(session)
            for w in wallets:
                await repo.upsert(address=w.address.value, label=w.label)
            await session.commit()
        log.info("tracked_wallets_synced", count=len(wallets))
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "tracked_wallets_sync_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )


def _make_repo_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> RepoFactory:
    """Cria factory que abre AsyncSession + commit/rollback automático."""
    from polycopy.infrastructure.persistence.wallet_trade_repository import (
        SqlAlchemyWalletTradeRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[WalletTradeRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyWalletTradeRepository(session)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return _factory


async def main() -> None:
    """Entrypoint: monta dependências, sobe /metrics, registra signal handlers, roda.

    Carrega wallets de `WALLETS_SEED_PATH` (default `config/wallets_seed.yaml`).
    Se `WATCH_WALLETS` (CSV) estiver setado, ele *substitui* o YAML — útil pra dev.
    """
    from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.polymarket.data_client import PolymarketDataClient

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.watcher_metrics_port)

    wallets = _load_wallets(settings)
    if not wallets:
        raise RuntimeError(
            f"No wallets configured. Add entries to {settings.wallets_seed_path} "
            f"or set WATCH_WALLETS=0xaaa,0xbbb."
        )

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(session_factory)

    # Sincroniza seed YAML pra tabela tracked_wallets (espelho pro dashboard).
    # Best-effort: erros aqui não bloqueiam o startup do watcher.
    await _sync_tracked_wallets(session_factory, wallets)

    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    data_client = PolymarketDataClient(
        base_url=settings.polymarket_base_url,
        metrics=metrics,
    )

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = WatcherAgent(
        stopping=stopping,
        interval_s=settings.watcher_interval_s,
        wallets=wallets,
        data_client=data_client,
        repo_factory=repo_factory,
        bus=bus,
        metrics=metrics,
        bootstrap_hours=settings.watcher_bootstrap_hours,
    )
    try:
        await agent.run()
    finally:
        await bus.close()
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
