"""backfill_wallet_history — popula wallet_trades com histórico via Polymarket /activity.

Idempotente via PK (tx_hash, log_index): re-rodar é seguro. Resolver agent
em produção pega novas condition_ids no próximo ciclo (~1h) e popula
market_resolutions, ativando as views wallet_realized_pnl + wallet_open_positions.

Usage:
    # Uma wallet específica
    uv run python -m polycopy.scripts.backfill_wallet_history \\
        --wallet 0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a --since-days 365

    # Todas as wallets do tracked_wallets (sincronizado do seed yaml)
    uv run python -m polycopy.scripts.backfill_wallet_history --all --since-days 365

Defaults:
    --since-days 365 (1 ano de histórico)
    --limit 1000 por wallet (avisa se atingir, indicando que pode ter mais)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime, timedelta


async def _backfill_wallet(
    *,
    wallet_address: str,
    since: datetime,
    limit: int,
) -> tuple[int, int]:
    """Backfill 1 wallet. Retorna (fetched, inserted)."""
    from polycopy.config import Settings
    from polycopy.domain.value_objects import WalletAddress
    from polycopy.infrastructure.observability.metrics import make_metrics
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.persistence.wallet_trade_repository import (
        SqlAlchemyWalletTradeRepository,
    )
    from polycopy.infrastructure.polymarket.data_client import PolymarketDataClient

    settings = Settings()
    engine = make_engine(settings)
    try:
        client = PolymarketDataClient(
            base_url=settings.polymarket_base_url,
            metrics=make_metrics(),
            timeout_s=30.0,
        )
        addr = WalletAddress(value=wallet_address)
        trades = await client.fetch_user_activity(addr, since=since, limit=limit)
        fetched = len(trades)

        session_factory = make_session_factory(engine)
        inserted = 0
        async with session_factory() as session:
            repo = SqlAlchemyWalletTradeRepository(session)
            for trade in trades:
                if await repo.insert_if_absent(trade):
                    inserted += 1
            await session.commit()
        return fetched, inserted
    finally:
        await engine.dispose()


async def _load_all_tracked_wallets() -> list[str]:
    """Lê tracked_wallets do DB pra modo --all."""
    from sqlalchemy import text

    from polycopy.config import Settings
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )

    settings = Settings()
    engine = make_engine(settings)
    try:
        session_factory = make_session_factory(engine)
        async with session_factory() as session:
            result = await session.execute(
                text("SELECT address FROM tracked_wallets ORDER BY label")
            )
            return [row[0] for row in result.all()]
    finally:
        await engine.dispose()


async def _async_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="backfill_wallet_history")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--wallet",
        action="append",
        help="Endereço da wallet (0x...). Pode repetir pra múltiplas.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Backfill todas as wallets em tracked_wallets.",
    )
    parser.add_argument(
        "--since-days",
        type=int,
        default=365,
        help="Quantos dias pra trás buscar (default 365).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Limit por chamada à API. Se atingido, log avisa.",
    )
    ns = parser.parse_args(argv)

    if ns.all:
        wallets = await _load_all_tracked_wallets()
        if not wallets:
            print("error: tracked_wallets vazia. Rode o watcher primeiro.", file=sys.stderr)
            return 1
    else:
        wallets = ns.wallet

    since = datetime.now(tz=UTC) - timedelta(days=ns.since_days)
    print(f"backfilling {len(wallets)} wallet(s) desde {since.isoformat()} (limit={ns.limit})")

    total_fetched = 0
    total_inserted = 0
    for i, addr in enumerate(wallets, 1):
        try:
            fetched, inserted = await _backfill_wallet(
                wallet_address=addr,
                since=since,
                limit=ns.limit,
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[{i}/{len(wallets)}] {addr}: ERROR {type(exc).__name__}: {exc}", file=sys.stderr
            )
            continue
        total_fetched += fetched
        total_inserted += inserted
        warn = " (limit atingido — pode ter mais histórico)" if fetched >= ns.limit else ""
        print(f"[{i}/{len(wallets)}] {addr}: fetched={fetched} inserted={inserted}{warn}")

    print(f"done. total_fetched={total_fetched} total_inserted={total_inserted}")
    return 0


def main() -> None:
    sys.exit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
