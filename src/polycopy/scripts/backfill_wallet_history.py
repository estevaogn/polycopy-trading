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


async def _backfill_one(
    *,
    wallet_address: str,
    since: datetime,
    page_size: int,
    max_pages: int,
    client: object,  # PolymarketDataClient — protocolo cumprido
    session_factory: object,
) -> tuple[int, int, int, str | None]:
    """Backfill 1 wallet paginando offset. Retorna (fetched, inserted, pages, hit_cap_msg)."""
    import httpx

    from polycopy.domain.value_objects import WalletAddress
    from polycopy.infrastructure.persistence.wallet_trade_repository import (
        SqlAlchemyWalletTradeRepository,
    )

    addr = WalletAddress(value=wallet_address)
    fetched = 0
    inserted = 0
    pages = 0
    hit_cap: str | None = None
    async with session_factory() as session:  # type: ignore[operator]
        repo = SqlAlchemyWalletTradeRepository(session)
        offset = 0
        while pages < max_pages:
            try:
                trades = await client.fetch_user_activity(  # type: ignore[attr-defined]
                    addr, since=since, limit=page_size, offset=offset
                )
            except httpx.HTTPStatusError as exc:
                # Polymarket retorna 400 quando offset excede limite interno (~3500).
                # Commitamos o que já pegamos e seguimos.
                if exc.response.status_code == 400:
                    hit_cap = f"API offset cap em offset={offset} (HTTP 400)"
                    break
                raise
            page_fetched = len(trades)
            if page_fetched == 0:
                break
            for trade in trades:
                if await repo.insert_if_absent(trade):
                    inserted += 1
            fetched += page_fetched
            pages += 1
            if page_fetched < page_size:
                break  # última página
            offset += page_size
        await session.commit()
    return fetched, inserted, pages, hit_cap


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
    from prometheus_client import CollectorRegistry

    from polycopy.config import Settings
    from polycopy.infrastructure.observability.metrics import make_metrics
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.polymarket.data_client import PolymarketDataClient

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
        "--page-size",
        type=int,
        default=500,
        help="Trades por página (Polymarket capa ~1000). Default 500.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Máximo de páginas por wallet (safety vs loops infinitos). Default 20 = ~10k trades.",
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
    print(
        f"backfilling {len(wallets)} wallet(s) desde {since.isoformat()} "
        f"(page_size={ns.page_size} max_pages={ns.max_pages})"
    )

    settings = Settings()
    engine = make_engine(settings)
    try:
        # Registry isolado pra evitar colisão com qualquer global REGISTRY pré-existente.
        metrics = make_metrics(registry=CollectorRegistry())
        client = PolymarketDataClient(
            base_url=settings.polymarket_base_url,
            metrics=metrics,
            timeout_s=30.0,
        )
        session_factory = make_session_factory(engine)

        total_fetched = 0
        total_inserted = 0
        for i, addr in enumerate(wallets, 1):
            try:
                fetched, inserted, pages, hit_cap = await _backfill_one(
                    wallet_address=addr,
                    since=since,
                    page_size=ns.page_size,
                    max_pages=ns.max_pages,
                    client=client,
                    session_factory=session_factory,
                )
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[{i}/{len(wallets)}] {addr}: ERROR {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                continue
            total_fetched += fetched
            total_inserted += inserted
            warn = ""
            if hit_cap is not None:
                warn = f" ({hit_cap})"
            elif pages >= ns.max_pages:
                warn = " (max_pages atingido — pode ter mais histórico)"
            print(
                f"[{i}/{len(wallets)}] {addr}: fetched={fetched} inserted={inserted} "
                f"pages={pages}{warn}"
            )

        print(f"done. total_fetched={total_fetched} total_inserted={total_inserted}")
        return 0
    finally:
        await engine.dispose()


def main() -> None:
    sys.exit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
