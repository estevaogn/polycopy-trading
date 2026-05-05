"""backfill_markets_from_trades — popula markets com condition_ids visto em trades.

MarketdataAgent só sincroniza top-N (default 200) ativos. Wallets monitoradas
operam em qualquer mercado, incluindo small-cap fora do top-N. Este script
fecha o gap: pra cada condition_id em wallet_trades ausente em markets,
chama Gamma e insere.

Idempotente via upsert (PK = token_id). Pode re-rodar.

Usage:
    uv run python -m polycopy.scripts.backfill_markets_from_trades [--batch 50] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import sys


async def _async_main(argv: list[str] | None = None) -> int:
    from prometheus_client import CollectorRegistry
    from sqlalchemy import text

    from polycopy.config import Settings
    from polycopy.infrastructure.observability.metrics import make_metrics
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.persistence.market_repository import (
        SqlAlchemyMarketRepository,
    )
    from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

    parser = argparse.ArgumentParser(prog="backfill_markets_from_trades")
    parser.add_argument(
        "--batch",
        type=int,
        default=50,
        help="Condition IDs por chamada Gamma (default 50, max ~100).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista o que faria, sem chamar Gamma nem escrever.",
    )
    ns = parser.parse_args(argv)

    settings = Settings()
    engine = make_engine(settings)
    try:
        session_factory = make_session_factory(engine)

        # Descobre conditions ausentes em markets.
        async with session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT DISTINCT wt.condition_id FROM wallet_trades wt "
                    "LEFT JOIN markets m ON m.condition_id = wt.condition_id "
                    "WHERE m.condition_id IS NULL"
                )
            )
            missing = [row[0] for row in result.all()]

        if not missing:
            print("nada a fazer: todos condition_ids de wallet_trades já em markets.")
            return 0

        print(f"{len(missing)} condition_ids ausentes em markets.")
        if ns.dry_run:
            print("--dry-run: não chamo Gamma. Exemplos:")
            for c in missing[:5]:
                print(f"  {c}")
            return 0

        metrics = make_metrics(registry=CollectorRegistry())
        gamma = PolymarketGammaClient(
            base_url=settings.gamma_api_base_url,
            metrics=metrics,
            timeout_s=15.0,
        )

        total_fetched = 0
        total_upserted = 0
        for i in range(0, len(missing), ns.batch):
            chunk = missing[i : i + ns.batch]
            try:
                markets = await gamma.list_markets_by_condition_ids(
                    condition_ids=chunk, limit=ns.batch * 2
                )
            except Exception as exc:  # noqa: BLE001
                print(
                    f"  batch {i // ns.batch + 1} ({len(chunk)} ids): "
                    f"ERROR {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                continue

            total_fetched += len(markets)
            if not markets:
                continue

            async with session_factory() as session:
                repo = SqlAlchemyMarketRepository(session, ttl_seconds=86400)
                upserted = await repo.upsert_many(markets)
                await session.commit()
                total_upserted += upserted

            print(
                f"  batch {i // ns.batch + 1}: chunk={len(chunk)} "
                f"fetched={len(markets)} upserted={upserted}"
            )

        print(f"done. total_fetched={total_fetched} total_upserted={total_upserted}")
        return 0
    finally:
        await engine.dispose()


def main() -> None:
    sys.exit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
