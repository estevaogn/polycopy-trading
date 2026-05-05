"""set_notifier_filter — atualiza o threshold do filtro K1 do notifier.

Hot reload: o NotifierAgent pollla a tabela `notifier_config` a cada 30s
e aplica o novo valor sem restart.

Usage:
    uv run python -m polycopy.scripts.set_notifier_filter --min-size 75
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from decimal import Decimal


async def _set(value: Decimal, updated_by: str) -> None:
    from polycopy.config import Settings
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.persistence.notifier_config_repository import (
        SqlAlchemyNotifierConfigRepository,
    )

    settings = Settings()
    engine = make_engine(settings)
    try:
        session_factory = make_session_factory(engine)
        async with session_factory() as session:
            repo = SqlAlchemyNotifierConfigRepository(session)
            old = await repo.get_min_size_usdc()
            await repo.set_min_size_usdc(value, updated_by=updated_by)
            await session.commit()
        print(f"min_size_usdc: {old} -> {value} (updated_by={updated_by})")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(prog="set_notifier_filter")
    parser.add_argument(
        "--min-size",
        type=Decimal,
        required=True,
        help="Novo threshold em USDC (>=0; 0 desativa filtro).",
    )
    parser.add_argument(
        "--by",
        default=os.environ.get("USER", "cli"),
        help="Identificador de quem mudou (default: $USER).",
    )
    ns = parser.parse_args()

    if ns.min_size < 0:
        print("error: --min-size must be >= 0", file=sys.stderr)
        sys.exit(1)

    asyncio.run(_set(ns.min_size, ns.by))


if __name__ == "__main__":
    main()
