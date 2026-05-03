"""Smoke opt-in contra Polygon mainnet — read-only, nunca submete order.

Rodar com:
    PYTEST_LIVE_POLYGON=1 uv run pytest tests/integration/test_polymarket_smoke_executor.py -v

Exige:
    - WALLET_PRIVATE_KEY configurado em .env
    - POLYGON_RPC_URL Alchemy válido
    - setup_wallet rodado (allowance > 0)

Pula automaticamente se PYTEST_LIVE_POLYGON != "1".
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest

from polycopy.config import Settings
from polycopy.infrastructure.execution.web3_clob_executor import (
    build_clob_client,
    verify_allowance,
)

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("PYTEST_LIVE_POLYGON") != "1",
        reason="set PYTEST_LIVE_POLYGON=1 to run live Polygon tests (requires wallet + RPC)",
    ),
]


def test_clob_client_can_authenticate() -> None:
    """Confirma que CLOB API responde a L1 authentication via Alchemy.

    NÃO submete order — apenas verifica auth + lê markets.
    """
    settings = Settings()  # type: ignore[call-arg]
    if settings.wallet_private_key is None:
        pytest.skip("WALLET_PRIVATE_KEY not set — cannot test auth")

    client = build_clob_client(settings)
    # Smoke: ler 1 market — confirma client OK + auth OK
    markets = client.get_markets(next_cursor="")
    assert markets is not None
    assert "data" in markets or "limit_orders" in markets or len(markets) > 0


async def test_wallet_has_funds_and_allowance() -> None:
    """Verifica allowance >= $1 USDC.

    Falha rápido se setup_wallet não rodou.
    """
    settings = Settings()  # type: ignore[call-arg]
    if settings.wallet_private_key is None:
        pytest.skip("WALLET_PRIVATE_KEY not set")

    # Não raise = passou
    await verify_allowance(settings, Decimal("1"))
