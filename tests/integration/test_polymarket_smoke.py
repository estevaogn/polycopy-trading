"""Smoke test opt-in contra Gamma e CLOB reais.

Rodar com:
    PYTEST_LIVE=1 uv run pytest tests/integration/test_polymarket_smoke.py -v

Exige internet. Pula automaticamente se PYTEST_LIVE != "1".
"""

from __future__ import annotations

import os

import pytest
from prometheus_client import CollectorRegistry

from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.clob_client import PolymarketClobClient
from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("PYTEST_LIVE") != "1",
        reason="set PYTEST_LIVE=1 to run real-network smoke tests",
    ),
]


async def test_gamma_list_active_markets_returns_data() -> None:
    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    markets = await client.list_active_markets(limit=2)
    assert len(markets) >= 1
    m = markets[0]
    assert m.token_id.value
    assert m.outcome in {"Yes", "No"}


async def test_clob_get_book_returns_data() -> None:
    # Pega um token id ativo via Gamma
    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    markets = await gamma.list_active_markets(limit=1)
    assert markets, "no active markets - Gamma returned empty?"
    token_id = markets[0].token_id

    clob = PolymarketClobClient(base_url="https://clob.polymarket.com", metrics=metrics)
    book = await clob.get_book(token_id)
    # Pode haver mercados sem profundidade - mas o book deve parsear sem explodir.
    assert book.token_id.value == token_id.value
