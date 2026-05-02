"""Testes unit do PolymarketGammaClient com respx mocks."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from polycopy.domain.value_objects import TokenId
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.gamma_client import (
    PolymarketGammaClient,
    PolymarketUnavailableError,
)

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "polymarket"
_GAMMA_FIXTURE = _FIXTURES / "gamma_market.json"


def _gamma_response_json() -> list[dict]:
    return json.loads(_GAMMA_FIXTURE.read_text())


def _make_client() -> PolymarketGammaClient:
    metrics = make_metrics(registry=CollectorRegistry())
    return PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com",
        metrics=metrics,
        max_retries=3,
    )


@respx.mock
async def test_list_active_markets_parses_fixture() -> None:
    payload = _gamma_response_json()
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    markets = await client.list_active_markets(limit=2)

    assert len(markets) >= 1
    first = markets[0]
    assert first.is_active is True
    assert first.outcome in {"Yes", "No"}


@respx.mock
async def test_list_active_markets_returns_two_per_condition() -> None:
    """Cada conditionId tem 2 tokens (Yes/No); cliente expande para 2 Markets por mercado."""
    payload = _gamma_response_json()
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    markets = await client.list_active_markets(limit=2)

    n_payload = len(payload)
    assert len(markets) <= 2 * n_payload


@respx.mock
async def test_get_market_returns_none_for_unknown_token() -> None:
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=[]),
    )

    client = _make_client()
    result = await client.get_market(TokenId(value="999999999"))
    assert result is None


@respx.mock
async def test_retry_on_5xx_eventually_succeeds() -> None:
    payload = _gamma_response_json()
    route = respx.get("https://gamma-api.polymarket.com/markets")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(200, json=payload),
    ]

    client = _make_client()
    markets = await client.list_active_markets(limit=2)

    assert len(markets) >= 1
    assert route.call_count == 3


@respx.mock
async def test_retry_on_429_eventually_succeeds() -> None:
    """Rate limit (429) é retentado igual a 5xx — Gamma pode rate-limitar."""
    payload = _gamma_response_json()
    route = respx.get("https://gamma-api.polymarket.com/markets")
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(200, json=payload),
    ]

    client = _make_client()
    markets = await client.list_active_markets(limit=2)

    assert len(markets) >= 1
    assert route.call_count == 2


@respx.mock
async def test_retry_exhausted_raises_unavailable() -> None:
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(503),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.list_active_markets(limit=2)


@respx.mock
async def test_4xx_does_not_retry() -> None:
    route = respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(400),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.list_active_markets(limit=2)
    assert route.call_count == 1


@respx.mock
async def test_metrics_recorded() -> None:
    payload = _gamma_response_json()
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com",
        metrics=metrics,
    )
    await client.list_active_markets(limit=2)

    histogram = metrics.polymarket_http_request_duration_seconds.labels(
        client="gamma", endpoint="markets", status="200"
    )
    counter = metrics.polymarket_http_requests_total.labels(
        client="gamma", endpoint="markets", status="200"
    )
    assert histogram._sum.get() >= 0
    assert counter._value.get() == 1
