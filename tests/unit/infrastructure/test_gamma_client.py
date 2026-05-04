"""Testes unit do PolymarketGammaClient com respx mocks."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from polycopy.domain.resolution import ResolvedMarketDTO
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


@respx.mock
async def test_list_markets_by_condition_ids_closed_parses_settled() -> None:
    """Parse de market settled YES — outcomePrices ['1.0', '0.0']."""
    payload = [
        {
            "conditionId": "0x" + "ab" * 32,
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["Yes", "No"]',
            "closed": True,
            "closedTime": "2026-04-01T12:00:00Z",
            "outcomePrices": '["1.0", "0.0"]',
            "umaResolutionStatuses": '["resolved"]',
        }
    ]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    dtos = await client.list_markets_by_condition_ids_closed(
        condition_ids=["0x" + "ab" * 32], limit=10
    )

    assert len(dtos) == 1
    dto = dtos[0]
    assert isinstance(dto, ResolvedMarketDTO)
    assert dto.closed is True
    assert dto.condition_id == "0x" + "ab" * 32
    assert dto.yes_token_id == "111"
    assert dto.no_token_id == "222"
    assert dto.outcome_prices_raw == '["1.0", "0.0"]'
    assert dto.uma_resolution_statuses_raw == '["resolved"]'


@respx.mock
async def test_list_markets_by_condition_ids_closed_passes_correct_params() -> None:
    """Confirma que params da request batem: condition_ids + closed=true."""
    captured: list[httpx.Request] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=[])

    respx.get("https://gamma-api.polymarket.com/markets").mock(side_effect=_capture)

    client = _make_client()
    await client.list_markets_by_condition_ids_closed(
        condition_ids=["0x" + "ab" * 32, "0x" + "cd" * 32], limit=50
    )

    assert len(captured) == 1
    params = dict(captured[0].url.params)
    assert params["closed"] == "true"
    assert "condition_ids" in params
    assert params["limit"] == "50"


@respx.mock
async def test_list_markets_by_condition_ids_closed_handles_missing_uma() -> None:
    """umaResolutionStatuses ausente — DTO recebe None."""
    payload = [
        {
            "conditionId": "0x" + "ab" * 32,
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["Yes", "No"]',
            "closed": True,
            "closedTime": None,
            "outcomePrices": '["0.5", "0.5"]',
            # sem umaResolutionStatuses
        }
    ]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    dtos = await client.list_markets_by_condition_ids_closed(
        condition_ids=["0x" + "ab" * 32], limit=10
    )

    assert len(dtos) == 1
    assert dtos[0].uma_resolution_statuses_raw is None
    assert dtos[0].closed_time is None
