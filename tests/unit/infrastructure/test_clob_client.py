"""Testes unit do PolymarketClobClient com respx mocks."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from polycopy.domain.value_objects import TokenId
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.clob_client import PolymarketClobClient
from polycopy.infrastructure.polymarket.gamma_client import PolymarketUnavailableError

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "polymarket"
_CLOB_FIXTURE = _FIXTURES / "clob_book.json"


def _clob_response_json() -> dict:
    return json.loads(_CLOB_FIXTURE.read_text())


def _make_client() -> PolymarketClobClient:
    metrics = make_metrics(registry=CollectorRegistry())
    return PolymarketClobClient(
        base_url="https://clob.polymarket.com",
        metrics=metrics,
        max_retries=3,
    )


@respx.mock
async def test_get_book_parses_fixture() -> None:
    payload = _clob_response_json()
    respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    book = await client.get_book(TokenId(value="42"))

    # Garante que parser não retornou listas vazias por bug silencioso.
    assert len(book.bids) > 0
    assert len(book.asks) > 0
    # Ordering deve respeitar invariantes do OrderBook após reordenação do parser.
    assert all(
        book.bids[i].price.value <= book.bids[i - 1].price.value for i in range(1, len(book.bids))
    )
    assert all(
        book.asks[i].price.value >= book.asks[i - 1].price.value for i in range(1, len(book.asks))
    )


@respx.mock
async def test_retry_on_5xx() -> None:
    payload = _clob_response_json()
    route = respx.get("https://clob.polymarket.com/book")
    route.side_effect = [
        httpx.Response(502),
        httpx.Response(200, json=payload),
    ]

    client = _make_client()
    book = await client.get_book(TokenId(value="42"))
    assert book is not None
    assert route.call_count == 2


@respx.mock
async def test_retry_exhausted_raises_unavailable() -> None:
    respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(503),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.get_book(TokenId(value="42"))


@respx.mock
async def test_4xx_does_not_retry() -> None:
    route = respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(400),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.get_book(TokenId(value="42"))
    assert route.call_count == 1


@respx.mock
async def test_metrics_recorded() -> None:
    payload = _clob_response_json()
    respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketClobClient(
        base_url="https://clob.polymarket.com",
        metrics=metrics,
    )
    await client.get_book(TokenId(value="42"))

    counter = metrics.polymarket_http_requests_total.labels(
        client="clob", endpoint="book", status="200"
    )
    assert counter._value.get() == 1


@respx.mock
async def test_retry_on_429() -> None:
    """Rate limit (429) é retentado igual a 5xx."""
    payload = _clob_response_json()
    route = respx.get("https://clob.polymarket.com/book")
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(200, json=payload),
    ]

    client = _make_client()
    book = await client.get_book(TokenId(value="42"))
    assert book is not None
    assert route.call_count == 2
