from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from prometheus_client import CollectorRegistry

from polycopy.domain.discovery import Category, OrderBy, TimePeriod
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.leaderboard_client import (
    PolymarketLeaderboardClient,
)


def _client(transport: httpx.MockTransport) -> PolymarketLeaderboardClient:
    metrics = make_metrics(registry=CollectorRegistry())
    return PolymarketLeaderboardClient(
        base_url="https://data-api.polymarket.com",
        metrics=metrics,
        transport=transport,
        timeout_s=1.0,
        max_retries=3,
    )


@pytest.mark.asyncio
async def test_fetch_parses_payload() -> None:
    payload = [
        {
            "rank": "1",
            "proxyWallet": "0x" + "a" * 40,
            "userName": "alice",
            "vol": 12345.67,
            "pnl": 999.99,
            "verifiedBadge": True,
        },
        {
            "rank": "2",
            "proxyWallet": "0x" + "b" * 40,
            "userName": None,
            "vol": 0,
            "pnl": -1.23,
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/leaderboard"
        params = dict(request.url.params)
        assert params["timePeriod"] == "MONTH"
        assert params["category"] == "OVERALL"
        assert params["orderBy"] == "PNL"
        assert params["limit"] == "50"
        assert params["offset"] == "0"
        return httpx.Response(200, json=payload)

    client = _client(httpx.MockTransport(handler))
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.MONTH,
        category=Category.OVERALL,
        order_by=OrderBy.PNL,
        limit=50,
        offset=0,
    )
    assert len(rows) == 2
    assert rows[0].rank == 1
    assert rows[0].user_name == "alice"
    assert rows[0].volume_usdc == Decimal("12345.67")
    assert rows[0].pnl_usdc == Decimal("999.99")
    assert rows[0].verified_badge is True
    assert rows[1].user_name is None
    assert rows[1].verified_badge is False
    assert rows[1].pnl_usdc == Decimal("-1.23")


@pytest.mark.asyncio
async def test_retry_on_5xx_then_success() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json=[])

    client = _client(httpx.MockTransport(handler))
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.WEEK,
        category=Category.OVERALL,
    )
    assert rows == []
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_persistent_5xx_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_leaderboard(
            time_period=TimePeriod.WEEK,
            category=Category.OVERALL,
        )


@pytest.mark.asyncio
async def test_4xx_no_retry() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, text="bad")

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_leaderboard(
            time_period=TimePeriod.WEEK,
            category=Category.OVERALL,
        )
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_rank_string_or_int_both_parsed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "rank": 1,
                    "proxyWallet": "0x" + "a" * 40,
                    "userName": "x",
                    "vol": 0,
                    "pnl": 0,
                    "verifiedBadge": False,
                },
                {
                    "rank": "2",
                    "proxyWallet": "0x" + "b" * 40,
                    "userName": "y",
                    "vol": 0,
                    "pnl": 0,
                    "verifiedBadge": False,
                },
            ],
        )

    client = _client(httpx.MockTransport(handler))
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.WEEK,
        category=Category.OVERALL,
    )
    assert rows[0].rank == 1
    assert rows[1].rank == 2


@pytest.mark.asyncio
async def test_request_error_increments_counter_with_error_status() -> None:
    """Network failures (DNS/Connect) também devem aparecer no counter."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(httpx.RequestError):
        await client.fetch_leaderboard(
            time_period=TimePeriod.WEEK,
            category=Category.OVERALL,
        )
    counter = client._metrics.leaderboard_requests_total.labels(
        endpoint="leaderboard",
        status="error",
    )
    assert counter._value.get() == 1
