"""Unit tests for PolymarketDataClient (com respx mockando httpx)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from polycopy.domain.models import Side
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.data_client import (
    PolymarketDataClient,
)

_BASE = "https://data-api.polymarket.com"
_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"


def _activity_response(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Polymarket Data API retorna array direto."""
    return rows


def _row(
    *,
    tx: str = "0x" + "cd" * 32,
    side: str = "BUY",
    price: str = "0.55",
    size_usdc: str = "10",
) -> dict[str, object]:
    # API real retorna `proxyWallet` (não `user`) e NÃO retorna `logIndex`.
    return {
        "transactionHash": tx,
        "proxyWallet": _VALID_ADDR,
        "conditionId": "0x" + "ab" * 32,
        "asset": "12345",
        "side": side,
        "price": price,
        "usdcSize": size_usdc,
        "timestamp": int(datetime(2026, 5, 1, 12, 0, tzinfo=UTC).timestamp()),
    }


@pytest.fixture
def metrics() -> object:
    return make_metrics(registry=CollectorRegistry())


@respx.mock
async def test_fetch_user_activity_returns_trades(metrics: object) -> None:
    respx.get(f"{_BASE}/activity").mock(
        return_value=httpx.Response(200, json=_activity_response([_row()]))
    )
    client = PolymarketDataClient(base_url=_BASE, metrics=metrics, timeout_s=5)  # type: ignore[arg-type]
    trades = await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
    assert len(trades) == 1
    assert trades[0].side is Side.BUY
    assert trades[0].size_usdc.amount == Decimal("10.000000")


@respx.mock
async def test_fetch_user_activity_handles_empty_list(metrics: object) -> None:
    respx.get(f"{_BASE}/activity").mock(
        return_value=httpx.Response(200, json=_activity_response([]))
    )
    client = PolymarketDataClient(base_url=_BASE, metrics=metrics, timeout_s=5)  # type: ignore[arg-type]
    trades = await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
    assert trades == []


@respx.mock
async def test_fetch_user_activity_passes_since_filter(metrics: object) -> None:
    route = respx.get(f"{_BASE}/activity").mock(
        return_value=httpx.Response(200, json=_activity_response([]))
    )
    client = PolymarketDataClient(base_url=_BASE, metrics=metrics, timeout_s=5)  # type: ignore[arg-type]
    since = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR), since=since)
    assert route.called
    request = route.calls[0].request
    assert "start" in request.url.params
    assert request.url.params["user"] == _VALID_ADDR


@respx.mock
async def test_fetch_user_activity_retries_on_5xx(metrics: object) -> None:
    route = respx.get(f"{_BASE}/activity").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(200, json=_activity_response([])),
        ]
    )
    client = PolymarketDataClient(
        base_url=_BASE,
        metrics=metrics,  # type: ignore[arg-type]
        timeout_s=5,
        max_retries=3,
    )
    await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
    assert route.call_count == 3


@respx.mock
async def test_fetch_user_activity_raises_after_max_retries(metrics: object) -> None:
    respx.get(f"{_BASE}/activity").mock(return_value=httpx.Response(503))
    client = PolymarketDataClient(
        base_url=_BASE,
        metrics=metrics,  # type: ignore[arg-type]
        timeout_s=5,
        max_retries=2,
    )
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
