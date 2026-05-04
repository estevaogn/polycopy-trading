"""Live integration test against Polymarket /v1/leaderboard.

Gated by env var to avoid CI dependency on third-party uptime.
Run locally with:

    PYTEST_LIVE_POLYMARKET=1 uv run pytest tests/integration/test_leaderboard_live.py -v
"""

from __future__ import annotations

import os

import pytest
from prometheus_client import CollectorRegistry

from polycopy.domain.discovery import Category, OrderBy, TimePeriod
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.leaderboard_client import (
    PolymarketLeaderboardClient,
)


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("PYTEST_LIVE_POLYMARKET") != "1",
    reason="set PYTEST_LIVE_POLYMARKET=1 to run live integration",
)
async def test_leaderboard_live_smoke() -> None:
    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketLeaderboardClient(
        base_url="https://data-api.polymarket.com",
        metrics=metrics,
        timeout_s=15.0,
    )
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.MONTH,
        category=Category.OVERALL,
        order_by=OrderBy.PNL,
        limit=5,
        offset=0,
    )
    assert len(rows) <= 5
    if rows:
        first = rows[0]
        assert first.address.value.startswith("0x")
        assert len(first.address.value) == 42
        assert first.rank >= 1
