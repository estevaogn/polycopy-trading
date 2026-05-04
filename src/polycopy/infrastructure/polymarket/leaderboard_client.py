"""PolymarketLeaderboardClient: httpx + tenacity + Prometheus metrics.

Endpoint: https://data-api.polymarket.com/v1/leaderboard
Retry: exponential backoff on 5xx and httpx.RequestError. No retry on 4xx.
Same shape as PolymarketDataClient (data_client.py).
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.discovery import (
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.observability.metrics import Metrics


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return isinstance(exc, httpx.RequestError)


class PolymarketLeaderboardClient:
    """Implements PolymarketLeaderboardPort."""

    def __init__(
        self,
        *,
        base_url: str,
        metrics: Metrics,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._metrics = metrics
        self._transport = transport
        self._timeout_s = timeout_s
        self._max_retries = max_retries

    async def fetch_leaderboard(
        self,
        *,
        time_period: TimePeriod,
        category: Category,
        order_by: OrderBy = OrderBy.PNL,
        limit: int = 50,
        offset: int = 0,
    ) -> list[LeaderboardEntry]:
        params: dict[str, Any] = {
            "timePeriod": time_period.value,
            "category": category.value,
            "orderBy": order_by.value,
            "limit": limit,
            "offset": offset,
        }

        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(
                timeout=self._timeout_s,
                transport=self._transport,
            ) as client:
                response = await client.get(
                    f"{self._base_url}/v1/leaderboard",
                    params=params,
                )
                response.raise_for_status()
                return response

        start = time.perf_counter()
        try:
            response = await self._with_retry(_do)
        except httpx.HTTPStatusError as exc:
            self._metrics.leaderboard_requests_total.labels(
                endpoint="leaderboard",
                status=str(exc.response.status_code),
            ).inc()
            raise
        finally:
            self._metrics.leaderboard_request_duration_seconds.labels(
                endpoint="leaderboard",
            ).observe(time.perf_counter() - start)

        self._metrics.leaderboard_requests_total.labels(
            endpoint="leaderboard",
            status=str(response.status_code),
        ).inc()

        rows = response.json()
        return [self._row_to_entry(row) for row in rows]

    async def _with_retry(
        self,
        fn: Callable[[], Awaitable[httpx.Response]],
    ) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=0.1, max=2),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                return await fn()
        raise RuntimeError("unreachable")

    @staticmethod
    def _row_to_entry(row: dict[str, Any]) -> LeaderboardEntry:
        rank_raw = row["rank"]
        rank = int(rank_raw) if not isinstance(rank_raw, int) else rank_raw
        return LeaderboardEntry(
            rank=rank,
            address=WalletAddress(value=row["proxyWallet"]),
            user_name=row.get("userName"),
            volume_usdc=Decimal(str(row.get("vol", 0))),
            pnl_usdc=Decimal(str(row.get("pnl", 0))),
            verified_badge=bool(row.get("verifiedBadge", False)),
        )
