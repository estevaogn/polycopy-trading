"""PolymarketDataClient: httpx + tenacity + métricas Prometheus.

Endpoint: https://data-api.polymarket.com/activity
Retry: exponential backoff em 5xx; não retenta em 4xx.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.observability.metrics import Metrics


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return isinstance(exc, httpx.RequestError)


class PolymarketDataClient:
    """Cliente da Polymarket Data API. Implementa `PolymarketDataPort`."""

    def __init__(
        self,
        *,
        base_url: str,
        metrics: Metrics,
        timeout_s: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._metrics = metrics
        self._timeout_s = timeout_s
        self._max_retries = max_retries

    async def fetch_user_activity(
        self,
        wallet: WalletAddress,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[Trade]:
        params: dict[str, Any] = {"user": wallet.value, "limit": limit}
        if since is not None:
            params["start"] = int(since.timestamp())

        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.get(f"{self._base_url}/activity", params=params)
                response.raise_for_status()
                return response

        start = time.perf_counter()
        try:
            response = await self._with_retry(_do)
        finally:
            self._metrics.polymarket_request_duration_seconds.labels(endpoint="activity").observe(
                time.perf_counter() - start
            )

        self._metrics.polymarket_requests_total.labels(
            endpoint="activity", status=str(response.status_code)
        ).inc()

        # Polymarket Data API retorna array direto (não envelopado em {"data": ...}).
        rows = response.json()
        return [self._row_to_trade(row) for row in rows]

    async def _with_retry(self, fn: Callable[[], Awaitable[httpx.Response]]) -> httpx.Response:
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
    def _row_to_trade(row: dict[str, Any]) -> Trade:
        # Endereço da wallet vem em `proxyWallet` (não `user`, que é só o query param).
        # `logIndex` não é exposto pela API; default 0 é seguro porque /activity
        # retorna 1 linha por transactionHash, então (tx_hash, 0) é único na prática.
        return Trade(
            tx_hash=row["transactionHash"],
            log_index=int(row.get("logIndex", 0)),
            wallet=WalletAddress(value=row["proxyWallet"]),
            condition_id=ConditionId(value=row["conditionId"]),
            token_id=TokenId(value=str(row["asset"])),
            side=Side(row["side"]),
            price=Price(value=Decimal(str(row["price"]))),
            size_usdc=Money.from_usdc(str(row["usdcSize"])),
            occurred_at=datetime.fromtimestamp(int(row["timestamp"]), tz=UTC),
        )
