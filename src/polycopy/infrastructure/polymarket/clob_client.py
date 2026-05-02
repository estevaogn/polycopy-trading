"""PolymarketClobClient: REST adapter do CLOB (orderbook).

Endpoint base: https://clob.polymarket.com
Retry: exponencial em 5xx + 429 + transport errors; não retenta em outros 4xx.
Sempre fresh — sem cache.
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
    RetryError,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.market import OrderBook, OrderBookLevel
from polycopy.domain.value_objects import Money, Price, TokenId
from polycopy.infrastructure.observability.metrics import Metrics
from polycopy.infrastructure.polymarket.gamma_client import (
    PolymarketUnavailableError,
    _is_retryable,
)


class PolymarketClobClient:
    """Cliente REST do CLOB. Implementa `PolymarketClobPort`."""

    def __init__(
        self,
        *,
        base_url: str,
        metrics: Metrics,
        timeout_s: float = 5.0,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._metrics = metrics
        self._timeout_s = timeout_s
        self._max_retries = max_retries

    async def get_book(self, token_id: TokenId) -> OrderBook:
        """Retorna o snapshot do orderbook para o token_id informado."""

        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.get(
                    f"{self._base_url}/book", params={"token_id": token_id.value}
                )
                response.raise_for_status()
                return response

        start = time.perf_counter()
        status: str = "error"
        try:
            response = await self._with_retry(_do)
            status = str(response.status_code)
        except RetryError as exc:
            status = "error"
            self._metrics.polymarket_http_requests_total.labels(
                client="clob", endpoint="book", status=status
            ).inc()
            raise PolymarketUnavailableError(
                f"CLOB /book unavailable after retries: {exc.last_attempt.exception()}"
            ) from exc
        except httpx.HTTPStatusError as exc:
            status = str(exc.response.status_code)
            self._metrics.polymarket_http_requests_total.labels(
                client="clob", endpoint="book", status=status
            ).inc()
            raise PolymarketUnavailableError(f"CLOB /book HTTP {exc.response.status_code}") from exc
        finally:
            self._metrics.polymarket_http_request_duration_seconds.labels(
                client="clob", endpoint="book", status=status
            ).observe(time.perf_counter() - start)

        self._metrics.polymarket_http_requests_total.labels(
            client="clob", endpoint="book", status=status
        ).inc()
        return self._parse_book(token_id, response.json())

    async def _with_retry(self, fn: Callable[[], Awaitable[httpx.Response]]) -> httpx.Response:
        """Executa fn com retry exponencial em 5xx, 429 e transport errors."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=0.2, max=2),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                return await fn()
        raise RuntimeError("unreachable")

    @staticmethod
    def _parse_book(token_id: TokenId, payload: Any) -> OrderBook:
        """Converte payload JSON do CLOB em OrderBook do domínio.

        A API retorna `bids` em ordem crescente de preço e `asks` em ordem
        decrescente (pior primeiro em ambos os casos). O parser ordena pra que
        `bids` fiquem descendentes (melhor primeiro) e `asks` ascendentes
        (melhor primeiro), respeitando a invariante de `OrderBook`.
        """
        if not isinstance(payload, dict):
            raise PolymarketUnavailableError(
                f"CLOB /book unexpected payload type: {type(payload).__name__}"
            )

        def _levels(items: Any, descending: bool) -> list[OrderBookLevel]:
            if not isinstance(items, list):
                return []
            parsed: list[OrderBookLevel] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                price_raw = item.get("price")
                size_raw = item.get("size")
                if price_raw is None or size_raw is None:
                    continue
                parsed.append(
                    OrderBookLevel(
                        price=Price(value=Decimal(str(price_raw))),
                        size=Money.from_usdc(str(size_raw)),
                    )
                )
            parsed.sort(key=lambda lvl: lvl.price.value, reverse=descending)
            return parsed

        bids = _levels(payload.get("bids"), descending=True)
        asks = _levels(payload.get("asks"), descending=False)
        return OrderBook(token_id=token_id, bids=bids, asks=asks, captured_at=datetime.now(tz=UTC))
