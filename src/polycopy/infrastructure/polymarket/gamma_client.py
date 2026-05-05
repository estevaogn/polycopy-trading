"""PolymarketGammaClient: REST adapter da Gamma API.

Endpoint base: https://gamma-api.polymarket.com
Retry: exponencial em 5xx + transport errors; não retenta em 4xx.
"""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.market import Market
from polycopy.domain.resolution import ResolvedMarketDTO
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.observability.metrics import Metrics


class PolymarketUnavailableError(RuntimeError):
    """API Polymarket indisponível após retries (Gamma ou CLOB)."""


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return isinstance(exc, httpx.RequestError)


class PolymarketGammaClient:
    """Cliente REST da Gamma. Implementa `PolymarketGammaPort`."""

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

    async def get_market(self, token_id: TokenId) -> Market | None:
        """Retorna o Market correspondente ao token_id, ou None se não encontrado."""
        rows = await self._fetch_markets(params={"clob_token_ids": token_id.value, "limit": 1})
        for row in rows:
            for market in self._row_to_markets(row):
                if market.token_id.value == token_id.value:
                    return market
        return None

    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ) -> list[ResolvedMarketDTO]:
        """Lista markets fechados por condition_ids.

        Gamma API NÃO suporta batch confiável: comma-separated com 2+ IDs
        retorna []; array notation `[]=` é ignorado. Workaround: iteramos
        1-by-1. ~100ms por call. Para resolver (100/cycle) = ~10s.
        """
        out: list[ResolvedMarketDTO] = []
        for cond in condition_ids:
            rows = await self._fetch_markets(
                params={"closed": "true", "condition_ids": cond, "limit": limit}
            )
            for row in rows:
                dto = self._row_to_resolved_market_dto(row)
                if dto is not None:
                    out.append(dto)
        return out

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        """Retorna lista de Markets ativos ordenados por volume24hr decrescente."""
        rows = await self._fetch_markets(
            params={
                "active": "true",
                "archived": "false",
                "order": "volume24hr",
                "ascending": "false",
                "limit": limit,
            }
        )
        out: list[Market] = []
        for row in rows:
            out.extend(self._row_to_markets(row))
        return out

    async def list_markets_by_condition_ids(
        self, *, condition_ids: list[str], limit: int
    ) -> list[Market]:
        """Lista markets por condition_ids em qualquer estado (open ou closed).

        Gamma só aceita `closed=true|false`; default filtra closed=false.
        Pra cobrir ambos estados, fazemos 2 queries por condition_id.
        Itera 1-by-1 porque batch é instável (vide nota da outra method).
        """
        out: list[Market] = []
        for cond in condition_ids:
            for closed_state in ("false", "true"):
                rows = await self._fetch_markets(
                    params={
                        "condition_ids": cond,
                        "closed": closed_state,
                        "limit": limit,
                    }
                )
                for row in rows:
                    out.extend(self._row_to_markets(row))
        return out

    async def _fetch_markets(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Executa GET /markets com retry e registra métricas."""

        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.get(f"{self._base_url}/markets", params=params)
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
                client="gamma", endpoint="markets", status=status
            ).inc()
            raise PolymarketUnavailableError(
                f"Gamma /markets unavailable after retries: {exc.last_attempt.exception()}"
            ) from exc
        except httpx.HTTPStatusError as exc:
            status = str(exc.response.status_code)
            self._metrics.polymarket_http_requests_total.labels(
                client="gamma", endpoint="markets", status=status
            ).inc()
            raise PolymarketUnavailableError(
                f"Gamma /markets HTTP {exc.response.status_code}"
            ) from exc
        finally:
            self._metrics.polymarket_http_request_duration_seconds.labels(
                client="gamma", endpoint="markets", status=status
            ).observe(time.perf_counter() - start)

        self._metrics.polymarket_http_requests_total.labels(
            client="gamma", endpoint="markets", status=status
        ).inc()
        data = response.json()
        if not isinstance(data, list):
            raise PolymarketUnavailableError(
                f"Gamma /markets unexpected payload type: {type(data).__name__}"
            )
        return data

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
    def _row_to_markets(row: dict[str, Any]) -> list[Market]:
        """Converte uma linha da Gamma API em 2 objetos Market (Yes e No).

        Cada row tem 2 tokens (clobTokenIds) e 2 outcomes. Retorna lista vazia
        se o shape for inválido ou os outcomes não forem Yes/No.
        """
        token_ids_raw = row.get("clobTokenIds")
        if isinstance(token_ids_raw, str):
            token_ids_raw = json.loads(token_ids_raw)
        if not isinstance(token_ids_raw, list) or len(token_ids_raw) != 2:
            return []

        outcomes_raw = row.get("outcomes")
        if isinstance(outcomes_raw, str):
            outcomes_raw = json.loads(outcomes_raw)
        if not isinstance(outcomes_raw, list) or len(outcomes_raw) != 2:
            return []

        condition_id = ConditionId(value=str(row["conditionId"]))
        question = str(row["question"])

        slug_raw = row.get("slug")
        slug = slug_raw if isinstance(slug_raw, str) else None

        end_date_raw = row.get("endDate")
        end_date = (
            datetime.fromisoformat(end_date_raw.replace("Z", "+00:00"))
            if isinstance(end_date_raw, str)
            else None
        )

        is_active = bool(row.get("active", False))
        is_archived = bool(row.get("archived", False))

        volume_raw = row.get("volume24hr")
        volume = Money.from_usdc(str(volume_raw)) if volume_raw is not None else None

        liq_raw = row.get("liquidity")
        liq = Money.from_usdc(str(liq_raw)) if liq_raw is not None else None

        out: list[Market] = []
        for token_id_str, outcome in zip(token_ids_raw, outcomes_raw, strict=True):
            outcome_str = str(outcome).strip()
            if not outcome_str:
                continue  # outcome vazio é malformado, skip
            out.append(
                Market(
                    token_id=TokenId(value=str(token_id_str)),
                    condition_id=condition_id,
                    question=question,
                    slug=slug,
                    outcome=outcome_str,
                    end_date=end_date,
                    is_active=is_active,
                    is_archived=is_archived,
                    volume_24h_usdc=volume,
                    liquidity_usdc=liq,
                )
            )
        return out

    @staticmethod
    def _row_to_resolved_market_dto(row: dict[str, Any]) -> ResolvedMarketDTO | None:
        """Converte uma linha da Gamma API em ResolvedMarketDTO para markets fechados.

        Retorna None se campos obrigatórios (conditionId, clobTokenIds) estiverem ausentes.
        """
        condition_id = row.get("conditionId")
        if not isinstance(condition_id, str):
            return None

        token_ids_raw = row.get("clobTokenIds")
        if isinstance(token_ids_raw, str):
            token_ids_raw = json.loads(token_ids_raw)
        if not isinstance(token_ids_raw, list) or len(token_ids_raw) != 2:
            return None

        yes_token_id = str(token_ids_raw[0])
        no_token_id = str(token_ids_raw[1])

        closed = bool(row.get("closed", False))

        closed_time_raw = row.get("closedTime") or row.get("endDate")
        closed_time = (
            datetime.fromisoformat(closed_time_raw.replace("Z", "+00:00"))
            if isinstance(closed_time_raw, str)
            else None
        )

        outcome_prices_raw_val = row.get("outcomePrices", "[]")
        outcome_prices_raw = (
            outcome_prices_raw_val
            if isinstance(outcome_prices_raw_val, str)
            else json.dumps(outcome_prices_raw_val)
        )

        uma_statuses_raw_val = row.get("umaResolutionStatuses")
        uma_resolution_statuses_raw = (
            uma_statuses_raw_val
            if isinstance(uma_statuses_raw_val, str) or uma_statuses_raw_val is None
            else json.dumps(uma_statuses_raw_val)
        )

        return ResolvedMarketDTO(
            condition_id=condition_id,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            closed=closed,
            closed_time=closed_time,
            outcome_prices_raw=outcome_prices_raw,
            uma_resolution_statuses_raw=uma_resolution_statuses_raw,
        )
