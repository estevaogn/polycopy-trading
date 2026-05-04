"""PolymarketGammaPort: contrato pra consultar metadata de mercados via Gamma."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.market import Market
from polycopy.domain.resolution import ResolvedMarketDTO
from polycopy.domain.value_objects import TokenId


class PolymarketGammaPort(Protocol):
    """Cliente da Polymarket Gamma REST API. Implementação concreta: httpx (Plano 2A)."""

    async def get_market(self, token_id: TokenId) -> Market | None:
        """Retorna `Market` correspondente ao token, ou None se não existir.

        Levanta `PolymarketUnavailableError` após N retries.
        """
        ...

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        """Retorna até `limit` mercados ativos, ordenados por volume 24h desc.

        Apenas mercados com `is_active=True` e `is_archived=False`.
        Levanta `PolymarketUnavailableError` após N retries.
        """
        ...

    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ) -> list[ResolvedMarketDTO]:
        """Lista markets COM filtro `closed=true` e `condition_ids`.

        Retorna até `limit` markets fechados. DTO carrega campos brutos
        (outcome_prices_raw, uma_resolution_statuses_raw, closed_time)
        necessários pra classificação no ResolverAgent.
        """
        ...
