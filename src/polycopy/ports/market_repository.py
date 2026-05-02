"""MarketRepository: contrato de persistência pra cache de Market."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from polycopy.domain.market import Market
from polycopy.domain.value_objects import TokenId


class CachedMarket(Protocol):
    """Resultado de leitura do cache. Encapsula market + freshness."""

    market: Market
    last_synced_at: datetime
    is_stale: bool


class MarketRepository(Protocol):
    """Cache read-through pra metadata de mercados. Plano 2A."""

    async def upsert_many(self, markets: list[Market]) -> int:
        """Insere/atualiza muitos mercados em batch. Retorna número de linhas afetadas."""
        ...

    async def get_market(self, token_id: TokenId) -> CachedMarket | None:
        """Retorna cached market do DB ou None se ausente.

        NÃO faz fetch externo. Caller decide se aceita stale ou refaz fetch via Gamma.
        Use `is_stale` (computado contra TTL) pra decidir.
        """
        ...
