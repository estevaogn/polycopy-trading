"""MarketResolutionRepository: contrato de persistência pra resoluções de markets."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.resolution import MarketResolution


class MarketResolutionRepository(Protocol):
    """Persistência idempotente de resoluções. Plano 5A.

    market_resolutions é puramente append-only — `insert` retorna False
    se já existe (PK conflict). Sem UPDATEs.
    """

    async def insert(self, resolution: MarketResolution) -> bool:
        """Insere; True se nova, False se já existia (PK conflict)."""
        ...

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        """Retorna até `limit` condition_ids únicos de wallet_trades que NÃO
        estão em market_resolutions ainda.

        Query: LEFT JOIN wallet_trades vs market_resolutions
                WHERE market_resolutions.condition_id IS NULL.
        """
        ...
