"""PolymarketDataPort: contrato para consultar dados de atividade na Polymarket."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from polycopy.domain.models import Trade
from polycopy.domain.value_objects import WalletAddress


class PolymarketDataPort(Protocol):
    """Cliente da Polymarket Data API. Implementação concreta: httpx (Plano 1B)."""

    async def fetch_user_activity(
        self,
        wallet: WalletAddress,
        since: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Trade]:
        """Retorna trades da wallet, ordenados por `occurred_at` desc.

        Se `since` for passado, retorna apenas trades com `occurred_at > since`.
        `offset` permite paginação (Polymarket limita ~1000 por chamada).
        """
        ...
