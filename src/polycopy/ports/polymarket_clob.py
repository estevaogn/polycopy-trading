"""PolymarketClobPort: contrato pra consultar orderbook do CLOB Polymarket."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.market import OrderBook
from polycopy.domain.value_objects import TokenId


class PolymarketClobPort(Protocol):
    """Cliente da Polymarket CLOB REST API. Implementação concreta: httpx (Plano 2A)."""

    async def get_book(self, token_id: TokenId) -> OrderBook:
        """Retorna snapshot do orderbook do token.

        Sempre fresh; sem cache. Levanta `PolymarketUnavailableError` após N retries.
        """
        ...
