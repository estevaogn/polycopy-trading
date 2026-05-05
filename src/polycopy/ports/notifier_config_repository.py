"""Protocol pra ler/escrever config dinâmica do notifier (hot reload via DB)."""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol


class NotifierConfigRepository(Protocol):
    """KV simples; hoje só `min_size_usdc`. Estende sem migration adicionando keys."""

    async def get_min_size_usdc(self) -> Decimal:
        """Retorna o threshold atual. Default 0 se key ausente."""
        ...

    async def set_min_size_usdc(self, value: Decimal, *, updated_by: str) -> None:
        """Upsert do threshold. updated_by registra quem mudou (audit trivial)."""
        ...
