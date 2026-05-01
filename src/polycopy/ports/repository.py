"""WalletTradeRepository: contrato para persistência de trades detectados."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from polycopy.domain.models import Trade
from polycopy.domain.value_objects import WalletAddress


class WalletTradeRepository(Protocol):
    """Persistência de trades. Implementação concreta: SQLAlchemy + Postgres (Plano 1B)."""

    async def insert_if_absent(self, trade: Trade) -> bool:
        """Insere trade. Retorna True se inseriu, False se já existia.

        Dedup por (tx_hash, log_index).
        """
        ...

    async def latest_occurred_at(self, wallet: WalletAddress) -> datetime | None:
        """Retorna `occurred_at` do trade mais recente da wallet, ou None."""
        ...
