"""Protocol pra sincronizar wallets_seed.yaml ↔ tabela tracked_wallets."""

from __future__ import annotations

from typing import Protocol


class TrackedWalletRepository(Protocol):
    """Espelha o seed YAML em DB pra dashboard consultar (com labels)."""

    async def upsert(self, *, address: str, label: str) -> None:
        """Insere se ausente; atualiza label e last_synced_at se já existe."""
        ...
