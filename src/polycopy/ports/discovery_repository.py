"""Protocol pra persistir runs do discover_wallets CLI no DB."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.discovery import CandidateWallet, ReportMetadata


class DiscoveryRepository(Protocol):
    """Persiste cada execução do CLI + candidates pra consumo do dashboard."""

    async def insert_run(
        self,
        metadata: ReportMetadata,
        candidates: list[CandidateWallet],
    ) -> int:
        """Insere run + candidates atomicamente; retorna run_id."""
        ...
