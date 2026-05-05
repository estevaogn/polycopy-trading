"""SqlAlchemyDiscoveryRepository: persiste runs do discover_wallets + candidates."""

from __future__ import annotations

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.discovery import CandidateWallet, ReportMetadata
from polycopy.infrastructure.persistence.models import (
    DiscoveryCandidateRow,
    DiscoveryRunRow,
)


class SqlAlchemyDiscoveryRepository:
    """Implementa DiscoveryRepository — persiste 1 run + N candidates atomicamente."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_run(
        self,
        metadata: ReportMetadata,
        candidates: list[CandidateWallet],
    ) -> int:
        """Insere run + candidates na mesma transação. Retorna run_id."""
        result = await self._session.execute(
            insert(DiscoveryRunRow)
            .values(
                generated_at=metadata.generated_at,
                time_period=metadata.time_period.value,
                category=metadata.category.value,
                order_by=metadata.order_by.value,
                top_requested=metadata.top_requested,
                min_volume_usdc=metadata.min_volume_usdc,
                seed_path=metadata.seed_path,
                seed_size=metadata.seed_size,
                total_fetched=metadata.total_fetched,
                total_excluded_existing=metadata.total_excluded_existing,
                total_excluded_min_volume=metadata.total_excluded_min_volume,
                total_candidates=metadata.total_candidates,
            )
            .returning(DiscoveryRunRow.id)
        )
        run_id = int(result.scalar_one())

        if candidates:
            await self._session.execute(
                insert(DiscoveryCandidateRow),
                [
                    {
                        "run_id": run_id,
                        "rank": c.rank,
                        "address": c.address.value,
                        "label": c.label,
                        "volume_usdc": c.volume_usdc,
                        "pnl_usdc": c.pnl_usdc,
                        "verified_badge": c.verified_badge,
                    }
                    for c in candidates
                ],
            )

        await self._session.flush()
        return run_id
