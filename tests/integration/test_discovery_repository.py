"""Integration tests do SqlAlchemyDiscoveryRepository — exige Postgres up."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.discovery import (
    CandidateWallet,
    Category,
    OrderBy,
    ReportMetadata,
    TimePeriod,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.persistence.discovery_repository import (
    SqlAlchemyDiscoveryRepository,
)
from polycopy.ports import DiscoveryRepository

pytestmark = pytest.mark.integration


def _metadata(**overrides: object) -> ReportMetadata:
    base: dict[str, object] = {
        "generated_at": datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
        "time_period": TimePeriod.MONTH,
        "category": Category.OVERALL,
        "order_by": OrderBy.PNL,
        "min_volume_usdc": Decimal("5000"),
        "top_requested": 50,
        "seed_path": "config/wallets_seed.yaml",
        "seed_size": 26,
        "total_fetched": 100,
        "total_excluded_existing": 26,
        "total_excluded_min_volume": 22,
        "total_candidates": 52,
    }
    base.update(overrides)
    return ReportMetadata(**base)  # type: ignore[arg-type]


def _candidate(
    rank: int, addr_hex: str, *, label: str = "user", verified: bool = False
) -> CandidateWallet:
    return CandidateWallet(
        address=WalletAddress(value="0x" + addr_hex),
        label=label,
        rank=rank,
        volume_usdc=Decimal("12345.67"),
        pnl_usdc=Decimal("999.99"),
        verified_badge=verified,
    )


async def test_insert_run_persists_metadata_and_candidates(db_session: AsyncSession) -> None:
    """Insere 1 run + 3 candidates, valida row counts + relação FK."""
    await db_session.execute(text("TRUNCATE discovery_runs, discovery_candidates CASCADE"))

    repo = SqlAlchemyDiscoveryRepository(db_session)
    candidates = [
        _candidate(1, "a" * 40, label="alice"),
        _candidate(2, "b" * 40, label="bob", verified=True),
        _candidate(3, "c" * 40),
    ]
    run_id = await repo.insert_run(_metadata(), candidates)
    assert run_id > 0

    runs = (await db_session.execute(text("SELECT count(*) FROM discovery_runs"))).scalar_one()
    cands = (
        await db_session.execute(text("SELECT count(*) FROM discovery_candidates"))
    ).scalar_one()
    assert runs == 1
    assert cands == 3


async def test_insert_run_with_empty_candidates(db_session: AsyncSession) -> None:
    """Edge case: run sem candidates ainda persiste a row do run (metadata)."""
    await db_session.execute(text("TRUNCATE discovery_runs, discovery_candidates CASCADE"))

    repo = SqlAlchemyDiscoveryRepository(db_session)
    run_id = await repo.insert_run(_metadata(total_candidates=0), [])
    assert run_id > 0

    cands = (
        await db_session.execute(text("SELECT count(*) FROM discovery_candidates"))
    ).scalar_one()
    assert cands == 0


async def test_cascade_delete_removes_candidates(db_session: AsyncSession) -> None:
    """ON DELETE CASCADE: deletar run apaga candidates."""
    await db_session.execute(text("TRUNCATE discovery_runs, discovery_candidates CASCADE"))

    repo = SqlAlchemyDiscoveryRepository(db_session)
    run_id = await repo.insert_run(_metadata(), [_candidate(1, "a" * 40)])

    await db_session.execute(text("DELETE FROM discovery_runs WHERE id = :id"), {"id": run_id})
    cands = (
        await db_session.execute(text("SELECT count(*) FROM discovery_candidates"))
    ).scalar_one()
    assert cands == 0


async def test_adapter_satisfies_protocol(db_session: AsyncSession) -> None:
    """Mypy garante que SqlAlchemyDiscoveryRepository satisfaz Protocol."""
    _: DiscoveryRepository = SqlAlchemyDiscoveryRepository(db_session)
