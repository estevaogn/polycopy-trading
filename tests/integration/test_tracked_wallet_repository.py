"""Integration tests do SqlAlchemyTrackedWalletRepository."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.infrastructure.persistence.tracked_wallet_repository import (
    SqlAlchemyTrackedWalletRepository,
)
from polycopy.ports import TrackedWalletRepository

pytestmark = pytest.mark.integration


async def test_upsert_inserts_new_wallet(db_session: AsyncSession) -> None:
    await db_session.execute(text("TRUNCATE tracked_wallets"))
    repo = SqlAlchemyTrackedWalletRepository(db_session)
    await repo.upsert(address="0x" + "a" * 40, label="alice")
    row = (
        await db_session.execute(
            text("SELECT label FROM tracked_wallets WHERE address = :a"),
            {"a": "0x" + "a" * 40},
        )
    ).scalar_one()
    assert row == "alice"


async def test_upsert_updates_label_on_conflict(db_session: AsyncSession) -> None:
    """Re-sync com label novo atualiza row existente sem duplicar."""
    await db_session.execute(text("TRUNCATE tracked_wallets"))
    repo = SqlAlchemyTrackedWalletRepository(db_session)
    await repo.upsert(address="0x" + "b" * 40, label="bob")
    await repo.upsert(address="0x" + "b" * 40, label="bob_v2")
    rows = (await db_session.execute(text("SELECT label FROM tracked_wallets"))).scalars().all()
    assert list(rows) == ["bob_v2"]


async def test_adapter_satisfies_protocol(db_session: AsyncSession) -> None:
    _: TrackedWalletRepository = SqlAlchemyTrackedWalletRepository(db_session)
