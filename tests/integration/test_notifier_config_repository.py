"""Integration tests do SqlAlchemyNotifierConfigRepository — exige Postgres up."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.infrastructure.persistence.notifier_config_repository import (
    SqlAlchemyNotifierConfigRepository,
)
from polycopy.ports import NotifierConfigRepository

pytestmark = pytest.mark.integration


async def test_get_min_size_returns_default_after_migration(db_session: AsyncSession) -> None:
    """Migration 0012 insere row default 50; sem TRUNCATE deve aparecer."""
    repo = SqlAlchemyNotifierConfigRepository(db_session)
    value = await repo.get_min_size_usdc()
    assert value == Decimal("50")


async def test_get_min_size_returns_zero_when_key_missing(db_session: AsyncSession) -> None:
    """Sem row (TRUNCATE'd): get retorna 0 (sem filtro)."""
    await db_session.execute(text("TRUNCATE notifier_config"))
    repo = SqlAlchemyNotifierConfigRepository(db_session)
    value = await repo.get_min_size_usdc()
    assert value == Decimal(0)


async def test_set_min_size_upserts_value(db_session: AsyncSession) -> None:
    """set_min_size_usdc cria ou atualiza atomicamente."""
    repo = SqlAlchemyNotifierConfigRepository(db_session)
    await repo.set_min_size_usdc(Decimal("75"), updated_by="test")
    assert await repo.get_min_size_usdc() == Decimal("75")

    await repo.set_min_size_usdc(Decimal("100"), updated_by="test_again")
    assert await repo.get_min_size_usdc() == Decimal("100")

    row = (
        await db_session.execute(
            text("SELECT updated_by FROM notifier_config WHERE key = 'min_size_usdc'")
        )
    ).scalar_one()
    assert row == "test_again"


async def test_adapter_satisfies_protocol(db_session: AsyncSession) -> None:
    _: NotifierConfigRepository = SqlAlchemyNotifierConfigRepository(db_session)
