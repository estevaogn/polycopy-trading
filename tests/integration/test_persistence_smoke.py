"""Smoke test: alembic migrou o schema e a tabela responde."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.integration


async def test_wallet_trades_table_exists(db_session: AsyncSession) -> None:
    result = await db_session.execute(
        text("SELECT 1 FROM information_schema.tables WHERE table_name = 'wallet_trades'")
    )
    assert result.scalar_one() == 1


async def test_wallet_trades_pk_is_composite(db_session: AsyncSession) -> None:
    result = await db_session.execute(
        text(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'wallet_trades'::regclass AND i.indisprimary
            ORDER BY a.attname
            """
        )
    )
    cols = [row[0] for row in result.all()]
    assert cols == ["log_index", "tx_hash"]


async def test_wallet_trades_index_on_wallet_occurred_at(db_session: AsyncSession) -> None:
    result = await db_session.execute(
        text("SELECT indexname FROM pg_indexes WHERE tablename = 'wallet_trades'")
    )
    names = {row[0] for row in result.all()}
    assert "wallet_trades_wallet_occurred_at_idx" in names
