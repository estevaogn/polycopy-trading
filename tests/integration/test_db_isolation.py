"""Canário: confirma que testes integration rodam contra polycopy_test.

Falha imediatamente se algum dia o monkeypatch quebrar — protege contra
regressão silenciosa que destrói dados de produção.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.integration


async def test_isolation_uses_test_database(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Garante que current_database() == polycopy_test (não polycopy)."""
    async with db_session_factory() as session:
        result = await session.execute(text("SELECT current_database()"))
        assert result.scalar() == "polycopy_test"
