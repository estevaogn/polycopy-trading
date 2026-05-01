"""Shared test fixtures and bootstrap.

- `settings`: singleton Settings carregada do `.env`.
- `db_engine`: engine async session-scope; roda `alembic upgrade head` no início.
- `db_session`: AsyncSession dentro de transação; rollback automático no teardown.

Notes on async/sync boundary:
  `db_engine` é fixture SYNC com scope=session. O alembic usa `asyncio.run()`
  internamente (via env.py) e não pode ser chamado de dentro de um event loop
  ativo. Fixtures sync session-scoped rodam fora do loop do pytest-asyncio,
  então `command.upgrade/downgrade` funcionam corretamente.
  O `engine.sync_engine.dispose()` síncrono é chamado no teardown.

  O engine de teste usa `NullPool` para que conexões não sejam reutilizadas
  entre event loops diferentes (cada test function tem seu próprio loop).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from polycopy.config import Settings
from polycopy.infrastructure.persistence.database import make_session_factory

_REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Singleton Settings carregada do `.env`. Use em testes integration."""
    return Settings()  # type: ignore[call-arg]


@pytest.fixture(scope="session")
def alembic_config() -> Config:
    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "alembic"))
    return cfg


@pytest.fixture(scope="session")
def db_engine(settings: Settings, alembic_config: Config) -> Iterator[AsyncEngine]:
    """Engine async session-scope. Migra schema antes; dropa tudo no fim.

    Fixture é SYNC para que alembic.command.upgrade/downgrade possam chamar
    `asyncio.run()` sem conflito com o event loop do pytest-asyncio.
    Usa NullPool para evitar conflito de event loops entre testes.
    """
    engine = create_async_engine(
        settings.postgres_async_dsn,
        echo=False,
        poolclass=NullPool,
    )
    command.upgrade(alembic_config, "head")
    try:
        yield engine
    finally:
        command.downgrade(alembic_config, "base")
        engine.sync_engine.dispose()


@pytest.fixture
async def db_session_factory(
    db_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(db_engine)


@pytest.fixture
async def db_session(
    db_engine: AsyncEngine,
) -> AsyncIterator[AsyncSession]:
    """Session em transação; rollback no teardown — testes são isolados."""
    async with db_engine.connect() as conn:
        trans = await conn.begin()
        session = AsyncSession(bind=conn, expire_on_commit=False)
        try:
            yield session
        finally:
            await session.close()
            await trans.rollback()
