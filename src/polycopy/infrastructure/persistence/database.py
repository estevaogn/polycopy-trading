"""SQLAlchemy async engine + session factory.

Engine é singleton por processo; session é criada por request/operação.
Não inicializa conexões no import — chamador decide quando subir.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from polycopy.config import Settings


def make_engine(settings: Settings) -> AsyncEngine:
    """Cria AsyncEngine. Caller é dono do lifecycle (chame `await engine.dispose()` ao parar)."""
    return create_async_engine(
        settings.postgres_async_dsn,
        echo=False,
        pool_pre_ping=True,
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
