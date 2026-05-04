"""Integration tests do SqlAlchemyMarketResolutionRepository — exige Postgres up."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution
from polycopy.infrastructure.persistence.market_resolution_repository import (
    SqlAlchemyMarketResolutionRepository,
)
from polycopy.ports import MarketResolutionRepository

pytestmark = pytest.mark.integration

_VALID_TOKEN_YES = "111"
_VALID_TOKEN_NO = "222"
_VALID_WALLET = "0x" + "1" * 40


def _unique_cond() -> str:
    """Gera um condition_id único pra evitar colisão entre testes (sem rollback)."""
    return "0x" + uuid.uuid4().hex.ljust(64, "0")[:64]


def _resolution_yes(condition_id: str | None = None) -> MarketResolution:
    return MarketResolution(
        condition_id=condition_id or _unique_cond(),
        resolved_outcome=ResolvedOutcome.YES,
        winning_token_id=_VALID_TOKEN_YES,
        closed_time=datetime.now(tz=UTC),
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw='["1.0", "0.0"]',
        uma_resolution_statuses_raw='["resolved"]',
    )


def _resolution_invalid(condition_id: str | None = None) -> MarketResolution:
    return MarketResolution(
        condition_id=condition_id or _unique_cond(),
        resolved_outcome=ResolvedOutcome.INVALID,
        winning_token_id=None,
        closed_time=datetime.now(tz=UTC),
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw='["0.5", "0.5"]',
        uma_resolution_statuses_raw=None,
    )


async def _seed_wallet_trade(
    session: AsyncSession,
    *,
    condition_id: str,
    log_index: int,
    token_id: str = "999",  # noqa: S107
) -> None:
    """Insere uma row em wallet_trades pra testar get_unresolved_condition_ids.

    log_index é parâmetro pra distinguir rows quando inserimos várias com mesmo tx_hash.
    """
    await session.execute(
        text(
            "INSERT INTO wallet_trades "
            "(tx_hash, log_index, wallet, condition_id, token_id, side, "
            " price, size_usdc, occurred_at) "
            "VALUES (:tx, :idx, :w, :c, :t, 'BUY', 0.5, 10, now())"
        ),
        {
            "tx": "0x" + uuid.uuid4().hex.ljust(64, "0")[:64],
            "idx": log_index,
            "w": _VALID_WALLET,
            "c": condition_id,
            "t": token_id,
        },
    )
    await session.commit()


async def test_insert_returns_true_for_new(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketResolutionRepository(session)
        result = await repo.insert(_resolution_yes())
        await session.commit()
        assert result is True


async def test_insert_returns_false_for_duplicate(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    cond = _unique_cond()
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketResolutionRepository(session)
        r = _resolution_yes(cond)
        first = await repo.insert(r)
        await session.commit()
        second = await repo.insert(r)
        await session.commit()
        assert first is True
        assert second is False


async def test_insert_invalid_persists_no_winning_token(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    cond = _unique_cond()
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketResolutionRepository(session)
        r = _resolution_invalid(cond)
        await repo.insert(r)
        await session.commit()

        result = await session.execute(
            text(
                "SELECT resolved_outcome, winning_token_id "
                "FROM market_resolutions WHERE condition_id = :c"
            ),
            {"c": cond},
        )
        row = result.one()
        assert row.resolved_outcome == "INVALID"
        assert row.winning_token_id is None


async def test_insert_yes_with_null_winning_token_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Bypass do __post_init__ via SQL cru pra validar CHECK no DB."""
    cond = _unique_cond()
    async with db_session_factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO market_resolutions "
                    "(condition_id, resolved_outcome, winning_token_id, "
                    " resolved_at, outcome_prices_raw) "
                    "VALUES (:c, 'YES', NULL, now(), '[\"1\",\"0\"]')"
                ),
                {"c": cond},
            )
            await session.commit()


async def test_insert_invalid_outcome_string_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """CHECK outcome_enum rejeita valor não-listado."""
    cond = _unique_cond()
    async with db_session_factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO market_resolutions "
                    "(condition_id, resolved_outcome, winning_token_id, "
                    " resolved_at, outcome_prices_raw) "
                    "VALUES (:c, 'MAYBE', '111', now(), '[\"0.5\",\"0.5\"]')"
                ),
                {"c": cond},
            )
            await session.commit()


async def test_get_unresolved_condition_ids_left_join_works(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    cond_a = _unique_cond()
    cond_b = _unique_cond()
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, condition_id=cond_a, log_index=1)
        await _seed_wallet_trade(session, condition_id=cond_b, log_index=2)

        repo = SqlAlchemyMarketResolutionRepository(session)
        # cond_a resolvido; cond_b não
        await repo.insert(_resolution_yes(cond_a))
        await session.commit()

        unresolved = await repo.get_unresolved_condition_ids(limit=10)
        assert cond_b in unresolved
        assert cond_a not in unresolved


async def test_adapter_satisfies_protocol(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Mypy garante que SqlAlchemyMarketResolutionRepository satisfaz Protocol."""
    async with db_session_factory() as session:
        _: MarketResolutionRepository = SqlAlchemyMarketResolutionRepository(session)
