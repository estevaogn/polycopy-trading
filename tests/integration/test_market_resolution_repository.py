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


async def _seed_resolved_execution(
    session: AsyncSession,
    *,
    condition_id: str,
    token_id: str,
    side: str,
    final_size_usdc: str,
    expected_avg_price: str,
    decided_at: datetime,
    resolved_at: datetime,
    winning_token_id: str | None,
    resolved_outcome: str = "YES",
) -> None:
    """Insere order_execution + market_resolution pra alimentar a view hypothetical_pnl."""
    await session.execute(
        text(
            "INSERT INTO order_executions "
            "(trade_event_id, wallet, condition_id, token_id, side, "
            " final_size_usdc, mode, result, decided_at, expected_avg_price) "
            "VALUES (:tid, :w, :c, :t, :side, :size, 'dry_run', 'dry_run', "
            "        :decided, :exp)"
        ),
        {
            "tid": uuid.uuid4(),
            "w": _VALID_WALLET,
            "c": condition_id,
            "t": token_id,
            "side": side,
            "size": final_size_usdc,
            "exp": expected_avg_price,
            "decided": decided_at,
        },
    )
    await session.execute(
        text(
            "INSERT INTO market_resolutions "
            "(condition_id, resolved_outcome, winning_token_id, "
            " resolved_at, outcome_prices_raw) "
            'VALUES (:c, :o, :w, :ra, \'["1","0"]\')'
        ),
        {"c": condition_id, "o": resolved_outcome, "w": winning_token_id, "ra": resolved_at},
    )


async def test_get_pnl_summary_populates_analytics_fields(
    db_session: AsyncSession,
) -> None:
    """Sharpe, max_drawdown e avg_holding_hours são calculados na agregação.

    Usa db_session (rollback) pra isolar — TRUNCATE antes garante view limpa,
    independente do que outros testes deixaram em order_executions.
    """
    from datetime import timedelta
    from decimal import Decimal

    # Limpa tudo dentro da transação — rollback no teardown restaura.
    await db_session.execute(text("TRUNCATE order_executions, market_resolutions CASCADE"))

    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    # 4 BUYs alternando win/lose @ price=0.5 size=10 → pnls +10, -10, +10, -10.
    # Cum: +10, 0, +10, 0. Peak: 10, 10, 10, 10. DD: 0, 10, 0, 10 → max_dd=10.
    # Returns: +1.0, -1.0, +1.0, -1.0 → variance > 0 → sharpe não-None.
    # Holding: 4h cada.
    for i, win in enumerate([True, False, True, False]):
        cond = _unique_cond()
        await _seed_resolved_execution(
            db_session,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price="0.5",
            decided_at=base + timedelta(hours=2 * i),
            resolved_at=base + timedelta(hours=2 * i + 4),
            winning_token_id="111" if win else "222",
        )

    repo = SqlAlchemyMarketResolutionRepository(db_session)
    summary = await repo.get_pnl_summary()

    assert summary.trades_resolved == 4
    assert summary.sharpe is not None  # variance > 0
    assert summary.max_drawdown_usdc == Decimal("10")
    assert summary.avg_holding_hours == 4.0
