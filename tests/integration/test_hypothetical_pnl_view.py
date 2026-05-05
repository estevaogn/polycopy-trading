"""Integration tests da view hypothetical_pnl — 10 cenários cobrindo PnL semantics."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.integration


def _unique_cond() -> str:
    return "0x" + uuid.uuid4().hex.ljust(64, "0")[:64]


async def _insert_execution(
    session: AsyncSession,
    *,
    trade_event_id: uuid.UUID,
    condition_id: str,
    token_id: str,
    side: str,
    final_size_usdc: str,
    expected_avg_price: str | None,
) -> None:
    """Insere row em order_executions com defaults DRY-RUN."""
    await session.execute(
        text(
            "INSERT INTO order_executions "
            "(trade_event_id, wallet, condition_id, token_id, side, "
            " final_size_usdc, mode, result, decided_at, expected_avg_price) "
            "VALUES (:tid, :w, :c, :t, :side, :size, 'dry_run', 'dry_run', "
            "        now(), :exp)"
        ),
        {
            "tid": trade_event_id,
            "w": "0x" + "1" * 40,
            "c": condition_id,
            "t": token_id,
            "side": side,
            "size": final_size_usdc,
            "exp": expected_avg_price,
        },
    )


async def _insert_resolution(
    session: AsyncSession,
    *,
    condition_id: str,
    resolved_outcome: str,
    winning_token_id: str | None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO market_resolutions "
            "(condition_id, resolved_outcome, winning_token_id, "
            " resolved_at, outcome_prices_raw) "
            'VALUES (:c, :o, :w, now(), \'["0","1"]\')'
        ),
        {"c": condition_id, "o": resolved_outcome, "w": winning_token_id},
    )


async def _query_pnl(session: AsyncSession, trade_event_id: uuid.UUID) -> Any:
    result = await session.execute(
        text(
            "SELECT side, qty_tokens, payout_per_token, pnl_usdc, status "
            "FROM hypothetical_pnl WHERE trade_event_id = :tid"
        ),
        {"tid": trade_event_id},
    )
    return result.one()


async def test_view_buy_winning_token_yields_positive_pnl(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """BUY token vencedor: pnl = (size/expected) - size = qty - size."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price="0.5",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="YES",
            winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "win"
        assert row.qty_tokens == Decimal("20")
        assert row.payout_per_token == Decimal("1.0")
        assert row.pnl_usdc == Decimal("10")  # 20 - 10 = +10


async def test_view_buy_losing_token_yields_negative_size(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """BUY token perdedor: pnl = -size."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price="0.5",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="YES",
            winning_token_id="222",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "lose"
        assert row.payout_per_token == Decimal("0")
        assert row.pnl_usdc == Decimal("-10")


async def test_view_invalid_resolution_pays_half(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """INVALID: pnl = qty * 0.5 - size."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price="0.4",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="INVALID",
            winning_token_id=None,
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "invalid"
        assert row.payout_per_token == Decimal("0.5")
        assert row.qty_tokens == Decimal("25")  # 10 / 0.4
        assert row.pnl_usdc == Decimal("2.5")  # 25 * 0.5 - 10 = 2.5


async def test_view_pending_resolution_yields_null_pnl(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Sem resolution: pnl NULL, status pending."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price="0.5",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "pending"
        assert row.pnl_usdc is None
        assert row.payout_per_token is None


async def test_view_null_expected_price_yields_no_expected_price_status(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """expected_avg_price NULL: status no_expected_price, pnl NULL."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price=None,
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="YES",
            winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "no_expected_price"
        assert row.pnl_usdc is None
        assert row.qty_tokens is None


async def test_view_sell_winning_token_yields_negative_pnl(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """SELL token vencedor: pnl = size - qty*1 (negativo). Vendemos o que valorizou."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="SELL",
            final_size_usdc="10",
            expected_avg_price="0.4",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="YES",
            winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "lose"
        # qty = 10/0.4 = 25; pnl = 10 - 25*1 = -15
        assert row.pnl_usdc == Decimal("-15")
        assert row.qty_tokens == Decimal("25")
        assert row.payout_per_token == Decimal("1")


async def test_view_sell_losing_token_yields_positive_size(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """SELL token perdedor: pnl = +size. Vendemos algo que virou pó."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="SELL",
            final_size_usdc="10",
            expected_avg_price="0.4",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="YES",
            winning_token_id="999",  # token_id != trade
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "win"
        # qty = 25; pnl = 10 - 25*0 = +10
        assert row.pnl_usdc == Decimal("10")
        assert row.payout_per_token == Decimal("0")


async def test_view_sell_invalid_resolution_pays_half(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """SELL INVALID: status='invalid' regardless de sinal do pnl; pnl reflete payout 0.5."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="SELL",
            final_size_usdc="10",
            expected_avg_price="0.4",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="INVALID",
            winning_token_id=None,
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "invalid"
        # qty = 25; pnl = 10 - 25*0.5 = -2.5 (preço baixo + INVALID = SELL perde economicamente)
        assert row.pnl_usdc == Decimal("-2.5")


async def test_view_zero_expected_price_treated_as_null(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """expected_avg_price = 0 (defensivo): pnl NULL."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price="0",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="YES",
            winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "no_expected_price"
        assert row.pnl_usdc is None


async def test_view_multiple_trades_same_condition(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Múltiplos trades mesmo condition: todos retornam, todos resolvidos coerentemente."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid1 = uuid.uuid4()
        tid2 = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid1,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="5",
            expected_avg_price="0.5",
        )
        await _insert_execution(
            session,
            trade_event_id=tid2,
            condition_id=cond,
            token_id="111",
            side="BUY",
            final_size_usdc="20",
            expected_avg_price="0.4",
        )
        await _insert_resolution(
            session,
            condition_id=cond,
            resolved_outcome="YES",
            winning_token_id="111",
        )
        await session.commit()

        row1 = await _query_pnl(session, tid1)
        row2 = await _query_pnl(session, tid2)
        assert row1.status == "win"
        assert row2.status == "win"
        assert row1.pnl_usdc == Decimal("5")  # 10 - 5
        assert row2.pnl_usdc == Decimal("30")  # 50 - 20


async def test_view_status_enum_completeness(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Confere que todos os status aparecem corretamente em runs separados (BUY + SELL)."""
    async with db_session_factory() as session:
        scenarios = [
            ("BUY", "111", "0.5", "YES", "111", "win"),
            ("BUY", "111", "0.5", "YES", "222", "lose"),
            ("BUY", "111", "0.4", "INVALID", None, "invalid"),
            ("BUY", "111", "0.5", None, None, "pending"),
            ("SELL", "111", "0.4", "YES", "222", "win"),
            ("SELL", "111", "0.4", "YES", "111", "lose"),
        ]
        tids = []
        for side, token, exp, outcome, winner, _ in scenarios:
            cond = _unique_cond()
            tid = uuid.uuid4()
            tids.append(tid)
            await _insert_execution(
                session,
                trade_event_id=tid,
                condition_id=cond,
                token_id=token,
                side=side,
                final_size_usdc="10",
                expected_avg_price=exp,
            )
            if outcome is not None:
                await _insert_resolution(
                    session,
                    condition_id=cond,
                    resolved_outcome=outcome,
                    winning_token_id=winner,
                )
        await session.commit()

        statuses = []
        for tid in tids:
            row = await _query_pnl(session, tid)
            statuses.append(row.status)

        assert statuses == ["win", "lose", "invalid", "pending", "win", "lose"]


async def test_view_no_resolution_match_yields_pending(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """JOIN sem match: trade existe, resolution não — status pending."""
    async with db_session_factory() as session:
        cond_a = _unique_cond()
        cond_b = _unique_cond()  # tem resolution mas trade não usa
        tid = uuid.uuid4()
        await _insert_execution(
            session,
            trade_event_id=tid,
            condition_id=cond_a,
            token_id="111",
            side="BUY",
            final_size_usdc="10",
            expected_avg_price="0.5",
        )
        await _insert_resolution(
            session,
            condition_id=cond_b,
            resolved_outcome="YES",
            winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "pending"
