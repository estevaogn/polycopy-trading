"""Integration tests das views wallet_realized_pnl + wallet_open_positions."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.integration


def _unique_cond() -> str:
    return "0x" + uuid.uuid4().hex.ljust(64, "0")[:64]


async def _insert_wallet_trade(
    session: AsyncSession,
    *,
    tx_hash: str,
    log_index: int,
    wallet: str,
    condition_id: str,
    token_id: str,
    side: str,
    price: str,
    size_usdc: str,
    occurred_at: datetime | None = None,
) -> None:
    occurred_at = occurred_at or datetime.now(tz=UTC)
    await session.execute(
        text(
            "INSERT INTO wallet_trades "
            "(tx_hash, log_index, wallet, condition_id, token_id, side, "
            " price, size_usdc, occurred_at) "
            "VALUES (:tx, :idx, :w, :c, :t, :side, :price, :size, :occ)"
        ),
        {
            "tx": tx_hash,
            "idx": log_index,
            "w": wallet,
            "c": condition_id,
            "t": token_id,
            "side": side,
            "price": price,
            "size": size_usdc,
            "occ": occurred_at,
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
            "(condition_id, resolved_outcome, winning_token_id, resolved_at, outcome_prices_raw) "
            'VALUES (:c, :o, :w, now(), \'["0","1"]\')'
        ),
        {"c": condition_id, "o": resolved_outcome, "w": winning_token_id},
    )


async def _query_realized(session: AsyncSession, tx_hash: str) -> Any:
    result = await session.execute(
        text(
            "SELECT side, qty_tokens, payout_per_token, pnl_usdc, status "
            "FROM wallet_realized_pnl WHERE tx_hash = :tx"
        ),
        {"tx": tx_hash},
    )
    return result.one()


async def test_realized_buy_winning_yields_positive_pnl(db_session: AsyncSession) -> None:
    """Wallet comprou token vencedor: pnl = qty - size (positivo)."""
    addr = "0x" + "1" * 40
    cond = _unique_cond()
    tx = "0x" + "ab" * 32
    await _insert_wallet_trade(
        db_session,
        tx_hash=tx,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="BUY",
        price="0.4",
        size_usdc="10",
    )
    await _insert_resolution(
        db_session,
        condition_id=cond,
        resolved_outcome="YES",
        winning_token_id="111",
    )

    row = await _query_realized(db_session, tx)
    assert row.status == "win"
    # qty = 25; pnl = 25*1 - 10 = 15
    assert row.pnl_usdc == Decimal("15")


async def test_realized_buy_losing_yields_negative_size(db_session: AsyncSession) -> None:
    addr = "0x" + "2" * 40
    cond = _unique_cond()
    tx = "0x" + "cd" * 32
    await _insert_wallet_trade(
        db_session,
        tx_hash=tx,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="BUY",
        price="0.4",
        size_usdc="10",
    )
    await _insert_resolution(
        db_session,
        condition_id=cond,
        resolved_outcome="YES",
        winning_token_id="222",
    )

    row = await _query_realized(db_session, tx)
    assert row.status == "lose"
    assert row.pnl_usdc == Decimal("-10")


async def test_realized_sell_winning_token_yields_negative_pnl(db_session: AsyncSession) -> None:
    """Wallet vendeu token que ganhou: pnl = size - qty (negativo)."""
    addr = "0x" + "3" * 40
    cond = _unique_cond()
    tx = "0x" + "ef" * 32
    await _insert_wallet_trade(
        db_session,
        tx_hash=tx,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="SELL",
        price="0.4",
        size_usdc="10",
    )
    await _insert_resolution(
        db_session,
        condition_id=cond,
        resolved_outcome="YES",
        winning_token_id="111",
    )

    row = await _query_realized(db_session, tx)
    assert row.status == "lose"
    # qty = 25; pnl = 10 - 25 = -15
    assert row.pnl_usdc == Decimal("-15")


async def test_realized_pending_resolution_yields_null_pnl(db_session: AsyncSession) -> None:
    addr = "0x" + "4" * 40
    cond = _unique_cond()
    tx = "0x" + "12" * 32
    await _insert_wallet_trade(
        db_session,
        tx_hash=tx,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="BUY",
        price="0.5",
        size_usdc="10",
    )
    # Sem resolution

    row = await _query_realized(db_session, tx)
    assert row.status == "pending"
    assert row.pnl_usdc is None


async def test_open_positions_aggregates_buys_minus_sells(db_session: AsyncSession) -> None:
    """Wallet comprou 100 tokens, vendeu 30 → posição aberta de 70."""
    await db_session.execute(text("TRUNCATE wallet_trades"))
    addr = "0x" + "5" * 40
    cond = _unique_cond()
    base = datetime.now(tz=UTC) - timedelta(days=10)
    # BUY 100 tokens (size=50, price=0.5)
    await _insert_wallet_trade(
        db_session,
        tx_hash="0x" + "01" * 32,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="BUY",
        price="0.5",
        size_usdc="50",
        occurred_at=base,
    )
    # SELL 30 tokens (size=18, price=0.6)
    await _insert_wallet_trade(
        db_session,
        tx_hash="0x" + "02" * 32,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="SELL",
        price="0.6",
        size_usdc="18",
        occurred_at=base + timedelta(days=1),
    )

    result = await db_session.execute(
        text(
            "SELECT net_qty, net_cost_usdc FROM wallet_open_positions "
            "WHERE wallet = :w AND token_id = :t"
        ),
        {"w": addr, "t": "111"},
    )
    row = result.one()
    # 100 - 30 = 70 tokens
    assert row.net_qty == Decimal("70")
    # net_cost = 50 - 18 = 32
    assert row.net_cost_usdc == Decimal("32")


async def test_open_positions_excludes_resolved_markets(db_session: AsyncSession) -> None:
    """Posições em mercados já resolvidos não aparecem (já são histórico, não 'open')."""
    await db_session.execute(text("TRUNCATE wallet_trades"))
    addr = "0x" + "6" * 40
    cond = _unique_cond()
    await _insert_wallet_trade(
        db_session,
        tx_hash="0x" + "03" * 32,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="BUY",
        price="0.5",
        size_usdc="10",
    )
    await _insert_resolution(
        db_session,
        condition_id=cond,
        resolved_outcome="YES",
        winning_token_id="111",
    )

    result = await db_session.execute(
        text("SELECT count(*) FROM wallet_open_positions WHERE wallet = :w"),
        {"w": addr},
    )
    assert result.scalar_one() == 0


async def test_open_positions_excludes_zero_or_negative_net(db_session: AsyncSession) -> None:
    """Wallet vendeu tudo (net_qty = 0): não aparece em open positions."""
    await db_session.execute(text("TRUNCATE wallet_trades"))
    addr = "0x" + "7" * 40
    cond = _unique_cond()
    await _insert_wallet_trade(
        db_session,
        tx_hash="0x" + "04" * 32,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="BUY",
        price="0.5",
        size_usdc="10",
    )
    await _insert_wallet_trade(
        db_session,
        tx_hash="0x" + "05" * 32,
        log_index=0,
        wallet=addr,
        condition_id=cond,
        token_id="111",
        side="SELL",
        price="0.5",
        size_usdc="10",
    )

    result = await db_session.execute(
        text("SELECT count(*) FROM wallet_open_positions WHERE wallet = :w"),
        {"w": addr},
    )
    assert result.scalar_one() == 0
