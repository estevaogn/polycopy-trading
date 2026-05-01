"""Integration tests for SqlAlchemyWalletTradeRepository."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.persistence.wallet_trade_repository import (
    SqlAlchemyWalletTradeRepository,
)
from polycopy.ports import WalletTradeRepository as WalletTradeRepositoryProtocol

pytestmark = pytest.mark.integration

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"
_OTHER_ADDR = "0x" + "9" * 40
_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN = "12345"


def _trade(
    *,
    tx_hash: str = "0x" + "cd" * 32,
    log_index: int = 0,
    wallet: str = _VALID_ADDR,
    occurred_at: datetime | None = None,
) -> Trade:
    return Trade(
        tx_hash=tx_hash,
        log_index=log_index,
        wallet=WalletAddress(value=wallet),
        condition_id=ConditionId(value=_VALID_COND),
        token_id=TokenId(value=_VALID_TOKEN),
        side=Side.BUY,
        price=Price(value=Decimal("0.55")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=occurred_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )


async def test_insert_if_absent_inserts_first_time(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    inserted = await repo.insert_if_absent(_trade())
    assert inserted is True


async def test_insert_if_absent_returns_false_on_duplicate(
    db_session: AsyncSession,
) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    trade = _trade()
    first = await repo.insert_if_absent(trade)
    second = await repo.insert_if_absent(trade)
    assert first is True
    assert second is False


async def test_insert_different_log_index_succeeds(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    a = await repo.insert_if_absent(_trade(log_index=0))
    b = await repo.insert_if_absent(_trade(log_index=1))
    assert a is True
    assert b is True


async def test_latest_occurred_at_returns_none_for_unknown_wallet(
    db_session: AsyncSession,
) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    result = await repo.latest_occurred_at(WalletAddress(value=_OTHER_ADDR))
    assert result is None


async def test_latest_occurred_at_returns_max(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await repo.insert_if_absent(_trade(tx_hash="0x" + "11" * 32, log_index=0, occurred_at=base))
    await repo.insert_if_absent(
        _trade(
            tx_hash="0x" + "22" * 32,
            log_index=0,
            occurred_at=base + timedelta(hours=1),
        )
    )
    result = await repo.latest_occurred_at(WalletAddress(value=_VALID_ADDR))
    assert result == base + timedelta(hours=1)


async def test_latest_occurred_at_filters_by_wallet(
    db_session: AsyncSession,
) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await repo.insert_if_absent(
        _trade(tx_hash="0x" + "11" * 32, log_index=0, wallet=_VALID_ADDR, occurred_at=base)
    )
    await repo.insert_if_absent(
        _trade(
            tx_hash="0x" + "22" * 32,
            log_index=0,
            wallet=_OTHER_ADDR,
            occurred_at=base + timedelta(hours=2),
        )
    )
    valid = await repo.latest_occurred_at(WalletAddress(value=_VALID_ADDR))
    other = await repo.latest_occurred_at(WalletAddress(value=_OTHER_ADDR))
    assert valid == base
    assert other == base + timedelta(hours=2)


def _accepts_protocol(_: WalletTradeRepositoryProtocol) -> None:
    return


async def test_adapter_satisfies_protocol(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    _accepts_protocol(repo)  # mypy strict valida
