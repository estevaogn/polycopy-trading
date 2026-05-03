"""Testes unit do order_mapper — Trade → OrderArgs (py-clob-client format)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from py_clob_client.order_builder.constants import BUY, SELL

from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.order_mapper import to_order_args


def _trade(*, side: Side = Side.BUY, price: str = "0.5") -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=side,
        price=Price(value=Decimal(price)),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def test_buy_price_half_one_usdc_yields_two_shares() -> None:
    """BUY @ price 0.5 com $1 USDC → 2 shares (1/0.5)."""
    args = to_order_args(_trade(side=Side.BUY, price="0.5"), Decimal("1"))
    assert args.token_id == "42"
    assert args.price == 0.5
    assert args.size == 2.0
    assert args.side == BUY


def test_buy_price_quarter_one_usdc_yields_four_shares() -> None:
    """BUY @ price 0.25 com $1 USDC → 4 shares (1/0.25)."""
    args = to_order_args(_trade(side=Side.BUY, price="0.25"), Decimal("1"))
    assert args.size == 4.0
    assert args.price == 0.25


def test_sell_price_half_one_usdc_yields_two_shares() -> None:
    """SELL @ price 0.5 com $1 USDC → 2 shares (mesma matemática)."""
    args = to_order_args(_trade(side=Side.SELL, price="0.5"), Decimal("1"))
    assert args.size == 2.0
    assert args.side == SELL


def test_token_id_passed_through() -> None:
    """token_id deve ser preservado intacto."""
    trade = _trade()
    args = to_order_args(trade, Decimal("1"))
    assert args.token_id == trade.token_id.value


def test_side_enum_mapping() -> None:
    """Side.BUY → BUY constant; Side.SELL → SELL constant."""
    args_buy = to_order_args(_trade(side=Side.BUY), Decimal("1"))
    args_sell = to_order_args(_trade(side=Side.SELL), Decimal("1"))
    assert args_buy.side == BUY
    assert args_sell.side == SELL


def test_fractional_size() -> None:
    """price 0.5 + size $0.005 → 0.01 shares."""
    args = to_order_args(_trade(price="0.5"), Decimal("0.005"))
    assert args.size == 0.01
