"""order_mapper: converte Trade (domain) + final_size_usdc em OrderArgs (py-clob-client).

Polymarket CLOB trabalha em SHARES (unidades de outcome token), não USDC.
Conversão crítica: shares = usdc / price. Bug-prone — coberto por testes.
"""

from __future__ import annotations

from decimal import Decimal

from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL

from polycopy.domain.models import Side, Trade


def to_order_args(trade: Trade, final_size_usdc: Decimal) -> OrderArgs:
    """Mapeia Trade domain + size USDC pra OrderArgs do py-clob-client.

    `shares = final_size_usdc / trade.price.value`.
    py-clob-client espera floats (não Decimal).
    """
    shares = final_size_usdc / trade.price.value
    return OrderArgs(
        token_id=trade.token_id.value,
        price=float(trade.price.value),
        size=float(shares),
        side=BUY if trade.side == Side.BUY else SELL,
    )
