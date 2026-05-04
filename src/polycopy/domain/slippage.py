"""Slippage / expected price calculation.

Função pura que percorre orderbook acumulando até target_usdc e retorna
weighted avg price. Usada pelos executors (DryRun + Web3CLOB) pra
gravar expected_avg_price em order_executions (Plano 5B).
"""

from __future__ import annotations

from decimal import Decimal

from polycopy.domain.market import OrderBook, OrderBookLevel
from polycopy.domain.models import Side


def calculate_expected_avg_price(
    *, book: OrderBook, side: Side, target_usdc: Decimal
) -> Decimal | None:
    """Weighted avg price pra preencher target_usdc.

    BUY: percorre asks ascendente, acumula custo até target.
    SELL: percorre bids descendente, acumula receita até target.

    Retorna None se liquidez total < target_usdc (book vazio ou insuficiente).
    """
    levels: list[OrderBookLevel] = book.asks if side == Side.BUY else book.bids

    if not levels:
        return None

    accumulated_usdc = Decimal("0")
    accumulated_qty = Decimal("0")

    for level in levels:
        price = level.price.value
        size = level.size.amount
        if size <= 0:
            continue

        slice_usdc = price * size
        if accumulated_usdc + slice_usdc >= target_usdc:
            # Fração final do nível — pega só o que falta
            remaining_usdc = target_usdc - accumulated_usdc
            slice_qty = remaining_usdc / price
            accumulated_qty += slice_qty
            return target_usdc / accumulated_qty

        # Consome nível inteiro e segue
        accumulated_usdc += slice_usdc
        accumulated_qty += size

    # Esgotou book sem atingir target
    return None
