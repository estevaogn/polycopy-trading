"""DryRunExecutor: implementação MVP de OrderExecutor.

Sempre retorna ExecutionResult(mode=DRY_RUN, success=True). Não chama
blockchain. Calcula expected_avg_price a partir do orderbook (Plano 5B).
"""

from __future__ import annotations

from decimal import Decimal

import structlog

from polycopy.domain.events import ExecutionMode
from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade
from polycopy.domain.slippage import calculate_expected_avg_price
from polycopy.infrastructure.observability.metrics import Metrics
from polycopy.ports import PolymarketClobPort


class DryRunExecutor:
    """Executor que apenas simula — não chama blockchain.

    Calcula expected_avg_price via clob.get_book + função pura
    calculate_expected_avg_price. None se book insuficiente.
    """

    def __init__(self, *, clob: PolymarketClobPort, metrics: Metrics) -> None:
        self._clob = clob
        self._metrics = metrics
        self._log = structlog.get_logger("dry_run_executor")

    async def execute(
        self,
        trade: Trade,
        final_size_usdc: Decimal,
    ) -> ExecutionResult:
        expected = await self._compute_expected_price(trade, final_size_usdc)
        return ExecutionResult(
            mode=ExecutionMode.DRY_RUN,
            success=True,
            tx_hash=None,
            gas_wei=None,
            failure_reason=None,
            error_message=None,
            expected_avg_price=expected,
        )

    async def _compute_expected_price(
        self, trade: Trade, final_size_usdc: Decimal
    ) -> Decimal | None:
        try:
            book = await self._clob.get_book(trade.token_id)
        except Exception as exc:  # noqa: BLE001
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="fetch_failed"
            ).inc()
            self._log.warning(
                "expected_price_fetch_failed",
                error=str(exc),
                error_type=type(exc).__name__,
                token_id=trade.token_id.value,
            )
            return None

        if not book.asks and not book.bids:
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="empty_book"
            ).inc()
            return None

        result = calculate_expected_avg_price(
            book=book, side=trade.side, target_usdc=final_size_usdc
        )
        if result is None:
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="insufficient_volume"
            ).inc()
        return result
