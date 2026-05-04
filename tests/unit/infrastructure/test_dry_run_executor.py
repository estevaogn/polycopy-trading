"""Testes unit do DryRunExecutor."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from prometheus_client import CollectorRegistry

from polycopy.domain.events import ExecutionMode
from polycopy.domain.market import OrderBook, OrderBookLevel
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.dry_run_executor import DryRunExecutor
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports.order_executor import OrderExecutor


class _StubCLOB:
    """Stub que satisfaz PolymarketClobPort."""

    def __init__(
        self,
        book: OrderBook | None = None,
        raise_exc: Exception | None = None,
    ) -> None:
        self._book = book
        self._raise = raise_exc

    async def get_book(self, token_id: TokenId) -> OrderBook:
        if self._raise is not None:
            raise self._raise
        if self._book is None:
            raise RuntimeError("book not configured")
        return self._book


def _level(price: str, size: str) -> OrderBookLevel:
    return OrderBookLevel(
        price=Price(value=Decimal(price)),
        size=Money(amount=Decimal(size)),
    )


def _book(
    *,
    asks: list[OrderBookLevel] | None = None,
    bids: list[OrderBookLevel] | None = None,
) -> OrderBook:
    return OrderBook(
        token_id=TokenId(value="111"),
        asks=asks or [],
        bids=bids or [],
        captured_at=datetime.now(tz=UTC),
    )


def _trade(*, side: Side = Side.BUY) -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="111"),
        side=side,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def _make_executor(
    book: OrderBook | None = None,
    raise_exc: Exception | None = None,
) -> tuple[DryRunExecutor, Metrics]:
    metrics = make_metrics(registry=CollectorRegistry())
    return (
        DryRunExecutor(clob=_StubCLOB(book=book, raise_exc=raise_exc), metrics=metrics),
        metrics,
    )


async def test_execute_returns_dry_run_success() -> None:
    executor, _ = _make_executor(book=_book(asks=[_level("0.5", "1000")]))
    result = await executor.execute(_trade(), Decimal("10"))
    assert result.mode == ExecutionMode.DRY_RUN
    assert result.success is True
    assert result.tx_hash is None
    assert result.gas_wei is None
    assert result.failure_reason is None
    assert result.error_message is None


async def test_dry_run_executor_satisfies_port() -> None:
    """Mypy garante que DryRunExecutor satisfaz OrderExecutor Protocol."""
    executor, _ = _make_executor(book=_book(asks=[_level("0.5", "1000")]))
    _: OrderExecutor = executor


async def test_execute_returns_expected_avg_price_when_book_available() -> None:
    book = _book(asks=[_level("0.6", "100")])
    executor, _ = _make_executor(book=book)
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("60"))

    assert result.expected_avg_price == Decimal("0.6")


async def test_execute_returns_none_when_book_empty() -> None:
    executor, metrics = _make_executor(book=_book())
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("10"))

    assert result.expected_avg_price is None
    assert (
        metrics.executor_expected_price_unavailable_total.labels(reason="empty_book")._value.get()
        == 1.0
    )


async def test_execute_returns_none_when_insufficient_volume() -> None:
    book = _book(asks=[_level("0.5", "10")])  # apenas 5 USDC
    executor, metrics = _make_executor(book=book)
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("100"))

    assert result.expected_avg_price is None
    assert (
        metrics.executor_expected_price_unavailable_total.labels(
            reason="insufficient_volume"
        )._value.get()
        == 1.0
    )


async def test_execute_returns_none_when_get_book_raises() -> None:
    executor, metrics = _make_executor(raise_exc=RuntimeError("network down"))
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("10"))

    assert result.expected_avg_price is None
    assert result.success is True  # DRY-RUN ainda retorna success
    assert (
        metrics.executor_expected_price_unavailable_total.labels(reason="fetch_failed")._value.get()
        == 1.0
    )
