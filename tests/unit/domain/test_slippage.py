"""Testes unit de calculate_expected_avg_price."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from polycopy.domain.market import OrderBook, OrderBookLevel
from polycopy.domain.models import Side
from polycopy.domain.slippage import calculate_expected_avg_price
from polycopy.domain.value_objects import Money, Price, TokenId

_TOKEN = TokenId(value="111")


def _level(price: str, size: str) -> OrderBookLevel:
    return OrderBookLevel(
        price=Price(value=Decimal(price)),
        size=Money(amount=Decimal(size)),
    )


def _book(*, asks: list[OrderBookLevel], bids: list[OrderBookLevel]) -> OrderBook:
    return OrderBook(
        token_id=_TOKEN,
        asks=asks,
        bids=bids,
        captured_at=datetime.now(tz=UTC),
    )


def test_buy_single_level_exact_fill() -> None:
    """1 ask cobre exatamente target_usdc."""
    book = _book(asks=[_level("0.6", "100")], bids=[])
    # target = 0.6 * 100 = 60 USDC
    result = calculate_expected_avg_price(book=book, side=Side.BUY, target_usdc=Decimal("60"))
    assert result == Decimal("0.6")


def test_buy_multi_level_fill() -> None:
    """3 asks combinados pra atingir target."""
    book = _book(
        asks=[
            _level("0.5", "10"),  # 5 USDC, 10 qty
            _level("0.6", "10"),  # 6 USDC, 10 qty
            _level("0.7", "10"),  # 7 USDC, 10 qty
        ],
        bids=[],
    )
    # target = 18 USDC, fills exatamente 3 níveis. avg = 18/30 = 0.6
    result = calculate_expected_avg_price(book=book, side=Side.BUY, target_usdc=Decimal("18"))
    assert result == Decimal("0.6")


def test_sell_multi_level_fill() -> None:
    """SELL percorre bids descendente."""
    book = _book(
        asks=[],
        bids=[
            _level("0.7", "10"),  # 7 USDC
            _level("0.6", "10"),  # 6 USDC
            _level("0.5", "10"),  # 5 USDC
        ],
    )
    # target = 18 USDC. avg = 18/30 = 0.6
    result = calculate_expected_avg_price(book=book, side=Side.SELL, target_usdc=Decimal("18"))
    assert result == Decimal("0.6")


def test_buy_partial_last_level() -> None:
    """Último ask preenche apenas fração."""
    book = _book(
        asks=[
            _level("0.5", "10"),  # 5 USDC fully consumed → qty=10
            _level("0.6", "100"),  # need 5 more USDC → qty=5/0.6=8.333...
        ],
        bids=[],
    )
    # target = 10 USDC. total_qty = 10 + 5/0.6 ≈ 18.333... avg = 10/18.333... ≈ 0.5454
    result = calculate_expected_avg_price(book=book, side=Side.BUY, target_usdc=Decimal("10"))
    assert result is not None
    expected = Decimal("10") / (Decimal("10") + Decimal("5") / Decimal("0.6"))
    assert abs(result - expected) < Decimal("0.00000001")


def test_returns_none_when_book_empty() -> None:
    """Asks vazios pra BUY → None."""
    book = _book(asks=[], bids=[_level("0.5", "10")])
    assert calculate_expected_avg_price(book=book, side=Side.BUY, target_usdc=Decimal("10")) is None


def test_returns_none_when_insufficient_volume() -> None:
    """Total liquidez < target → None."""
    book = _book(
        asks=[_level("0.5", "10")],  # apenas 5 USDC disponível
        bids=[],
    )
    assert (
        calculate_expected_avg_price(book=book, side=Side.BUY, target_usdc=Decimal("100")) is None
    )


def test_buy_single_ask_partial() -> None:
    """1 ask com volume excedente; preenche fração."""
    book = _book(
        asks=[_level("0.5", "100")],  # 50 USDC disponível
        bids=[],
    )
    # target = 10 USDC. qty = 20. avg = 10/20 = 0.5
    result = calculate_expected_avg_price(book=book, side=Side.BUY, target_usdc=Decimal("10"))
    assert result == Decimal("0.5")


def test_returns_none_for_sell_with_empty_bids() -> None:
    """SELL com bids vazios → None."""
    book = _book(asks=[_level("0.6", "100")], bids=[])
    assert (
        calculate_expected_avg_price(book=book, side=Side.SELL, target_usdc=Decimal("10")) is None
    )


def test_decimal_precision_8_places() -> None:
    """Confirma que Decimal é usado internamente (sem float rounding).

    Price é quantizado para 4 casas decimais pelo domínio (0.33333333 → 0.3333).
    O resultado reflete o price quantizado: avg = target / qty = 0.3333.
    """
    book = _book(
        asks=[_level("0.33333333", "1000")],
        bids=[],
    )
    # Price é quantizado para 0.3333 pelo validator de Price.
    # target = 0.3333 * 1000 = 333.3 USDC; mas usamos target menor pra fill parcial.
    # target = 33.333333 USDC. qty = 33.333333 / 0.3333. avg = 0.3333
    result = calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("33.333333")
    )
    # Resultado deve ser Decimal (sem perda de float) e igual ao price quantizado
    assert result is not None
    assert isinstance(result, Decimal)
    assert abs(result - Decimal("0.3333")) < Decimal("0.000001")


def test_zero_size_level_skipped() -> None:
    """Nível com size=0 não consome target (defensivo)."""
    book = _book(
        asks=[
            _level("0.5", "0"),  # zero size — pula
            _level("0.6", "10"),  # 6 USDC, 10 qty
        ],
        bids=[],
    )
    # target = 6 USDC. avg = 0.6
    result = calculate_expected_avg_price(book=book, side=Side.BUY, target_usdc=Decimal("6"))
    assert result == Decimal("0.6")
