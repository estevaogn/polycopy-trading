"""Testes pra value objects de mercado: OrderBook, Market."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from polycopy.domain.market import Market, OrderBook, OrderBookLevel
from polycopy.domain.value_objects import ConditionId, Money, Price, TokenId


def _market(
    *,
    token_id: str = "42",  # noqa: S107
    is_active: bool = True,
    is_archived: bool = False,
    end_date: datetime | None = None,
    volume_24h: Decimal | None = Decimal("75000"),
    liquidity: Decimal | None = Decimal("12000"),
) -> Market:
    return Market(
        token_id=TokenId(value=token_id),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        question="Will X happen?",
        slug="will-x-happen",
        outcome="Yes",
        end_date=end_date,
        is_active=is_active,
        is_archived=is_archived,
        volume_24h_usdc=None if volume_24h is None else Money.from_usdc(str(volume_24h)),
        liquidity_usdc=None if liquidity is None else Money.from_usdc(str(liquidity)),
    )


class TestMarket:
    def test_valid_market(self) -> None:
        m = _market()
        assert m.token_id.value == "42"
        assert m.is_active is True

    def test_outcome_must_be_non_empty(self) -> None:
        """Após PR #21: aceitamos qualquer string non-empty (sport teams, multi-option)."""
        with pytest.raises(ValidationError):
            Market(
                token_id=TokenId(value="42"),
                condition_id=ConditionId(value="0x" + "ab" * 32),
                question="?",
                slug=None,
                outcome="",
                end_date=None,
                is_active=True,
                is_archived=False,
                volume_24h_usdc=None,
                liquidity_usdc=None,
            )

    def test_outcome_accepts_non_binary(self) -> None:
        """Sport teams, multi-option markets têm outcomes nominais."""
        m = Market(
            token_id=TokenId(value="42"),
            condition_id=ConditionId(value="0x" + "ab" * 32),
            question="Phillies vs Marlins",
            slug=None,
            outcome="Philadelphia Phillies",
            end_date=None,
            is_active=True,
            is_archived=False,
            volume_24h_usdc=None,
            liquidity_usdc=None,
        )
        assert m.outcome == "Philadelphia Phillies"

    def test_immutable(self) -> None:
        m = _market()
        with pytest.raises(ValidationError):
            m.is_active = False  # type: ignore[misc]

    def test_archived_excludes_active(self) -> None:
        # Regra: arquivado só é válido se não-ativo.
        with pytest.raises(ValidationError):
            _market(is_active=True, is_archived=True)

    def test_archived_and_inactive_ok(self) -> None:
        m = _market(is_active=False, is_archived=True)
        assert m.is_archived is True
        assert m.is_active is False

    def test_inactive_and_not_archived_ok(self) -> None:
        """Mercado encerrado mas ainda não arquivado é estado válido."""
        m = _market(is_active=False, is_archived=False)
        assert m.is_active is False
        assert m.is_archived is False


class TestOrderBookLevel:
    def test_level_valid(self) -> None:
        lvl = OrderBookLevel(price=Price(value=Decimal("0.55")), size=Money.from_usdc("100"))
        assert lvl.price.value == Decimal("0.5500")

    def test_size_non_negative(self) -> None:
        with pytest.raises(ValidationError):
            OrderBookLevel(
                price=Price(value=Decimal("0.5")),
                size=Money(amount=Decimal("-1")),
            )

    def test_size_microscopic_negative_quantized_to_zero(self) -> None:
        """Documenta: amounts negativos com magnitude < 1e-6 são quantizados a 0
        pelo Money antes do validador de OrderBookLevel rodar — então passam.

        Isso é comportamento consciente do Money (USDC tem 6 decimais on-chain).
        Validador rejeita só amounts negativos REPRESENTÁVEIS (>= 1e-6 em módulo).
        """
        from decimal import Decimal as _Decimal

        lvl = OrderBookLevel(
            price=Price(value=_Decimal("0.5")),
            size=Money(amount=_Decimal("-0.0000001")),
        )
        assert lvl.size.amount == _Decimal("0.000000")


class TestOrderBook:
    def _book(
        self,
        *,
        bids: list[tuple[str, str]] | None = None,
        asks: list[tuple[str, str]] | None = None,
    ) -> OrderBook:
        bids = bids if bids is not None else [("0.50", "100"), ("0.49", "200")]
        asks = asks if asks is not None else [("0.51", "150"), ("0.52", "250")]
        return OrderBook(
            token_id=TokenId(value="42"),
            bids=[
                OrderBookLevel(price=Price(value=Decimal(p)), size=Money.from_usdc(s))
                for p, s in bids
            ],
            asks=[
                OrderBookLevel(price=Price(value=Decimal(p)), size=Money.from_usdc(s))
                for p, s in asks
            ],
            captured_at=datetime.now(tz=UTC),
        )

    def test_best_bid_and_ask(self) -> None:
        book = self._book()
        assert book.best_bid is not None
        assert book.best_bid.price.value == Decimal("0.5000")
        assert book.best_ask is not None
        assert book.best_ask.price.value == Decimal("0.5100")

    def test_empty_book_no_best(self) -> None:
        book = self._book(bids=[], asks=[])
        assert book.best_bid is None
        assert book.best_ask is None

    def test_single_level_book_valid(self) -> None:
        """Book com 1 bid + 1 ask passa pelo validador de ordenação (loop range(1, 1) é vazio)."""
        book = self._book(bids=[("0.50", "100")], asks=[("0.51", "150")])
        assert book.best_bid is not None
        assert book.best_bid.price.value == Decimal("0.5000")
        assert book.best_ask is not None
        assert book.best_ask.price.value == Decimal("0.5100")

    def test_bids_must_be_descending(self) -> None:
        with pytest.raises(ValidationError):
            self._book(bids=[("0.49", "100"), ("0.50", "200")])

    def test_asks_must_be_ascending(self) -> None:
        with pytest.raises(ValidationError):
            self._book(asks=[("0.52", "100"), ("0.51", "200")])

    def test_immutable(self) -> None:
        book = self._book()
        with pytest.raises(ValidationError):
            book.bids = []  # type: ignore[misc]
