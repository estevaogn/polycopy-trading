"""Testes unit do RiskAgent — Gamma, repo, bus mockados via Protocol."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.risk import RiskAgent
from polycopy.domain.events import (
    OrderApproved,
    RejectionReason,
    TradeRejected,
    WalletTradeDetected,
)
from polycopy.domain.market import Market
from polycopy.domain.models import Side, Trade
from polycopy.domain.risk import RiskDecision
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import (
    CachedMarket,
    MarketRepository,
    PolymarketGammaPort,
    RiskDecisionRepository,
)

_VALID_COND = "0x" + "cd" * 32
_VALID_WALLET = "0x" + "1" * 40


def _trade(*, size_usdc: str = "10", price: str = "0.5") -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value=_VALID_WALLET),
        condition_id=ConditionId(value=_VALID_COND),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal(price)),
        size_usdc=Money.from_usdc(size_usdc),
        occurred_at=datetime.now(tz=UTC),
    )


def _wallet_trade_event(trade: Trade | None = None) -> WalletTradeDetected:
    return WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=trade if trade is not None else _trade(),
    )


def _market(
    *,
    is_active: bool = True,
    is_archived: bool = False,
    liquidity_usdc: str | None = "5000",
) -> Market:
    return Market(
        token_id=TokenId(value="42"),
        condition_id=ConditionId(value=_VALID_COND),
        question="?",
        slug="?",
        outcome="Yes",
        end_date=datetime.now(tz=UTC) + timedelta(days=14),
        is_active=is_active,
        is_archived=is_archived,
        volume_24h_usdc=Money.from_usdc("100000"),
        liquidity_usdc=Money.from_usdc(liquidity_usdc) if liquidity_usdc is not None else None,
    )


class _FakeCachedMarket:
    def __init__(self, market: Market, is_stale: bool = False) -> None:
        self.market = market
        self.last_synced_at = datetime.now(tz=UTC)
        self.is_stale = is_stale


class _StubRepoMarket:
    """MarketRepository stub. Configurável: cache_state."""

    def __init__(self, cached: CachedMarket | None) -> None:
        self._cached = cached
        self.upserts: list[list[Market]] = []

    async def upsert_many(self, markets: list[Market]) -> int:
        self.upserts.append(list(markets))
        return len(markets)

    async def get_market(self, token_id: TokenId) -> CachedMarket | None:
        return self._cached


class _StubGamma:
    """PolymarketGammaPort stub. Configurável: response."""

    def __init__(
        self,
        *,
        get_market_response: Market | None = None,
        raises: Exception | None = None,
    ) -> None:
        self._response = get_market_response
        self._raises = raises
        self.calls: list[TokenId] = []

    async def get_market(self, token_id: TokenId) -> Market | None:
        self.calls.append(token_id)
        if self._raises is not None:
            raise self._raises
        return self._response

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        return []


class _StubRepoDecision:
    """RiskDecisionRepository stub. Configurável: insert_returns_new."""

    def __init__(self, insert_returns_new: bool = True) -> None:
        self._returns_new = insert_returns_new
        self.inserted: list[RiskDecision] = []

    async def insert(self, decision: RiskDecision) -> bool:
        self.inserted.append(decision)
        return self._returns_new


class _StubBus:
    def __init__(self) -> None:
        self.approved: list[OrderApproved] = []
        self.rejected: list[TradeRejected] = []

    async def publish_wallet_trade_detected(self, event: object) -> None:
        return None

    async def publish_order_approved(self, event: OrderApproved) -> None:
        self.approved.append(event)

    async def publish_trade_rejected(self, event: TradeRejected) -> None:
        self.rejected.append(event)

    async def subscribe(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    async def close(self) -> None:
        return None


def _accepts_market_repo(_: MarketRepository) -> None: ...
def _accepts_gamma(_: PolymarketGammaPort) -> None: ...
def _accepts_decision_repo(_: RiskDecisionRepository) -> None: ...


@pytest.fixture
def metrics() -> Metrics:
    return make_metrics(registry=CollectorRegistry())


def _make_agent(
    *,
    metrics: Metrics,
    bus: _StubBus,
    gamma: _StubGamma,
    repo_decision: _StubRepoDecision,
    repo_market: _StubRepoMarket,
    copy_allowlist: frozenset[str] = frozenset(),
) -> RiskAgent:
    @asynccontextmanager
    async def _decision_factory() -> AsyncIterator[RiskDecisionRepository]:
        yield repo_decision

    @asynccontextmanager
    async def _market_factory() -> AsyncIterator[MarketRepository]:
        yield repo_market

    return RiskAgent(
        stopping=asyncio.Event(),
        bus=bus,
        gamma=gamma,
        decision_repo_factory=_decision_factory,
        market_repo_factory=_market_factory,
        max_trade_usdc=Decimal("100"),
        min_price=Decimal("0.05"),
        max_price=Decimal("0.95"),
        min_liquidity_usdc=Decimal("1000"),
        metrics=metrics,
        copy_allowlist=copy_allowlist,
    )


# ----- _evaluate cobertura: 5 regras + happy path -----


async def test_approve_when_all_rules_pass(metrics: Metrics) -> None:
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo_d.inserted) == 1
    assert repo_d.inserted[0].decision == "approved"
    assert repo_d.inserted[0].reason is None
    assert len(bus.approved) == 1
    assert len(bus.rejected) == 0
    # I-1: occurred_at do evento publicado preserva o do evento original
    # (não é sobrescrito por decided_at). decided_at vai num campo separado.
    assert bus.approved[0].occurred_at == event.occurred_at
    assert bus.approved[0].decided_at == repo_d.inserted[0].decided_at


async def test_approve_when_wallet_in_allowlist(metrics: Metrics) -> None:
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    agent = _make_agent(
        metrics=metrics,
        bus=bus,
        gamma=gamma,
        repo_decision=repo_d,
        repo_market=repo_m,
        copy_allowlist=frozenset({_VALID_WALLET.lower()}),
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert repo_d.inserted[0].decision == "approved"
    assert len(bus.approved) == 1


async def test_reject_when_wallet_not_in_allowlist(metrics: Metrics) -> None:
    """Wallet fora da allowlist é rejeitada sem fetch de market (fail-fast)."""
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    other_wallet = "0x" + "2" * 40
    agent = _make_agent(
        metrics=metrics,
        bus=bus,
        gamma=gamma,
        repo_decision=repo_d,
        repo_market=repo_m,
        copy_allowlist=frozenset({other_wallet.lower()}),
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert repo_d.inserted[0].reason == RejectionReason.WALLET_NOT_IN_ALLOWLIST
    assert len(bus.rejected) == 1
    assert bus.rejected[0].reason == RejectionReason.WALLET_NOT_IN_ALLOWLIST
    # Fail-fast: nenhum lookup de market (sem I/O Gamma, sem upsert no repo).
    assert gamma.calls == []
    assert repo_m.upserts == []


async def test_allowlist_matches_mixed_case_wallet(metrics: Metrics) -> None:
    """Wallet do trade vem mixed-case; agent normaliza com .lower() antes do match."""
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    mixed_case = "0x" + "Ab" * 20  # 40 chars, mixed case
    trade_mixed = Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value=mixed_case),
        condition_id=ConditionId(value=_VALID_COND),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )
    agent = _make_agent(
        metrics=metrics,
        bus=bus,
        gamma=gamma,
        repo_decision=repo_d,
        repo_market=repo_m,
        copy_allowlist=frozenset({mixed_case.lower()}),
    )

    await agent._handle_message(_wallet_trade_event(trade_mixed).model_dump_json().encode(), 1)

    assert repo_d.inserted[0].decision == "approved"


async def test_reject_size_exceeded(metrics: Metrics) -> None:
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event(_trade(size_usdc="500"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert repo_d.inserted[0].decision == "rejected"
    assert repo_d.inserted[0].reason == RejectionReason.SIZE_EXCEEDED
    assert len(bus.rejected) == 1
    assert bus.rejected[0].reason == RejectionReason.SIZE_EXCEEDED


async def test_reject_market_inactive(metrics: Metrics) -> None:
    market = _market(is_active=False)
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert repo_d.inserted[0].reason == RejectionReason.MARKET_INACTIVE


async def test_reject_price_out_of_range(metrics: Metrics) -> None:
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event(_trade(price="0.99"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert repo_d.inserted[0].reason == RejectionReason.PRICE_OUT_OF_RANGE


async def test_reject_insufficient_liquidity(metrics: Metrics) -> None:
    market = _market(liquidity_usdc="500")
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert repo_d.inserted[0].reason == RejectionReason.INSUFFICIENT_LIQUIDITY


# ----- _fetch_market: 5 cenários cache+gamma -----


async def test_fetch_market_cache_hit_fresh(metrics: Metrics) -> None:
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()  # nunca chamado
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market, is_stale=False))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(gamma.calls) == 0
    assert repo_d.inserted[0].decision == "approved"


async def test_fetch_market_stale_then_gamma_success(metrics: Metrics) -> None:
    fresh_market = _market()
    bus = _StubBus()
    gamma = _StubGamma(get_market_response=fresh_market)
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(_market(), is_stale=True))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(gamma.calls) == 1
    # upsert atualizou cache pra próxima
    assert len(repo_m.upserts) == 1
    assert repo_d.inserted[0].decision == "approved"


async def test_fetch_market_miss_then_gamma_success(metrics: Metrics) -> None:
    fresh_market = _market()
    bus = _StubBus()
    gamma = _StubGamma(get_market_response=fresh_market)
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=None)
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(gamma.calls) == 1
    assert len(repo_m.upserts) == 1
    assert repo_d.inserted[0].decision == "approved"


async def test_fetch_market_miss_gamma_fail_rejects_market_not_cached(metrics: Metrics) -> None:
    from polycopy.infrastructure.polymarket.gamma_client import PolymarketUnavailableError

    bus = _StubBus()
    gamma = _StubGamma(raises=PolymarketUnavailableError("boom"))
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=None)
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert repo_d.inserted[0].reason == RejectionReason.MARKET_NOT_CACHED
    assert len(bus.rejected) == 1


async def test_fetch_market_stale_gamma_fail_falls_back_to_stale(metrics: Metrics) -> None:
    """Cache stale + Gamma fail → aceita stale (fail-safe brando), decide normal."""
    from polycopy.infrastructure.polymarket.gamma_client import PolymarketUnavailableError

    market = _market()
    bus = _StubBus()
    gamma = _StubGamma(raises=PolymarketUnavailableError("boom"))
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market, is_stale=True))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    # Decisão usa o stale market — passa todas as regras (mesmo `_market()`)
    assert repo_d.inserted[0].decision == "approved"


# ----- Idempotência -----


async def test_idempotent_duplicate_skip_no_publish(metrics: Metrics) -> None:
    market = _market()
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision(insert_returns_new=False)  # simula PK conflict
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(bus.approved) == 0
    assert len(bus.rejected) == 0


# ----- Payload malformado -----


async def test_invalid_payload_is_silently_acked(metrics: Metrics) -> None:
    bus = _StubBus()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=None)
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    # Não lança exceção (se lançasse, JetStream redeliveria infinito)
    await agent._handle_message(b"not-valid-json", 1)
    assert len(repo_d.inserted) == 0
    assert len(bus.approved) == 0
    assert len(bus.rejected) == 0


async def test_bus_publish_failure_propagates_after_persist(metrics: Metrics) -> None:
    """Invariante crítico: persist commit antes de publish.

    Se publish falha, exceção propaga pro durable wrapper (que NÃO acka,
    causando redelivery). Decisão JÁ foi gravada no DB — re-delivery vê
    is_new=False e skipa publish (caveat known: evento pode ser perdido
    se Risk crash entre persist e publish; documentado na spec §11).
    """
    market = _market()
    gamma = _StubGamma()
    repo_d = _StubRepoDecision()
    repo_m = _StubRepoMarket(cached=_FakeCachedMarket(market))

    class _BusFailsOnPublish(_StubBus):
        async def publish_order_approved(self, event: OrderApproved) -> None:
            raise RuntimeError("simulated bus down")

    bus = _BusFailsOnPublish()
    agent = _make_agent(
        metrics=metrics, bus=bus, gamma=gamma, repo_decision=repo_d, repo_market=repo_m
    )

    event = _wallet_trade_event()  # trade aprovado (size, price, liquidez OK)
    with pytest.raises(RuntimeError, match="simulated bus down"):
        await agent._handle_message(event.model_dump_json().encode(), 1)

    # Persist aconteceu ANTES do publish que falhou
    assert len(repo_d.inserted) == 1
    assert repo_d.inserted[0].decision == "approved"
    # Bus não conseguiu publicar
    assert len(bus.approved) == 0
