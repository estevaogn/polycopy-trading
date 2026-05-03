"""Testes unit do SizingAgent — bus, repo mockados via Protocol."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.sizing import SizingAgent
from polycopy.domain.events import (
    OrderApproved,
    OrderSized,
    OrderSkipped,
    SkipReason,
)
from polycopy.domain.models import Side, Trade
from polycopy.domain.sizing import OrderSizing
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import OrderSizingRepository

_VALID_COND = "0x" + "cd" * 32
_VALID_WALLET = "0x" + "1" * 40


def _trade(*, size_usdc: str = "100", price: str = "0.5") -> Trade:
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


def _order_approved_event(trade: Trade | None = None) -> OrderApproved:
    return OrderApproved(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=trade if trade is not None else _trade(),
    )


class _StubRepoSizing:
    """OrderSizingRepository stub. Configurável: insert_returns_new."""

    def __init__(self, insert_returns_new: bool = True) -> None:
        self._returns_new = insert_returns_new
        self.inserted: list[OrderSizing] = []

    async def insert(self, sizing: OrderSizing) -> bool:
        self.inserted.append(sizing)
        return self._returns_new


class _StubBus:
    def __init__(self) -> None:
        self.sized: list[OrderSized] = []
        self.skipped: list[OrderSkipped] = []

    async def publish_wallet_trade_detected(self, event: object) -> None:
        return None

    async def publish_order_approved(self, event: object) -> None:
        return None

    async def publish_trade_rejected(self, event: object) -> None:
        return None

    async def publish_order_sized(self, event: OrderSized) -> None:
        self.sized.append(event)

    async def publish_order_skipped(self, event: OrderSkipped) -> None:
        self.skipped.append(event)

    async def subscribe(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    async def close(self) -> None:
        return None


def _accepts_sizing_repo(_: OrderSizingRepository) -> None: ...


@pytest.fixture
def metrics() -> Metrics:
    return make_metrics(registry=CollectorRegistry())


def _make_agent(
    *,
    metrics: Metrics,
    bus: _StubBus,
    repo_sizing: _StubRepoSizing,
    proportion_ratio: Decimal = Decimal("0.1"),
    max_size_usdc: Decimal = Decimal("50"),
    min_size_usdc: Decimal = Decimal("1"),
) -> SizingAgent:
    @asynccontextmanager
    async def _sizing_factory() -> AsyncIterator[OrderSizingRepository]:
        yield repo_sizing

    return SizingAgent(
        stopping=asyncio.Event(),
        bus=bus,
        repo_factory=_sizing_factory,
        proportion_ratio=proportion_ratio,
        max_size_usdc=max_size_usdc,
        min_size_usdc=min_size_usdc,
        metrics=metrics,
    )


# ----- _size: lógica pura (scaled → capped → floor check) -----


def test_size_happy_path_scaled(metrics: Metrics) -> None:
    """Trade 100 USDC * 0.1 = 10 USDC final (sem cap, acima de min)."""
    agent = _make_agent(
        metrics=metrics,
        bus=_StubBus(),
        repo_sizing=_StubRepoSizing(),
    )
    result = agent._size(_trade(size_usdc="100"))
    assert result.decision == "sized"
    assert result.final_size_usdc == Decimal("10.000000")
    assert result.reason is None


def test_size_capped_at_max(metrics: Metrics) -> None:
    """Trade 10000 USDC * 0.1 = 1000 USDC scaled → capeado em 50."""
    agent = _make_agent(
        metrics=metrics,
        bus=_StubBus(),
        repo_sizing=_StubRepoSizing(),
    )
    result = agent._size(_trade(size_usdc="10000"))
    assert result.decision == "sized"
    assert result.final_size_usdc == Decimal("50.000000")
    assert result.reason is None


def test_size_exactly_at_min(metrics: Metrics) -> None:
    """Trade 10 USDC * 0.1 = 1.0 USDC == min → sized (borderline aceito)."""
    agent = _make_agent(
        metrics=metrics,
        bus=_StubBus(),
        repo_sizing=_StubRepoSizing(),
    )
    result = agent._size(_trade(size_usdc="10"))
    assert result.decision == "sized"
    assert result.final_size_usdc == Decimal("1.000000")
    assert result.reason is None


def test_size_below_min_skipped(metrics: Metrics) -> None:
    """Trade 1 USDC * 0.1 = 0.1 USDC < min 1 → skipped."""
    agent = _make_agent(
        metrics=metrics,
        bus=_StubBus(),
        repo_sizing=_StubRepoSizing(),
    )
    result = agent._size(_trade(size_usdc="1"))
    assert result.decision == "skipped"
    assert result.final_size_usdc is None
    assert result.reason == SkipReason.BELOW_MIN_SIZE


# ----- _handle_message: fluxo end-to-end -----


async def test_handle_message_sized_flow(metrics: Metrics) -> None:
    """Happy path: trade 100 → persist sized + publish order.sized."""
    bus = _StubBus()
    repo_s = _StubRepoSizing()
    agent = _make_agent(metrics=metrics, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="100"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo_s.inserted) == 1
    assert repo_s.inserted[0].decision == "sized"
    assert repo_s.inserted[0].final_size_usdc == Decimal("10.000000")
    assert repo_s.inserted[0].reason is None
    assert len(bus.sized) == 1
    assert len(bus.skipped) == 0
    # I-1: occurred_at preservado do evento original
    assert bus.sized[0].occurred_at == event.occurred_at
    assert bus.sized[0].decided_at == repo_s.inserted[0].decided_at
    # event_id idempotência cross-agent
    assert bus.sized[0].event_id == event.event_id
    # Money envoltório
    assert bus.sized[0].final_size_usdc.amount == Decimal("10.000000")
    assert bus.sized[0].original_size_usdc.amount == event.trade.size_usdc.amount


async def test_handle_message_skipped_flow(metrics: Metrics) -> None:
    """Trade 1 USDC → scaled 0.1 < min 1 → persist skipped + publish order.skipped."""
    bus = _StubBus()
    repo_s = _StubRepoSizing()
    agent = _make_agent(metrics=metrics, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="1"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo_s.inserted) == 1
    assert repo_s.inserted[0].decision == "skipped"
    assert repo_s.inserted[0].final_size_usdc is None
    assert repo_s.inserted[0].reason == SkipReason.BELOW_MIN_SIZE
    assert len(bus.sized) == 0
    assert len(bus.skipped) == 1
    assert bus.skipped[0].reason == SkipReason.BELOW_MIN_SIZE
    assert bus.skipped[0].occurred_at == event.occurred_at
    assert bus.skipped[0].decided_at == repo_s.inserted[0].decided_at


# ----- Idempotência -----


async def test_handle_message_idempotent_duplicate_skip_no_publish(metrics: Metrics) -> None:
    """Re-delivery (insert returns False) → não re-publica."""
    bus = _StubBus()
    repo_s = _StubRepoSizing(insert_returns_new=False)
    agent = _make_agent(metrics=metrics, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="100"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    # Persist tentado (stub registra), mas publish NÃO acontece
    assert len(repo_s.inserted) == 1
    assert len(bus.sized) == 0
    assert len(bus.skipped) == 0


async def test_handle_message_idempotent_duplicate_skipped_no_publish(metrics: Metrics) -> None:
    """Re-delivery de skipped → não re-publica."""
    bus = _StubBus()
    repo_s = _StubRepoSizing(insert_returns_new=False)
    agent = _make_agent(metrics=metrics, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="1"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo_s.inserted) == 1
    assert len(bus.sized) == 0
    assert len(bus.skipped) == 0


# ----- Payload malformado -----


async def test_handle_message_invalid_payload_silent_ack(metrics: Metrics) -> None:
    """Poison message: validação falha → ack silencioso, sem persist nem publish."""
    bus = _StubBus()
    repo_s = _StubRepoSizing()
    agent = _make_agent(metrics=metrics, bus=bus, repo_sizing=repo_s)

    # Não lança exceção (se lançasse, JetStream redeliveria infinito)
    await agent._handle_message(b"not-valid-json", 1)
    assert len(repo_s.inserted) == 0
    assert len(bus.sized) == 0
    assert len(bus.skipped) == 0


# ----- Invariante I-6: persist antes de publish -----


async def test_handle_message_bus_publish_failure_propagates_after_persist(
    metrics: Metrics,
) -> None:
    """Invariante crítico: persist commit antes de publish.

    Se publish falha, exceção propaga pro durable wrapper (que NÃO acka,
    causando redelivery). Decisão JÁ foi gravada no DB — re-delivery vê
    is_new=False e skipa publish (mesma garantia documentada na spec 2B-T6 I-6).
    """
    repo_s = _StubRepoSizing()

    class _BusFailsOnPublish(_StubBus):
        async def publish_order_sized(self, event: OrderSized) -> None:
            raise RuntimeError("simulated bus down")

    bus = _BusFailsOnPublish()
    agent = _make_agent(metrics=metrics, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="100"))
    with pytest.raises(RuntimeError, match="simulated bus down"):
        await agent._handle_message(event.model_dump_json().encode(), 1)

    # Persist aconteceu ANTES do publish que falhou
    assert len(repo_s.inserted) == 1
    assert repo_s.inserted[0].decision == "sized"
    # Bus não conseguiu publicar
    assert len(bus.sized) == 0


async def test_handle_message_bus_publish_skipped_failure_propagates(metrics: Metrics) -> None:
    """Mesmo invariante I-6 pro caminho skipped."""
    repo_s = _StubRepoSizing()

    class _BusFailsOnPublishSkipped(_StubBus):
        async def publish_order_skipped(self, event: OrderSkipped) -> None:
            raise RuntimeError("simulated bus down")

    bus = _BusFailsOnPublishSkipped()
    agent = _make_agent(metrics=metrics, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="1"))
    with pytest.raises(RuntimeError, match="simulated bus down"):
        await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo_s.inserted) == 1
    assert repo_s.inserted[0].decision == "skipped"
    assert len(bus.skipped) == 0


# ----- Métricas -----


async def test_handle_message_observes_size_ratio_metric(metrics: Metrics) -> None:
    """Trade 100 → final 10 → ratio 0.1 observado no histograma."""
    bus = _StubBus()
    repo_s = _StubRepoSizing()
    registry = CollectorRegistry()
    metrics_local = make_metrics(registry=registry)
    agent = _make_agent(metrics=metrics_local, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="100"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_size_ratio_observed"]
    assert matching, "histograma sizing_size_ratio_observed não foi observado"
    # Verifica count == 1 (uma observação)
    sample_count = sum(
        s.value for fam in matching for s in fam.samples if s.name.endswith("_count")
    )
    assert sample_count == 1


async def test_handle_message_skipped_does_not_observe_ratio(metrics: Metrics) -> None:
    """Skipped não observa size_ratio (ratio só faz sentido pra sized)."""
    bus = _StubBus()
    repo_s = _StubRepoSizing()
    registry = CollectorRegistry()
    metrics_local = make_metrics(registry=registry)
    agent = _make_agent(metrics=metrics_local, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="1"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_size_ratio_observed"]
    sample_count = sum(
        s.value for fam in matching for s in fam.samples if s.name.endswith("_count")
    )
    assert sample_count == 0


async def test_handle_message_observes_decision_duration(metrics: Metrics) -> None:
    """Cada decisão (sized ou skipped) observa duration histogram."""
    bus = _StubBus()
    repo_s = _StubRepoSizing()
    registry = CollectorRegistry()
    metrics_local = make_metrics(registry=registry)
    agent = _make_agent(metrics=metrics_local, bus=bus, repo_sizing=repo_s)

    event = _order_approved_event(_trade(size_usdc="100"))
    await agent._handle_message(event.model_dump_json().encode(), 1)

    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_decision_duration_seconds"]
    sample_count = sum(
        s.value for fam in matching for s in fam.samples if s.name.endswith("_count")
    )
    assert sample_count == 1


# ----- Type-check helper (Protocol satisfaction) -----


def test_stub_repo_satisfies_order_sizing_repo_protocol() -> None:
    """Compile-time guarantee: _StubRepoSizing implementa o Protocol."""
    _accepts_sizing_repo(_StubRepoSizing())
