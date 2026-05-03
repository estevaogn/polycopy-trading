"""Testes unit do ExecutorAgent — bus, repo, executor mockados via Protocol."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.executor import ExecutorAgent
from polycopy.domain.events import (
    ExecutionMode,
    FailureReason,
    OrderDryRun,
    OrderExecuted,
    OrderFailed,
    OrderSized,
)
from polycopy.domain.execution import ExecutionResult, OrderExecution
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import OrderExecutionRepository, OrderExecutor

_VALID_COND = "0x" + "cd" * 32
_VALID_WALLET = "0x" + "1" * 40
_VALID_TX_HASH = "0x" + "ef" * 32


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


def _order_sized_event(
    trade: Trade | None = None,
    *,
    final_size_usdc: str = "10",
) -> OrderSized:
    real_trade = trade if trade is not None else _trade()
    return OrderSized(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=real_trade,
        final_size_usdc=Money.from_usdc(final_size_usdc),
        original_size_usdc=real_trade.size_usdc,
    )


class _StubExecutionRepo:
    """OrderExecutionRepository stub. Configurável: insert_returns_new."""

    def __init__(self, insert_returns_new: bool = True) -> None:
        self._returns_new = insert_returns_new
        self.inserted: list[OrderExecution] = []

    async def insert(self, execution: OrderExecution) -> bool:
        self.inserted.append(execution)
        return self._returns_new


class _StubBus:
    def __init__(self) -> None:
        self.dry_run: list[OrderDryRun] = []
        self.executed: list[OrderExecuted] = []
        self.failed: list[OrderFailed] = []

    async def publish_wallet_trade_detected(self, event: object) -> None:
        return None

    async def publish_order_approved(self, event: object) -> None:
        return None

    async def publish_trade_rejected(self, event: object) -> None:
        return None

    async def publish_order_sized(self, event: object) -> None:
        return None

    async def publish_order_skipped(self, event: object) -> None:
        return None

    async def publish_order_dry_run(self, event: OrderDryRun) -> None:
        self.dry_run.append(event)

    async def publish_order_executed(self, event: OrderExecuted) -> None:
        self.executed.append(event)

    async def publish_order_failed(self, event: OrderFailed) -> None:
        self.failed.append(event)

    async def subscribe(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    async def close(self) -> None:
        return None


class _StubExecutorDryRun:
    """Executor que sempre retorna DRY_RUN success."""

    async def execute(
        self,
        trade: Trade,  # noqa: ARG002
        final_size_usdc: Decimal,  # noqa: ARG002
    ) -> ExecutionResult:
        return ExecutionResult(
            mode=ExecutionMode.DRY_RUN,
            success=True,
            tx_hash=None,
            gas_wei=None,
            failure_reason=None,
            error_message=None,
        )


class _StubExecutorRealSuccess:
    """Executor REAL que sempre retorna executed com tx_hash + gas_wei."""

    def __init__(
        self,
        *,
        tx_hash: str = _VALID_TX_HASH,
        gas_wei: int = 100_000_000,
    ) -> None:
        self._tx_hash = tx_hash
        self._gas_wei = gas_wei

    async def execute(
        self,
        trade: Trade,  # noqa: ARG002
        final_size_usdc: Decimal,  # noqa: ARG002
    ) -> ExecutionResult:
        return ExecutionResult(
            mode=ExecutionMode.REAL,
            success=True,
            tx_hash=self._tx_hash,
            gas_wei=self._gas_wei,
            failure_reason=None,
            error_message=None,
        )


class _StubExecutorRealFailure:
    """Executor REAL que sempre retorna failed (reason + error_message)."""

    def __init__(
        self,
        *,
        reason: FailureReason = FailureReason.INVALID_TRADE_PARAMS,
        error_message: str = "simulated real-mode failure",
    ) -> None:
        self._reason = reason
        self._error_message = error_message

    async def execute(
        self,
        trade: Trade,  # noqa: ARG002
        final_size_usdc: Decimal,  # noqa: ARG002
    ) -> ExecutionResult:
        return ExecutionResult(
            mode=ExecutionMode.REAL,
            success=False,
            tx_hash=None,
            gas_wei=None,
            failure_reason=self._reason,
            error_message=self._error_message,
        )


class _StubExecutorRaises:
    """Executor que lança exceção arbitrária — agente deve capturar e virar OrderFailed."""

    def __init__(self, *, exc: Exception | None = None) -> None:
        self._exc = exc if exc is not None else RuntimeError("boom")

    async def execute(
        self,
        trade: Trade,  # noqa: ARG002
        final_size_usdc: Decimal,  # noqa: ARG002
    ) -> ExecutionResult:
        raise self._exc


def _accepts_execution_repo(_: OrderExecutionRepository) -> None: ...


def _accepts_executor(_: OrderExecutor) -> None: ...


@pytest.fixture
def metrics() -> Metrics:
    return make_metrics(registry=CollectorRegistry())


def _make_agent(
    *,
    metrics: Metrics,
    bus: _StubBus,
    repo: _StubExecutionRepo,
    executor: OrderExecutor,
    dry_run: bool = True,
) -> ExecutorAgent:
    @asynccontextmanager
    async def _factory() -> AsyncIterator[OrderExecutionRepository]:
        yield repo

    return ExecutorAgent(
        stopping=asyncio.Event(),
        bus=bus,
        executor=executor,
        repo_factory=_factory,
        metrics=metrics,
        dry_run=dry_run,
    )


# ----- _result_label: pure mapping logic -----


def test_select_publish_dry_run_when_dry_run_mode(metrics: Metrics) -> None:
    """ExecutionMode.DRY_RUN sempre mapeia pra label 'dry_run'."""
    agent = _make_agent(
        metrics=metrics,
        bus=_StubBus(),
        repo=_StubExecutionRepo(),
        executor=_StubExecutorDryRun(),
        dry_run=True,
    )
    result = ExecutionResult(mode=ExecutionMode.DRY_RUN, success=True)
    assert agent._result_label(result) == "dry_run"


def test_select_publish_executed_when_real_success(metrics: Metrics) -> None:
    """ExecutionMode.REAL + success=True mapeia pra 'executed'."""
    agent = _make_agent(
        metrics=metrics,
        bus=_StubBus(),
        repo=_StubExecutionRepo(),
        executor=_StubExecutorRealSuccess(),
        dry_run=False,
    )
    result = ExecutionResult(
        mode=ExecutionMode.REAL,
        success=True,
        tx_hash=_VALID_TX_HASH,
        gas_wei=100_000_000,
    )
    assert agent._result_label(result) == "executed"


def test_select_publish_failed_when_real_failure(metrics: Metrics) -> None:
    """ExecutionMode.REAL + success=False mapeia pra 'failed'."""
    agent = _make_agent(
        metrics=metrics,
        bus=_StubBus(),
        repo=_StubExecutionRepo(),
        executor=_StubExecutorRealFailure(),
        dry_run=False,
    )
    result = ExecutionResult(
        mode=ExecutionMode.REAL,
        success=False,
        failure_reason=FailureReason.INVALID_TRADE_PARAMS,
        error_message="x",
    )
    assert agent._result_label(result) == "failed"


# ----- _handle_message: end-to-end happy paths -----


async def test_handle_message_dry_run_happy_path(metrics: Metrics) -> None:
    """Dry-run: executor stub → persist (mode=dry_run, result=dry_run) + publish order.dry_run."""
    bus = _StubBus()
    repo = _StubExecutionRepo()
    agent = _make_agent(
        metrics=metrics, bus=bus, repo=repo, executor=_StubExecutorDryRun(), dry_run=True
    )

    event = _order_sized_event(_trade(size_usdc="100"), final_size_usdc="10")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo.inserted) == 1
    persisted = repo.inserted[0]
    assert persisted.mode == ExecutionMode.DRY_RUN
    assert persisted.result == "dry_run"
    assert persisted.tx_hash is None
    assert persisted.gas_wei is None
    assert persisted.failure_reason is None
    assert persisted.error_message is None
    assert persisted.final_size_usdc == Decimal("10")
    assert persisted.trade_event_id == event.event_id

    assert len(bus.dry_run) == 1
    assert len(bus.executed) == 0
    assert len(bus.failed) == 0
    published = bus.dry_run[0]
    assert published.event_id == event.event_id
    assert published.occurred_at == event.occurred_at
    assert published.decided_at == persisted.decided_at
    assert published.trade == event.trade
    assert published.final_size_usdc.amount == Decimal("10")


async def test_handle_message_executed_happy_path(metrics: Metrics) -> None:
    """Real-mode success: executor returns tx_hash + gas_wei → publish order.executed
    + observa gas_wei metric."""
    bus = _StubBus()
    repo = _StubExecutionRepo()
    registry = CollectorRegistry()
    metrics_local = make_metrics(registry=registry)
    agent = _make_agent(
        metrics=metrics_local,
        bus=bus,
        repo=repo,
        executor=_StubExecutorRealSuccess(tx_hash=_VALID_TX_HASH, gas_wei=200_000_000),
        dry_run=False,
    )

    event = _order_sized_event(final_size_usdc="10")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo.inserted) == 1
    persisted = repo.inserted[0]
    assert persisted.mode == ExecutionMode.REAL
    assert persisted.result == "executed"
    assert persisted.tx_hash == _VALID_TX_HASH
    assert persisted.gas_wei == 200_000_000
    assert persisted.failure_reason is None

    assert len(bus.executed) == 1
    assert len(bus.dry_run) == 0
    assert len(bus.failed) == 0
    published = bus.executed[0]
    assert published.event_id == event.event_id
    assert published.tx_hash == _VALID_TX_HASH
    assert published.gas_wei == 200_000_000
    assert published.occurred_at == event.occurred_at
    assert published.decided_at == persisted.decided_at

    # Gas metric observado
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_gas_wei"]
    sample_count = sum(
        s.value for fam in matching for s in fam.samples if s.name.endswith("_count")
    )
    assert sample_count == 1


async def test_handle_message_failed_happy_path(metrics: Metrics) -> None:
    """Real-mode failure: executor returns failed → publish order.failed."""
    bus = _StubBus()
    repo = _StubExecutionRepo()
    agent = _make_agent(
        metrics=metrics,
        bus=bus,
        repo=repo,
        executor=_StubExecutorRealFailure(
            reason=FailureReason.INVALID_TRADE_PARAMS,
            error_message="bad params",
        ),
        dry_run=False,
    )

    event = _order_sized_event(final_size_usdc="5")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo.inserted) == 1
    persisted = repo.inserted[0]
    assert persisted.mode == ExecutionMode.REAL
    assert persisted.result == "failed"
    assert persisted.failure_reason == FailureReason.INVALID_TRADE_PARAMS
    assert persisted.error_message == "bad params"
    assert persisted.tx_hash is None
    assert persisted.gas_wei is None

    assert len(bus.failed) == 1
    assert len(bus.dry_run) == 0
    assert len(bus.executed) == 0
    published = bus.failed[0]
    assert published.event_id == event.event_id
    assert published.reason == FailureReason.INVALID_TRADE_PARAMS
    assert published.error_message == "bad params"
    assert published.occurred_at == event.occurred_at
    assert published.decided_at == persisted.decided_at


async def test_handle_message_executor_raises_persists_failed(metrics: Metrics) -> None:
    """Executor lança exception em real-mode → agent constrói failed result
    com EXECUTOR_DISABLED + str(exc), mode=REAL."""
    bus = _StubBus()
    repo = _StubExecutionRepo()
    agent = _make_agent(
        metrics=metrics,
        bus=bus,
        repo=repo,
        executor=_StubExecutorRaises(exc=RuntimeError("network down")),
        dry_run=False,
    )

    event = _order_sized_event(final_size_usdc="10")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo.inserted) == 1
    persisted = repo.inserted[0]
    assert persisted.mode == ExecutionMode.REAL
    assert persisted.result == "failed"
    assert persisted.failure_reason == FailureReason.EXECUTOR_DISABLED
    assert persisted.error_message is not None
    assert "network down" in persisted.error_message

    assert len(bus.failed) == 1
    assert len(bus.executed) == 0
    assert len(bus.dry_run) == 0
    published = bus.failed[0]
    assert published.reason == FailureReason.EXECUTOR_DISABLED
    assert "network down" in published.error_message


async def test_handle_message_dry_run_executor_raises_persists_dry_run_failed(
    metrics: Metrics,
) -> None:
    """C-1 fix: dry_run mode + executor raises → mode=DRY_RUN, result=failed.

    Antes da correção (skeleton original do plano), forçava mode=REAL — mentira
    no audit trail. Agora a invariante (relaxada na migration 0006) permite
    (mode=DRY_RUN, result=failed) e o agente escolhe o mode pelo flag self._dry_run.
    """
    bus = _StubBus()
    repo = _StubExecutionRepo()
    agent = _make_agent(
        metrics=metrics,
        bus=bus,
        repo=repo,
        executor=_StubExecutorRaises(exc=RuntimeError("dry-run boom")),
        dry_run=True,
    )

    event = _order_sized_event(final_size_usdc="10")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    assert len(repo.inserted) == 1
    persisted = repo.inserted[0]
    assert persisted.mode == ExecutionMode.DRY_RUN
    assert persisted.result == "failed"
    assert persisted.failure_reason == FailureReason.EXECUTOR_DISABLED
    assert persisted.error_message is not None
    assert "dry-run boom" in persisted.error_message

    # Bus recebe order.failed (não order.dry_run, porque _result_label é "failed")
    assert len(bus.failed) == 1
    assert len(bus.executed) == 0
    assert len(bus.dry_run) == 0
    assert bus.failed[0].reason == FailureReason.EXECUTOR_DISABLED
    assert "dry-run boom" in bus.failed[0].error_message


# ----- Idempotência -----


async def test_handle_message_idempotent_skip(metrics: Metrics) -> None:
    """Repo retorna False (duplicate) → não publish nada."""
    bus = _StubBus()
    repo = _StubExecutionRepo(insert_returns_new=False)
    agent = _make_agent(
        metrics=metrics, bus=bus, repo=repo, executor=_StubExecutorDryRun(), dry_run=True
    )

    event = _order_sized_event(final_size_usdc="10")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    # Insert tentado, mas publish NÃO acontece
    assert len(repo.inserted) == 1
    assert len(bus.dry_run) == 0
    assert len(bus.executed) == 0
    assert len(bus.failed) == 0


# ----- Payload malformado -----


async def test_handle_message_invalid_payload_silent_ack(metrics: Metrics) -> None:
    """Poison message: validação falha → ack silencioso, sem persist nem publish."""
    bus = _StubBus()
    repo = _StubExecutionRepo()
    agent = _make_agent(
        metrics=metrics, bus=bus, repo=repo, executor=_StubExecutorDryRun(), dry_run=True
    )

    await agent._handle_message(b"not-valid-json", 1)
    assert len(repo.inserted) == 0
    assert len(bus.dry_run) == 0
    assert len(bus.executed) == 0
    assert len(bus.failed) == 0


# ----- Invariante I-6: persist antes de publish -----


async def test_handle_message_bus_publish_failure_propagates_after_persist(
    metrics: Metrics,
) -> None:
    """Invariante I-6: persist commit antes de publish. Se publish falha, exceção
    propaga pro durable wrapper (que NÃO acka, causando redelivery). Decisão JÁ
    foi gravada no DB → re-delivery vê is_new=False e skipa publish."""
    repo = _StubExecutionRepo()

    class _BusFailsOnDryRun(_StubBus):
        async def publish_order_dry_run(self, event: OrderDryRun) -> None:
            raise RuntimeError("simulated bus down")

    bus = _BusFailsOnDryRun()
    agent = _make_agent(
        metrics=metrics, bus=bus, repo=repo, executor=_StubExecutorDryRun(), dry_run=True
    )

    event = _order_sized_event(final_size_usdc="10")
    with pytest.raises(RuntimeError, match="simulated bus down"):
        await agent._handle_message(event.model_dump_json().encode(), 1)

    # Persist aconteceu ANTES do publish que falhou
    assert len(repo.inserted) == 1
    assert repo.inserted[0].result == "dry_run"
    # Bus não conseguiu publicar
    assert len(bus.dry_run) == 0


# ----- Métricas -----


async def test_handle_message_observes_decision_duration(metrics: Metrics) -> None:
    """Cada decisão observa o duration histogram (no finally)."""
    bus = _StubBus()
    repo = _StubExecutionRepo()
    registry = CollectorRegistry()
    metrics_local = make_metrics(registry=registry)
    agent = _make_agent(
        metrics=metrics_local,
        bus=bus,
        repo=repo,
        executor=_StubExecutorDryRun(),
        dry_run=True,
    )

    event = _order_sized_event(final_size_usdc="10")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_decision_duration_seconds"]
    sample_count = sum(
        s.value for fam in matching for s in fam.samples if s.name.endswith("_count")
    )
    assert sample_count == 1


async def test_handle_message_dry_run_does_not_observe_gas(metrics: Metrics) -> None:
    """Dry-run NÃO observa gas_wei (gas só em real-mode com result=executed)."""
    bus = _StubBus()
    repo = _StubExecutionRepo()
    registry = CollectorRegistry()
    metrics_local = make_metrics(registry=registry)
    agent = _make_agent(
        metrics=metrics_local,
        bus=bus,
        repo=repo,
        executor=_StubExecutorDryRun(),
        dry_run=True,
    )

    event = _order_sized_event(final_size_usdc="10")
    await agent._handle_message(event.model_dump_json().encode(), 1)

    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_gas_wei"]
    sample_count = sum(
        s.value for fam in matching for s in fam.samples if s.name.endswith("_count")
    )
    assert sample_count == 0


# ----- Type-check helpers (Protocol satisfaction) -----


def test_stub_repo_satisfies_order_execution_repo_protocol() -> None:
    """Compile-time guarantee: _StubExecutionRepo implementa o Protocol."""
    _accepts_execution_repo(_StubExecutionRepo())


def test_stub_executors_satisfy_order_executor_protocol() -> None:
    """Compile-time guarantee: stubs implementam OrderExecutor Protocol."""
    _accepts_executor(_StubExecutorDryRun())
    _accepts_executor(_StubExecutorRealSuccess())
    _accepts_executor(_StubExecutorRealFailure())
    _accepts_executor(_StubExecutorRaises())


# ----- main() safety gate -----


def _stub_main_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub side effects de main() (logging, metrics server, db engine) para que
    os testes de safety gate não abram portas, conexões ou sessions reais."""
    from polycopy.agents import executor as executor_module
    from polycopy.infrastructure.observability import logging as _logging_mod
    from polycopy.infrastructure.persistence import database as _db_mod

    monkeypatch.setattr(executor_module, "make_metrics", lambda: object())
    monkeypatch.setattr(
        executor_module, "start_metrics_server", lambda *_, **__: (object(), object())
    )
    monkeypatch.setattr(_logging_mod, "configure_logging", lambda **_: None)
    monkeypatch.setattr(_db_mod, "make_engine", lambda _settings: object())
    monkeypatch.setattr(_db_mod, "make_session_factory", lambda _engine: object())


async def test_main_raises_when_real_mode_without_confirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Triple safety gate 1: dry_run=false + real_mode_confirmed=false → raise."""
    from polycopy.agents.executor import main

    monkeypatch.setenv("EXECUTOR_DRY_RUN", "false")
    monkeypatch.setenv("EXECUTOR_REAL_MODE_CONFIRMED", "false")
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    _stub_main_side_effects(monkeypatch)

    with pytest.raises(RuntimeError, match="EXECUTOR_REAL_MODE_CONFIRMED"):
        await main()


async def test_main_raises_when_real_mode_without_private_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Triple safety gate 2: real-mode confirmed sem WALLET_PRIVATE_KEY → raise."""
    from polycopy.agents.executor import main

    monkeypatch.setenv("EXECUTOR_DRY_RUN", "false")
    monkeypatch.setenv("EXECUTOR_REAL_MODE_CONFIRMED", "true")
    # WALLET_PRIVATE_KEY ausente
    monkeypatch.delenv("WALLET_PRIVATE_KEY", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    _stub_main_side_effects(monkeypatch)

    with pytest.raises(RuntimeError, match="WALLET_PRIVATE_KEY"):
        await main()


async def test_main_dry_run_default_does_not_require_wallet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dry-run default: não exige wallet nem real_mode_confirmed.

    Não vamos rodar main() até o fim (precisa NATS+Postgres). Apenas validamos
    que o gate não dispara em dry-run.
    """
    monkeypatch.setenv("EXECUTOR_DRY_RUN", "true")
    monkeypatch.delenv("WALLET_PRIVATE_KEY", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    # Settings carrega sem erro — gate de dry_run não toca em real-mode checks.
    from polycopy.config import Settings

    settings = Settings()
    assert settings.executor_dry_run is True
    assert settings.wallet_private_key is None
    assert settings.executor_real_mode_confirmed is False
    # Esses 3 NÃO bloqueiam dry-run mode.


async def test_main_raises_when_allowance_insufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Triple safety gate 3: verify_allowance raise no startup → main() propaga."""
    from polycopy.agents.executor import main

    test_pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
    monkeypatch.setenv("EXECUTOR_DRY_RUN", "false")
    monkeypatch.setenv("EXECUTOR_REAL_MODE_CONFIRMED", "true")
    monkeypatch.setenv("WALLET_PRIVATE_KEY", test_pk)
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    # Stub side effects (start_metrics_server, make_engine, etc) — usar helper existente
    _stub_main_side_effects(monkeypatch)

    # Patch build_clob_client + verify_allowance pra forçar Gate 3 falhar
    async def _raises_insufficient(_settings, _min_usdc) -> None:
        raise RuntimeError("USDC allowance insufficient — run setup_wallet")

    from unittest.mock import MagicMock

    monkeypatch.setattr(
        "polycopy.infrastructure.execution.web3_clob_executor.build_clob_client",
        lambda _settings: MagicMock(),
    )
    monkeypatch.setattr(
        "polycopy.infrastructure.execution.web3_clob_executor.verify_allowance",
        _raises_insufficient,
    )

    with pytest.raises(RuntimeError, match="USDC allowance insufficient"):
        await main()
