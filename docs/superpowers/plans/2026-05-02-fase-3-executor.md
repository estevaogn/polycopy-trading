# Plano 3 — Executor Agent (DRY-RUN MVP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Entregar `ExecutorAgent` em modo DRY-RUN — fecha pipeline E2E da Fase 2 sem mover dinheiro real. Real-mode (Web3.py + EIP-712) fica pra Fase 4.

**Architecture:** Agente novo (`AgentBase` + durable consumer JetStream em `order.sized`) → `OrderExecutor` Protocol (strategy pattern: `DryRunExecutor` MVP, `Web3CLOBExecutor` Fase 4) → persiste `OrderExecution` (idempotente PK) → publica `order.executed`/`order.failed`/`order.dry_run`.

**Tech Stack:** Mesmo da Fase 2 — Python 3.12, pydantic v2, SQLAlchemy 2 async, alembic, prometheus_client, NATS JetStream, pytest + asyncio.

**Predecessor:** Plano 2C completo (head `c3d761d` + spec Fase 3 `28f636a`).

**Spec:** `docs/superpowers/specs/2026-05-02-fase-3-executor-design.md`.

---

## File Structure

**Novos (10):**
- `src/polycopy/domain/execution.py` — `OrderExecution` value object + `ExecutionResult` dataclass.
- `src/polycopy/ports/order_execution_repository.py` — `OrderExecutionRepository` Protocol.
- `src/polycopy/ports/order_executor.py` — `OrderExecutor` Protocol.
- `src/polycopy/infrastructure/execution/__init__.py`
- `src/polycopy/infrastructure/execution/dry_run_executor.py` — `DryRunExecutor`.
- `src/polycopy/infrastructure/persistence/order_execution_repository.py` — `SqlAlchemyOrderExecutionRepository`.
- `alembic/versions/0005_add_order_executions.py`.
- `src/polycopy/agents/executor.py` — `ExecutorAgent` + main().
- `tests/unit/agents/test_executor.py`.
- `tests/unit/domain/test_execution_events.py`.
- `tests/unit/infrastructure/test_dry_run_executor.py`.
- `tests/integration/test_order_execution_repository.py`.
- `tests/integration/test_executor_e2e.py`.

**Modificados (8):**
- `src/polycopy/domain/events.py` — +`ExecutionMode`, `FailureReason`, `OrderExecuted`, `OrderFailed`, `OrderDryRun`.
- `src/polycopy/ports/messaging.py` — +3 publish methods.
- `src/polycopy/ports/__init__.py` — +exports.
- `src/polycopy/infrastructure/messaging/nats_bus.py` — +stream `EXECUTION_RESULTS` + 3 publishes.
- `src/polycopy/infrastructure/persistence/models.py` — +`OrderExecutionRow` ORM.
- `src/polycopy/config.py` — +4 settings.
- `src/polycopy/infrastructure/observability/metrics.py` — +3 métricas.
- `tests/unit/infrastructure/test_metrics.py` — +3 testes.
- `tests/unit/test_ports_typecheck.py` — +stubs + asserts.
- `tests/integration/test_jetstream_bus.py` — +6 testes pros 3 publishes.
- `docker-compose.yml` — +service `executor`.
- `infra/prometheus/prometheus.yml` — +scrape job.
- `ARCHITECTURE.md` — +seção Executor + nó Mermaid.
- `.env.example` — +bloco "Executor agent (Plano 3)".

---

## Task 1: Domain — events + enums + OrderExecution + ExecutionResult

**Files:**
- Modify: `src/polycopy/domain/events.py`
- Create: `src/polycopy/domain/execution.py`
- Create: `tests/unit/domain/test_execution_events.py`

---

- [ ] **Step 1.1: Adicionar enums + 3 events em `events.py`**

LEIA primeiro. Adicionar no fim:

```python
class ExecutionMode(StrEnum):
    """Modo de execução do ExecutorAgent."""

    REAL = "real"
    DRY_RUN = "dry_run"


class FailureReason(StrEnum):
    """Razões pelas quais Executor falha. Aberto pra extensão (Fase 4)."""

    INVALID_TRADE_PARAMS = "invalid_trade_params"
    EXECUTOR_DISABLED = "executor_disabled"


class OrderExecuted(BaseModel):
    """Evento publicado quando Executor submete trade real on-chain com sucesso.

    NATS subject: `order.executed`. `tx_hash` é a transação on-chain (Polygon).
    """

    SUBJECT: ClassVar[str] = "order.executed"
    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money
    tx_hash: str
    gas_wei: int

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v

    @field_validator("gas_wei", mode="after")
    @classmethod
    def _gas_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("gas_wei must be non-negative")
        return v


class OrderFailed(BaseModel):
    """Evento publicado quando Executor tenta submeter trade real e falha.

    NATS subject: `order.failed`. Inclui `reason` + `error_message` pra audit.
    """

    SUBJECT: ClassVar[str] = "order.failed"
    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money
    reason: FailureReason
    error_message: str

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v


class OrderDryRun(BaseModel):
    """Evento publicado quando Executor simula trade em modo dry-run.

    NATS subject: `order.dry_run`. Sem dados de tx — apenas snapshot do
    que teria sido feito.
    """

    SUBJECT: ClassVar[str] = "order.dry_run"
    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware_occurred(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @field_validator("decided_at", mode="after")
    @classmethod
    def _require_tzaware_decided(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        return v
```

- [ ] **Step 1.2: Criar `src/polycopy/domain/execution.py`**

```python
"""OrderExecution: value object interno de uma decisão de execução persistida.
ExecutionResult: dataclass intermediário retornado por OrderExecutor.execute().
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from polycopy.domain.events import ExecutionMode, FailureReason


@dataclass(frozen=True)
class ExecutionResult:
    """Retorno de OrderExecutor.execute(). Convertido em OrderExecution pelo agente."""

    mode: ExecutionMode
    success: bool
    tx_hash: str | None = None
    gas_wei: int | None = None
    failure_reason: FailureReason | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class OrderExecution:
    """Snapshot imutável de uma decisão de execução.

    Persistido em order_executions; PK = trade_event_id.

    Invariantes:
    - mode == REAL ↔ result ∈ {executed, failed}
    - mode == DRY_RUN ↔ result == "dry_run"
    - result == "executed" → tx_hash IS NOT NULL
    - result == "failed" → failure_reason IS NOT NULL AND error_message IS NOT NULL
    - result == "dry_run" → tx_hash IS NULL AND gas_wei IS NULL AND failure_reason IS NULL
    """

    trade_event_id: UUID
    wallet: str
    condition_id: str
    token_id: str
    final_size_usdc: Decimal
    mode: ExecutionMode
    result: Literal["executed", "failed", "dry_run"]
    tx_hash: str | None
    gas_wei: int | None
    failure_reason: FailureReason | None
    error_message: str | None
    decided_at: datetime

    def __post_init__(self) -> None:
        if self.mode == ExecutionMode.REAL:
            if self.result not in ("executed", "failed"):
                raise ValueError("real mode must produce executed or failed")
        else:  # DRY_RUN
            if self.result != "dry_run":
                raise ValueError("dry_run mode must produce result='dry_run'")

        if self.result == "executed":
            if self.tx_hash is None:
                raise ValueError("executed result must have tx_hash")
        if self.result == "failed":
            if self.failure_reason is None:
                raise ValueError("failed result must have failure_reason")
            if self.error_message is None:
                raise ValueError("failed result must have error_message")
        if self.result == "dry_run":
            if self.tx_hash is not None:
                raise ValueError("dry_run must have tx_hash=None")
            if self.gas_wei is not None:
                raise ValueError("dry_run must have gas_wei=None")
            if self.failure_reason is not None:
                raise ValueError("dry_run must have failure_reason=None")
        if self.final_size_usdc <= 0:
            raise ValueError("final_size_usdc must be positive")
        if self.gas_wei is not None and self.gas_wei < 0:
            raise ValueError("gas_wei must be non-negative")
        if self.decided_at.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
```

- [ ] **Step 1.3: Testes unit (RED → GREEN)**

`tests/unit/domain/test_execution_events.py` — ~20 testes:
- 3 events: tz-aware × 2 (occurred + decided), subject constants (3), gas non-negative (1), reason+error_message validation (1).
- 2 enums: values check.
- OrderExecution: 5 invariants × 2 (happy + raise paths) + size positive + gas non-negative + tz-aware.

Helper `_trade()` reusado.

- [ ] **Step 1.4: Verificações + commit**

```bash
uv run pytest tests/unit/domain/test_execution_events.py -v
uv run mypy src/polycopy
uv run ruff check ...
git add src/polycopy/domain/events.py src/polycopy/domain/execution.py tests/unit/domain/test_execution_events.py
git commit -m "feat(domain): add ExecutionMode, FailureReason, OrderExecuted, OrderFailed, OrderDryRun, OrderExecution"
```

---

## Task 2: Ports — `OrderExecutionRepository` + `OrderExecutor` + extensão `MessagingPort` + adapter NATS impl

**Escopo expandido (mesmo padrão 2B-T2/2C-T2):** estender Protocol + implementar adapter mínimo no `nats_bus.py`. Stream `EXECUTION_RESULTS` fica pra T5.

**Files:**
- Create: `src/polycopy/ports/order_execution_repository.py`
- Create: `src/polycopy/ports/order_executor.py`
- Modify: `src/polycopy/ports/messaging.py`
- Modify: `src/polycopy/ports/__init__.py`
- Modify: `src/polycopy/infrastructure/messaging/nats_bus.py`
- Modify: `tests/unit/test_ports_typecheck.py`

---

- [ ] **Step 2.1: Criar `order_execution_repository.py`**

```python
"""OrderExecutionRepository: contrato de persistência pra decisões de execução."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.execution import OrderExecution


class OrderExecutionRepository(Protocol):
    """Persistência idempotente. Plano 3."""

    async def insert(self, execution: OrderExecution) -> bool:
        """Insere; True se nova, False se duplicate (PK trade_event_id)."""
        ...
```

- [ ] **Step 2.2: Criar `order_executor.py`**

```python
"""OrderExecutor: strategy pattern pra execução de ordens (real ou dry-run)."""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade


class OrderExecutor(Protocol):
    """Strategy pra executar uma ordem. Implementações: DryRunExecutor (MVP),
    Web3CLOBExecutor (Fase 4)."""

    async def execute(self, trade: Trade, final_size_usdc: Decimal) -> ExecutionResult:
        """Executa (ou simula) a ordem. Retorna ExecutionResult com mode + outcome."""
        ...
```

- [ ] **Step 2.3: Estender `MessagingPort`**

Adicionar imports + 3 métodos publish (após `publish_order_skipped`, antes `subscribe`).

- [ ] **Step 2.4: Atualizar `ports/__init__.py`**

Adicionar `OrderExecutionRepository` + `OrderExecutor` ao `__all__`.

- [ ] **Step 2.5: Implementar 3 publishes em `nats_bus.py`**

Sem criar stream (vai pra T5). Padrão idêntico aos publishes existentes:

```python
    async def publish_order_executed(self, event: OrderExecuted) -> None:
        _, js = self._require_connected()
        payload = event.model_dump_json().encode("utf-8")
        await js.publish(
            OrderExecuted.SUBJECT,
            payload,
            headers={"Nats-Msg-Id": str(event.event_id)},
        )

    async def publish_order_failed(self, event: OrderFailed) -> None:
        _, js = self._require_connected()
        payload = event.model_dump_json().encode("utf-8")
        await js.publish(
            OrderFailed.SUBJECT,
            payload,
            headers={"Nats-Msg-Id": str(event.event_id)},
        )

    async def publish_order_dry_run(self, event: OrderDryRun) -> None:
        _, js = self._require_connected()
        payload = event.model_dump_json().encode("utf-8")
        await js.publish(
            OrderDryRun.SUBJECT,
            payload,
            headers={"Nats-Msg-Id": str(event.event_id)},
        )
```

- [ ] **Step 2.6: Estender `test_ports_typecheck.py`**

Imports + estender `_FakeMessaging` com 3 publishes + criar `_FakeOrderExecutionRepo` + `_FakeOrderExecutor` + 2 helpers + 2 testes.

- [ ] **Step 2.7: Verificações + commit**

```bash
uv run mypy src/polycopy
uv run pytest tests/unit/test_ports_typecheck.py -v
git add src/polycopy/ports/order_execution_repository.py src/polycopy/ports/order_executor.py src/polycopy/ports/messaging.py src/polycopy/ports/__init__.py src/polycopy/infrastructure/messaging/nats_bus.py tests/unit/test_ports_typecheck.py
git commit -m "feat(ports): add OrderExecutionRepository, OrderExecutor, extend MessagingPort with execution publishes"
```

---

## Task 3: Tabela `order_executions` + migration alembic + ORM

**Files:**
- Modify: `src/polycopy/infrastructure/persistence/models.py`
- Create: `alembic/versions/0005_add_order_executions.py`

---

- [ ] **Step 3.1: `OrderExecutionRow` ORM**

```python
class OrderExecutionRow(Base):
    __tablename__ = "order_executions"

    trade_event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, nullable=False)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    token_id: Mapped[str] = mapped_column(String, nullable=False)
    final_size_usdc: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    result: Mapped[str] = mapped_column(String, nullable=False)
    tx_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    gas_wei: Mapped[Decimal | None] = mapped_column(Numeric(40, 0), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "mode IN ('real', 'dry_run')",
            name="order_executions_mode_enum",
        ),
        CheckConstraint(
            "result IN ('executed', 'failed', 'dry_run')",
            name="order_executions_result_enum",
        ),
        CheckConstraint(
            "(mode = 'real' AND result IN ('executed', 'failed')) "
            "OR (mode = 'dry_run' AND result = 'dry_run')",
            name="order_executions_mode_result_consistency",
        ),
        CheckConstraint(
            "(result = 'executed' AND tx_hash IS NOT NULL) "
            "OR result IN ('failed', 'dry_run')",
            name="order_executions_executed_has_tx",
        ),
        CheckConstraint(
            "(result = 'failed' AND failure_reason IS NOT NULL AND error_message IS NOT NULL) "
            "OR result IN ('executed', 'dry_run')",
            name="order_executions_failed_has_reason",
        ),
        CheckConstraint(
            "(result = 'dry_run' AND tx_hash IS NULL AND gas_wei IS NULL AND failure_reason IS NULL) "
            "OR result IN ('executed', 'failed')",
            name="order_executions_dry_run_no_tx",
        ),
        CheckConstraint(
            "final_size_usdc > 0",
            name="order_executions_size_positive",
        ),
        Index("idx_order_executions_wallet_decided_at", "wallet", "decided_at", postgresql_using="btree"),
        Index(
            "idx_order_executions_failed_decided_at",
            "decided_at",
            postgresql_where="result = 'failed'",
            postgresql_using="btree",
        ),
        Index(
            "idx_order_executions_real_executed",
            "decided_at",
            postgresql_where="mode = 'real' AND result = 'executed'",
            postgresql_using="btree",
        ),
    )
```

- [ ] **Step 3.2: Migration `0005`**

`upgrade()` cria tabela + 7 CHECKs + PK + 3 indexes (2 parciais). `downgrade()` simétrico (drop indexes + drop_table).

- [ ] **Step 3.3: Validar alembic + commit**

```bash
docker compose ps postgres
uv run alembic upgrade head
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d order_executions"
uv run alembic downgrade -1
uv run alembic upgrade head
git add src/polycopy/infrastructure/persistence/models.py alembic/versions/0005_add_order_executions.py
git commit -m "feat(persistence): add order_executions table and OrderExecutionRow ORM"
```

---

## Task 4: `SqlAlchemyOrderExecutionRepository` + integration tests

**Files:**
- Create: `src/polycopy/infrastructure/persistence/order_execution_repository.py`
- Create: `tests/integration/test_order_execution_repository.py`

---

- [ ] **Step 4.1: Testes integration (RED)**

6 testes:
1. `test_insert_dry_run_returns_true`
2. `test_insert_duplicate_returns_false`
3. `test_insert_executed_persists_tx_hash`
4. `test_insert_failed_persists_reason_and_error`
5. `test_insert_real_with_dry_run_result_violates_constraint` (SQL cru)
6. `test_adapter_satisfies_protocol`

Helpers `_execution_dry_run()`, `_execution_executed()`, `_execution_failed()`.

- [ ] **Step 4.2: Implementar repository**

Padrão idêntico aos outros repos: `pg_insert(...).on_conflict_do_nothing(index_elements=["trade_event_id"])` + `result.rowcount == 1`.

- [ ] **Step 4.3: Verificações + commit**

```bash
uv run pytest tests/integration/test_order_execution_repository.py -v
git add ...
git commit -m "feat(persistence): add SqlAlchemyOrderExecutionRepository with PK-based idempotency"
```

---

## Task 5: Stream `EXECUTION_RESULTS` + `DryRunExecutor` + 6 integration tests (REVIEWER OBRIGATÓRIO)

**Files:**
- Modify: `src/polycopy/infrastructure/messaging/nats_bus.py`
- Create: `src/polycopy/infrastructure/execution/__init__.py`
- Create: `src/polycopy/infrastructure/execution/dry_run_executor.py`
- Create: `tests/unit/infrastructure/test_dry_run_executor.py`
- Modify: `tests/integration/test_jetstream_bus.py`

---

- [ ] **Step 5.1: Stream `EXECUTION_RESULTS` em `nats_bus.py`**

Constantes:
```python
_EXECUTION_STREAM_NAME = "EXECUTION_RESULTS"
_EXECUTION_STREAM_SUBJECTS = ["order.executed", "order.failed", "order.dry_run"]
```

Adicionar 4ª config no loop de `_ensure_streams` usando a factory `_make_stream_config`:
```python
configs = [
    self._make_stream_config(_STREAM_NAME, _STREAM_SUBJECTS),
    self._make_stream_config(_RISK_STREAM_NAME, _RISK_STREAM_SUBJECTS),
    self._make_stream_config(_SIZING_STREAM_NAME, _SIZING_STREAM_SUBJECTS),
    self._make_stream_config(_EXECUTION_STREAM_NAME, _EXECUTION_STREAM_SUBJECTS),
]
```

- [ ] **Step 5.2: `DryRunExecutor`**

```python
"""DryRunExecutor: implementação MVP de OrderExecutor.

Sempre retorna ExecutionResult(mode=DRY_RUN, success=True). Não chama
blockchain. Real-mode (Web3CLOBExecutor) entra na Fase 4.
"""

from __future__ import annotations

from decimal import Decimal

from polycopy.domain.events import ExecutionMode
from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade


class DryRunExecutor:
    """Executor que apenas simula — não chama blockchain."""

    async def execute(self, trade: Trade, final_size_usdc: Decimal) -> ExecutionResult:
        return ExecutionResult(
            mode=ExecutionMode.DRY_RUN,
            success=True,
            tx_hash=None,
            gas_wei=None,
            failure_reason=None,
            error_message=None,
        )
```

E o `__init__.py` vazio.

- [ ] **Step 5.3: Testes unit `DryRunExecutor`**

```python
"""Testes unit do DryRunExecutor."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polycopy.domain.events import ExecutionMode
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.dry_run_executor import DryRunExecutor
from polycopy.ports.order_executor import OrderExecutor


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


async def test_execute_returns_dry_run_success() -> None:
    executor = DryRunExecutor()
    result = await executor.execute(_trade(), Decimal("10"))
    assert result.mode == ExecutionMode.DRY_RUN
    assert result.success is True
    assert result.tx_hash is None
    assert result.gas_wei is None
    assert result.failure_reason is None
    assert result.error_message is None


async def test_dry_run_executor_satisfies_port() -> None:
    """Mypy garante que DryRunExecutor satisfaz OrderExecutor Protocol."""
    _: OrderExecutor = DryRunExecutor()
```

- [ ] **Step 5.4: 6 testes integration em `test_jetstream_bus.py`**

3 publishes × 2 testes (received + dedup). Helpers `_order_executed_event()`, `_order_failed_event()`, `_order_dry_run_event()`.

- [ ] **Step 5.5: Verificações + STOP — code reviewer + commit**

Reviewer obrigatório (mexe em mensageria + nova execution lib).

---

## Task 6: `ExecutorAgent` + 4 settings + 3 métricas + .env + 12 unit tests (REVIEWER OBRIGATÓRIO)

**Files:**
- Create: `src/polycopy/agents/executor.py`
- Create: `tests/unit/agents/test_executor.py`
- Modify: `src/polycopy/config.py`
- Modify: `src/polycopy/infrastructure/observability/metrics.py`
- Modify: `tests/unit/infrastructure/test_metrics.py`
- Modify: `.env.example`

---

- [ ] **Step 6.1: 4 settings em `config.py`**

```python
    # Executor agent (Plano 3 — DRY-RUN MVP)
    executor_metrics_port: int = Field(9106, alias="EXECUTOR_METRICS_PORT")
    executor_max_deliver: int = Field(5, alias="EXECUTOR_MAX_DELIVER")
    executor_durable_name: str = Field("executor-1", alias="EXECUTOR_DURABLE_NAME")
    executor_dry_run: bool = Field(True, alias="EXECUTOR_DRY_RUN")
    """DRY-RUN by default — Fase 3 MVP. Set to false ONLY after Fase 4
    real-mode (Web3CLOBExecutor) is implemented + tested on testnet."""
```

- [ ] **Step 6.2: 3 métricas em `metrics.py`**

```python
    executor_orders_total: Counter
    executor_decision_duration_seconds: Histogram
    executor_gas_wei: Histogram
```

```python
        executor_orders_total=Counter(
            "polycopy_executor_orders",
            "Decisões do ExecutorAgent.",
            labelnames=["result", "mode", "reason"],
            registry=target,
        ),
        executor_decision_duration_seconds=Histogram(
            "polycopy_executor_decision_duration_seconds",
            "Duração end-to-end de uma decisão de execução.",
            registry=target,
        ),
        executor_gas_wei=Histogram(
            "polycopy_executor_gas_wei",
            "Gas usado em wei (real-mode com result=executed; vazio em dry_run).",
            buckets=(1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12),
            registry=target,
        ),
```

- [ ] **Step 6.3: 3 testes em `test_metrics.py`**

Pattern do arquivo (1 teste por métrica).

- [ ] **Step 6.4: `.env.example`**

```bash
# --- Executor agent (Plano 3 — DRY-RUN MVP) ---
EXECUTOR_METRICS_PORT=9106
EXECUTOR_MAX_DELIVER=5
EXECUTOR_DURABLE_NAME=executor-1
EXECUTOR_DRY_RUN=true
```

- [ ] **Step 6.5: Testes unit (RED)**

`tests/unit/agents/test_executor.py` — 12+ testes:
- `test_handle_message_dry_run_happy_path` — publica order.dry_run, persiste OK
- `test_handle_message_executor_raises_persists_failed` — executor lança exception → OrderExecution(result=failed) + publish order.failed
- `test_handle_message_idempotent_skip` — duplicate → ack sem publish
- `test_handle_message_invalid_payload_silent_ack`
- `test_handle_message_bus_publish_failure_propagates_after_persist` (invariante I-6)
- 4 testes pra cada select_publish (_dry_run/executed/failed) — happy path por executor stub
- `test_executor_satisfies_port` (já em test_dry_run_executor)

Stubs: `_StubBus`, `_StubExecutionRepo`, `_StubExecutorSuccess`, `_StubExecutorFailure`, `_StubExecutorRaises`.

- [ ] **Step 6.6: Implementar `agents/executor.py`**

Padrão similar ao `risk.py`/`sizing.py`. Ponto chave: ao construir `OrderExecution`, derivar `result` a partir de `ExecutionResult.mode` e `success`:

```python
def _result_label(self, exec_result: ExecutionResult) -> Literal["executed", "failed", "dry_run"]:
    if exec_result.mode == ExecutionMode.DRY_RUN:
        return "dry_run"
    return "executed" if exec_result.success else "failed"
```

`_handle_message` skeleton:
```python
async def _handle_message(self, payload: bytes, num_delivered: int) -> None:
    start = time.perf_counter()
    try:
        try:
            event = OrderSized.model_validate_json(payload)
        except ValidationError as exc:
            self._log.warning("executor_invalid_payload", ...)
            self._metrics.executor_orders_total.labels(
                result="failed", mode="dry_run", reason="invalid_payload"
            ).inc()
            return

        try:
            exec_result = await self._executor.execute(
                event.trade, event.final_size_usdc.amount
            )
        except Exception as exc:  # captura exceção do executor → vira OrderFailed
            exec_result = ExecutionResult(
                mode=ExecutionMode.DRY_RUN if self._dry_run else ExecutionMode.REAL,
                success=False,
                failure_reason=FailureReason.EXECUTOR_DISABLED,
                error_message=str(exc),
            )

        result_label = self._result_label(exec_result)

        execution = OrderExecution(
            trade_event_id=event.event_id,
            wallet=event.trade.wallet.value,
            condition_id=event.trade.condition_id.value,
            token_id=event.trade.token_id.value,
            final_size_usdc=event.final_size_usdc.amount,
            mode=exec_result.mode,
            result=result_label,
            tx_hash=exec_result.tx_hash,
            gas_wei=exec_result.gas_wei,
            failure_reason=exec_result.failure_reason,
            error_message=exec_result.error_message,
            decided_at=datetime.now(tz=UTC),
        )

        async with self._repo_factory() as repo:
            is_new = await repo.insert(execution)

        if not is_new:
            self._metrics.executor_orders_total.labels(
                result="duplicate_skip",
                mode=exec_result.mode.value,
                reason=(exec_result.failure_reason.value
                        if exec_result.failure_reason is not None else "none"),
            ).inc()
            return

        if result_label == "dry_run":
            await self._bus.publish_order_dry_run(
                OrderDryRun(
                    event_id=event.event_id,
                    occurred_at=event.occurred_at,
                    decided_at=execution.decided_at,
                    trade=event.trade,
                    final_size_usdc=event.final_size_usdc,
                )
            )
        elif result_label == "executed":
            if exec_result.tx_hash is None or exec_result.gas_wei is None:
                raise RuntimeError("executed result missing tx_hash or gas_wei")
            await self._bus.publish_order_executed(
                OrderExecuted(
                    event_id=event.event_id,
                    occurred_at=event.occurred_at,
                    decided_at=execution.decided_at,
                    trade=event.trade,
                    final_size_usdc=event.final_size_usdc,
                    tx_hash=exec_result.tx_hash,
                    gas_wei=exec_result.gas_wei,
                )
            )
            self._metrics.executor_gas_wei.observe(float(exec_result.gas_wei))
        else:  # failed
            if exec_result.failure_reason is None or exec_result.error_message is None:
                raise RuntimeError("failed result missing reason or error_message")
            await self._bus.publish_order_failed(
                OrderFailed(
                    event_id=event.event_id,
                    occurred_at=event.occurred_at,
                    decided_at=execution.decided_at,
                    trade=event.trade,
                    final_size_usdc=event.final_size_usdc,
                    reason=exec_result.failure_reason,
                    error_message=exec_result.error_message,
                )
            )

        self._metrics.executor_orders_total.labels(
            result=result_label,
            mode=exec_result.mode.value,
            reason=(exec_result.failure_reason.value
                    if exec_result.failure_reason is not None else "none"),
        ).inc()
        self._log.info(
            "executor_decision",
            trade_event_id=str(event.event_id),
            wallet=event.trade.wallet.value,
            mode=exec_result.mode.value,
            result=result_label,
            final_size_usdc=str(event.final_size_usdc.amount),
            tx_hash=exec_result.tx_hash,
            gas_wei=exec_result.gas_wei,
            reason=(exec_result.failure_reason.value
                    if exec_result.failure_reason is not None else None),
        )
    finally:
        self._metrics.executor_decision_duration_seconds.observe(time.perf_counter() - start)
```

`__init__` recebe: `bus`, `executor: OrderExecutor`, `repo_factory`, `metrics`, `dry_run: bool`, `durable_name`, `max_deliver`.

`main()`: instancia `DryRunExecutor` se `settings.executor_dry_run`, senão `raise RuntimeError("Real-mode not yet implemented — Fase 4 required")`.

- [ ] **Step 6.7: Verificações + STOP — reviewer + commit**

---

## Task 7: Container `polycopy-executor:9106` + scrape Prometheus + ARCHITECTURE.md

Mesmo padrão das Tasks 7 anteriores. Service `executor` no compose (depends_on postgres+nats), scrape job, subseção ARCHITECTURE.

---

## Task 8: Integration E2E `test_executor_e2e.py`

2 testes:
1. `test_e2e_dry_run_flow` — publica `OrderSized` → DB tem `mode=dry_run, result=dry_run` + bus tem `order.dry_run`.
2. `test_e2e_redelivery_idempotent` — mesmo trade 2x → 1 row + 1 evento.

(Failure path não-trivial em E2E sem mockar executor — fica unit-only.)

Pré-req: `docker compose stop executor` antes de rodar.

---

## Self-Review (executor autônomo)

**Spec coverage:**
- §3.1 events/enums/value objects: T1 ✓
- §3.1 OrderExecutor + OrderExecutionRepository ports + extensão MessagingPort: T2 ✓
- §3.1 Tabela order_executions + migration + ORM: T3 ✓
- §3.1 SqlAlchemyOrderExecutionRepository: T4 ✓
- §3.1 Stream EXECUTION_RESULTS + DryRunExecutor: T5 ✓
- §3.1 ExecutorAgent + 4 settings + 3 métricas: T6 ✓
- §3.1 Container: T7 ✓
- §3.1 Testes: T1+T4+T6+T8 ✓
- §5 Schema (PK + 7 CHECKs + 3 indexes): T3 ✓
- §6 Fluxo persist→publish: T6 ✓
- §7 Tratamento de falhas: T6 ✓
- §11 Open questions documentadas na spec.

**Placeholder scan:** sem TBD/TODO. Real-mode marcado como Fase 4.

**Type consistency:** `OrderExecution.result` é `Literal["executed","failed","dry_run"]` em T1, T3, T4, T6. `ExecutionMode` enum em T1, T2, T6. `OrderExecutionRepository.insert(execution) -> bool` em T2, T4, T6. `OrderExecutor.execute(trade, final_size_usdc) -> ExecutionResult` em T2 (port), T5 (DryRunExecutor impl), T6 (agente caller).

**Atenção operacional:** mesmo padrão herdado (parar container `polycopy-executor` antes de pytest E2E).
