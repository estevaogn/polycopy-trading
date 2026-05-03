# Plano 2C — Sizing Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entregar o `SizingAgent` — última peça da Fase 2. Consome `order.approved`, aplica proporcionalidade hardcoded com cap+floor, persiste em `order_sizings`, publica `order.sized` ou `order.skipped`.

**Architecture:** Agente novo (mesmo padrão Risk/Notifier — `AgentBase` + durable consumer JetStream). Lógica puramente local (multiplicação Decimal). Idempotência via PK + `Nats-Msg-Id`. Container `polycopy-sizing:9105`.

**Tech Stack:** Python 3.12, pydantic v2, SQLAlchemy 2 async, alembic, prometheus_client, NATS JetStream (via `NatsMessagingBus`), pytest + asyncio.

**Predecessor:** Plano 2B (head `e7bb367`) — entregou `OrderApproved` event, `polycopy-risk:9104`, stream `RISK_DECISIONS`.

**Spec:** `docs/superpowers/specs/2026-05-02-fase-2c-sizing-design.md` (commit `62f3742`).

---

## File Structure

**Novos arquivos (8):**
- `src/polycopy/domain/sizing.py` — `OrderSizing` value object.
- `src/polycopy/ports/order_sizing_repository.py` — `OrderSizingRepository` Protocol.
- `src/polycopy/infrastructure/persistence/order_sizing_repository.py` — `SqlAlchemyOrderSizingRepository`.
- `alembic/versions/0004_add_order_sizings.py` — migration.
- `src/polycopy/agents/sizing.py` — `SizingAgent` + `main()`.
- `tests/unit/agents/test_sizing.py`.
- `tests/unit/domain/test_sizing_events.py`.
- `tests/integration/test_order_sizing_repository.py`.
- `tests/integration/test_sizing_e2e.py`.

**Arquivos modificados (8):**
- `src/polycopy/domain/events.py` — +`SkipReason`, `OrderSized`, `OrderSkipped`.
- `src/polycopy/ports/messaging.py` — +2 publish methods.
- `src/polycopy/ports/__init__.py` — +export `OrderSizingRepository`.
- `src/polycopy/infrastructure/messaging/nats_bus.py` — +stream `SIZING_DECISIONS` + 2 publish methods.
- `src/polycopy/infrastructure/persistence/models.py` — +`OrderSizingRow` ORM.
- `src/polycopy/config.py` — +6 settings.
- `src/polycopy/infrastructure/observability/metrics.py` — +3 métricas.
- `tests/unit/infrastructure/test_metrics.py` — +3 testes.
- `tests/unit/test_ports_typecheck.py` — +stub + asserts.
- `tests/integration/test_jetstream_bus.py` — +4 testes pros publishes.
- `docker-compose.yml` — +service `sizing`.
- `infra/prometheus/prometheus.yml` — +scrape job.
- `ARCHITECTURE.md` — +seção SizingAgent + nó Mermaid.
- `.env.example` — +bloco "Sizing agent (Plano 2C)".

---

## Task 1: Domain — `SkipReason`, `OrderSized`, `OrderSkipped`, `OrderSizing`

**Files:**
- Modify: `src/polycopy/domain/events.py`
- Create: `src/polycopy/domain/sizing.py`
- Create: `tests/unit/domain/test_sizing_events.py`

---

- [ ] **Step 1.1: Adicionar enum + 2 events em `events.py`**

LEIA `events.py` (atual ~92 linhas com `WalletTradeDetected`, `RejectionReason`, `OrderApproved`, `TradeRejected`). Adicionar `Money` aos imports (do `value_objects`), depois adicionar no fim:

```python
class SkipReason(StrEnum):
    """Razões pelas quais Sizing pula um trade aprovado."""

    BELOW_MIN_SIZE = "below_min_size"


class OrderSized(BaseModel):
    """Evento publicado quando Sizing escala um trade aprovado.

    NATS subject: `order.sized`. `event_id` é o mesmo do `WalletTradeDetected`
    original. `occurred_at` preserva o timestamp do trade (pra Sizing/Risk
    medir lag); `decided_at` marca quando Sizing efetivamente decidiu.
    """

    SUBJECT: ClassVar[str] = "order.sized"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    final_size_usdc: Money
    original_size_usdc: Money

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


class OrderSkipped(BaseModel):
    """Evento publicado quando Sizing pula um trade aprovado (final_size < min).

    NATS subject: `order.skipped`. Inclui `reason` pra audit.
    """

    SUBJECT: ClassVar[str] = "order.skipped"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    decided_at: datetime
    trade: Trade
    reason: SkipReason

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

`Money` precisa estar nos imports do topo: `from polycopy.domain.value_objects import Money`.

- [ ] **Step 1.2: Criar `src/polycopy/domain/sizing.py` com `OrderSizing`**

```python
"""OrderSizing: value object interno representando uma decisão de sizing persistida."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from polycopy.domain.events import SkipReason


@dataclass(frozen=True)
class OrderSizing:
    """Snapshot imutável de uma decisão de sizing.

    Persistido em `order_sizings` table; PK = `trade_event_id`.

    Invariante: `decision == "sized"` ↔ `final_size_usdc is not None and reason is None`.
    """

    trade_event_id: UUID
    wallet: str
    condition_id: str
    token_id: str
    original_size_usdc: Decimal
    final_size_usdc: Decimal | None
    decision: Literal["sized", "skipped"]
    reason: SkipReason | None
    decided_at: datetime

    def __post_init__(self) -> None:
        if self.decision == "sized":
            if self.final_size_usdc is None:
                raise ValueError("sized decision must have final_size_usdc")
            if self.reason is not None:
                raise ValueError("sized decision must have reason=None")
            if self.final_size_usdc <= 0:
                raise ValueError("final_size_usdc must be positive")
        if self.decision == "skipped":
            if self.final_size_usdc is not None:
                raise ValueError("skipped decision must have final_size_usdc=None")
            if self.reason is None:
                raise ValueError("skipped decision must have a reason")
        if self.original_size_usdc <= 0:
            raise ValueError("original_size_usdc must be positive")
        if self.decided_at.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
```

- [ ] **Step 1.3: Escrever testes unit**

Create `tests/unit/domain/test_sizing_events.py` com cobertura:
- `test_order_sized_requires_tzaware_occurred`
- `test_order_sized_requires_tzaware_decided`
- `test_order_sized_subject_constant`
- `test_order_skipped_requires_reason`
- `test_order_skipped_subject_constant`
- `test_skip_reason_values`
- `test_order_sizing_sized_with_reason_raises`
- `test_order_sizing_sized_without_size_raises`
- `test_order_sizing_skipped_with_size_raises`
- `test_order_sizing_skipped_without_reason_raises`
- `test_order_sizing_negative_size_raises`
- `test_order_sizing_naive_decided_at_raises`
- `test_order_sizing_valid_sized`
- `test_order_sizing_valid_skipped`

Helper `_trade()` igual ao do `test_risk_events.py`.

Run: `uv run pytest tests/unit/domain/test_sizing_events.py -v` → ImportError esperado (RED).

- [ ] **Step 1.4: GREEN + verificações + STOP**

Após implementação: 14 PASS.
mypy + ruff clean.
Commit (controller faz):
```bash
git add src/polycopy/domain/events.py src/polycopy/domain/sizing.py tests/unit/domain/test_sizing_events.py
git commit -m "feat(domain): add SkipReason, OrderSized, OrderSkipped, OrderSizing"
```

---

## Task 2: Ports — `OrderSizingRepository` + extensão `MessagingPort` + adapter NATS impl

**Escopo expandido (mesmo padrão 2B-T2):** estender Protocol + implementar adapter mínimo no `nats_bus.py` pra evitar quebra mypy. Stream `SIZING_DECISIONS` fica pra T5.

**Files:**
- Create: `src/polycopy/ports/order_sizing_repository.py`
- Modify: `src/polycopy/ports/messaging.py`
- Modify: `src/polycopy/ports/__init__.py`
- Modify: `src/polycopy/infrastructure/messaging/nats_bus.py`
- Modify: `tests/unit/test_ports_typecheck.py`

---

- [ ] **Step 2.1: Criar `order_sizing_repository.py` Protocol**

```python
"""OrderSizingRepository: contrato de persistência pra decisões de sizing."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.sizing import OrderSizing


class OrderSizingRepository(Protocol):
    """Persistência idempotente de decisões de sizing. Plano 2C."""

    async def insert(self, sizing: OrderSizing) -> bool:
        """Insere sizing; retorna True se nova, False se já existia.

        Idempotência via PK `trade_event_id`.
        """
        ...
```

- [ ] **Step 2.2: Estender `MessagingPort`**

Modify `src/polycopy/ports/messaging.py`. Adicionar imports:
```python
from polycopy.domain.events import (
    OrderApproved,
    OrderSized,
    OrderSkipped,
    TradeRejected,
    WalletTradeDetected,
)
```

Adicionar 2 métodos no Protocol (depois de `publish_trade_rejected`, antes de `subscribe`):

```python
    async def publish_order_sized(self, event: OrderSized) -> None:
        """Publica evento no subject `order.sized`."""
        ...

    async def publish_order_skipped(self, event: OrderSkipped) -> None:
        """Publica evento no subject `order.skipped`."""
        ...
```

- [ ] **Step 2.3: Atualizar `ports/__init__.py`**

Adicionar:
```python
from polycopy.ports.order_sizing_repository import OrderSizingRepository
```

E `"OrderSizingRepository"` ao `__all__` (alfabético).

- [ ] **Step 2.4: Implementar publishes em `nats_bus.py` (stream fica pra T5)**

LEIA `nats_bus.py`. Estender import:
```python
from polycopy.domain.events import (
    OrderApproved,
    OrderSized,
    OrderSkipped,
    TradeRejected,
    WalletTradeDetected,
)
```

Adicionar 2 métodos depois de `publish_trade_rejected`:

```python
    async def publish_order_sized(self, event: OrderSized) -> None:
        _, js = self._require_connected()
        payload = event.model_dump_json().encode("utf-8")
        await js.publish(
            OrderSized.SUBJECT,
            payload,
            headers={"Nats-Msg-Id": str(event.event_id)},
        )

    async def publish_order_skipped(self, event: OrderSkipped) -> None:
        _, js = self._require_connected()
        payload = event.model_dump_json().encode("utf-8")
        await js.publish(
            OrderSkipped.SUBJECT,
            payload,
            headers={"Nats-Msg-Id": str(event.event_id)},
        )
```

NÃO criar stream `SIZING_DECISIONS` agora (vai pra T5). Publishes vão falhar em runtime ("no stream available") até T5.

- [ ] **Step 2.5: Estender `test_ports_typecheck.py`**

Imports:
```python
from polycopy.domain.events import OrderSized, OrderSkipped, SkipReason
from polycopy.domain.sizing import OrderSizing
from polycopy.ports import OrderSizingRepository
```

Estender `_FakeMessaging` com:
```python
    async def publish_order_sized(self, event: OrderSized) -> None:
        return None

    async def publish_order_skipped(self, event: OrderSkipped) -> None:
        return None
```

Adicionar fake stub:
```python
class _FakeOrderSizingRepo:
    """Stub que implementa OrderSizingRepository."""

    def __init__(self) -> None:
        self.inserted: list[OrderSizing] = []

    async def insert(self, sizing: OrderSizing) -> bool:
        self.inserted.append(sizing)
        return True
```

Helper + 2 testes:
```python
def _accepts_order_sizing_repo(_: OrderSizingRepository) -> None:
    """Helper: mypy falha aqui se o argumento não satisfizer OrderSizingRepository."""


def test_fake_order_sizing_repo_satisfies_port() -> None:
    fake = _FakeOrderSizingRepo()
    _accepts_order_sizing_repo(fake)


def test_sizing_ports_importable() -> None:
    assert OrderSizingRepository is not None
    assert OrderSized is not None
    assert OrderSkipped is not None
    assert SkipReason is not None
```

- [ ] **Step 2.6: Verificações + commit**

```bash
uv run mypy src/polycopy
uv run pytest tests/unit/test_ports_typecheck.py -v
uv run ruff check ...
uv run pytest tests/
```

Commit:
```bash
git add src/polycopy/ports/order_sizing_repository.py src/polycopy/ports/messaging.py src/polycopy/ports/__init__.py src/polycopy/infrastructure/messaging/nats_bus.py tests/unit/test_ports_typecheck.py
git commit -m "feat(ports): add OrderSizingRepository and extend MessagingPort with sizing publishes"
```

---

## Task 3: Tabela `order_sizings` + migration alembic + `OrderSizingRow` ORM

**Files:**
- Modify: `src/polycopy/infrastructure/persistence/models.py`
- Create: `alembic/versions/0004_add_order_sizings.py`

---

- [ ] **Step 3.1: Adicionar `OrderSizingRow` em `models.py`**

LEIA `models.py` (~165 linhas atualmente). Adicionar classe ao final:

```python
class OrderSizingRow(Base):
    __tablename__ = "order_sizings"

    trade_event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, nullable=False)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    token_id: Mapped[str] = mapped_column(String, nullable=False)
    original_size_usdc: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    final_size_usdc: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    decision: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str | None] = mapped_column(String, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "decision IN ('sized', 'skipped')",
            name="order_sizings_decision_enum",
        ),
        CheckConstraint(
            "(decision = 'sized' AND final_size_usdc IS NOT NULL AND reason IS NULL) "
            "OR (decision = 'skipped' AND final_size_usdc IS NULL AND reason IS NOT NULL)",
            name="order_sizings_consistency",
        ),
        CheckConstraint(
            "original_size_usdc > 0 AND (final_size_usdc IS NULL OR final_size_usdc > 0)",
            name="order_sizings_size_positive",
        ),
        Index(
            "idx_order_sizings_wallet_decided_at",
            "wallet",
            "decided_at",
            postgresql_using="btree",
        ),
        Index(
            "idx_order_sizings_skipped_decided_at",
            "decided_at",
            postgresql_where="decision = 'skipped'",
            postgresql_using="btree",
        ),
    )
```

- [ ] **Step 3.2: Criar migration `0004_add_order_sizings.py`**

```python
"""add order_sizings table

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-02 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "order_sizings",
        sa.Column("trade_event_id", sa.Uuid(), nullable=False),
        sa.Column("wallet", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("original_size_usdc", sa.Numeric(20, 6), nullable=False),
        sa.Column("final_size_usdc", sa.Numeric(20, 6), nullable=True),
        sa.Column("decision", sa.String(), nullable=False),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "decision IN ('sized', 'skipped')",
            name="order_sizings_decision_enum",
        ),
        sa.CheckConstraint(
            "(decision = 'sized' AND final_size_usdc IS NOT NULL AND reason IS NULL) "
            "OR (decision = 'skipped' AND final_size_usdc IS NULL AND reason IS NOT NULL)",
            name="order_sizings_consistency",
        ),
        sa.CheckConstraint(
            "original_size_usdc > 0 AND (final_size_usdc IS NULL OR final_size_usdc > 0)",
            name="order_sizings_size_positive",
        ),
        sa.PrimaryKeyConstraint("trade_event_id"),
    )
    op.create_index(
        "idx_order_sizings_wallet_decided_at",
        "order_sizings",
        ["wallet", "decided_at"],
    )
    op.create_index(
        "idx_order_sizings_skipped_decided_at",
        "order_sizings",
        ["decided_at"],
        postgresql_where=sa.text("decision = 'skipped'"),
    )


def downgrade() -> None:
    op.drop_index("idx_order_sizings_skipped_decided_at", table_name="order_sizings")
    op.drop_index("idx_order_sizings_wallet_decided_at", table_name="order_sizings")
    op.drop_table("order_sizings")
```

- [ ] **Step 3.3: Validar via alembic**

```bash
docker compose ps postgres
uv run alembic upgrade head
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d order_sizings"
uv run alembic downgrade -1
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\dt order_sizings"
uv run alembic upgrade head
```

- [ ] **Step 3.4: Commit**

```bash
git add src/polycopy/infrastructure/persistence/models.py alembic/versions/0004_add_order_sizings.py
git commit -m "feat(persistence): add order_sizings table and OrderSizingRow ORM"
```

---

## Task 4: `SqlAlchemyOrderSizingRepository` + integration tests

**Files:**
- Create: `src/polycopy/infrastructure/persistence/order_sizing_repository.py`
- Create: `tests/integration/test_order_sizing_repository.py`

---

- [ ] **Step 4.1: Escrever testes integration (RED)**

`tests/integration/test_order_sizing_repository.py`:

Cobertura (6 testes):
1. `test_insert_sized_returns_true`
2. `test_insert_duplicate_returns_false`
3. `test_insert_skipped_persists_reason`
4. `test_insert_sized_with_reason_violates_constraint` (SQL cru bypass post_init)
5. `test_insert_invalid_decision_violates_constraint`
6. `test_adapter_satisfies_protocol`

Helpers `_sizing_sized()`, `_sizing_skipped()`.

- [ ] **Step 4.2: Implementar `order_sizing_repository.py`**

```python
"""SqlAlchemyOrderSizingRepository: persistência idempotente de decisões de sizing."""

from __future__ import annotations

from typing import cast

from sqlalchemy import CursorResult
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.sizing import OrderSizing
from polycopy.infrastructure.persistence.models import OrderSizingRow


class SqlAlchemyOrderSizingRepository:
    """Persistência de OrderSizing. Idempotente via PK `trade_event_id`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, sizing: OrderSizing) -> bool:
        """Insere sizing. True se novo, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(OrderSizingRow)
            .values(
                trade_event_id=sizing.trade_event_id,
                wallet=sizing.wallet,
                condition_id=sizing.condition_id,
                token_id=sizing.token_id,
                original_size_usdc=sizing.original_size_usdc,
                final_size_usdc=sizing.final_size_usdc,
                decision=sizing.decision,
                reason=sizing.reason.value if sizing.reason is not None else None,
                decided_at=sizing.decided_at,
            )
            .on_conflict_do_nothing(index_elements=["trade_event_id"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1
```

- [ ] **Step 4.3: GREEN + verificações + commit**

```bash
uv run pytest tests/integration/test_order_sizing_repository.py -v
uv run mypy src/polycopy
uv run ruff check ...
git add src/polycopy/infrastructure/persistence/order_sizing_repository.py tests/integration/test_order_sizing_repository.py
git commit -m "feat(persistence): add SqlAlchemyOrderSizingRepository with PK-based idempotency"
```

---

## Task 5: Stream `SIZING_DECISIONS` + 4 integration tests pros publishes (REVIEWER OBRIGATÓRIO)

**Files:**
- Modify: `src/polycopy/infrastructure/messaging/nats_bus.py`
- Modify: `tests/integration/test_jetstream_bus.py`

---

- [ ] **Step 5.1: Adicionar stream constants em `nats_bus.py`**

Adicionar:
```python
_SIZING_STREAM_NAME = "SIZING_DECISIONS"
_SIZING_STREAM_SUBJECTS = ["order.sized", "order.skipped"]
```

Estender `_ensure_streams` com mais 1 config (3 streams agora):

```python
    async def _ensure_streams(self) -> None:
        """Garante que os streams existem (idempotente)."""
        _, js = self._require_connected()

        wallet_config = StreamConfig(
            name=_STREAM_NAME,
            subjects=_STREAM_SUBJECTS,
            retention=RetentionPolicy.LIMITS,
            max_age=_STREAM_MAX_AGE_S,
            storage=StorageType.FILE,
            num_replicas=1,
            duplicate_window=300,
        )
        risk_config = StreamConfig(
            name=_RISK_STREAM_NAME,
            subjects=_RISK_STREAM_SUBJECTS,
            retention=RetentionPolicy.LIMITS,
            max_age=_STREAM_MAX_AGE_S,
            storage=StorageType.FILE,
            num_replicas=1,
            duplicate_window=300,
        )
        sizing_config = StreamConfig(
            name=_SIZING_STREAM_NAME,
            subjects=_SIZING_STREAM_SUBJECTS,
            retention=RetentionPolicy.LIMITS,
            max_age=_STREAM_MAX_AGE_S,
            storage=StorageType.FILE,
            num_replicas=1,
            duplicate_window=300,
        )
        for config in (wallet_config, risk_config, sizing_config):
            with contextlib.suppress(BadRequestError):
                await js.add_stream(config=config)
```

- [ ] **Step 5.2: Adicionar 4 testes integration em `test_jetstream_bus.py`**

```python
async def test_publish_order_sized_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderSized.SUBJECT, handler)
    await bus.publish_order_sized(_order_sized_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_skipped_received_by_subscriber(bus: NatsMessagingBus) -> None:
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(OrderSkipped.SUBJECT, handler)
    await bus.publish_order_skipped(_order_skipped_event())
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_sized_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    """Mesmo event_id → mesmo Nats-Msg-Id → JetStream dedupa server-side.

    Validado via durable subscriber (ephemeral não vê dedup — ver T5/2B).
    """
    durable = f"sizing-dedup-{uuid4().hex[:8]}"
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    await bus.subscribe(OrderSized.SUBJECT, handler, durable=durable)
    event = _order_sized_event()
    await bus.publish_order_sized(event)
    await bus.publish_order_sized(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()


async def test_publish_order_skipped_dedup_by_event_id(bus: NatsMessagingBus) -> None:
    durable = f"sizing-skip-dedup-{uuid4().hex[:8]}"
    received: list[bytes] = []

    async def handler(payload: bytes, num_delivered: int) -> None:
        received.append(payload)

    await bus.subscribe(OrderSkipped.SUBJECT, handler, durable=durable)
    event = _order_skipped_event()
    await bus.publish_order_skipped(event)
    await bus.publish_order_skipped(event)
    await asyncio.sleep(0.5)
    assert len(received) == 1
    await bus.close()
```

Helpers `_order_sized_event()`/`_order_skipped_event()` análogos aos do 2B:

```python
def _order_sized_event() -> OrderSized:
    trade = _trade()
    return OrderSized(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=trade,
        final_size_usdc=Money.from_usdc("10"),
        original_size_usdc=trade.size_usdc,
    )


def _order_skipped_event() -> OrderSkipped:
    return OrderSkipped(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        decided_at=datetime.now(tz=UTC),
        trade=_trade(),
        reason=SkipReason.BELOW_MIN_SIZE,
    )
```

- [ ] **Step 5.3: Verificações + STOP — code reviewer**

```bash
uv run pytest tests/integration/test_jetstream_bus.py -v -k "publish_order_sized or publish_order_skipped"
uv run mypy src/polycopy
```

Code reviewer obrigatório (mexe em mensageria em produção).

Commit (após reviewer):
```bash
git add src/polycopy/infrastructure/messaging/nats_bus.py tests/integration/test_jetstream_bus.py
git commit -m "feat(messaging): add SIZING_DECISIONS stream and dedup tests for sizing publishes"
```

---

## Task 6: `SizingAgent` + 6 settings + 3 métricas + .env.example + unit tests (REVIEWER OBRIGATÓRIO)

**Files:**
- Create: `src/polycopy/agents/sizing.py`
- Create: `tests/unit/agents/test_sizing.py`
- Modify: `src/polycopy/config.py`
- Modify: `src/polycopy/infrastructure/observability/metrics.py`
- Modify: `tests/unit/infrastructure/test_metrics.py`
- Modify: `.env.example`

---

- [ ] **Step 6.1: Adicionar 6 settings em `config.py`**

Adicionar bloco no fim do `Settings`:

```python
    # Sizing agent (Plano 2C)
    sizing_metrics_port: int = Field(9105, alias="SIZING_METRICS_PORT")
    sizing_max_deliver: int = Field(5, alias="SIZING_MAX_DELIVER")
    sizing_durable_name: str = Field("sizing-1", alias="SIZING_DURABLE_NAME")
    sizing_proportion_ratio: Decimal = Field(Decimal("0.1"), alias="SIZING_PROPORTION_RATIO")
    sizing_max_size_usdc: Decimal = Field(Decimal("50"), alias="SIZING_MAX_SIZE_USDC")
    sizing_min_size_usdc: Decimal = Field(Decimal("1"), alias="SIZING_MIN_SIZE_USDC")
```

- [ ] **Step 6.2: Adicionar 3 métricas em `metrics.py`**

3 campos no dataclass `Metrics`:
```python
    sizing_decisions_total: Counter
    sizing_decision_duration_seconds: Histogram
    sizing_size_ratio_observed: Histogram
```

3 entries em `make_metrics()`:
```python
        sizing_decisions_total=Counter(
            "polycopy_sizing_decisions",
            "Decisões do SizingAgent.",
            labelnames=["result", "reason"],
            registry=target,
        ),
        sizing_decision_duration_seconds=Histogram(
            "polycopy_sizing_decision_duration_seconds",
            "Duração end-to-end de uma decisão de sizing.",
            registry=target,
        ),
        sizing_size_ratio_observed=Histogram(
            "polycopy_sizing_size_ratio_observed",
            "Razão final_size / original_size observada por decisão sized.",
            registry=target,
        ),
```

- [ ] **Step 6.3: 3 testes em `test_metrics.py`**

```python
def test_metrics_sizing_decisions_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.sizing_decisions_total.labels(result="sized", reason="none").inc()
    metrics.sizing_decisions_total.labels(result="skipped", reason="below_min_size").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_decisions"]
    assert len(matching) == 1


def test_metrics_sizing_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.sizing_decision_duration_seconds.observe(0.05)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_decision_duration_seconds"]
    assert matching


def test_metrics_sizing_size_ratio_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.sizing_size_ratio_observed.observe(0.1)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_size_ratio_observed"]
    assert matching
```

- [ ] **Step 6.4: Atualizar `.env.example`**

Adicionar bloco no fim:
```bash
# --- Sizing agent (Plano 2C) ---
SIZING_METRICS_PORT=9105
SIZING_MAX_DELIVER=5
SIZING_DURABLE_NAME=sizing-1
SIZING_PROPORTION_RATIO=0.1
SIZING_MAX_SIZE_USDC=50
SIZING_MIN_SIZE_USDC=1
```

- [ ] **Step 6.5: Escrever testes unit (RED)**

`tests/unit/agents/test_sizing.py` cobertura (12+ testes):
- `_size` happy path: trade 100 → final 10 (10% scaled, no cap)
- `_size` capped: trade 10000 → scaled 1000 → capped 50
- `_size` exactly at min: trade 10 → scaled 1 → sized=1.0
- `_size` skipped: trade 1 → scaled 0.1 < min 1 → skipped
- `_handle_message` happy path approved
- `_handle_message` skipped flow
- `_handle_message` idempotent skip (is_new=False, no publish)
- `_handle_message` invalid payload silent ack
- `_handle_message` bus publish failure propagates after persist
- `_size` ratio metric observed (test that observes the ratio)

Pattern de stubs (`_StubBus`, `_StubRepoSizing`, `_make_agent`) análogo ao `test_risk.py`.

- [ ] **Step 6.6: Implementar `src/polycopy/agents/sizing.py`**

```python
"""SizingAgent: aplica proporcionalidade hardcoded em trades aprovados pelo Risk.

Rodando local:
    uv run python -m polycopy.agents.sizing
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import OrderApproved, OrderSized, OrderSkipped, SkipReason
from polycopy.domain.models import Trade
from polycopy.domain.sizing import OrderSizing
from polycopy.domain.value_objects import Money
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MessagingPort, OrderSizingRepository

SizingRepoFactory = Callable[[], AbstractAsyncContextManager[OrderSizingRepository]]

_USDC_QUANTUM = Decimal("0.000001")


@dataclass(frozen=True)
class _SizeResult:
    decision: str  # "sized" | "skipped"
    final_size_usdc: Decimal | None
    reason: SkipReason | None


class SizingAgent(AgentBase):
    name = "sizing"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        bus: MessagingPort,
        repo_factory: SizingRepoFactory,
        proportion_ratio: Decimal,
        max_size_usdc: Decimal,
        min_size_usdc: Decimal,
        metrics: Metrics,
        durable_name: str = "sizing-1",
        max_deliver: int = 5,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=1.0)
        self._bus = bus
        self._repo_factory = repo_factory
        self._proportion_ratio = proportion_ratio
        self._max_size_usdc = max_size_usdc
        self._min_size_usdc = min_size_usdc
        self._metrics = metrics
        self._durable_name = durable_name
        self._max_deliver = max_deliver

    async def start(self) -> None:
        """Registra durable consumer no JetStream; chamar antes de run()."""
        await self._bus.subscribe(
            OrderApproved.SUBJECT,
            self._handle_message,
            durable=self._durable_name,
            max_deliver=self._max_deliver,
        )

    async def run_once(self) -> None:
        await asyncio.sleep(self._interval_s)

    async def _handle_message(self, payload: bytes, num_delivered: int) -> None:
        start = time.perf_counter()
        try:
            try:
                event = OrderApproved.model_validate_json(payload)
            except ValidationError as exc:
                self._log.warning(
                    "sizing_invalid_payload",
                    num_delivered=num_delivered,
                    payload_preview=payload[:200].decode("utf-8", errors="replace"),
                    error=str(exc),
                )
                self._metrics.sizing_decisions_total.labels(
                    result="skipped", reason="invalid_payload"
                ).inc()
                return

            result = self._size(event.trade)

            sizing = OrderSizing(
                trade_event_id=event.event_id,
                wallet=event.trade.wallet.value,
                condition_id=event.trade.condition_id.value,
                token_id=event.trade.token_id.value,
                original_size_usdc=event.trade.size_usdc.amount,
                final_size_usdc=result.final_size_usdc,
                decision=result.decision,  # type: ignore[arg-type]
                reason=result.reason,
                decided_at=datetime.now(tz=UTC),
            )

            async with self._repo_factory() as repo:
                is_new = await repo.insert(sizing)

            if not is_new:
                self._metrics.sizing_decisions_total.labels(
                    result="duplicate_skip",
                    reason=result.reason.value if result.reason is not None else "none",
                ).inc()
                return

            if result.decision == "sized":
                assert result.final_size_usdc is not None  # invariante já validado
                await self._bus.publish_order_sized(
                    OrderSized(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=sizing.decided_at,
                        trade=event.trade,
                        final_size_usdc=Money(amount=result.final_size_usdc),
                        original_size_usdc=event.trade.size_usdc,
                    )
                )
                # Observa razão final/original (gauge gauge histogram)
                ratio = result.final_size_usdc / event.trade.size_usdc.amount
                self._metrics.sizing_size_ratio_observed.observe(float(ratio))
            else:
                assert result.reason is not None
                await self._bus.publish_order_skipped(
                    OrderSkipped(
                        event_id=event.event_id,
                        occurred_at=event.occurred_at,
                        decided_at=sizing.decided_at,
                        trade=event.trade,
                        reason=result.reason,
                    )
                )

            self._metrics.sizing_decisions_total.labels(
                result=result.decision,
                reason=result.reason.value if result.reason is not None else "none",
            ).inc()
            self._log.info(
                "sizing_decision",
                trade_event_id=str(event.event_id),
                wallet=event.trade.wallet.value,
                decision=result.decision,
                original_size_usdc=str(event.trade.size_usdc.amount),
                final_size_usdc=(
                    str(result.final_size_usdc) if result.final_size_usdc is not None else None
                ),
                reason=result.reason.value if result.reason is not None else None,
            )
        finally:
            self._metrics.sizing_decision_duration_seconds.observe(time.perf_counter() - start)

    def _size(self, trade: Trade) -> _SizeResult:
        """Aplica proporcionalidade + cap + floor.

        scaled = original * ratio
        capped = min(scaled, max)
        se capped < min → skipped
        senão → sized=capped (quantizado pra USDC quantum)
        """
        original = trade.size_usdc.amount
        scaled = original * self._proportion_ratio
        capped = min(scaled, self._max_size_usdc)
        if capped < self._min_size_usdc:
            return _SizeResult(
                decision="skipped",
                final_size_usdc=None,
                reason=SkipReason.BELOW_MIN_SIZE,
            )
        return _SizeResult(
            decision="sized",
            final_size_usdc=capped.quantize(_USDC_QUANTUM),
            reason=None,
        )


def _make_repo_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> SizingRepoFactory:
    from polycopy.infrastructure.persistence.order_sizing_repository import (
        SqlAlchemyOrderSizingRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[OrderSizingRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyOrderSizingRepository(session)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return _factory


async def main() -> None:
    """Entrypoint."""
    from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.sizing_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(session_factory)

    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = SizingAgent(
        stopping=stopping,
        bus=bus,
        repo_factory=repo_factory,
        proportion_ratio=settings.sizing_proportion_ratio,
        max_size_usdc=settings.sizing_max_size_usdc,
        min_size_usdc=settings.sizing_min_size_usdc,
        metrics=metrics,
        durable_name=settings.sizing_durable_name,
        max_deliver=settings.sizing_max_deliver,
    )
    await agent.start()
    try:
        await agent.run()
    finally:
        await bus.close()
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6.7: GREEN + verificações + STOP — reviewer + commit**

Reviewer obrigatório. Commit após:
```bash
git add src/polycopy/agents/sizing.py src/polycopy/config.py src/polycopy/infrastructure/observability/metrics.py tests/unit/agents/test_sizing.py tests/unit/infrastructure/test_metrics.py .env.example
git commit -m "feat(agents): add SizingAgent with proportional scaling and floor/cap"
```

---

## Task 7: Container `polycopy-sizing:9105` + scrape Prometheus + ARCHITECTURE.md

**Files:**
- Modify: `docker-compose.yml`
- Modify: `infra/prometheus/prometheus.yml`
- Modify: `ARCHITECTURE.md`

---

- [ ] **Step 7.1: Service `sizing` em `docker-compose.yml`** (mesmo padrão risk/marketdata, depends_on postgres+nats, env vars completas, port 9105, healthcheck)

- [ ] **Step 7.2: Scrape job `polycopy-sizing` em `prometheus.yml`**

- [ ] **Step 7.3: ARCHITECTURE.md** — nó `sizing` no Mermaid (consumer order.approved, write order_sizings, publisher order.sized/order.skipped) + 3 linhas na tabela de métricas + subseção "SizingAgent (Plano 2C)" + entrada na lista de endpoints.

- [ ] **Step 7.4: Build + up + validar healthy + curl /metrics + Prometheus target**

- [ ] **Step 7.5: Commit**

---

## Task 8: Integration E2E `test_sizing_e2e.py`

**Files:**
- Create: `tests/integration/test_sizing_e2e.py`

---

- [ ] **Step 8.1: 3 testes E2E** (igual ao padrão T8 do 2B):
1. `test_e2e_sized_flow` — publica `OrderApproved` (size 100) → DB sized + bus order.sized
2. `test_e2e_skipped_flow` — publica `OrderApproved` (size 1, scaled 0.1 < min 1) → DB skipped + bus order.skipped
3. `test_e2e_redelivery_idempotent` — mesmo trade 2x → 1 row + 1 evento

Pré-req: `docker compose stop sizing` antes de rodar (mesma operacional do 2B-T8 com risk container).

- [ ] **Step 8.2: Commit final do plano 2C**

---

## Self-Review (executor autônomo)

**Spec coverage:**
- §3.1 Eventos novos: T1 ✓
- §3.1 OrderSizing value object: T1 ✓
- §3.1 OrderSizingRepository Protocol: T2 ✓
- §3.1 Extensão MessagingPort: T2 ✓
- §3.1 Tabela order_sizings + migration + ORM: T3 ✓
- §3.1 SqlAlchemyOrderSizingRepository: T4 ✓
- §3.1 Stream SIZING_DECISIONS + publishes integration tests: T5 ✓
- §3.1 SizingAgent + 6 settings + 3 métricas: T6 ✓
- §3.1 Containerização: T7 ✓
- §3.1 Testes unit + integration E2E: T1+T4+T6+T8 ✓
- §5 Schema completo: T3 ✓
- §6 Fluxos persist→publish: T6 ✓
- §7 Tratamento de falhas: T6 ✓
- §8 Settings + métricas + logs: T6 ✓
- §11 Open questions documentadas na spec.

**Placeholder scan:** sem TBD/TODO/"implement later".

**Type consistency:** `OrderSizing.decision` é `Literal["sized","skipped"]` em T1, T3, T4, T6. `SkipReason` enum em T1, T4, T6. `OrderSizingRepository.insert(sizing) -> bool` em T2, T4, T6.

**Atenção operacional:** mesmo padrão herdado (parar container `polycopy-sizing` antes de pytest E2E pra evitar interferência).
