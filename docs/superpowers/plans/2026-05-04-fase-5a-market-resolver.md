# Plano 5A — Market Resolution Tracker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. **Cadência: checkpoint humano por task** (mesma das fases anteriores).

**Goal:** Detectar quando markets do Polymarket resolvem (YES/NO/INVALID) e gravar em `market_resolutions` table — primeira peça da Fase 5 (backtest infra). Sem isso, `order_executions` em DRY-RUN é hipótese sem PnL real.

**Architecture:** Agente novo `polycopy-resolver:9107` herda `AgentBase`, polling-driven (não consome JetStream). Loop a cada 1h: lê `condition_ids` de `wallet_trades` que ainda não estão em `market_resolutions`, consulta Gamma com filtro `closed=true`, classifica `outcomePrices` por tolerância (≥0.99/≤0.01 terminais; 0.45-0.55 INVALID; senão pending=skip), insere idempotentemente.

**Tech Stack:** Python 3.12, pydantic v2, SQLAlchemy 2 async, alembic, prometheus_client, httpx + tenacity (já em uso por `PolymarketGammaClient`), pytest + asyncio + respx.

**Predecessor:** Fase 4 completa (head `348c83c`) + spec 5A (`f98c9bd`).

**Spec:** `docs/superpowers/specs/2026-05-04-fase-5a-market-resolver-design.md`.

---

## File Structure

**Novos arquivos (10):**
- `src/polycopy/domain/resolution.py` — `MarketResolution` value object + `ResolvedMarketDTO` mapper-only DTO.
- `src/polycopy/ports/market_resolution_repository.py` — Protocol.
- `src/polycopy/infrastructure/persistence/market_resolution_repository.py` — `SqlAlchemyMarketResolutionRepository`.
- `alembic/versions/0007_add_market_resolutions.py` — migration.
- `src/polycopy/agents/resolver.py` — `ResolverAgent` + `_classify_resolution` + `main()`.
- `tests/unit/domain/test_resolution.py` — 14 testes do value object + enum.
- `tests/unit/agents/test_resolver.py` — 12 testes do agent + classify.
- `tests/integration/test_market_resolution_repository.py` — 7 testes integration repo.
- `tests/integration/test_resolver_e2e.py` — 3 E2E tests.

**Modificados (10):**
- `src/polycopy/domain/events.py` — +`ResolvedOutcome` StrEnum.
- `src/polycopy/ports/polymarket_gamma.py` — +`list_markets_by_condition_ids_closed`.
- `src/polycopy/ports/__init__.py` — +export `MarketResolutionRepository`.
- `src/polycopy/infrastructure/polymarket/gamma_client.py` — +impl `list_markets_by_condition_ids_closed` + DTO mapper.
- `src/polycopy/infrastructure/persistence/models.py` — +`MarketResolutionRow` ORM.
- `src/polycopy/config.py` — +3 settings (resolver_*).
- `src/polycopy/infrastructure/observability/metrics.py` — +4 métricas.
- `tests/unit/infrastructure/test_metrics.py` — +4 testes.
- `tests/unit/infrastructure/test_gamma_client.py` — +3 testes pra novo método.
- `tests/unit/test_ports_typecheck.py` — +stub `_FakeMarketResolutionRepo` + helper.
- `docker-compose.yml` — +service `resolver`.
- `infra/prometheus/prometheus.yml` — +scrape job.
- `ARCHITECTURE.md` — +seção ResolverAgent + nó Mermaid.
- `.env.example` — +bloco "Resolver agent (Plano 5A)".

---

## Task 1: Domain — `ResolvedOutcome` enum + `MarketResolution` + `ResolvedMarketDTO`

**Files:**
- Modify: `src/polycopy/domain/events.py`
- Create: `src/polycopy/domain/resolution.py`
- Create: `tests/unit/domain/test_resolution.py`

**Reviewer:** opcional.

---

- [ ] **Step 1.1: Adicionar `ResolvedOutcome` em `events.py`**

LEIA `src/polycopy/domain/events.py` primeiro pra ver os enums existentes (`SkipReason`, `RejectionReason`, `ExecutionMode`, `FailureReason`). Adicionar no fim do arquivo:

```python
class ResolvedOutcome(StrEnum):
    """Outcome final de um market resolvido (Plano 5A)."""

    YES = "YES"
    NO = "NO"
    INVALID = "INVALID"  # disputed, cancelled, ou outcomes 50/50 split
```

- [ ] **Step 1.2: Criar `src/polycopy/domain/resolution.py`**

```python
"""MarketResolution value object + ResolvedMarketDTO mapper-only DTO.

ResolvedMarketDTO carrega campos brutos extras (closed, outcome_prices_raw,
uma_resolution_statuses_raw, yes_token_id, no_token_id, closed_time) que
NÃO pertencem ao Market value object canonical (que é só pra markets ativos).
DTO usado SOMENTE pelo mapper Gamma → ResolverAgent.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from polycopy.domain.events import ResolvedOutcome


@dataclass(frozen=True)
class ResolvedMarketDTO:
    """DTO mapper-only — carrega campos brutos do Gamma pra classificação."""

    condition_id: str
    yes_token_id: str
    no_token_id: str
    closed: bool
    closed_time: datetime | None
    outcome_prices_raw: str  # JSON string original do Gamma
    uma_resolution_statuses_raw: str | None  # JSON string original (pode ser '[]' ou null)


@dataclass(frozen=True)
class MarketResolution:
    """Snapshot imutável de um market resolvido.

    Persistido em market_resolutions; PK = condition_id.

    Invariantes:
    - resolved_outcome ∈ {YES, NO} ↔ winning_token_id is not None
    - resolved_outcome == INVALID ↔ winning_token_id is None
    - resolved_at e closed_time (se presente) precisam ser tz-aware
    """

    condition_id: str
    resolved_outcome: ResolvedOutcome
    winning_token_id: str | None
    closed_time: datetime | None
    resolved_at: datetime
    outcome_prices_raw: str
    uma_resolution_statuses_raw: str | None

    def __post_init__(self) -> None:
        if self.resolved_outcome in (ResolvedOutcome.YES, ResolvedOutcome.NO):
            if self.winning_token_id is None:
                raise ValueError(
                    f"{self.resolved_outcome.value} resolution must have winning_token_id"
                )
        elif self.resolved_outcome == ResolvedOutcome.INVALID:
            if self.winning_token_id is not None:
                raise ValueError("INVALID resolution must have winning_token_id=None")
        if self.resolved_at.tzinfo is None:
            raise ValueError("resolved_at must be timezone-aware")
        if self.closed_time is not None and self.closed_time.tzinfo is None:
            raise ValueError("closed_time must be timezone-aware")
```

- [ ] **Step 1.3: Escrever 14 testes unit (RED)**

Create `tests/unit/domain/test_resolution.py`:

```python
"""Testes unit dos value objects do Plano 5A."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution, ResolvedMarketDTO


_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN_YES = "111"
_VALID_TOKEN_NO = "222"
_VALID_RAW = '["0.0", "1.0"]'


def _make_resolution(
    *,
    outcome: ResolvedOutcome = ResolvedOutcome.YES,
    winning_token_id: str | None = _VALID_TOKEN_YES,
    decided_at_naive: bool = False,
) -> MarketResolution:
    decided_at = (
        datetime(2026, 5, 1) if decided_at_naive else datetime.now(tz=UTC)
    )
    return MarketResolution(
        condition_id=_VALID_COND,
        resolved_outcome=outcome,
        winning_token_id=winning_token_id,
        closed_time=datetime.now(tz=UTC),
        resolved_at=decided_at,
        outcome_prices_raw=_VALID_RAW,
        uma_resolution_statuses_raw='[]',
    )


def test_resolved_outcome_values() -> None:
    assert ResolvedOutcome.YES.value == "YES"
    assert ResolvedOutcome.NO.value == "NO"
    assert ResolvedOutcome.INVALID.value == "INVALID"


def test_resolution_yes_with_winning_token_valid() -> None:
    r = _make_resolution(outcome=ResolvedOutcome.YES, winning_token_id=_VALID_TOKEN_YES)
    assert r.resolved_outcome == ResolvedOutcome.YES
    assert r.winning_token_id == _VALID_TOKEN_YES


def test_resolution_no_with_winning_token_valid() -> None:
    r = _make_resolution(outcome=ResolvedOutcome.NO, winning_token_id=_VALID_TOKEN_NO)
    assert r.winning_token_id == _VALID_TOKEN_NO


def test_resolution_invalid_without_winning_token_valid() -> None:
    r = _make_resolution(outcome=ResolvedOutcome.INVALID, winning_token_id=None)
    assert r.winning_token_id is None


def test_resolution_yes_without_winning_token_raises() -> None:
    with pytest.raises(ValueError, match="must have winning_token_id"):
        _make_resolution(outcome=ResolvedOutcome.YES, winning_token_id=None)


def test_resolution_no_without_winning_token_raises() -> None:
    with pytest.raises(ValueError, match="must have winning_token_id"):
        _make_resolution(outcome=ResolvedOutcome.NO, winning_token_id=None)


def test_resolution_invalid_with_winning_token_raises() -> None:
    with pytest.raises(ValueError, match="INVALID resolution must have winning_token_id=None"):
        _make_resolution(outcome=ResolvedOutcome.INVALID, winning_token_id=_VALID_TOKEN_YES)


def test_resolution_naive_resolved_at_raises() -> None:
    with pytest.raises(ValueError, match="resolved_at must be timezone-aware"):
        _make_resolution(decided_at_naive=True)


def test_resolution_naive_closed_time_raises() -> None:
    with pytest.raises(ValueError, match="closed_time must be timezone-aware"):
        MarketResolution(
            condition_id=_VALID_COND,
            resolved_outcome=ResolvedOutcome.YES,
            winning_token_id=_VALID_TOKEN_YES,
            closed_time=datetime(2026, 5, 1),  # naive
            resolved_at=datetime.now(tz=UTC),
            outcome_prices_raw=_VALID_RAW,
            uma_resolution_statuses_raw='[]',
        )


def test_resolution_closed_time_none_valid() -> None:
    r = MarketResolution(
        condition_id=_VALID_COND,
        resolved_outcome=ResolvedOutcome.YES,
        winning_token_id=_VALID_TOKEN_YES,
        closed_time=None,
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw=_VALID_RAW,
        uma_resolution_statuses_raw='[]',
    )
    assert r.closed_time is None


def test_resolution_uma_statuses_none_valid() -> None:
    r = MarketResolution(
        condition_id=_VALID_COND,
        resolved_outcome=ResolvedOutcome.YES,
        winning_token_id=_VALID_TOKEN_YES,
        closed_time=datetime.now(tz=UTC),
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw=_VALID_RAW,
        uma_resolution_statuses_raw=None,
    )
    assert r.uma_resolution_statuses_raw is None


def test_resolution_frozen() -> None:
    r = _make_resolution()
    with pytest.raises(Exception):  # FrozenInstanceError
        r.resolved_outcome = ResolvedOutcome.NO  # type: ignore[misc]


def test_resolved_market_dto_basic() -> None:
    dto = ResolvedMarketDTO(
        condition_id=_VALID_COND,
        yes_token_id=_VALID_TOKEN_YES,
        no_token_id=_VALID_TOKEN_NO,
        closed=True,
        closed_time=datetime.now(tz=UTC),
        outcome_prices_raw='["1.0", "0.0"]',
        uma_resolution_statuses_raw='[]',
    )
    assert dto.closed is True
    assert dto.condition_id == _VALID_COND


def test_resolved_market_dto_frozen() -> None:
    dto = ResolvedMarketDTO(
        condition_id=_VALID_COND,
        yes_token_id=_VALID_TOKEN_YES,
        no_token_id=_VALID_TOKEN_NO,
        closed=False,
        closed_time=None,
        outcome_prices_raw='["0.5", "0.5"]',
        uma_resolution_statuses_raw=None,
    )
    with pytest.raises(Exception):
        dto.closed = True  # type: ignore[misc]
```

Run:
```bash
uv run pytest tests/unit/domain/test_resolution.py -v 2>&1 | tail -10
```
Expected: ImportError (`MarketResolution`, `ResolvedMarketDTO`, `ResolvedOutcome` não existem ainda).

- [ ] **Step 1.4: GREEN — implementar Steps 1.1 e 1.2**

Após implementação:
```bash
uv run pytest tests/unit/domain/test_resolution.py -v 2>&1 | tail -20
```
Expected: 14 PASS.

- [ ] **Step 1.5: Verificações + STOP — commit**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/domain/events.py src/polycopy/domain/resolution.py tests/unit/domain/test_resolution.py
uv run ruff format --check src/polycopy/domain/events.py src/polycopy/domain/resolution.py tests/unit/domain/test_resolution.py
uv run pytest tests/ 2>&1 | tail -5
```
Expected: tudo PASS.

Implementer NÃO commita. Controller pede confirmação humana, depois:
```bash
git add src/polycopy/domain/events.py src/polycopy/domain/resolution.py tests/unit/domain/test_resolution.py
git commit -m "feat(domain): add ResolvedOutcome, MarketResolution, ResolvedMarketDTO (Fase 5A)"
```

---

## Task 2: Ports — `MarketResolutionRepository` Protocol + extensão `PolymarketGammaPort`

**Files:**
- Create: `src/polycopy/ports/market_resolution_repository.py`
- Modify: `src/polycopy/ports/polymarket_gamma.py`
- Modify: `src/polycopy/ports/__init__.py`
- Modify: `tests/unit/test_ports_typecheck.py`

**Reviewer:** opcional.

---

- [ ] **Step 2.1: Criar `MarketResolutionRepository` Protocol**

Create `src/polycopy/ports/market_resolution_repository.py`:

```python
"""MarketResolutionRepository: contrato de persistência pra resoluções de markets."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.resolution import MarketResolution


class MarketResolutionRepository(Protocol):
    """Persistência idempotente de resoluções. Plano 5A.

    market_resolutions é puramente append-only — `insert` retorna False
    se já existe (PK condition_id). Sem UPDATEs.
    """

    async def insert(self, resolution: MarketResolution) -> bool:
        """Insere; True se nova, False se já existia (PK conflict)."""
        ...

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        """Retorna até `limit` condition_ids únicos de wallet_trades que NÃO
        estão em market_resolutions ainda.

        Query: LEFT JOIN wallet_trades vs market_resolutions
                WHERE market_resolutions.condition_id IS NULL.
        """
        ...
```

- [ ] **Step 2.2: Estender `PolymarketGammaPort`**

LEIA `src/polycopy/ports/polymarket_gamma.py` primeiro. Adicionar import:

```python
from polycopy.domain.resolution import ResolvedMarketDTO
```

Adicionar método ao Protocol (após `list_active_markets`):

```python
    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ) -> list[ResolvedMarketDTO]:
        """Lista markets COM filtro `closed=true` e `condition_ids`.

        Retorna até `limit` markets fechados. DTO carrega campos brutos
        (outcome_prices_raw, uma_resolution_statuses_raw, closed_time)
        necessários pra classificação no ResolverAgent.
        """
        ...
```

- [ ] **Step 2.3: Atualizar `ports/__init__.py`**

```python
from polycopy.ports.market_resolution_repository import MarketResolutionRepository
```

E adicionar `"MarketResolutionRepository"` ao `__all__` (alfabético).

- [ ] **Step 2.4: Estender `test_ports_typecheck.py`**

LEIA primeiro pra ver pattern (`_FakeMarketRepo`, `_FakeOrderExecutionRepo`).

Adicionar imports:

```python
from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution, ResolvedMarketDTO
from polycopy.ports import MarketResolutionRepository
```

Estender `_FakeGamma` com novo método:

```python
    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ) -> list[ResolvedMarketDTO]:
        return []
```

Adicionar fake stub:

```python
class _FakeMarketResolutionRepo:
    """Stub que implementa MarketResolutionRepository."""

    def __init__(self) -> None:
        self.inserted: list[MarketResolution] = []
        self.unresolved_to_return: list[str] = []

    async def insert(self, resolution: MarketResolution) -> bool:
        self.inserted.append(resolution)
        return True

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        return self.unresolved_to_return[:limit]
```

Helper + 2 testes:

```python
def _accepts_market_resolution_repo(_: MarketResolutionRepository) -> None:
    """Helper: mypy falha se o argumento não satisfizer MarketResolutionRepository."""


def test_fake_market_resolution_repo_satisfies_port() -> None:
    fake = _FakeMarketResolutionRepo()
    _accepts_market_resolution_repo(fake)


def test_resolution_ports_importable() -> None:
    assert MarketResolutionRepository is not None
    assert ResolvedOutcome is not None
    assert MarketResolution is not None
    assert ResolvedMarketDTO is not None
```

- [ ] **Step 2.5: Verificações + commit**

```bash
uv run mypy src/polycopy
uv run pytest tests/unit/test_ports_typecheck.py -v 2>&1 | tail -10
uv run ruff check src/polycopy/ports/market_resolution_repository.py src/polycopy/ports/polymarket_gamma.py src/polycopy/ports/__init__.py tests/unit/test_ports_typecheck.py
uv run pytest tests/ 2>&1 | tail -5
```

Commit:
```bash
git add src/polycopy/ports/market_resolution_repository.py src/polycopy/ports/polymarket_gamma.py src/polycopy/ports/__init__.py tests/unit/test_ports_typecheck.py
git commit -m "feat(ports): add MarketResolutionRepository and extend PolymarketGammaPort with closed query"
```

---

## Task 3: Tabela `market_resolutions` + migration alembic + ORM

**Files:**
- Modify: `src/polycopy/infrastructure/persistence/models.py`
- Create: `alembic/versions/0007_add_market_resolutions.py`

**Reviewer:** opcional (DDL puro).

---

- [ ] **Step 3.1: Adicionar `MarketResolutionRow` em `models.py`**

LEIA `models.py` primeiro pra ver pattern dos outros Rows (PK, CHECKs, indexes). Adicionar ao final do arquivo:

```python
class MarketResolutionRow(Base):
    __tablename__ = "market_resolutions"

    condition_id: Mapped[str] = mapped_column(String, primary_key=True)
    resolved_outcome: Mapped[str] = mapped_column(String, nullable=False)
    winning_token_id: Mapped[str | None] = mapped_column(String, nullable=True)
    closed_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    resolved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    outcome_prices_raw: Mapped[str] = mapped_column(String, nullable=False)
    uma_resolution_statuses_raw: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "resolved_outcome IN ('YES', 'NO', 'INVALID')",
            name="market_resolutions_outcome_enum",
        ),
        CheckConstraint(
            "(resolved_outcome IN ('YES', 'NO') AND winning_token_id IS NOT NULL) "
            "OR (resolved_outcome = 'INVALID' AND winning_token_id IS NULL)",
            name="market_resolutions_winning_token_consistency",
        ),
        Index("idx_market_resolutions_resolved_at", "resolved_at"),
        Index("idx_market_resolutions_outcome", "resolved_outcome"),
    )
```

- [ ] **Step 3.2: Criar migration `0007_add_market_resolutions.py`**

```python
"""add market_resolutions table

Revision ID: 0007
Revises: 0006
Create Date: 2026-05-04 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_resolutions",
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("resolved_outcome", sa.String(), nullable=False),
        sa.Column("winning_token_id", sa.String(), nullable=True),
        sa.Column("closed_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("outcome_prices_raw", sa.String(), nullable=False),
        sa.Column("uma_resolution_statuses_raw", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "resolved_outcome IN ('YES', 'NO', 'INVALID')",
            name="market_resolutions_outcome_enum",
        ),
        sa.CheckConstraint(
            "(resolved_outcome IN ('YES', 'NO') AND winning_token_id IS NOT NULL) "
            "OR (resolved_outcome = 'INVALID' AND winning_token_id IS NULL)",
            name="market_resolutions_winning_token_consistency",
        ),
        sa.PrimaryKeyConstraint("condition_id"),
    )
    op.create_index(
        "idx_market_resolutions_resolved_at",
        "market_resolutions",
        ["resolved_at"],
    )
    op.create_index(
        "idx_market_resolutions_outcome",
        "market_resolutions",
        ["resolved_outcome"],
    )


def downgrade() -> None:
    op.drop_index("idx_market_resolutions_outcome", table_name="market_resolutions")
    op.drop_index("idx_market_resolutions_resolved_at", table_name="market_resolutions")
    op.drop_table("market_resolutions")
```

- [ ] **Step 3.3: Validar alembic + commit**

```bash
docker compose ps postgres
uv run alembic upgrade head
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d market_resolutions"
uv run alembic downgrade -1
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\dt market_resolutions"
uv run alembic upgrade head
uv run mypy src/polycopy
uv run ruff check ...
uv run pytest tests/ 2>&1 | tail -5
```

Commit:
```bash
git add src/polycopy/infrastructure/persistence/models.py alembic/versions/0007_add_market_resolutions.py
git commit -m "feat(persistence): add market_resolutions table and MarketResolutionRow ORM"
```

---

## Task 4: `SqlAlchemyMarketResolutionRepository` + integration tests

**Files:**
- Create: `src/polycopy/infrastructure/persistence/market_resolution_repository.py`
- Create: `tests/integration/test_market_resolution_repository.py`

**Reviewer:** opcional.

---

- [ ] **Step 4.1: Escrever testes integration (RED)**

Create `tests/integration/test_market_resolution_repository.py`:

```python
"""Integration tests do SqlAlchemyMarketResolutionRepository — exige Postgres up."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution
from polycopy.infrastructure.persistence.market_resolution_repository import (
    SqlAlchemyMarketResolutionRepository,
)
from polycopy.ports import MarketResolutionRepository

pytestmark = pytest.mark.integration

_VALID_COND_A = "0x" + "ab" * 32
_VALID_COND_B = "0x" + "cd" * 32
_VALID_TOKEN_YES = "111"
_VALID_TOKEN_NO = "222"
_VALID_WALLET = "0x" + "1" * 40


def _resolution_yes(condition_id: str = _VALID_COND_A) -> MarketResolution:
    return MarketResolution(
        condition_id=condition_id,
        resolved_outcome=ResolvedOutcome.YES,
        winning_token_id=_VALID_TOKEN_YES,
        closed_time=datetime.now(tz=UTC),
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw='["1.0", "0.0"]',
        uma_resolution_statuses_raw='["resolved"]',
    )


def _resolution_invalid(condition_id: str) -> MarketResolution:
    return MarketResolution(
        condition_id=condition_id,
        resolved_outcome=ResolvedOutcome.INVALID,
        winning_token_id=None,
        closed_time=datetime.now(tz=UTC),
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw='["0.5", "0.5"]',
        uma_resolution_statuses_raw=None,
    )


async def _seed_wallet_trade(
    session: AsyncSession, *, condition_id: str, token_id: str = "999"
) -> None:
    """Insere uma row em wallet_trades pra testar get_unresolved_condition_ids."""
    await session.execute(
        text(
            "INSERT INTO wallet_trades "
            "(tx_hash, log_index, wallet, condition_id, token_id, side, "
            " price, size_usdc, occurred_at) "
            "VALUES (:tx, :idx, :w, :c, :t, 'BUY', 0.5, 10, now())"
        ),
        {
            "tx": "0x" + "f" * 64,
            "idx": condition_id[-1],  # diferenciar log_index entre rows
            "w": _VALID_WALLET,
            "c": condition_id,
            "t": token_id,
        },
    )
    await session.commit()


async def test_insert_returns_true_for_new(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketResolutionRepository(session)
        result = await repo.insert(_resolution_yes())
        await session.commit()
        assert result is True


async def test_insert_returns_false_for_duplicate(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketResolutionRepository(session)
        r = _resolution_yes()
        first = await repo.insert(r)
        await session.commit()
        second = await repo.insert(r)
        await session.commit()
        assert first is True
        assert second is False


async def test_insert_invalid_persists_no_winning_token(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketResolutionRepository(session)
        r = _resolution_invalid(_VALID_COND_A)
        await repo.insert(r)
        await session.commit()

        result = await session.execute(
            text(
                "SELECT resolved_outcome, winning_token_id "
                "FROM market_resolutions WHERE condition_id = :c"
            ),
            {"c": _VALID_COND_A},
        )
        row = result.one()
        assert row.resolved_outcome == "INVALID"
        assert row.winning_token_id is None


async def test_insert_yes_with_null_winning_token_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Bypass do __post_init__ via SQL cru pra validar CHECK no DB."""
    async with db_session_factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO market_resolutions "
                    "(condition_id, resolved_outcome, winning_token_id, "
                    " resolved_at, outcome_prices_raw) "
                    "VALUES (:c, 'YES', NULL, now(), '[\"1\",\"0\"]')"
                ),
                {"c": _VALID_COND_A},
            )
            await session.commit()


async def test_insert_invalid_outcome_string_violates_constraint(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """CHECK outcome_enum rejeita valor não-listado."""
    async with db_session_factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO market_resolutions "
                    "(condition_id, resolved_outcome, winning_token_id, "
                    " resolved_at, outcome_prices_raw) "
                    "VALUES (:c, 'MAYBE', '111', now(), '[\"0.5\",\"0.5\"]')"
                ),
                {"c": _VALID_COND_A},
            )
            await session.commit()


async def test_get_unresolved_condition_ids_left_join_works(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, condition_id=_VALID_COND_A)
        await _seed_wallet_trade(session, condition_id=_VALID_COND_B)

        repo = SqlAlchemyMarketResolutionRepository(session)
        # COND_A resolvido; COND_B não
        await repo.insert(_resolution_yes(_VALID_COND_A))
        await session.commit()

        unresolved = await repo.get_unresolved_condition_ids(limit=10)
        assert _VALID_COND_B in unresolved
        assert _VALID_COND_A not in unresolved


async def test_adapter_satisfies_protocol(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Mypy garante que SqlAlchemyMarketResolutionRepository satisfaz Protocol."""
    async with db_session_factory() as session:
        _: MarketResolutionRepository = SqlAlchemyMarketResolutionRepository(session)
```

Run:
```bash
uv run pytest tests/integration/test_market_resolution_repository.py -v 2>&1 | tail -10
```
Expected: ImportError (`SqlAlchemyMarketResolutionRepository` não existe).

- [ ] **Step 4.2: Implementar `market_resolution_repository.py`**

```python
"""SqlAlchemyMarketResolutionRepository: persistência idempotente de resoluções."""

from __future__ import annotations

from typing import cast

from sqlalchemy import CursorResult, distinct, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.resolution import MarketResolution
from polycopy.infrastructure.persistence.models import (
    MarketResolutionRow,
    WalletTradeRow,
)


class SqlAlchemyMarketResolutionRepository:
    """Persistência idempotente. PK = condition_id (1 row por mercado).

    market_resolutions é puramente append-only — sem UPDATEs.
    `insert` retorna False se já existe (PK conflict).
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, resolution: MarketResolution) -> bool:
        """Insere; True se nova, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(MarketResolutionRow)
            .values(
                condition_id=resolution.condition_id,
                resolved_outcome=resolution.resolved_outcome.value,
                winning_token_id=resolution.winning_token_id,
                closed_time=resolution.closed_time,
                resolved_at=resolution.resolved_at,
                outcome_prices_raw=resolution.outcome_prices_raw,
                uma_resolution_statuses_raw=resolution.uma_resolution_statuses_raw,
            )
            .on_conflict_do_nothing(index_elements=["condition_id"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        """LEFT JOIN wallet_trades vs market_resolutions WHERE resolution IS NULL."""
        stmt = (
            select(distinct(WalletTradeRow.condition_id))
            .outerjoin(
                MarketResolutionRow,
                MarketResolutionRow.condition_id == WalletTradeRow.condition_id,
            )
            .where(MarketResolutionRow.condition_id.is_(None))
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.all()]
```

- [ ] **Step 4.3: GREEN + verificações + commit**

```bash
uv run pytest tests/integration/test_market_resolution_repository.py -v 2>&1 | tail -15
uv run mypy src/polycopy
uv run ruff check ...
```

Commit:
```bash
git add src/polycopy/infrastructure/persistence/market_resolution_repository.py tests/integration/test_market_resolution_repository.py
git commit -m "feat(persistence): add SqlAlchemyMarketResolutionRepository with append-only insert + unresolved query"
```

---

## Task 5: Estensão `PolymarketGammaClient` com `list_markets_by_condition_ids_closed`

**Files:**
- Modify: `src/polycopy/infrastructure/polymarket/gamma_client.py`
- Modify: `tests/unit/infrastructure/test_gamma_client.py`

**Reviewer:** opcional.

---

- [ ] **Step 5.1: Escrever 3 testes respx (RED)**

LEIA `tests/unit/infrastructure/test_gamma_client.py` primeiro pra ver pattern dos testes existentes.

Adicionar imports:

```python
from polycopy.domain.resolution import ResolvedMarketDTO
```

Adicionar 3 testes:

```python
@respx.mock
async def test_list_markets_by_condition_ids_closed_parses_settled() -> None:
    """Parse de market settled YES — outcomePrices ["1.0", "0.0"]."""
    payload = [
        {
            "conditionId": "0x" + "ab" * 32,
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["Yes", "No"]',
            "closed": True,
            "closedTime": "2026-04-01T12:00:00Z",
            "outcomePrices": '["1.0", "0.0"]',
            "umaResolutionStatuses": '["resolved"]',
        }
    ]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    dtos = await client.list_markets_by_condition_ids_closed(
        condition_ids=["0x" + "ab" * 32], limit=10
    )

    assert len(dtos) == 1
    dto = dtos[0]
    assert isinstance(dto, ResolvedMarketDTO)
    assert dto.closed is True
    assert dto.condition_id == "0x" + "ab" * 32
    assert dto.yes_token_id == "111"
    assert dto.no_token_id == "222"
    assert dto.outcome_prices_raw == '["1.0", "0.0"]'
    assert dto.uma_resolution_statuses_raw == '["resolved"]'


@respx.mock
async def test_list_markets_by_condition_ids_closed_passes_correct_params() -> None:
    """Confirma que params da request batem: condition_ids + closed=true."""
    captured_request: list[httpx.Request] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        captured_request.append(request)
        return httpx.Response(200, json=[])

    respx.get("https://gamma-api.polymarket.com/markets").mock(side_effect=_capture)

    client = _make_client()
    await client.list_markets_by_condition_ids_closed(
        condition_ids=["0x" + "ab" * 32, "0x" + "cd" * 32], limit=50
    )

    assert len(captured_request) == 1
    params = dict(captured_request[0].url.params)
    assert params["closed"] == "true"
    assert "condition_ids" in params
    assert params["limit"] == "50"


@respx.mock
async def test_list_markets_by_condition_ids_closed_handles_null_uma() -> None:
    """umaResolutionStatuses pode vir como string vazia ou ausente — DTO recebe None."""
    payload = [
        {
            "conditionId": "0x" + "ab" * 32,
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["Yes", "No"]',
            "closed": True,
            "closedTime": None,
            "outcomePrices": '["0.5", "0.5"]',
            # sem umaResolutionStatuses
        }
    ]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    dtos = await client.list_markets_by_condition_ids_closed(
        condition_ids=["0x" + "ab" * 32], limit=10
    )

    assert len(dtos) == 1
    assert dtos[0].uma_resolution_statuses_raw is None
    assert dtos[0].closed_time is None
```

Run:
```bash
uv run pytest tests/unit/infrastructure/test_gamma_client.py -v -k "closed" 2>&1 | tail -10
```
Expected: AttributeError (`list_markets_by_condition_ids_closed` não existe).

- [ ] **Step 5.2: Implementar método em `gamma_client.py`**

LEIA `gamma_client.py` primeiro pra ver pattern de `list_active_markets` + `_row_to_markets`.

Adicionar imports:

```python
from polycopy.domain.resolution import ResolvedMarketDTO
```

Adicionar método em `PolymarketGammaClient` (após `list_active_markets`):

```python
    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ) -> list[ResolvedMarketDTO]:
        """Lista markets fechados filtrados por condition_ids.

        Gamma aceita filtro `condition_ids` (CSV) + `closed=true`.
        Retorna ResolvedMarketDTOs com campos brutos pra classificação.
        """
        rows = await self._fetch_markets(
            params={
                "condition_ids": ",".join(condition_ids),
                "closed": "true",
                "limit": limit,
            }
        )
        out: list[ResolvedMarketDTO] = []
        for row in rows:
            dto = self._row_to_resolved_dto(row)
            if dto is not None:
                out.append(dto)
        return out

    @staticmethod
    def _row_to_resolved_dto(row: dict[str, Any]) -> ResolvedMarketDTO | None:
        """Mapeia row Gamma pra ResolvedMarketDTO. Retorna None se shape inválido."""
        token_ids_raw = row.get("clobTokenIds")
        if isinstance(token_ids_raw, str):
            token_ids_raw = json.loads(token_ids_raw)
        if not isinstance(token_ids_raw, list) or len(token_ids_raw) != 2:
            return None

        condition_id_raw = row.get("conditionId")
        if not isinstance(condition_id_raw, str):
            return None

        outcome_prices_raw = row.get("outcomePrices")
        if not isinstance(outcome_prices_raw, str):
            return None

        closed_raw = row.get("closed", False)
        closed = bool(closed_raw)

        closed_time_raw = row.get("closedTime")
        closed_time = (
            datetime.fromisoformat(closed_time_raw.replace("Z", "+00:00"))
            if isinstance(closed_time_raw, str)
            else None
        )

        uma_raw = row.get("umaResolutionStatuses")
        # Pode ser None, string vazia, ou JSON string. Normaliza pra string ou None.
        uma_normalized: str | None
        if uma_raw is None or uma_raw == "":
            uma_normalized = None
        elif isinstance(uma_raw, str):
            uma_normalized = uma_raw
        else:
            uma_normalized = json.dumps(uma_raw)

        return ResolvedMarketDTO(
            condition_id=condition_id_raw,
            yes_token_id=str(token_ids_raw[0]),
            no_token_id=str(token_ids_raw[1]),
            closed=closed,
            closed_time=closed_time,
            outcome_prices_raw=outcome_prices_raw,
            uma_resolution_statuses_raw=uma_normalized,
        )
```

- [ ] **Step 5.3: GREEN + verificações + commit**

```bash
uv run pytest tests/unit/infrastructure/test_gamma_client.py -v 2>&1 | tail -15
uv run mypy src/polycopy
uv run ruff check ...
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -5
```

Commit:
```bash
git add src/polycopy/infrastructure/polymarket/gamma_client.py tests/unit/infrastructure/test_gamma_client.py
git commit -m "feat(polymarket): add list_markets_by_condition_ids_closed to PolymarketGammaClient"
```

---

## Task 6: `ResolverAgent` + 3 settings + 4 métricas + .env.example + 12 unit tests (REVIEWER OBRIGATÓRIO)

**Files:**
- Create: `src/polycopy/agents/resolver.py`
- Create: `tests/unit/agents/test_resolver.py`
- Modify: `src/polycopy/config.py`
- Modify: `src/polycopy/infrastructure/observability/metrics.py`
- Modify: `tests/unit/infrastructure/test_metrics.py`
- Modify: `.env.example`

**Reviewer:** **OBRIGATÓRIO** (lógica principal — `_classify_resolution` tem múltiplos edge cases sensíveis).

---

- [ ] **Step 6.1: Adicionar 3 settings em `config.py`**

LEIA primeiro pra ver pattern. Adicionar bloco no fim do `Settings`:

```python
    # Resolver agent (Plano 5A)
    resolver_metrics_port: int = Field(9107, alias="RESOLVER_METRICS_PORT")
    resolver_sync_interval_s: float = Field(3600.0, alias="RESOLVER_SYNC_INTERVAL_SECONDS")
    resolver_batch_size: int = Field(100, alias="RESOLVER_BATCH_SIZE")
```

- [ ] **Step 6.2: Adicionar 4 métricas em `metrics.py`**

LEIA primeiro. Adicionar campos no dataclass `Metrics` (após `executor_consecutive_failures`):

```python
    resolver_sync_total: Counter
    resolver_sync_duration_seconds: Histogram
    resolver_resolutions_detected_total: Counter
    resolver_unresolved_pending: Gauge
```

Adicionar entries em `make_metrics()`:

```python
        resolver_sync_total=Counter(
            "polycopy_resolver_sync",
            "Iterações de sync do ResolverAgent.",
            labelnames=["result"],
            registry=target,
        ),
        resolver_sync_duration_seconds=Histogram(
            "polycopy_resolver_sync_duration_seconds",
            "Duração end-to-end de uma iteração de sync.",
            registry=target,
        ),
        resolver_resolutions_detected_total=Counter(
            "polycopy_resolver_resolutions_detected",
            "Resoluções gravadas em market_resolutions.",
            labelnames=["outcome"],
            registry=target,
        ),
        resolver_unresolved_pending=Gauge(
            "polycopy_resolver_unresolved_pending",
            "Backlog atual de condition_ids unresolved (atualizado a cada loop).",
            registry=target,
        ),
```

- [ ] **Step 6.3: Adicionar 4 testes em `test_metrics.py`**

```python
def test_metrics_resolver_sync_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_sync_total.labels(result="ok").inc()
    metrics.resolver_sync_total.labels(result="fail").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_sync"]
    assert len(matching) == 1


def test_metrics_resolver_sync_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_sync_duration_seconds.observe(1.5)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_sync_duration_seconds"]
    assert matching


def test_metrics_resolver_resolutions_detected_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_resolutions_detected_total.labels(outcome="yes").inc()
    metrics.resolver_resolutions_detected_total.labels(outcome="no").inc()
    metrics.resolver_resolutions_detected_total.labels(outcome="invalid").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_resolutions_detected"]
    assert len(matching) == 1


def test_metrics_resolver_unresolved_pending_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_unresolved_pending.set(42)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_unresolved_pending"]
    assert matching
```

- [ ] **Step 6.4: Atualizar `.env.example`**

Adicionar bloco no fim:

```bash
# --- Resolver agent (Plano 5A) ---
RESOLVER_METRICS_PORT=9107
RESOLVER_SYNC_INTERVAL_SECONDS=3600
RESOLVER_BATCH_SIZE=100
```

- [ ] **Step 6.5: Escrever 12 testes unit do agent (RED)**

Create `tests/unit/agents/test_resolver.py`:

```python
"""Testes unit do ResolverAgent — Gamma + repo mockados via Protocol."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.resolver import ResolverAgent, _classify_resolution
from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution, ResolvedMarketDTO
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MarketResolutionRepository, PolymarketGammaPort


_VALID_COND = "0x" + "ab" * 32
_TOKEN_YES = "111"
_TOKEN_NO = "222"


def _dto(
    *,
    closed: bool = True,
    outcome_prices_raw: str = '["1.0", "0.0"]',
    uma_raw: str | None = '["resolved"]',
    closed_time: datetime | None = None,
) -> ResolvedMarketDTO:
    return ResolvedMarketDTO(
        condition_id=_VALID_COND,
        yes_token_id=_TOKEN_YES,
        no_token_id=_TOKEN_NO,
        closed=closed,
        closed_time=closed_time or datetime.now(tz=UTC),
        outcome_prices_raw=outcome_prices_raw,
        uma_resolution_statuses_raw=uma_raw,
    )


class _StubGamma:
    def __init__(self, response: list[ResolvedMarketDTO] | None = None) -> None:
        self._response = response or []
        self.calls: list[list[str]] = []

    async def get_market(self, token_id):  # pragma: no cover
        return None

    async def list_active_markets(self, *, limit: int):  # pragma: no cover
        return []

    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ) -> list[ResolvedMarketDTO]:
        self.calls.append(list(condition_ids))
        return self._response


class _StubResolutionRepo:
    def __init__(
        self, *, unresolved: list[str] | None = None, insert_returns_new: bool = True
    ) -> None:
        self.inserted: list[MarketResolution] = []
        self._unresolved = unresolved or []
        self._returns_new = insert_returns_new

    async def insert(self, resolution: MarketResolution) -> bool:
        self.inserted.append(resolution)
        return self._returns_new

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        return self._unresolved[:limit]


@pytest.fixture
def metrics() -> Metrics:
    return make_metrics(registry=CollectorRegistry())


def _make_agent(
    *, metrics: Metrics, gamma: _StubGamma, repo: _StubResolutionRepo
) -> ResolverAgent:
    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        yield repo

    return ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )


# ----- _classify_resolution: 6 cenários -----


def test_classify_settled_yes() -> None:
    dto = _dto(outcome_prices_raw='["1.0", "0.0"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.YES
    assert r.winning_token_id == _TOKEN_YES


def test_classify_settled_no() -> None:
    dto = _dto(outcome_prices_raw='["0.0", "1.0"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.NO
    assert r.winning_token_id == _TOKEN_NO


def test_classify_invalid_50_50() -> None:
    dto = _dto(outcome_prices_raw='["0.5", "0.5"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.INVALID
    assert r.winning_token_id is None


def test_classify_pending_non_terminal() -> None:
    """Preços fora das tolerâncias terminais E fora do split INVALID — pending."""
    dto = _dto(outcome_prices_raw='["0.7", "0.3"]')
    r = _classify_resolution(dto)
    assert r is None


def test_classify_edge_rounding_yes() -> None:
    """0.999/0.001 ainda dentro da tolerância 0.99/0.01 → YES."""
    dto = _dto(outcome_prices_raw='["0.999", "0.001"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.YES


def test_classify_edge_invalid_skewed() -> None:
    """0.49/0.51 dentro da tolerância 0.45-0.55 → INVALID."""
    dto = _dto(outcome_prices_raw='["0.49", "0.51"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.INVALID


# ----- run_once -----


async def test_run_once_happy_path_inserts_resolution(metrics: Metrics) -> None:
    repo = _StubResolutionRepo(unresolved=[_VALID_COND])
    gamma = _StubGamma(response=[_dto(outcome_prices_raw='["1.0", "0.0"]')])
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    assert len(repo.inserted) == 1
    assert repo.inserted[0].resolved_outcome == ResolvedOutcome.YES
    assert len(gamma.calls) == 1
    assert gamma.calls[0] == [_VALID_COND]


async def test_run_once_empty_unresolved_skips_gamma(metrics: Metrics) -> None:
    repo = _StubResolutionRepo(unresolved=[])
    gamma = _StubGamma()
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    assert len(gamma.calls) == 0
    assert len(repo.inserted) == 0


async def test_run_once_pending_market_not_inserted(metrics: Metrics) -> None:
    repo = _StubResolutionRepo(unresolved=[_VALID_COND])
    gamma = _StubGamma(response=[_dto(outcome_prices_raw='["0.7", "0.3"]')])
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    assert len(repo.inserted) == 0  # pending → skip


async def test_run_once_gamma_exception_records_fail(metrics: Metrics) -> None:
    class _RaisingGamma(_StubGamma):
        async def list_markets_by_condition_ids_closed(
            self, *, condition_ids, limit
        ):
            raise RuntimeError("gamma down")

    repo = _StubResolutionRepo(unresolved=[_VALID_COND])
    agent = _make_agent(metrics=metrics, gamma=_RaisingGamma(), repo=repo)

    # Não propaga (capturada no try/except do run_once)
    await agent.run_once()

    assert len(repo.inserted) == 0
    fail_count = metrics.resolver_sync_total.labels(result="fail")._value.get()
    assert fail_count == 1.0


async def test_run_once_duplicate_does_not_increment_detected(metrics: Metrics) -> None:
    """repo.insert retorna False (PK conflict) — métrica não conta como detected."""
    repo = _StubResolutionRepo(unresolved=[_VALID_COND], insert_returns_new=False)
    gamma = _StubGamma(response=[_dto(outcome_prices_raw='["1.0", "0.0"]')])
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    yes_count = metrics.resolver_resolutions_detected_total.labels(outcome="yes")._value.get()
    assert yes_count == 0  # PK conflict — não conta
```

Run:
```bash
uv run pytest tests/unit/agents/test_resolver.py -v 2>&1 | tail -10
```
Expected: ImportError.

- [ ] **Step 6.6: Implementar `agents/resolver.py`**

```python
"""ResolverAgent: detecta quando markets do Polymarket resolvem (YES/NO/INVALID).

Loop polling-driven (não consome JetStream). A cada RESOLVER_SYNC_INTERVAL_SECONDS:
1. Lê condition_ids de wallet_trades sem resolução em market_resolutions.
2. Consulta Gamma com filtro closed=true + condition_ids.
3. Classifica cada market via _classify_resolution (tolerâncias de pricing).
4. Insere idempotentemente em market_resolutions.

Plano 5A — primeira peça da Fase 5 (backtest infra).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution, ResolvedMarketDTO
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MarketResolutionRepository, PolymarketGammaPort


RepoFactory = Callable[[], AbstractAsyncContextManager[MarketResolutionRepository]]


_TOLERANCE_TERMINAL = Decimal("0.01")  # extremos: ≥0.99 / ≤0.01
_TOLERANCE_INVALID_LOW = Decimal("0.45")
_TOLERANCE_INVALID_HIGH = Decimal("0.55")


def _classify_resolution(dto: ResolvedMarketDTO) -> MarketResolution | None:
    """Classifica um ResolvedMarketDTO em MarketResolution ou None (pending).

    Tolerâncias:
    - Terminal (YES/NO): preços ≥0.99 e ≤0.01
    - INVALID: ambos preços ∈ [0.45, 0.55]
    - Senão: pending UMA — retorna None
    """
    if not dto.closed:
        return None  # defensivo (query filtrou closed=true)

    try:
        prices = json.loads(dto.outcome_prices_raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(prices, list) or len(prices) != 2:
        return None

    try:
        yes_price = Decimal(str(prices[0]))
        no_price = Decimal(str(prices[1]))
    except (ValueError, TypeError):
        return None

    now = datetime.now(tz=UTC)

    # Settled YES
    if yes_price >= (Decimal("1") - _TOLERANCE_TERMINAL) and no_price <= _TOLERANCE_TERMINAL:
        return MarketResolution(
            condition_id=dto.condition_id,
            resolved_outcome=ResolvedOutcome.YES,
            winning_token_id=dto.yes_token_id,
            closed_time=dto.closed_time,
            resolved_at=now,
            outcome_prices_raw=dto.outcome_prices_raw,
            uma_resolution_statuses_raw=dto.uma_resolution_statuses_raw,
        )

    # Settled NO
    if no_price >= (Decimal("1") - _TOLERANCE_TERMINAL) and yes_price <= _TOLERANCE_TERMINAL:
        return MarketResolution(
            condition_id=dto.condition_id,
            resolved_outcome=ResolvedOutcome.NO,
            winning_token_id=dto.no_token_id,
            closed_time=dto.closed_time,
            resolved_at=now,
            outcome_prices_raw=dto.outcome_prices_raw,
            uma_resolution_statuses_raw=dto.uma_resolution_statuses_raw,
        )

    # INVALID (split 50/50 com tolerância)
    if (
        _TOLERANCE_INVALID_LOW <= yes_price <= _TOLERANCE_INVALID_HIGH
        and _TOLERANCE_INVALID_LOW <= no_price <= _TOLERANCE_INVALID_HIGH
    ):
        return MarketResolution(
            condition_id=dto.condition_id,
            resolved_outcome=ResolvedOutcome.INVALID,
            winning_token_id=None,
            closed_time=dto.closed_time,
            resolved_at=now,
            outcome_prices_raw=dto.outcome_prices_raw,
            uma_resolution_statuses_raw=dto.uma_resolution_statuses_raw,
        )

    # Preços não-terminais — UMA ainda processando
    return None


class ResolverAgent(AgentBase):
    """Polling-driven agent — detecta resoluções de markets em wallet_trades.

    Não consome JetStream. Loop a cada sync_interval_s:
    repo.get_unresolved_condition_ids → gamma.list_markets_by_condition_ids_closed →
    classify → repo.insert (idempotente).
    """

    name = "resolver"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        sync_interval_s: float,
        gamma: PolymarketGammaPort,
        repo_factory: RepoFactory,
        batch_size: int,
        metrics: Metrics,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=sync_interval_s)
        self._gamma = gamma
        self._repo_factory = repo_factory
        self._batch_size = batch_size
        self._metrics = metrics

    async def run_once(self) -> None:
        start = time.perf_counter()
        try:
            async with self._repo_factory() as repo:
                unresolved = await repo.get_unresolved_condition_ids(limit=self._batch_size)

            if not unresolved:
                self._metrics.resolver_sync_total.labels(result="ok").inc()
                self._metrics.resolver_unresolved_pending.set(0)
                self._log.info("resolver_sync_no_unresolved")
                return

            markets = await self._gamma.list_markets_by_condition_ids_closed(
                condition_ids=unresolved,
                limit=len(unresolved),
            )

            resolutions_detected = 0
            outcomes_count = {"yes": 0, "no": 0, "invalid": 0}
            async with self._repo_factory() as repo:
                for market_dto in markets:
                    resolution = _classify_resolution(market_dto)
                    if resolution is None:
                        continue
                    inserted = await repo.insert(resolution)
                    if inserted:
                        resolutions_detected += 1
                        outcomes_count[resolution.resolved_outcome.value.lower()] += 1

            for outcome_label, count in outcomes_count.items():
                if count > 0:
                    self._metrics.resolver_resolutions_detected_total.labels(
                        outcome=outcome_label
                    ).inc(count)

            self._metrics.resolver_sync_total.labels(result="ok").inc()
            self._metrics.resolver_unresolved_pending.set(
                len(unresolved) - resolutions_detected
            )
            self._log.info(
                "resolver_sync_completed",
                unresolved_checked=len(unresolved),
                resolutions_detected=resolutions_detected,
                yes=outcomes_count["yes"],
                no=outcomes_count["no"],
                invalid=outcomes_count["invalid"],
            )
        except Exception as exc:  # noqa: BLE001 — qualquer falha → métrica + log + retry no próximo ciclo
            self._metrics.resolver_sync_total.labels(result="fail").inc()
            self._log.warning(
                "resolver_sync_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
        finally:
            self._metrics.resolver_sync_duration_seconds.observe(time.perf_counter() - start)


def _make_repo_factory(session_factory: async_sessionmaker[AsyncSession]) -> RepoFactory:
    from polycopy.infrastructure.persistence.market_resolution_repository import (
        SqlAlchemyMarketResolutionRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyMarketResolutionRepository(session)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return _factory


async def main() -> None:
    """Entrypoint: monta dependências, sobe /metrics, registra signal handlers, roda."""
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.resolver_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(session_factory)

    gamma = PolymarketGammaClient(
        base_url=settings.gamma_api_base_url,
        metrics=metrics,
    )

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = ResolverAgent(
        stopping=stopping,
        sync_interval_s=settings.resolver_sync_interval_s,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=settings.resolver_batch_size,
        metrics=metrics,
    )
    try:
        await agent.run()
    finally:
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6.7: GREEN + verificações + STOP — code reviewer obrigatório**

```bash
uv run pytest tests/unit/agents/test_resolver.py -v 2>&1 | tail -25
uv run pytest tests/unit/infrastructure/test_metrics.py -v 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check ...
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -5
```

Reviewer obrigatório (lógica de classificação tem múltiplos edge cases). Após reviewer + fixes:

```bash
git add src/polycopy/agents/resolver.py src/polycopy/config.py src/polycopy/infrastructure/observability/metrics.py tests/unit/agents/test_resolver.py tests/unit/infrastructure/test_metrics.py .env.example
git commit -m "feat(agents): add ResolverAgent with pricing-tolerance classification"
```

---

## Task 7: Container `polycopy-resolver:9107` + scrape Prometheus + ARCHITECTURE.md

**Files:**
- Modify: `docker-compose.yml`
- Modify: `infra/prometheus/prometheus.yml`
- Modify: `ARCHITECTURE.md`

**Reviewer:** opcional.

---

- [ ] **Step 7.1: Adicionar service `resolver` em `docker-compose.yml`**

LEIA primeiro pra ver pattern de outros agentes (marketdata, executor). Adicionar service após `executor`:

```yaml
  resolver:
    build:
      context: .
      dockerfile: Dockerfile.agent
      args:
        AGENT_MODULE: resolver
    image: polycopy/resolver:dev
    container_name: polycopy-resolver
    restart: unless-stopped
    labels:
      com.polycopy.role: agent
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      ENV: ${ENV}
      LOG_LEVEL: ${LOG_LEVEL}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_PORT: 5432
      POSTGRES_HOST: polycopy-postgres
      NATS_URL: nats://polycopy-nats:4222
      REDIS_URL: redis://polycopy-redis:6379/0
      GAMMA_API_BASE_URL: https://gamma-api.polymarket.com
      CLOB_API_BASE_URL: https://clob.polymarket.com
      RESOLVER_METRICS_PORT: "9107"
      RESOLVER_SYNC_INTERVAL_SECONDS: "3600"
      RESOLVER_BATCH_SIZE: "100"
    ports:
      - "127.0.0.1:9107:9107"
    healthcheck:
      test: ["CMD-SHELL", "python -c 'import urllib.request; urllib.request.urlopen(\"http://localhost:9107/metrics\", timeout=2).read()' || exit 1"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 15s
```

- [ ] **Step 7.2: Adicionar scrape job em `prometheus.yml`**

```yaml
  - job_name: polycopy-resolver
    static_configs:
      - targets: ['polycopy-resolver:9107']
```

- [ ] **Step 7.3: Atualizar `ARCHITECTURE.md`**

LEIA primeiro. Aplicar:

1. Adicionar `resolver` na lista/tabela de containers + porta 9107.
2. Se houver Mermaid, adicionar nó conectando: `wallet_trades` (read condition_ids) → resolver → Gamma `/markets?closed=true` → `market_resolutions` (write).
3. Adicionar subseção:

```markdown
## ResolverAgent (Plano 5A)

Polling-driven agent que detecta quando markets do Polymarket resolvem
(YES/NO/INVALID) e grava em `market_resolutions`. Loop a cada 1h:
lê `condition_ids` de `wallet_trades` sem resolução, consulta Gamma
com filtro `closed=true`, classifica `outcomePrices` por tolerância
(≥0.99/≤0.01 terminais; 0.45-0.55 INVALID; senão pending=skip),
insere idempotentemente.

Sem JetStream — append-only, fonte de verdade pra PnL hipotético do
backtest (Plano 5C).

Métricas: `polycopy_resolver_sync_total{result}`,
`polycopy_resolver_sync_duration_seconds`,
`polycopy_resolver_resolutions_detected_total{outcome}`,
`polycopy_resolver_unresolved_pending`.

Container: `polycopy-resolver`. Endpoint `/metrics`: porta 9107.
```

4. Adicionar 4 linhas na tabela de métricas se houver.

- [ ] **Step 7.4: Build + up + validar**

```bash
docker compose ps postgres
uv run alembic upgrade head
docker compose build resolver 2>&1 | tail -5
docker compose up -d resolver
sleep 20
docker compose ps resolver
docker compose logs --tail=30 resolver
curl -sf http://127.0.0.1:9107/metrics | grep polycopy_resolver | head -10
docker compose restart prometheus
sleep 10
curl -sf http://127.0.0.1:9090/api/v1/targets | python3 -m json.tool 2>/dev/null | grep -A 2 "resolver:9107" | head -10
uv run pytest tests/ 2>&1 | tail -5
```

- [ ] **Step 7.5: STOP — commit**

```bash
git add docker-compose.yml infra/prometheus/prometheus.yml ARCHITECTURE.md
git commit -m "feat(deploy): containerize resolver agent and wire Prometheus scrape"
```

---

## Task 8: Integration E2E `test_resolver_e2e.py`

**Files:**
- Create: `tests/integration/test_resolver_e2e.py`

**Reviewer:** opcional.

---

- [ ] **Step 8.1: Escrever 3 E2E tests**

Create `tests/integration/test_resolver_e2e.py`:

```python
"""E2E do ResolverAgent: agente real + Postgres real + Gamma fake (respx).

Exige `docker compose up -d postgres` antes.
Recomendado: `docker compose stop resolver` antes pra evitar interferência
do container de produção rodando.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.resolver import ResolverAgent, _make_repo_factory
from polycopy.domain.events import ResolvedOutcome
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.models import MarketResolutionRow
from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

pytestmark = pytest.mark.integration


_VALID_COND_YES = "0xaabbccddee" + "11" * 27
_VALID_COND_INVALID = "0xaabbccddee" + "22" * 27
_VALID_COND_PENDING = "0xaabbccddee" + "33" * 27
_VALID_WALLET = "0x" + "1" * 40


async def _seed_wallet_trade(session: AsyncSession, condition_id: str, idx: int) -> None:
    await session.execute(
        text(
            "INSERT INTO wallet_trades "
            "(tx_hash, log_index, wallet, condition_id, token_id, side, "
            " price, size_usdc, occurred_at) "
            "VALUES (:tx, :idx, :w, :c, '999', 'BUY', 0.5, 10, now())"
        ),
        {"tx": "0x" + "f" * 64, "idx": idx, "w": _VALID_WALLET, "c": condition_id},
    )
    await session.commit()


def _gamma_fixture(condition_id: str, outcome_prices: str) -> dict:
    return {
        "conditionId": condition_id,
        "clobTokenIds": '["111", "222"]',
        "outcomes": '["Yes", "No"]',
        "closed": True,
        "closedTime": "2026-04-15T10:00:00Z",
        "outcomePrices": outcome_prices,
        "umaResolutionStatuses": '["resolved"]',
    }


@respx.mock
async def test_e2e_yes_resolution_detected(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E: wallet_trade existe + Gamma retorna YES → DB tem row YES."""
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, _VALID_COND_YES, 1)

    payload = [_gamma_fixture(_VALID_COND_YES, '["1.0", "0.0"]')]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com", metrics=metrics
    )
    repo_factory = _make_repo_factory(db_session_factory)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(
            select(MarketResolutionRow).where(
                MarketResolutionRow.condition_id == _VALID_COND_YES
            )
        )
        row = result.scalar_one()
    assert row.resolved_outcome == ResolvedOutcome.YES.value
    assert row.winning_token_id == "111"


@respx.mock
async def test_e2e_invalid_resolution_detected(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E: market 50/50 → DB tem row INVALID com winning_token_id NULL."""
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, _VALID_COND_INVALID, 2)

    payload = [_gamma_fixture(_VALID_COND_INVALID, '["0.5", "0.5"]')]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com", metrics=metrics
    )
    repo_factory = _make_repo_factory(db_session_factory)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(
            select(MarketResolutionRow).where(
                MarketResolutionRow.condition_id == _VALID_COND_INVALID
            )
        )
        row = result.scalar_one()
    assert row.resolved_outcome == ResolvedOutcome.INVALID.value
    assert row.winning_token_id is None


@respx.mock
async def test_e2e_pending_market_not_resolved(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E: market closed mas preço 0.7/0.3 → não cria row em market_resolutions."""
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, _VALID_COND_PENDING, 3)

    payload = [_gamma_fixture(_VALID_COND_PENDING, '["0.7", "0.3"]')]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com", metrics=metrics
    )
    repo_factory = _make_repo_factory(db_session_factory)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(
            select(MarketResolutionRow).where(
                MarketResolutionRow.condition_id == _VALID_COND_PENDING
            )
        )
        row = result.scalar_one_or_none()
    assert row is None  # pending — não inserido
```

- [ ] **Step 8.2: GREEN + verificações + commit**

```bash
docker compose stop resolver  # evitar interferência
uv run alembic upgrade head
uv run pytest tests/integration/test_resolver_e2e.py -v 2>&1 | tail -15
docker compose start resolver
uv run mypy src/polycopy
uv run ruff check tests/integration/test_resolver_e2e.py
uv run ruff format --check tests/integration/test_resolver_e2e.py
uv run pytest tests/ 2>&1 | tail -10
```

Esperado: 3 PASS dos novos.

Commit:
```bash
git add tests/integration/test_resolver_e2e.py
git commit -m "test(resolver): add E2E integration covering YES, INVALID, pending flows"
```

---

## Self-Review (autor do plano)

**Spec coverage:**

| Spec § | Coberto em |
|---|---|
| §3.1 ResolvedOutcome enum + MarketResolution + ResolvedMarketDTO | T1 |
| §3.1 MarketResolutionRepository Protocol + extensão PolymarketGammaPort | T2 |
| §3.1 Tabela market_resolutions + migration + ORM | T3 |
| §3.1 SqlAlchemyMarketResolutionRepository | T4 |
| §3.1 PolymarketGammaClient extension | T5 |
| §3.1 ResolverAgent + 3 settings + 4 métricas | T6 |
| §3.1 Containerização + Prometheus + ARCHITECTURE | T7 |
| §3.1 Testes unit + integration + E2E | T1+T4+T6+T8 |
| §5 Schema completo (PK, CHECKs, indexes) | T3 |
| §6 Fluxos (run_once + classify_resolution) | T6 |
| §7 Tratamento de falhas (12 cenários) | T6 (try/except em run_once + classify retorna None pra inputs inválidos) |
| §8.1 Settings flat | T6 |
| §8.2 4 métricas | T6 |
| §8.3 Logs estruturados | T6 (`resolver_sync_completed`, `resolver_sync_no_unresolved`, `resolver_sync_failed`) |
| §11 Open questions documentadas na spec |

**Placeholder scan:** sem TBD/TODO/"implement later".

**Type consistency:**
- `ResolvedOutcome` enum em T1 (def), T4 (test), T6 (classify return), T8 (assertion).
- `MarketResolution` em T1 (def), T2 (Protocol type), T4 (test), T6 (classify return + repo.insert input), T8 (assertion).
- `ResolvedMarketDTO` em T1 (def), T2 (port type), T5 (mapper return + adapter return), T6 (classify input).
- `MarketResolutionRepository.insert(resolution) -> bool` em T2, T4, T6.
- `MarketResolutionRepository.get_unresolved_condition_ids(*, limit) -> list[str]` em T2, T4, T6.
- `list_markets_by_condition_ids_closed(*, condition_ids, limit)` em T2 (port), T5 (impl), T6 (call).

**Atenção operacional herdada:**
- Container `polycopy-resolver` deve ser parado antes de E2E tests (mesmo padrão dos outros).
- Pytest da suíte completa dropa tabelas no teardown — após testes: `uv run alembic upgrade head`.

**Code reviewer obrigatório em:** T6 (lógica de classificação tem edge cases sensíveis). Outras opcionais.

**Bite-sized check:** cada step é 2-5 minutos. T6 é a maior (~250 linhas resolver.py + 200 linhas tests). Implementer faz copy-paste do plano + roda. RED→GREEN→COMMIT respeitado.
