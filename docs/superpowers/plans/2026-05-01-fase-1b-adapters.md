# Fase 1B — Adapters (persistência, Data API, NATS, agent base): Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar os 5 adapters concretos que conectam o domínio puro da Fase 1A à infra: SQLAlchemy + alembic (passo 1.6), `SqlAlchemyWalletTradeRepository` (1.7), `PolymarketDataClient` httpx + tenacity (1.8), `NatsMessagingBus` (1.9), e `AgentBase` com graceful shutdown (1.10). Zero agente concreto rodando ainda — `agents/watcher` e `agents/notifier` entram no Plano 1C.

**Architecture:** Continua hexagonal. Cada adapter implementa um `Protocol` definido em `src/polycopy/ports/`. Stack 100% async. Schema do banco gerenciado por alembic em arquivo único na raiz; testes integration usam savepoint+rollback por test pra isolamento. Métricas Prometheus expostas via módulo central `infrastructure/observability/metrics.py`.

**Tech Stack adicionado nesta fase:**
- `sqlalchemy[asyncio]>=2.0` (ORM async)
- `alembic>=1.13` (migrations)
- `httpx>=0.27` (HTTP client async)
- `tenacity>=9` (retry com backoff)
- `prometheus-client>=0.21` (métricas)
- `respx>=0.21` (dev — mock httpx)

**Source spec:** `PROMPT_POLYCOPY_v2.md` (Fase 1, passos 1.6 a 1.10). Decisões técnicas adicionais estão na seção "Decisões técnicas" deste plano.

**Execution model:** Mesma cadência do Plano 1A. Usuário pede uma Task por vez (ex: "execute Task 1"). Implementador segue os steps, valida, commita. **NÃO avança pra Task N+1 sem confirmação explícita do usuário.** Pausa antes de `git add`/`git commit` mesmo quando o plano original mencionou "commit" (regra `feedback_commits.md`).

---

## Pre-flight checklist (uma vez, antes da Task 1)

- [ ] **Step P.1: Working directory correto**

Run: `pwd`
Expected: `/home/polycopy/projects/polycopy`

- [ ] **Step P.2: Plano 1A completo (11 commits convencionais)**

Run: `git log --oneline | head -15`
Expected: pelo menos os 6 commits da Fase 1A no topo, mais os 5 da Fase 0:
```
49e6713 feat(config): add Settings and structlog logging with secret filter
893e481 feat(ports): add MessagingPort, PolymarketDataPort, WalletTradeRepository protocols
8077344 feat(domain): add WalletTradeDetected event with NATS subject
4bd1dfb feat(domain): add Wallet, Trade, Position models with Side enum
6a73b24 feat(domain): add value objects (Money, Price, Bps, WalletAddress, ConditionId, TokenId)
d7b5b98 test: add infra connectivity smoke tests
414a906 chore: add pre-commit hooks and commitizen
30adf09 ci: add github actions pipeline for lint type test
396be15 feat: add docker-compose infra with postgres timescale nats redis prometheus
3ec527c docs: add README, env example, and bootstrap script
c7684ce chore: bootstrap pyproject and project skeleton
```

- [ ] **Step P.3: Working tree limpo**

Run: `git status`
Expected: working tree clean (apenas untracked esperados como `PROMPT_POLYCOPY_v2.md` e `docs/`).

- [ ] **Step P.4: Infra healthy + `.env` populado**

Run: `docker compose ps && ls -l .env`
Expected: 4 containers `(healthy)` e `.env` chmod 600.

Se faltar: `docker compose up -d --wait` e/ou `bash scripts/bootstrap-env.sh`.

- [ ] **Step P.5: Suite verde no estado atual (54 testes)**

Run:
```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0, 54 tests passados (50 unit + 4 integration).

Se falhar: corrija ANTES de Task 1. Não comece a Fase 1B em codebase vermelho.

- [ ] **Step P.6: Pre-commit hooks ativos**

Run: `uv run pre-commit run --all-files`
Expected: tudo passa.

---

## Decisões técnicas fixadas neste plano

| Item | Decisão | Justificativa |
|---|---|---|
| ORM | SQLAlchemy 2.x **async** com asyncpg | Stack async já estabelecida; SQLAlchemy 2.x async é estável e amplamente adotado. |
| Migrations | Alembic na raiz do repo (`alembic/` + `alembic.ini`) | Padrão da comunidade; Plano 1A já reservou o nome "alembic" no spec. |
| Strategy de migrations | `--autogenerate` parcialmente — schema inicial escrito à mão, autogenerate só pra deltas futuras | Autogenerate em primeira migration tende a gerar SQL ruim; a tabela inicial vai ser pequena e clara. |
| Cliente HTTP | httpx + tenacity | httpx async-first; tenacity tem decoradores limpos pra retry exponencial. |
| Mock HTTP em testes | respx | Foi feito pelo autor do httpx; suporta async out of the box. |
| Métricas | prometheus-client com Counter/Histogram em módulo central | Centralizar evita registry duplicado em testes. Endpoint `/metrics` HTTP fica pro Plano 1C (quando o watcher subir um servidor). |
| NATS modo | Core pub/sub (sem JetStream) | Plano 1B só precisa de fire-and-forget. JetStream (durability, replay) entra quando o sistema tiver mais consumidores. |
| Schema isolation em testes | Savepoint+rollback por test | Veloz; não suja banco entre tests. Cria schema uma vez via `alembic upgrade head` no fixture session-scope. |
| Schema da tabela `wallet_trades` | PK composto `(tx_hash, log_index)` + índice `(wallet, occurred_at DESC)` | PK provê dedup; índice acelera `latest_occurred_at(wallet)`. |
| Mapeamento ORM ↔ domain | Métodos explícitos `_to_row(trade)` / `_from_row(row)` no repositório | Mantém domain desacoplado de SQLAlchemy. Sem dependência de pydantic-sqlalchemy. |
| Agent base | ABC com `run_once()` abstrato + loop com `asyncio.Event` parando | Testável: testes injetam o Event direto. Signal handlers ficam num helper separado, não no `__init__`. |
| Coverage target | `domain/` ≥ 90% (mantém alvo do Plano 1A); `infrastructure/` ≥ 70% | Adapters têm I/O; cobertura 100% requer mocks excessivos. 70% cobre branches principais. |

---

## Estrutura de arquivos criada nesta fase

```
src/polycopy/
├── infrastructure/
│   ├── persistence/                       # NEW
│   │   ├── __init__.py                    # Task 1
│   │   ├── database.py                    # Task 1 — AsyncEngine + sessionmaker
│   │   ├── models.py                      # Task 1 — WalletTradeRow ORM
│   │   └── wallet_trade_repository.py     # Task 2
│   ├── messaging/                         # NEW
│   │   ├── __init__.py                    # Task 4
│   │   └── nats_bus.py                    # Task 4
│   ├── polymarket/                        # NEW
│   │   ├── __init__.py                    # Task 3
│   │   └── data_client.py                 # Task 3
│   └── observability/
│       ├── __init__.py                    # já existe
│       ├── logging.py                     # já existe
│       └── metrics.py                     # Task 3 — Counter/Histogram registry
└── agents/                                # NEW
    ├── __init__.py                        # Task 5
    └── _base.py                           # Task 5

alembic.ini                                # Task 1
alembic/
├── env.py                                 # Task 1
├── script.py.mako                         # Task 1
└── versions/
    └── 0001_initial_wallet_trades.py      # Task 1

tests/
├── conftest.py                            # Modify Task 1 (adiciona db_engine, db_session)
├── integration/
│   ├── test_infra_connectivity.py         # já existe
│   ├── test_persistence_smoke.py          # Task 1
│   ├── test_wallet_trade_repository.py    # Task 2
│   └── test_nats_bus.py                   # Task 4
└── unit/
    ├── infrastructure/
    │   ├── __init__.py                    # já existe
    │   ├── test_logging.py                # já existe
    │   ├── test_metrics.py                # Task 3
    │   └── test_data_client.py            # Task 3
    └── agents/                            # NEW
        ├── __init__.py                    # Task 5
        └── test_base.py                   # Task 5
```

---

## Task 1: Passo 1.6 — SQLAlchemy + alembic + tabela `wallet_trades`

**Objetivo:** ter SQLAlchemy 2.x async configurado lendo URL do `Settings`, alembic gerenciando o schema, e a primeira migration criando `wallet_trades`. Smoke test integration confirmando que `alembic upgrade head` funciona e que a tabela responde a `SELECT 1`.

**Files:**
- Modify: `pyproject.toml` (deps: `sqlalchemy[asyncio]>=2.0`, `alembic>=1.13`)
- Modify: `src/polycopy/config.py` (adicionar `postgres_async_dsn` property)
- Create: `src/polycopy/infrastructure/persistence/__init__.py`
- Create: `src/polycopy/infrastructure/persistence/database.py`
- Create: `src/polycopy/infrastructure/persistence/models.py`
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/script.py.mako`
- Create: `alembic/versions/0001_initial_wallet_trades.py`
- Modify: `tests/conftest.py` (adicionar fixtures `db_engine`, `db_session`)
- Create: `tests/integration/test_persistence_smoke.py`

---

- [ ] **Step 1.1: Modify `pyproject.toml` — adicionar deps**

Em `[project] dependencies`, adicionar (manter ordem alfabética não é obrigatório):

```toml
dependencies = [
    "pydantic>=2.9",
    "pydantic-settings>=2.5",
    "structlog>=24.4",
    "asyncpg>=0.30",
    "nats-py>=2.7",
    "redis>=5.1",
    "sqlalchemy[asyncio]>=2.0",
    "alembic>=1.13",
]
```

- [ ] **Step 1.2: Re-sync**

Run: `uv sync`
Expected: SQLAlchemy + alembic instaladas. `greenlet` entra como dep transitiva do SQLAlchemy async.

- [ ] **Step 1.3: Modify `src/polycopy/config.py` — adicionar `postgres_async_dsn`**

Localizar a property `postgres_dsn` e adicionar abaixo dela:

```python
    @property
    def postgres_async_dsn(self) -> str:
        """DSN async (asyncpg-style). Usado pelo SQLAlchemy async engine."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:"
            f"{self.postgres_password.get_secret_value()}@127.0.0.1:"
            f"{self.postgres_port}/{self.postgres_db}"
        )
```

- [ ] **Step 1.4: Create `src/polycopy/infrastructure/persistence/__init__.py`** (vazio)

```python
```

- [ ] **Step 1.5: Create `src/polycopy/infrastructure/persistence/database.py`**

```python
"""SQLAlchemy async engine + session factory.

Engine é singleton por processo; session é criada por request/operação.
Não inicializa conexões no import — chamador decide quando subir.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from polycopy.config import Settings


def make_engine(settings: Settings) -> AsyncEngine:
    """Cria AsyncEngine. Caller é dono do lifecycle (chame `await engine.dispose()` ao parar)."""
    return create_async_engine(
        settings.postgres_async_dsn,
        echo=False,
        pool_pre_ping=True,
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 1.6: Create `src/polycopy/infrastructure/persistence/models.py`**

```python
"""SQLAlchemy ORM models. Não vazam pra fora do package `persistence/`."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class WalletTradeRow(Base):
    __tablename__ = "wallet_trades"

    tx_hash: Mapped[str] = mapped_column(String, primary_key=True)
    log_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, nullable=False)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    token_id: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    size_usdc: Mapped[Decimal] = mapped_column(Numeric(28, 6), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint("log_index >= 0", name="wallet_trades_log_index_nonneg"),
        CheckConstraint("side IN ('BUY', 'SELL')", name="wallet_trades_side_enum"),
        CheckConstraint("price >= 0 AND price <= 1", name="wallet_trades_price_range"),
        Index("wallet_trades_wallet_occurred_at_idx", "wallet", "occurred_at", postgresql_using="btree"),
    )
```

- [ ] **Step 1.7: Create `alembic.ini`**

```ini
[alembic]
script_location = alembic
prepend_sys_path = .
version_path_separator = os
output_encoding = utf-8

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

- [ ] **Step 1.8: Create `alembic/env.py`**

```python
"""Alembic env: lê URL do `Settings` e usa metadata do `Base` da app."""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from polycopy.config import Settings
from polycopy.infrastructure.persistence.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _get_url() -> str:
    return Settings().postgres_async_dsn  # type: ignore[call-arg]


def run_migrations_offline() -> None:
    context.configure(
        url=_get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _get_url()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 1.9: Create `alembic/script.py.mako`**

```mako
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision: str = ${repr(up_revision)}
down_revision: Union[str, None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
```

- [ ] **Step 1.10: Create `alembic/versions/0001_initial_wallet_trades.py`**

```python
"""initial wallet_trades table

Revision ID: 0001
Revises:
Create Date: 2026-05-01 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "wallet_trades",
        sa.Column("tx_hash", sa.String(), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("wallet", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("side", sa.String(), nullable=False),
        sa.Column("price", sa.Numeric(8, 4), nullable=False),
        sa.Column("size_usdc", sa.Numeric(28, 6), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "inserted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("log_index >= 0", name="wallet_trades_log_index_nonneg"),
        sa.CheckConstraint("side IN ('BUY', 'SELL')", name="wallet_trades_side_enum"),
        sa.CheckConstraint(
            "price >= 0 AND price <= 1", name="wallet_trades_price_range"
        ),
        sa.PrimaryKeyConstraint("tx_hash", "log_index"),
    )
    op.create_index(
        "wallet_trades_wallet_occurred_at_idx",
        "wallet_trades",
        ["wallet", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("wallet_trades_wallet_occurred_at_idx", table_name="wallet_trades")
    op.drop_table("wallet_trades")
```

- [ ] **Step 1.11: Modify `tests/conftest.py` — adicionar fixtures `db_engine`/`db_session`**

Substituir o conteúdo atual por:

```python
"""Shared test fixtures and bootstrap.

- `settings`: singleton Settings carregada do `.env`.
- `db_engine`: engine async session-scope; roda `alembic upgrade head` no início.
- `db_session`: AsyncSession dentro de transação; rollback automático no teardown.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from polycopy.config import Settings
from polycopy.infrastructure.persistence.database import (
    make_engine,
    make_session_factory,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Singleton Settings carregada do `.env`. Use em testes integration."""
    return Settings()  # type: ignore[call-arg]


@pytest.fixture(scope="session")
def alembic_config() -> Config:
    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "alembic"))
    return cfg


@pytest.fixture(scope="session")
async def db_engine(
    settings: Settings, alembic_config: Config
) -> AsyncIterator[AsyncEngine]:
    """Engine async session-scope. Migra schema antes; dropa tudo no fim."""
    engine = make_engine(settings)
    # Aplica migrations no schema atual.
    command.upgrade(alembic_config, "head")
    try:
        yield engine
    finally:
        # Limpa schema entre runs de teste pra evitar lixo cumulativo.
        command.downgrade(alembic_config, "base")
        await engine.dispose()


@pytest.fixture
async def db_session_factory(
    db_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(db_engine)


@pytest.fixture
async def db_session(
    db_engine: AsyncEngine,
) -> AsyncIterator[AsyncSession]:
    """Session em transação; rollback no teardown — testes são isolados."""
    async with db_engine.connect() as conn:
        trans = await conn.begin()
        session = AsyncSession(bind=conn, expire_on_commit=False)
        try:
            yield session
        finally:
            await session.close()
            await trans.rollback()
```

Nota: `alembic.command.upgrade/downgrade` são síncronos. Como o env.py usa `asyncio.run` internamente, eles funcionam corretamente fora de event loop ativo. A chamada acontece **fora** do `async` do fixture (linha sem `await`), antes do `yield`.

- [ ] **Step 1.12: Write the failing test — `tests/integration/test_persistence_smoke.py`**

```python
"""Smoke test: alembic migrou o schema e a tabela responde."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.integration


async def test_wallet_trades_table_exists(db_session: AsyncSession) -> None:
    result = await db_session.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'wallet_trades'"
        )
    )
    assert result.scalar_one() == 1


async def test_wallet_trades_pk_is_composite(db_session: AsyncSession) -> None:
    result = await db_session.execute(
        text(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'wallet_trades'::regclass AND i.indisprimary
            ORDER BY a.attname
            """
        )
    )
    cols = [row[0] for row in result.all()]
    assert cols == ["log_index", "tx_hash"]


async def test_wallet_trades_index_on_wallet_occurred_at(db_session: AsyncSession) -> None:
    result = await db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'wallet_trades'"
        )
    )
    names = {row[0] for row in result.all()}
    assert "wallet_trades_wallet_occurred_at_idx" in names
```

- [ ] **Step 1.13: Rodar pre-flight do alembic manualmente — sanity**

Run: `uv run alembic current`
Expected: imprime nada (banco ainda virgem) ou `0001` se já migrado em sessão anterior.

Se erro de import: confira que `alembic/env.py` importa `polycopy.infrastructure.persistence.models` corretamente.

- [ ] **Step 1.14: Run integration tests — esperado PASS**

Run: `uv run pytest tests/integration/test_persistence_smoke.py -v`
Expected: 3 tests passados.

Se falhar `relation "wallet_trades" does not exist`: o fixture não rodou `upgrade head`. Confira `db_engine` no conftest.
Se falhar `connection refused` ou erro asyncpg: docker compose ps — postgres healthy?

- [ ] **Step 1.15: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0. Test count: 54 anteriores + 3 novos = 57.

- [ ] **Step 1.16: Stage e commit**

Pause antes do commit pra usuário inspecionar (regra `feedback_commits.md`).

Stage:
```bash
git add pyproject.toml uv.lock src/polycopy/config.py src/polycopy/infrastructure/persistence/ alembic.ini alembic/ tests/conftest.py tests/integration/test_persistence_smoke.py
git status
```

Confirmar nada fora do escopo. Commit:

```bash
git commit -m "feat(persistence): add SQLAlchemy async engine, alembic, wallet_trades table"
```

- [ ] **Step 1.17: STOP — esperar confirmação humana antes de Task 2**

---

## Task 2: Passo 1.7 — `SqlAlchemyWalletTradeRepository`

**Objetivo:** ter o adapter concreto que implementa `WalletTradeRepository` (port da Fase 1A) usando SQLAlchemy. Suporta `insert_if_absent` (idempotente via PK) e `latest_occurred_at`. Testes integration cobrem inserção, dedup e query.

**Files:**
- Create: `src/polycopy/infrastructure/persistence/wallet_trade_repository.py`
- Create: `tests/integration/test_wallet_trade_repository.py`

---

- [ ] **Step 2.1: Write the failing test — `tests/integration/test_wallet_trade_repository.py`**

```python
"""Integration tests for SqlAlchemyWalletTradeRepository."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.persistence.wallet_trade_repository import (
    SqlAlchemyWalletTradeRepository,
)

pytestmark = pytest.mark.integration

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"
_OTHER_ADDR = "0x" + "9" * 40
_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN = "12345"


def _trade(
    *,
    tx_hash: str = "0x" + "cd" * 32,
    log_index: int = 0,
    wallet: str = _VALID_ADDR,
    occurred_at: datetime | None = None,
) -> Trade:
    return Trade(
        tx_hash=tx_hash,
        log_index=log_index,
        wallet=WalletAddress(value=wallet),
        condition_id=ConditionId(value=_VALID_COND),
        token_id=TokenId(value=_VALID_TOKEN),
        side=Side.BUY,
        price=Price(value=Decimal("0.55")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=occurred_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )


async def test_insert_if_absent_inserts_first_time(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    inserted = await repo.insert_if_absent(_trade())
    assert inserted is True


async def test_insert_if_absent_returns_false_on_duplicate(
    db_session: AsyncSession,
) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    trade = _trade()
    first = await repo.insert_if_absent(trade)
    second = await repo.insert_if_absent(trade)
    assert first is True
    assert second is False


async def test_insert_different_log_index_succeeds(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    a = await repo.insert_if_absent(_trade(log_index=0))
    b = await repo.insert_if_absent(_trade(log_index=1))
    assert a is True
    assert b is True


async def test_latest_occurred_at_returns_none_for_unknown_wallet(
    db_session: AsyncSession,
) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    result = await repo.latest_occurred_at(WalletAddress(value=_OTHER_ADDR))
    assert result is None


async def test_latest_occurred_at_returns_max(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await repo.insert_if_absent(
        _trade(tx_hash="0x" + "11" * 32, log_index=0, occurred_at=base)
    )
    await repo.insert_if_absent(
        _trade(
            tx_hash="0x" + "22" * 32,
            log_index=0,
            occurred_at=base + timedelta(hours=1),
        )
    )
    result = await repo.latest_occurred_at(WalletAddress(value=_VALID_ADDR))
    assert result == base + timedelta(hours=1)


async def test_latest_occurred_at_filters_by_wallet(
    db_session: AsyncSession,
) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await repo.insert_if_absent(
        _trade(tx_hash="0x" + "11" * 32, log_index=0, wallet=_VALID_ADDR, occurred_at=base)
    )
    await repo.insert_if_absent(
        _trade(
            tx_hash="0x" + "22" * 32,
            log_index=0,
            wallet=_OTHER_ADDR,
            occurred_at=base + timedelta(hours=2),
        )
    )
    valid = await repo.latest_occurred_at(WalletAddress(value=_VALID_ADDR))
    other = await repo.latest_occurred_at(WalletAddress(value=_OTHER_ADDR))
    assert valid == base
    assert other == base + timedelta(hours=2)
```

- [ ] **Step 2.2: Run tests — esperado FAIL**

Run: `uv run pytest tests/integration/test_wallet_trade_repository.py -v`
Expected: ImportError em `polycopy.infrastructure.persistence.wallet_trade_repository`.

- [ ] **Step 2.3: Implement `src/polycopy/infrastructure/persistence/wallet_trade_repository.py`**

```python
"""SqlAlchemyWalletTradeRepository: implementação concreta de WalletTradeRepository."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.models import Trade
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.persistence.models import WalletTradeRow


class SqlAlchemyWalletTradeRepository:
    """Repositório de trades. Idempotente via PK `(tx_hash, log_index)`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, trade: Trade) -> bool:
        """Insere trade. True se inseriu, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(WalletTradeRow)
            .values(
                tx_hash=trade.tx_hash,
                log_index=trade.log_index,
                wallet=trade.wallet.value,
                condition_id=trade.condition_id.value,
                token_id=trade.token_id.value,
                side=trade.side.value,
                price=trade.price.value,
                size_usdc=trade.size_usdc.amount,
                occurred_at=trade.occurred_at,
            )
            .on_conflict_do_nothing(index_elements=["tx_hash", "log_index"])
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount == 1

    async def latest_occurred_at(self, wallet: WalletAddress) -> datetime | None:
        stmt = select(func.max(WalletTradeRow.occurred_at)).where(
            WalletTradeRow.wallet == wallet.value
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
```

- [ ] **Step 2.4: Run tests — esperado PASS**

Run: `uv run pytest tests/integration/test_wallet_trade_repository.py -v`
Expected: 6 tests passados.

Se falhar `rowcount` retornando -1 ou comportamento estranho: asyncpg + ON CONFLICT requer `flush()` antes de ler `rowcount`. Já está no código.

- [ ] **Step 2.5: Verificar que adapter satisfaz o Protocol**

Adicionar no fim de `tests/integration/test_wallet_trade_repository.py`:

```python
from polycopy.ports import WalletTradeRepository as WalletTradeRepositoryProtocol


def _accepts_protocol(_: WalletTradeRepositoryProtocol) -> None:
    return


async def test_adapter_satisfies_protocol(db_session: AsyncSession) -> None:
    repo = SqlAlchemyWalletTradeRepository(db_session)
    _accepts_protocol(repo)  # mypy strict valida
```

Re-rode e confirme: `uv run pytest tests/integration/test_wallet_trade_repository.py -v` → 7 passados.

- [ ] **Step 2.6: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0. Test count: 57 + 7 = 64.

- [ ] **Step 2.7: Stage e commit**

Pause antes. Stage:
```bash
git add src/polycopy/infrastructure/persistence/wallet_trade_repository.py tests/integration/test_wallet_trade_repository.py
git status
git commit -m "feat(persistence): add SqlAlchemyWalletTradeRepository with insert_if_absent dedup"
```

- [ ] **Step 2.8: STOP — esperar confirmação humana antes de Task 3**

---

## Task 3: Passo 1.8 — `PolymarketDataClient` (httpx + tenacity + métricas)

**Objetivo:** ter cliente HTTP da Polymarket Data API com retry exponencial, timeout, e métricas Prometheus. Implementa `PolymarketDataPort`. Testes unitários usam respx pra mockar respostas. Cria também `infrastructure/observability/metrics.py` com registry central.

**Files:**
- Modify: `pyproject.toml` (deps: `httpx>=0.27`, `tenacity>=9`, `prometheus-client>=0.21`; dev: `respx>=0.21`)
- Create: `src/polycopy/infrastructure/observability/metrics.py`
- Create: `src/polycopy/infrastructure/polymarket/__init__.py`
- Create: `src/polycopy/infrastructure/polymarket/data_client.py`
- Create: `tests/unit/infrastructure/test_metrics.py`
- Create: `tests/unit/infrastructure/test_data_client.py`

---

- [ ] **Step 3.1: Modify `pyproject.toml` — adicionar deps**

`[project] dependencies`:
```toml
dependencies = [
    "pydantic>=2.9",
    "pydantic-settings>=2.5",
    "structlog>=24.4",
    "asyncpg>=0.30",
    "nats-py>=2.7",
    "redis>=5.1",
    "sqlalchemy[asyncio]>=2.0",
    "alembic>=1.13",
    "httpx>=0.27",
    "tenacity>=9",
    "prometheus-client>=0.21",
]
```

`[dependency-groups] dev`:
```toml
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.24",
    "pytest-cov>=5",
    "mypy>=1.13",
    "ruff>=0.7",
    "pre-commit>=4",
    "commitizen>=4",
    "respx>=0.21",
]
```

- [ ] **Step 3.2: Re-sync**

Run: `uv sync`
Expected: httpx, tenacity, prometheus-client, respx instaladas.

- [ ] **Step 3.3: Write failing test — `tests/unit/infrastructure/test_metrics.py`**

```python
"""Tests for prometheus metrics registry."""

from __future__ import annotations

from prometheus_client import CollectorRegistry

from polycopy.infrastructure.observability.metrics import (
    Metrics,
    make_metrics,
)


def test_make_metrics_returns_metrics_instance() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    assert isinstance(metrics, Metrics)


def test_metrics_polymarket_request_counter_labels() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.polymarket_requests_total.labels(endpoint="activity", status="200").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_polymarket_requests"]
    assert len(matching) == 1


def test_metrics_polymarket_latency_histogram_records() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.polymarket_request_duration_seconds.labels(endpoint="activity").observe(0.123)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_polymarket_request_duration_seconds"]
    assert matching, "histogram não foi registrado"
```

- [ ] **Step 3.4: Run — esperado FAIL**

Run: `uv run pytest tests/unit/infrastructure/test_metrics.py -v`
Expected: ImportError.

- [ ] **Step 3.5: Implement `src/polycopy/infrastructure/observability/metrics.py`**

```python
"""Prometheus metrics registry. Centraliza counters/histograms da app.

Em testes, passe um `CollectorRegistry` próprio pra evitar colisão com o registry global.
Em produção, o servidor HTTP `/metrics` (Plano 1C) usa o registry default.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram


@dataclass(frozen=True)
class Metrics:
    polymarket_requests_total: Counter
    polymarket_request_duration_seconds: Histogram


def make_metrics(registry: CollectorRegistry | None = None) -> Metrics:
    target = registry if registry is not None else REGISTRY
    return Metrics(
        polymarket_requests_total=Counter(
            "polycopy_polymarket_requests",
            "Total HTTP requests para Polymarket Data API",
            labelnames=["endpoint", "status"],
            registry=target,
        ),
        polymarket_request_duration_seconds=Histogram(
            "polycopy_polymarket_request_duration_seconds",
            "Latência de requests pra Polymarket Data API",
            labelnames=["endpoint"],
            registry=target,
        ),
    )
```

- [ ] **Step 3.6: Run — esperado PASS**

Run: `uv run pytest tests/unit/infrastructure/test_metrics.py -v`
Expected: 3 tests.

- [ ] **Step 3.7: Create `src/polycopy/infrastructure/polymarket/__init__.py`** (vazio)

```python
```

- [ ] **Step 3.8: Write failing test — `tests/unit/infrastructure/test_data_client.py`**

```python
"""Unit tests for PolymarketDataClient (com respx mockando httpx)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from polycopy.domain.models import Side
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.data_client import (
    PolymarketDataClient,
)

_BASE = "https://data-api.polymarket.com"
_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"


def _activity_response(rows: list[dict[str, object]]) -> dict[str, object]:
    return {"data": rows}


def _row(
    *,
    tx: str = "0x" + "cd" * 32,
    log_index: int = 0,
    side: str = "BUY",
    price: str = "0.55",
    size_usdc: str = "10",
) -> dict[str, object]:
    return {
        "transactionHash": tx,
        "logIndex": log_index,
        "user": _VALID_ADDR,
        "conditionId": "0x" + "ab" * 32,
        "asset": "12345",
        "side": side,
        "price": price,
        "usdcSize": size_usdc,
        "timestamp": int(datetime(2026, 5, 1, 12, 0, tzinfo=UTC).timestamp()),
    }


@pytest.fixture
def metrics() -> object:
    return make_metrics(registry=CollectorRegistry())


@respx.mock
async def test_fetch_user_activity_returns_trades(metrics: object) -> None:
    respx.get(f"{_BASE}/activity").mock(
        return_value=httpx.Response(200, json=_activity_response([_row()]))
    )
    client = PolymarketDataClient(base_url=_BASE, metrics=metrics, timeout_s=5)  # type: ignore[arg-type]
    trades = await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
    assert len(trades) == 1
    assert trades[0].side is Side.BUY
    assert trades[0].size_usdc.amount == Decimal("10.000000")


@respx.mock
async def test_fetch_user_activity_handles_empty_list(metrics: object) -> None:
    respx.get(f"{_BASE}/activity").mock(
        return_value=httpx.Response(200, json=_activity_response([]))
    )
    client = PolymarketDataClient(base_url=_BASE, metrics=metrics, timeout_s=5)  # type: ignore[arg-type]
    trades = await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
    assert trades == []


@respx.mock
async def test_fetch_user_activity_passes_since_filter(metrics: object) -> None:
    route = respx.get(f"{_BASE}/activity").mock(
        return_value=httpx.Response(200, json=_activity_response([]))
    )
    client = PolymarketDataClient(base_url=_BASE, metrics=metrics, timeout_s=5)  # type: ignore[arg-type]
    since = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR), since=since)
    assert route.called
    request = route.calls[0].request
    assert "start" in request.url.params
    assert request.url.params["user"] == _VALID_ADDR


@respx.mock
async def test_fetch_user_activity_retries_on_5xx(metrics: object) -> None:
    route = respx.get(f"{_BASE}/activity").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(200, json=_activity_response([])),
        ]
    )
    client = PolymarketDataClient(
        base_url=_BASE, metrics=metrics, timeout_s=5, max_retries=3  # type: ignore[arg-type]
    )
    await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
    assert route.call_count == 3


@respx.mock
async def test_fetch_user_activity_raises_after_max_retries(metrics: object) -> None:
    respx.get(f"{_BASE}/activity").mock(return_value=httpx.Response(503))
    client = PolymarketDataClient(
        base_url=_BASE, metrics=metrics, timeout_s=5, max_retries=2  # type: ignore[arg-type]
    )
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_user_activity(WalletAddress(value=_VALID_ADDR))
```

- [ ] **Step 3.9: Run — esperado FAIL**

Run: `uv run pytest tests/unit/infrastructure/test_data_client.py -v`
Expected: ImportError.

- [ ] **Step 3.10: Implement `src/polycopy/infrastructure/polymarket/data_client.py`**

```python
"""PolymarketDataClient: httpx + tenacity + métricas Prometheus.

Endpoint: https://data-api.polymarket.com/activity
Retry: exponential backoff em 5xx; não retenta em 4xx.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.observability.metrics import Metrics


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return isinstance(exc, httpx.RequestError)


class PolymarketDataClient:
    """Cliente da Polymarket Data API. Implementa `PolymarketDataPort`."""

    def __init__(
        self,
        *,
        base_url: str,
        metrics: Metrics,
        timeout_s: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._metrics = metrics
        self._timeout_s = timeout_s
        self._max_retries = max_retries

    async def fetch_user_activity(
        self,
        wallet: WalletAddress,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[Trade]:
        params: dict[str, Any] = {"user": wallet.value, "limit": limit}
        if since is not None:
            params["start"] = int(since.timestamp())

        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.get(f"{self._base_url}/activity", params=params)
                response.raise_for_status()
                return response

        start = time.perf_counter()
        try:
            response = await self._with_retry(_do)
        finally:
            self._metrics.polymarket_request_duration_seconds.labels(
                endpoint="activity"
            ).observe(time.perf_counter() - start)

        self._metrics.polymarket_requests_total.labels(
            endpoint="activity", status=str(response.status_code)
        ).inc()

        rows = response.json().get("data", [])
        return [self._row_to_trade(row) for row in rows]

    async def _with_retry(self, fn: Any) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=0.1, max=2),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                return await fn()
        raise RuntimeError("unreachable")

    @staticmethod
    def _row_to_trade(row: dict[str, Any]) -> Trade:
        return Trade(
            tx_hash=row["transactionHash"],
            log_index=int(row["logIndex"]),
            wallet=WalletAddress(value=row["user"]),
            condition_id=ConditionId(value=row["conditionId"]),
            token_id=TokenId(value=str(row["asset"])),
            side=Side(row["side"]),
            price=Price(value=Decimal(str(row["price"]))),
            size_usdc=Money.from_usdc(str(row["usdcSize"])),
            occurred_at=datetime.fromtimestamp(int(row["timestamp"]), tz=UTC),
        )
```

- [ ] **Step 3.11: Run — esperado PASS**

Run: `uv run pytest tests/unit/infrastructure/test_data_client.py -v`
Expected: 5 tests passados.

Se falhar com erro tenacity sobre `retry_if_exception_type`: confira que está usando `retry_if_exception` (callable predicate), não `retry_if_exception_type`. Os tipos diferem.

- [ ] **Step 3.12: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0. Test count: 64 + 3 (metrics) + 5 (data_client) = 72.

- [ ] **Step 3.13: Stage e commit**

Pause antes. Stage:
```bash
git add pyproject.toml uv.lock src/polycopy/infrastructure/observability/metrics.py src/polycopy/infrastructure/polymarket/ tests/unit/infrastructure/test_metrics.py tests/unit/infrastructure/test_data_client.py
git status
git commit -m "feat(polymarket): add PolymarketDataClient with tenacity retry and prometheus metrics"
```

- [ ] **Step 3.14: STOP — esperar confirmação humana antes de Task 4**

---

## Task 4: Passo 1.9 — `NatsMessagingBus`

**Objetivo:** adapter que implementa `MessagingPort` usando `nats-py`. Publica `WalletTradeDetected` no subject correspondente, suporta subscribe genérico, fecha gracefully. Testes integration usam o NATS do docker-compose.

**Files:**
- Create: `src/polycopy/infrastructure/messaging/__init__.py`
- Create: `src/polycopy/infrastructure/messaging/nats_bus.py`
- Create: `tests/integration/test_nats_bus.py`

---

- [ ] **Step 4.1: Create `src/polycopy/infrastructure/messaging/__init__.py`** (vazio)

```python
```

- [ ] **Step 4.2: Write failing test — `tests/integration/test_nats_bus.py`**

```python
"""Integration tests for NatsMessagingBus (requer NATS up no docker-compose)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from polycopy.config import Settings
from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.messaging.nats_bus import NatsMessagingBus

pytestmark = pytest.mark.integration


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "cd" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def _event() -> WalletTradeDetected:
    return WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        trade=_trade(),
    )


async def test_publish_and_subscribe_roundtrip(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()

    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await bus.subscribe(WalletTradeDetected.SUBJECT, handler)
    await asyncio.sleep(0.05)  # garante que o subscribe está pronto

    event = _event()
    await bus.publish_wallet_trade_detected(event)

    # Polling curto até receber ou timeout
    for _ in range(20):
        if received:
            break
        await asyncio.sleep(0.05)

    await bus.close()
    assert len(received) == 1
    parsed = WalletTradeDetected.model_validate_json(received[0])
    assert parsed.event_id == event.event_id


async def test_close_is_idempotent(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    await bus.connect()
    await bus.close()
    await bus.close()  # não deve levantar


async def test_publish_without_connect_raises(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    with pytest.raises(RuntimeError, match="not connected"):
        await bus.publish_wallet_trade_detected(_event())
```

- [ ] **Step 4.3: Run — esperado FAIL**

Run: `uv run pytest tests/integration/test_nats_bus.py -v`
Expected: ImportError.

- [ ] **Step 4.4: Implement `src/polycopy/infrastructure/messaging/nats_bus.py`**

```python
"""NatsMessagingBus: adapter de MessagingPort usando nats-py (core pub/sub)."""

from __future__ import annotations

import nats
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from polycopy.domain.events import WalletTradeDetected
from polycopy.ports.messaging import EventHandler


class NatsMessagingBus:
    """Bus core NATS. JetStream entra em fase posterior se precisar de durability."""

    def __init__(self, *, url: str) -> None:
        self._url = url
        self._nc: NatsClient | None = None

    async def connect(self) -> None:
        if self._nc is not None and self._nc.is_connected:
            return
        self._nc = await nats.connect(self._url)

    async def publish_wallet_trade_detected(self, event: WalletTradeDetected) -> None:
        if self._nc is None or not self._nc.is_connected:
            raise RuntimeError("NatsMessagingBus not connected; call connect() first")
        payload = event.model_dump_json().encode("utf-8")
        await self._nc.publish(WalletTradeDetected.SUBJECT, payload)
        await self._nc.flush()

    async def subscribe(self, subject: str, handler: EventHandler) -> None:
        if self._nc is None or not self._nc.is_connected:
            raise RuntimeError("NatsMessagingBus not connected; call connect() first")

        async def _wrapper(msg: Msg) -> None:
            await handler(msg.data)

        await self._nc.subscribe(subject, cb=_wrapper)

    async def close(self) -> None:
        if self._nc is None:
            return
        if self._nc.is_connected:
            await self._nc.drain()
        self._nc = None
```

- [ ] **Step 4.5: Verificar Protocol satisfação**

Adicionar no fim de `tests/integration/test_nats_bus.py`:

```python
from polycopy.ports import MessagingPort


def _accepts(_: MessagingPort) -> None:
    return


async def test_adapter_satisfies_protocol(settings: Settings) -> None:
    bus = NatsMessagingBus(url=settings.nats_url)
    _accepts(bus)
```

- [ ] **Step 4.6: Run — esperado PASS**

Run: `uv run pytest tests/integration/test_nats_bus.py -v`
Expected: 4 tests passados.

Se `test_publish_and_subscribe_roundtrip` flakey por timing: aumentar o sleep inicial pra 0.1s ou o número de iterações de polling.

- [ ] **Step 4.7: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0. Test count: 72 + 4 = 76.

- [ ] **Step 4.8: Stage e commit**

Pause antes.
```bash
git add src/polycopy/infrastructure/messaging/ tests/integration/test_nats_bus.py
git status
git commit -m "feat(messaging): add NatsMessagingBus implementing MessagingPort"
```

- [ ] **Step 4.9: STOP — esperar confirmação humana antes de Task 5**

---

## Task 5: Passo 1.10 — `AgentBase` (heartbeat, graceful shutdown)

**Objetivo:** classe base abstrata que todos os agents (`watcher`, `notifier`, futuros) herdam. Loop principal `await run_once(); await asyncio.sleep(interval)` controlado por `asyncio.Event`. Heartbeat: log periódico estruturado a cada N iterações. Helper separado `setup_signal_handlers()` que registra SIGTERM/SIGINT — não chamado em testes.

**Files:**
- Create: `src/polycopy/agents/__init__.py`
- Create: `src/polycopy/agents/_base.py`
- Create: `tests/unit/agents/__init__.py`
- Create: `tests/unit/agents/test_base.py`

---

- [ ] **Step 5.1: Create `src/polycopy/agents/__init__.py`** (vazio)

```python
```

- [ ] **Step 5.2: Create `tests/unit/agents/__init__.py`** (vazio)

```python
```

- [ ] **Step 5.3: Write failing test — `tests/unit/agents/test_base.py`**

```python
"""Unit tests for AgentBase."""

from __future__ import annotations

import asyncio

import pytest

from polycopy.agents._base import AgentBase


class _CountingAgent(AgentBase):
    name = "counting"

    def __init__(self, *, stopping: asyncio.Event, interval_s: float) -> None:
        super().__init__(stopping=stopping, interval_s=interval_s)
        self.count = 0

    async def run_once(self) -> None:
        self.count += 1


class _FailingAgent(AgentBase):
    name = "failing"

    async def run_once(self) -> None:
        raise RuntimeError("boom")


async def test_run_loop_invokes_run_once_until_stopped() -> None:
    stopping = asyncio.Event()
    agent = _CountingAgent(stopping=stopping, interval_s=0.01)

    task = asyncio.create_task(agent.run())
    await asyncio.sleep(0.05)
    stopping.set()
    await task

    assert agent.count >= 2


async def test_run_loop_exits_immediately_if_stopping_already_set() -> None:
    stopping = asyncio.Event()
    stopping.set()
    agent = _CountingAgent(stopping=stopping, interval_s=0.01)
    await agent.run()
    assert agent.count == 0


async def test_run_loop_propagates_run_once_exception_after_stopping() -> None:
    stopping = asyncio.Event()
    agent = _FailingAgent(stopping=stopping, interval_s=0.01)
    with pytest.raises(RuntimeError, match="boom"):
        await agent.run()


async def test_setup_signal_handlers_sets_event_on_signal() -> None:
    """Smoke test: o helper registra handlers e o event é setado quando SIGTERM dispara."""
    import signal

    from polycopy.agents._base import setup_signal_handlers

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    loop = asyncio.get_running_loop()
    loop.call_soon(lambda: signal.raise_signal(signal.SIGTERM))
    try:
        await asyncio.wait_for(stopping.wait(), timeout=0.5)
    finally:
        # Limpa handlers pra não vazar pra outros testes
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
    assert stopping.is_set()
```

- [ ] **Step 5.4: Run — esperado FAIL**

Run: `uv run pytest tests/unit/agents/test_base.py -v`
Expected: ImportError em `polycopy.agents._base`.

- [ ] **Step 5.5: Implement `src/polycopy/agents/_base.py`**

```python
"""Base class para agents: loop assíncrono com graceful shutdown e heartbeat."""

from __future__ import annotations

import asyncio
import signal
from abc import ABC, abstractmethod
from typing import ClassVar

from polycopy.infrastructure.observability.logging import get_logger


class AgentBase(ABC):
    """Loop padrão: roda `run_once()` até `stopping` ser setado.

    Subclasses devem definir `name` (ClassVar) e implementar `run_once()`.
    """

    name: ClassVar[str] = "agent"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        interval_s: float,
        heartbeat_every_n: int = 10,
    ) -> None:
        self._stopping = stopping
        self._interval_s = interval_s
        self._heartbeat_every_n = heartbeat_every_n
        self._log = get_logger(self.name)

    @abstractmethod
    async def run_once(self) -> None:
        """Uma iteração de trabalho. Subclasses implementam."""

    async def run(self) -> None:
        """Loop principal. Sai quando `stopping` é setado."""
        iteration = 0
        self._log.info("agent_started", interval_s=self._interval_s)
        try:
            while not self._stopping.is_set():
                await self.run_once()
                iteration += 1
                if iteration % self._heartbeat_every_n == 0:
                    self._log.info("agent_heartbeat", iteration=iteration)
                try:
                    await asyncio.wait_for(
                        self._stopping.wait(), timeout=self._interval_s
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            self._log.info("agent_stopped", iterations=iteration)


def setup_signal_handlers(stopping: asyncio.Event) -> None:
    """Registra handlers SIGTERM/SIGINT que setam `stopping`. Chamar no main()."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)
```

- [ ] **Step 5.6: Run — esperado PASS**

Run: `uv run pytest tests/unit/agents/test_base.py -v`
Expected: 4 tests passados.

Se `test_run_loop_propagates_run_once_exception_after_stopping` falhar porque o `_log.info` no `finally` quebra (sem logging configurado): o test não chama `configure_logging` mas o `get_logger` ainda funciona com defaults. Se quebrar, adicione `structlog.reset_defaults()` no início do teste.

- [ ] **Step 5.7: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0. Test count: 76 + 4 = 80.

- [ ] **Step 5.8: Stage e commit**

Pause antes.
```bash
git add src/polycopy/agents/ tests/unit/agents/
git status
git commit -m "feat(agents): add AgentBase with graceful shutdown and heartbeat"
```

- [ ] **Step 5.9: STOP — esperar confirmação humana antes do fim do Plano 1B**

---

## Final: Validação completa do Plano 1B

Após as 5 tasks, rode o checklist final:

- [ ] **Step F.1: Working tree limpo + 5 commits novos**

Run:
```bash
git status
git log --oneline | head -20
```
Expected:
- `git status`: working tree clean (exceto untracked esperados)
- `git log`: além dos 11 commits anteriores, mais 5 commits desta fase (ordem do mais recente):
  ```
  feat(agents): add AgentBase with graceful shutdown and heartbeat
  feat(messaging): add NatsMessagingBus implementing MessagingPort
  feat(polymarket): add PolymarketDataClient with tenacity retry and prometheus metrics
  feat(persistence): add SqlAlchemyWalletTradeRepository with insert_if_absent dedup
  feat(persistence): add SQLAlchemy async engine, alembic, wallet_trades table
  ```

- [ ] **Step F.2: Suite verde inteira (~80 testes)**

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: tudo exit 0. Test count: ~80.

- [ ] **Step F.3: Coverage `domain/` ≥ 90% (mantém alvo da Fase 1A)**

Run: `uv run pytest tests/unit/domain --cov=src/polycopy/domain --cov-report=term-missing`
Expected: ≥ 90%.

- [ ] **Step F.4: Coverage `infrastructure/` ≥ 70%**

Run: `uv run pytest --cov=src/polycopy/infrastructure --cov-report=term-missing`
Expected: ≥ 70%. Linhas faltantes provavelmente em branches de erro raros.

- [ ] **Step F.5: Pre-commit verde**

Run: `uv run pre-commit run --all-files`
Expected: tudo passa.

- [ ] **Step F.6: GitHub Actions verde**

Após o último push, verifique manualmente que CI ficou ✅.

Se TODOS os steps F.1 a F.6 passarem: **Plano 1B está completo.** Próximo: criar Plano 1C (passos 1.11 a 1.14 — watcher esqueleto, watcher com dedup, notifier, ARCHITECTURE.md).

---

## Notas de execução

**Regras absolutas que valem em todas as Tasks** (mesmas do Plano 1A):

1. NÃO avance pra Task seguinte sem confirmação explícita do humano.
2. NÃO crie arquivos fora do escopo declarado da Task atual.
3. NÃO instale dependência fora da lista da Task atual sem perguntar.
4. SEMPRE rode os comandos de validação ANTES de declarar a Task pronta.
5. SEMPRE faça commit conventional ao fim de cada Task, mas pause antes de `git add`/`git commit` por `feedback_commits.md`.
6. Se descobrir que a Task precisa ser dividida (escopo maior do que parecia), pare e proponha a subdivisão antes de continuar.
7. Se descobrir que a Task está errada ou impossível como descrita, pare e exponha o problema antes de improvisar.

**Sobre coverage:** Plano 1A não enforçou % no CI. Plano 1B segue assim — só medimos coverage manualmente. Enforce no CI vai entrar quando o Plano 1C terminar (e tiver código de adapter suficiente pra alvos realistas).

**Sobre dependências adicionadas:** este plano adiciona `sqlalchemy[asyncio]`, `alembic`, `httpx`, `tenacity`, `prometheus-client` ao runtime e `respx` ao dev. Se quiser substituir alguma (ex: `psycopg` em vez de `asyncpg`, `urllib3` em vez de `httpx`), pare antes da Task correspondente e renegocie.

**Sobre métricas Prometheus:** o `Metrics` registry é criado mas não há servidor `/metrics` HTTP ainda. Isso entra no Plano 1C, junto com o boot do `agents/watcher`. Por enquanto, métricas são registradas mas não expostas — testes confirmam que os Counter/Histogram aceitam `.inc()` e `.observe()` sem erro.

**Sobre alembic em testes:** o `db_engine` fixture chama `command.upgrade(cfg, "head")` no setup e `command.downgrade(cfg, "base")` no teardown. Isso significa que cada **execução de test suite** (não cada test) cria e destrói o schema. Para acelerar testes locais, considere rodar `uv run alembic upgrade head` manualmente uma vez e remover o `downgrade` do teardown — mas isso é otimização que fica fora deste plano.

**Sobre desempenho do test suite:** com 80 tests a suite ainda roda em < 5s. Se passar de 30s, considere paralelizar com `pytest-xdist`.

**Sobre signal handlers:** o test `test_setup_signal_handlers_sets_event_on_signal` é levemente arriscado porque dispara SIGTERM no processo de teste. Se causar instabilidade no CI, marque-o `@pytest.mark.skipif(os.environ.get("CI") == "true", ...)` e rode só localmente.
