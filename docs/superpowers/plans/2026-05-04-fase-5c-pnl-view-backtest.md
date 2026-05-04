# Fase 5C — PnL View + Backtest Tooling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. **Cadência: checkpoint humano por task** (mesma das fases anteriores).

**Goal:** Cruzar `order_executions` (5B) com `market_resolutions` (5A) numa view SQL `hypothetical_pnl` + CLI `backtest.py` + 5 gauges Prometheus no `ResolverAgent` — fecha o ciclo de backtest da Fase 5.

**Architecture:** Migration 0009 adiciona coluna `side` em `order_executions` (DEFAULT 'BUY' retroativo, depois drop) e cria view `hypothetical_pnl` (LEFT JOIN com colunas computadas: qty, payout_per_token, pnl_usdc, status). `MarketResolutionRepository` Protocol ganha `get_pnl_summary()` retornando `PnlSummary` dataclass. `ResolverAgent` chama `get_pnl_summary` ao final de cada loop e popula 5 gauges. CLI script `polycopy.scripts.backtest` consulta a view via SQLAlchemy.

**Tech Stack:** Python 3.12, SQLAlchemy 2 async, alembic, Postgres view, prometheus_client, pytest, argparse + standard library formatting (sem `rich` ou `tabulate` — manter zero dep extra).

**Predecessor:** Fase 5A (head `563c2f6`) + Fase 5B (head `6d9215a`) + test-db-isolation (head `77bea2f`) + spec 5C (`bffa214`).

**Spec:** `docs/superpowers/specs/2026-05-04-fase-5c-pnl-view-backtest-design.md`.

---

## File Structure

**Novos arquivos (4):**
- `alembic/versions/0009_add_side_and_hypothetical_pnl_view.py` — migration.
- `src/polycopy/scripts/backtest.py` — CLI script.
- `tests/integration/test_hypothetical_pnl_view.py` — 10 cenários da view.
- `tests/integration/test_resolver_pnl_metrics.py` — 3 cenários das gauges.
- `tests/unit/scripts/test_backtest.py` — 4-5 cenários de formatação.

**Modificados (~10):**
- `src/polycopy/domain/execution.py` — `OrderExecution.side` field.
- `src/polycopy/infrastructure/persistence/models.py` — `OrderExecutionRow.side` column.
- `src/polycopy/infrastructure/persistence/order_execution_repository.py` — propaga `side` no insert.
- `src/polycopy/agents/executor.py` — `_handle_message` propaga `side=event.trade.side.value`.
- `src/polycopy/ports/market_resolution_repository.py` — Protocol ganha `get_pnl_summary()`.
- `src/polycopy/infrastructure/persistence/market_resolution_repository.py` — impl concreta.
- `src/polycopy/domain/pnl.py` (novo) — `PnlSummary` dataclass.
- `src/polycopy/infrastructure/observability/metrics.py` — 5 gauges.
- `src/polycopy/agents/resolver.py` — chama `_compute_pnl_metrics` ao final de `run_once`.
- `tests/unit/infrastructure/test_metrics.py` — +5 testes pras gauges.
- `tests/unit/test_ports_typecheck.py` — atualizar `_FakeMarketResolutionRepo` com `get_pnl_summary`.
- `ARCHITECTURE.md` — seção Backtest.

---

## Task 1: Migration 0009 — `side` column + view `hypothetical_pnl`

**Files:**
- Create: `alembic/versions/0009_add_side_and_hypothetical_pnl_view.py`

**Reviewer:** opcional (DDL puro).

---

- [ ] **Step 1.1: Create migration file**

LEIA `alembic/versions/0008_add_expected_avg_price.py` primeiro pra confirmar style.

Create `alembic/versions/0009_add_side_and_hypothetical_pnl_view.py`:

```python
"""add side column to order_executions and hypothetical_pnl view

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-04 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_VIEW_SQL = """
CREATE OR REPLACE VIEW hypothetical_pnl AS
SELECT
    oe.trade_event_id,
    oe.wallet,
    oe.condition_id,
    oe.token_id,
    oe.side,
    oe.final_size_usdc,
    oe.expected_avg_price,
    oe.decided_at,
    oe.mode,
    oe.result,
    mr.resolved_outcome,
    mr.winning_token_id,
    mr.resolved_at,
    CASE
        WHEN oe.expected_avg_price IS NOT NULL AND oe.expected_avg_price > 0
        THEN oe.final_size_usdc / oe.expected_avg_price
        ELSE NULL
    END AS qty_tokens,
    CASE
        WHEN mr.resolved_outcome IS NULL THEN NULL
        WHEN oe.side = 'SELL' THEN NULL
        WHEN mr.resolved_outcome = 'INVALID' THEN 0.5
        WHEN mr.winning_token_id = oe.token_id THEN 1.0
        ELSE 0.0
    END AS payout_per_token,
    CASE
        WHEN mr.resolved_outcome IS NULL OR oe.side = 'SELL'
          OR oe.expected_avg_price IS NULL OR oe.expected_avg_price = 0
        THEN NULL
        WHEN mr.resolved_outcome = 'INVALID'
        THEN (oe.final_size_usdc / oe.expected_avg_price) * 0.5 - oe.final_size_usdc
        WHEN mr.winning_token_id = oe.token_id
        THEN (oe.final_size_usdc / oe.expected_avg_price) - oe.final_size_usdc
        ELSE -oe.final_size_usdc
    END AS pnl_usdc,
    CASE
        WHEN mr.resolved_outcome IS NULL THEN 'pending'
        WHEN oe.side = 'SELL' THEN 'sell_excluded'
        WHEN oe.expected_avg_price IS NULL OR oe.expected_avg_price = 0 THEN 'no_expected_price'
        WHEN mr.resolved_outcome = 'INVALID' THEN 'invalid'
        WHEN mr.winning_token_id = oe.token_id THEN 'win'
        ELSE 'lose'
    END AS status
FROM order_executions oe
LEFT JOIN market_resolutions mr ON oe.condition_id = mr.condition_id;
"""


def upgrade() -> None:
    op.add_column(
        "order_executions",
        sa.Column("side", sa.String(), nullable=False, server_default="BUY"),
    )
    op.create_check_constraint(
        "order_executions_side_enum",
        "order_executions",
        "side IN ('BUY', 'SELL')",
    )
    op.alter_column("order_executions", "side", server_default=None)

    op.execute(_VIEW_SQL)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS hypothetical_pnl;")
    op.drop_constraint("order_executions_side_enum", "order_executions", type_="check")
    op.drop_column("order_executions", "side")
```

- [ ] **Step 1.2: Validate alembic round-trip**

```bash
docker compose ps postgres
uv run alembic upgrade head
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d order_executions" | grep side
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d+ hypothetical_pnl" | head -30
```
Expected: `side` column appears (NOT NULL, default null after drop), view `hypothetical_pnl` lists columns.

```bash
uv run alembic downgrade -1
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d hypothetical_pnl" 2>&1 | grep -i "did not find" || echo "view ainda existe — bug"
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d order_executions" | grep side && echo "side ainda existe — bug" || echo "side removida (esperado)"
uv run alembic upgrade head
```

- [ ] **Step 1.3: Verifications + STOP**

```bash
uv run mypy src/polycopy
uv run ruff check alembic/versions/0009_add_side_and_hypothetical_pnl_view.py
uv run ruff format --check alembic/versions/0009_add_side_and_hypothetical_pnl_view.py
uv run pytest tests/ 2>&1 | tail -5
```

Esperado: tudo limpo. Nota: testes existentes que usam `OrderExecution(...)` sem `side` ainda passam pq a coluna tem DEFAULT 'BUY' até esse ponto. T2 fixa as call sites.

Implementer NÃO commita. Controller pede confirmação humana, depois:

```bash
git add alembic/versions/0009_add_side_and_hypothetical_pnl_view.py
git commit -m "feat(persistence): add side column and hypothetical_pnl view (Fase 5C)"
```

---

## Task 2: `OrderExecution.side` propagation pelo pipeline

**Files:**
- Modify: `src/polycopy/domain/execution.py`
- Modify: `src/polycopy/infrastructure/persistence/models.py`
- Modify: `src/polycopy/infrastructure/persistence/order_execution_repository.py`
- Modify: `src/polycopy/agents/executor.py`

**Reviewer:** opcional.

---

- [ ] **Step 2.1: Add `side` field to `OrderExecution` dataclass**

LEIA `src/polycopy/domain/execution.py` primeiro. Adicionar campo (sem default — força call sites a fornecer):

```python
@dataclass(frozen=True)
class OrderExecution:
    """..."""

    trade_event_id: UUID
    wallet: str
    condition_id: str
    token_id: str
    side: Literal["BUY", "SELL"]  # NOVO — Plano 5C
    final_size_usdc: Decimal
    mode: ExecutionMode
    result: Literal["executed", "failed", "dry_run"]
    tx_hash: str | None
    gas_wei: int | None
    failure_reason: FailureReason | None
    error_message: str | None
    decided_at: datetime
    expected_avg_price: Decimal | None = None
```

**Atenção:** posicionar `side` ANTES dos campos opcionais (que têm default), pra evitar erro "non-default argument follows default argument".

- [ ] **Step 2.2: Add column to ORM**

LEIA `src/polycopy/infrastructure/persistence/models.py` (procure `OrderExecutionRow`). Adicionar **antes** de `final_size_usdc` (mantém ordem da migration):

```python
    side: Mapped[str] = mapped_column(String, nullable=False)
```

(Imports `String` e `Mapped` já existem.)

- [ ] **Step 2.3: Propagate in repository insert**

LEIA `src/polycopy/infrastructure/persistence/order_execution_repository.py`. Find `pg_insert(...).values(...)`. Adicionar:

```python
        stmt = (
            pg_insert(OrderExecutionRow)
            .values(
                trade_event_id=execution.trade_event_id,
                wallet=execution.wallet,
                condition_id=execution.condition_id,
                token_id=execution.token_id,
                side=execution.side,  # NOVO
                final_size_usdc=execution.final_size_usdc,
                # ... resto inalterado ...
                expected_avg_price=execution.expected_avg_price,
            )
            .on_conflict_do_nothing(...)
        )
```

- [ ] **Step 2.4: Propagate in `_handle_message`**

LEIA `src/polycopy/agents/executor.py`. Find `OrderExecution(...)` construction. Adicionar `side=event.trade.side.value`:

```python
            execution = OrderExecution(
                trade_event_id=event.event_id,
                wallet=event.trade.wallet.value,
                condition_id=event.trade.condition_id.value,
                token_id=event.trade.token_id.value,
                side=event.trade.side.value,  # NOVO
                final_size_usdc=event.final_size_usdc.amount,
                mode=exec_result.mode,
                result=...,  # already there
                # ... resto inalterado ...
                expected_avg_price=exec_result.expected_avg_price,
            )
```

`event.trade.side` é `Side` enum (BUY/SELL); `.value` retorna a string.

- [ ] **Step 2.5: Verifications + STOP**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/domain/execution.py src/polycopy/infrastructure/persistence/models.py src/polycopy/infrastructure/persistence/order_execution_repository.py src/polycopy/agents/executor.py
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -10
```

Esperado: tudo limpo. **Atenção:** alguns testes existentes que constroem `OrderExecution(...)` em fixtures podem falhar por falta do novo campo `side`. Adapte os fixtures pra passar `side="BUY"` (default razoável). Procure por `OrderExecution(` em tests/ e ajuste todos.

Commit:
```bash
git add src/polycopy/domain/execution.py src/polycopy/infrastructure/persistence/models.py src/polycopy/infrastructure/persistence/order_execution_repository.py src/polycopy/agents/executor.py
git commit -m "feat(executor): propagate side into OrderExecution and persistence"
```

(Note: testes adaptados podem precisar entrar no mesmo commit.)

---

## Task 3: Integration tests da view `hypothetical_pnl`

**Files:**
- Create: `tests/integration/test_hypothetical_pnl_view.py`

**Reviewer:** opcional.

---

- [ ] **Step 3.1: Write integration tests (10 scenarios)**

Create `tests/integration/test_hypothetical_pnl_view.py`:

```python
"""Integration tests da view hypothetical_pnl — 10 cenários cobrindo PnL semantics."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.integration


def _unique_cond() -> str:
    return "0x" + uuid.uuid4().hex.ljust(64, "0")[:64]


async def _insert_execution(
    session: AsyncSession,
    *,
    trade_event_id: uuid.UUID,
    condition_id: str,
    token_id: str,
    side: str,
    final_size_usdc: str,
    expected_avg_price: str | None,
) -> None:
    """Insere row em order_executions com defaults DRY-RUN."""
    await session.execute(
        text(
            "INSERT INTO order_executions "
            "(trade_event_id, wallet, condition_id, token_id, side, "
            " final_size_usdc, mode, result, decided_at, expected_avg_price) "
            "VALUES (:tid, :w, :c, :t, :side, :size, 'dry_run', 'dry_run', "
            "        now(), :exp)"
        ),
        {
            "tid": trade_event_id,
            "w": "0x" + "1" * 40,
            "c": condition_id,
            "t": token_id,
            "side": side,
            "size": final_size_usdc,
            "exp": expected_avg_price,
        },
    )


async def _insert_resolution(
    session: AsyncSession,
    *,
    condition_id: str,
    resolved_outcome: str,
    winning_token_id: str | None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO market_resolutions "
            "(condition_id, resolved_outcome, winning_token_id, "
            " resolved_at, outcome_prices_raw) "
            "VALUES (:c, :o, :w, now(), '[\"0\",\"1\"]')"
        ),
        {"c": condition_id, "o": resolved_outcome, "w": winning_token_id},
    )


async def _query_pnl(session: AsyncSession, trade_event_id: uuid.UUID):
    result = await session.execute(
        text(
            "SELECT side, qty_tokens, payout_per_token, pnl_usdc, status "
            "FROM hypothetical_pnl WHERE trade_event_id = :tid"
        ),
        {"tid": trade_event_id},
    )
    return result.one()


async def test_view_buy_winning_token_yields_positive_pnl(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """BUY token vencedor: pnl = (size/expected) - size = qty - size."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="10", expected_avg_price="0.5",
        )
        await _insert_resolution(
            session, condition_id=cond, resolved_outcome="YES", winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "win"
        assert row.qty_tokens == Decimal("20")
        assert row.payout_per_token == Decimal("1.0")
        assert row.pnl_usdc == Decimal("10")  # 20 - 10 = +10


async def test_view_buy_losing_token_yields_negative_size(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """BUY token perdedor: pnl = -size."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="10", expected_avg_price="0.5",
        )
        await _insert_resolution(
            session, condition_id=cond, resolved_outcome="YES", winning_token_id="222",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "lose"
        assert row.payout_per_token == Decimal("0")
        assert row.pnl_usdc == Decimal("-10")


async def test_view_invalid_resolution_pays_half(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """INVALID: pnl = qty * 0.5 - size."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="10", expected_avg_price="0.4",
        )
        await _insert_resolution(
            session, condition_id=cond, resolved_outcome="INVALID", winning_token_id=None,
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "invalid"
        assert row.payout_per_token == Decimal("0.5")
        assert row.qty_tokens == Decimal("25")  # 10 / 0.4
        assert row.pnl_usdc == Decimal("2.5")  # 25 * 0.5 - 10 = 2.5


async def test_view_pending_resolution_yields_null_pnl(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Sem resolution: pnl NULL, status pending."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="10", expected_avg_price="0.5",
        )
        # SEM resolution
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "pending"
        assert row.pnl_usdc is None
        assert row.payout_per_token is None


async def test_view_null_expected_price_yields_no_expected_price_status(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """expected_avg_price NULL: status no_expected_price, pnl NULL."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="10", expected_avg_price=None,
        )
        await _insert_resolution(
            session, condition_id=cond, resolved_outcome="YES", winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "no_expected_price"
        assert row.pnl_usdc is None
        assert row.qty_tokens is None


async def test_view_sell_excluded_from_v1(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """SELL: pnl NULL, status sell_excluded."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond, token_id="111",
            side="SELL", final_size_usdc="10", expected_avg_price="0.5",
        )
        await _insert_resolution(
            session, condition_id=cond, resolved_outcome="YES", winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "sell_excluded"
        assert row.pnl_usdc is None
        assert row.payout_per_token is None


async def test_view_zero_expected_price_treated_as_null(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """expected_avg_price = 0 (defensivo): pnl NULL."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="10", expected_avg_price="0",
        )
        await _insert_resolution(
            session, condition_id=cond, resolved_outcome="YES", winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "no_expected_price"
        assert row.pnl_usdc is None


async def test_view_multiple_trades_same_condition(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Múltiplos trades mesmo condition: todos retornam, todos resolvidos coerentemente."""
    async with db_session_factory() as session:
        cond = _unique_cond()
        tid1 = uuid.uuid4()
        tid2 = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid1, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="5", expected_avg_price="0.5",
        )
        await _insert_execution(
            session, trade_event_id=tid2, condition_id=cond, token_id="111",
            side="BUY", final_size_usdc="20", expected_avg_price="0.4",
        )
        await _insert_resolution(
            session, condition_id=cond, resolved_outcome="YES", winning_token_id="111",
        )
        await session.commit()

        row1 = await _query_pnl(session, tid1)
        row2 = await _query_pnl(session, tid2)
        assert row1.status == "win"
        assert row2.status == "win"
        assert row1.pnl_usdc == Decimal("5")  # 10 - 5
        assert row2.pnl_usdc == Decimal("30")  # 50 - 20


async def test_view_status_enum_completeness(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Confere que todos os 5 status aparecem corretamente em runs separados."""
    async with db_session_factory() as session:
        # 1 win, 1 lose, 1 invalid, 1 pending, 1 sell_excluded
        scenarios = [
            ("BUY", "111", "0.5", "YES", "111", "win"),
            ("BUY", "111", "0.5", "YES", "222", "lose"),
            ("BUY", "111", "0.4", "INVALID", None, "invalid"),
            ("BUY", "111", "0.5", None, None, "pending"),
            ("SELL", "111", "0.5", "YES", "111", "sell_excluded"),
        ]
        tids = []
        for side, token, exp, outcome, winner, _ in scenarios:
            cond = _unique_cond()
            tid = uuid.uuid4()
            tids.append(tid)
            await _insert_execution(
                session, trade_event_id=tid, condition_id=cond, token_id=token,
                side=side, final_size_usdc="10", expected_avg_price=exp,
            )
            if outcome is not None:
                await _insert_resolution(
                    session, condition_id=cond,
                    resolved_outcome=outcome, winning_token_id=winner,
                )
        await session.commit()

        statuses = []
        for tid in tids:
            row = await _query_pnl(session, tid)
            statuses.append(row.status)

        assert statuses == ["win", "lose", "invalid", "pending", "sell_excluded"]


async def test_view_no_resolution_match_yields_pending(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """JOIN sem match: trade existe, resolution não — status pending."""
    async with db_session_factory() as session:
        cond_a = _unique_cond()
        cond_b = _unique_cond()  # tem resolution mas trade não usa
        tid = uuid.uuid4()
        await _insert_execution(
            session, trade_event_id=tid, condition_id=cond_a, token_id="111",
            side="BUY", final_size_usdc="10", expected_avg_price="0.5",
        )
        # resolução pra cond_b (não casa com trade em cond_a)
        await _insert_resolution(
            session, condition_id=cond_b, resolved_outcome="YES", winning_token_id="111",
        )
        await session.commit()

        row = await _query_pnl(session, tid)
        assert row.status == "pending"
```

- [ ] **Step 3.2: Run tests**

```bash
docker compose stop resolver  # evita interferência
uv run pytest tests/integration/test_hypothetical_pnl_view.py -v 2>&1 | tail -20
docker compose start resolver
```
Expected: 10 PASS.

- [ ] **Step 3.3: Verifications + STOP**

```bash
uv run mypy tests/integration/test_hypothetical_pnl_view.py
uv run ruff check tests/integration/test_hypothetical_pnl_view.py
uv run ruff format --check tests/integration/test_hypothetical_pnl_view.py
uv run pytest tests/ 2>&1 | tail -5
```

Commit:
```bash
git add tests/integration/test_hypothetical_pnl_view.py
git commit -m "test(integration): add hypothetical_pnl view tests covering 10 scenarios"
```

---

## Task 4: `MarketResolutionRepository.get_pnl_summary` + ResolverAgent gauges

**Files:**
- Create: `src/polycopy/domain/pnl.py`
- Modify: `src/polycopy/ports/market_resolution_repository.py`
- Modify: `src/polycopy/infrastructure/persistence/market_resolution_repository.py`
- Modify: `src/polycopy/infrastructure/observability/metrics.py`
- Modify: `src/polycopy/agents/resolver.py`
- Modify: `tests/unit/test_ports_typecheck.py`
- Modify: `tests/unit/infrastructure/test_metrics.py`
- Create: `tests/integration/test_resolver_pnl_metrics.py`

**Reviewer:** opcional.

---

- [ ] **Step 4.1: Create `PnlSummary` dataclass**

Create `src/polycopy/domain/pnl.py`:

```python
"""PnlSummary: snapshot agregado de PnL hipotético (Plano 5C)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PnlSummary:
    """Snapshot dos totais de PnL retornado por get_pnl_summary."""

    total_pnl_usdc: Decimal
    pnl_24h_usdc: Decimal
    winrate: float  # 0..1
    trades_resolved: int
    trades_pending: int
```

- [ ] **Step 4.2: Extend Protocol**

LEIA `src/polycopy/ports/market_resolution_repository.py`. Adicionar import + método:

```python
from polycopy.domain.pnl import PnlSummary


class MarketResolutionRepository(Protocol):
    """..."""

    # ... métodos existentes ...

    async def get_pnl_summary(self) -> PnlSummary:
        """Snapshot agregado da view hypothetical_pnl. Pra métricas Prometheus."""
        ...
```

- [ ] **Step 4.3: Implement in repository**

LEIA `src/polycopy/infrastructure/persistence/market_resolution_repository.py`. Adicionar:

```python
from sqlalchemy import text
from polycopy.domain.pnl import PnlSummary


class SqlAlchemyMarketResolutionRepository:
    """..."""

    # ... métodos existentes ...

    async def get_pnl_summary(self) -> PnlSummary:
        """Query agregada na view hypothetical_pnl."""
        result = await self._session.execute(
            text("""
                SELECT
                    COALESCE(SUM(pnl_usdc), 0) as total_pnl,
                    COALESCE(SUM(pnl_usdc) FILTER (
                        WHERE decided_at > now() - interval '24 hours'
                    ), 0) as pnl_24h,
                    COUNT(*) FILTER (WHERE status IN ('win','lose','invalid')) as resolved,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE status = 'win') as wins,
                    COUNT(*) FILTER (WHERE status IN ('win','lose')) as decided
                FROM hypothetical_pnl
            """)
        )
        row = result.one()
        winrate = (
            float(row.wins) / float(row.decided) if row.decided > 0 else 0.0
        )
        return PnlSummary(
            total_pnl_usdc=Decimal(str(row.total_pnl)),
            pnl_24h_usdc=Decimal(str(row.pnl_24h)),
            winrate=winrate,
            trades_resolved=int(row.resolved),
            trades_pending=int(row.pending),
        )
```

- [ ] **Step 4.4: Update `_FakeMarketResolutionRepo` in test_ports_typecheck**

LEIA `tests/unit/test_ports_typecheck.py`. Find `_FakeMarketResolutionRepo`. Add:

```python
async def get_pnl_summary(self) -> PnlSummary:
    return PnlSummary(
        total_pnl_usdc=Decimal("0"),
        pnl_24h_usdc=Decimal("0"),
        winrate=0.0,
        trades_resolved=0,
        trades_pending=0,
    )
```

Adicionar import `from polycopy.domain.pnl import PnlSummary` e `from decimal import Decimal` se não tiver.

- [ ] **Step 4.5: Add 5 gauges in metrics.py**

LEIA `src/polycopy/infrastructure/observability/metrics.py`. Adicionar fields no `Metrics` dataclass + entries em `make_metrics()`:

Fields:
```python
hypothetical_pnl_total_usdc: Gauge
hypothetical_pnl_24h_usdc: Gauge
hypothetical_winrate: Gauge
hypothetical_trades_resolved: Gauge
hypothetical_trades_pending: Gauge
```

Entries:
```python
hypothetical_pnl_total_usdc=Gauge(
    "polycopy_hypothetical_pnl_total_usdc",
    "PnL hipotético acumulado em USDC (todos os trades resolvidos).",
    registry=target,
),
hypothetical_pnl_24h_usdc=Gauge(
    "polycopy_hypothetical_pnl_24h_usdc",
    "PnL hipotético dos últimos 24h em USDC.",
    registry=target,
),
hypothetical_winrate=Gauge(
    "polycopy_hypothetical_winrate",
    "Taxa de vitória dos trades resolvidos (0..1, exclui invalid).",
    registry=target,
),
hypothetical_trades_resolved=Gauge(
    "polycopy_hypothetical_trades_resolved",
    "Quantidade de trades resolvidos (win+lose+invalid).",
    registry=target,
),
hypothetical_trades_pending=Gauge(
    "polycopy_hypothetical_trades_pending",
    "Quantidade de trades aguardando resolução de mercado.",
    registry=target,
),
```

- [ ] **Step 4.6: Add 5 metric tests**

In `tests/unit/infrastructure/test_metrics.py`:

```python
def test_metrics_hypothetical_pnl_total_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_pnl_total_usdc.set(42.5)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_pnl_total_usdc"]
    assert matching


def test_metrics_hypothetical_pnl_24h_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_pnl_24h_usdc.set(-12.3)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_pnl_24h_usdc"]
    assert matching


def test_metrics_hypothetical_winrate_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_winrate.set(0.61)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_winrate"]
    assert matching


def test_metrics_hypothetical_trades_resolved_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_trades_resolved.set(35)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_trades_resolved"]
    assert matching


def test_metrics_hypothetical_trades_pending_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_trades_pending.set(7)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_trades_pending"]
    assert matching
```

- [ ] **Step 4.7: Wire `_compute_pnl_metrics` into ResolverAgent.run_once**

LEIA `src/polycopy/agents/resolver.py`. Adicionar método helper + chamar ao final do try-block:

```python
async def _compute_pnl_metrics(self) -> None:
    """Recomputa e seta gauges Prometheus a partir da view hypothetical_pnl. Best-effort."""
    try:
        async with self._repo_factory() as repo:
            summary = await repo.get_pnl_summary()
        self._metrics.hypothetical_pnl_total_usdc.set(float(summary.total_pnl_usdc))
        self._metrics.hypothetical_pnl_24h_usdc.set(float(summary.pnl_24h_usdc))
        self._metrics.hypothetical_winrate.set(summary.winrate)
        self._metrics.hypothetical_trades_resolved.set(summary.trades_resolved)
        self._metrics.hypothetical_trades_pending.set(summary.trades_pending)
    except Exception as exc:  # noqa: BLE001
        self._log.warning(
            "pnl_metrics_compute_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
```

Em `run_once`, ao final do try-block (após resolutions_processed completarem), chamar:

```python
async def run_once(self) -> None:
    try:
        # ... lógica existente ...
        await self._compute_pnl_metrics()
    except Exception:
        # ... handler existente ...
```

Posicione `await self._compute_pnl_metrics()` ANTES do `except` (dentro do try). Best-effort: se falhar a query, log warning, mas não derruba o ciclo.

**Atenção:** `_compute_pnl_metrics` tem seu próprio try/except internamente, então um erro lá não propaga. Posicione ANTES de `self._metrics.resolver_sync_total.labels(result="ok").inc()` se quiser que falha de PnL metric não conte como ciclo bem-sucedido — ou DEPOIS pra desacoplar. Recomendo **DEPOIS** (PnL metrics são bonus; falha não invalida o sync).

- [ ] **Step 4.8: Add integration test pra ResolverAgent gauges**

Create `tests/integration/test_resolver_pnl_metrics.py`:

```python
"""Integration: ResolverAgent popula gauges Prometheus após cada loop."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.resolver import ResolverAgent
from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.market_resolution_repository import (
    SqlAlchemyMarketResolutionRepository,
)
from polycopy.ports import MarketResolutionRepository

pytestmark = pytest.mark.integration


def _unique_cond() -> str:
    return "0x" + uuid.uuid4().hex.ljust(64, "0")[:64]


class _StubGamma:
    async def get_market(self, token_id):
        return None

    async def list_active_markets(self, *, limit: int):
        return []

    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ):
        return []


async def test_resolver_metrics_populated_after_loop(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Após run_once, os 5 gauges aparecem com valores plausíveis."""
    cond = _unique_cond()
    tid = uuid.uuid4()

    # Prepara dados: 1 trade BUY win + 1 trade BUY lose
    async with db_session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO order_executions "
                "(trade_event_id, wallet, condition_id, token_id, side, "
                " final_size_usdc, mode, result, decided_at, expected_avg_price) "
                "VALUES (:t, :w, :c, '111', 'BUY', 10, 'dry_run', 'dry_run', "
                "        now(), 0.5)"
            ),
            {"t": tid, "w": "0x" + "1" * 40, "c": cond},
        )
        await session.execute(
            text(
                "INSERT INTO market_resolutions "
                "(condition_id, resolved_outcome, winning_token_id, "
                " resolved_at, outcome_prices_raw) "
                "VALUES (:c, 'YES', '111', now(), '[\"1\",\"0\"]')"
            ),
            {"c": cond},
        )
        await session.commit()

    metrics = make_metrics(registry=CollectorRegistry())

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        async with db_session_factory() as session:
            yield SqlAlchemyMarketResolutionRepository(session)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=_StubGamma(),
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    # Gauges devem refletir o estado: 1 win, 0 pending, pnl=10
    assert metrics.hypothetical_trades_resolved._value.get() >= 1
    assert metrics.hypothetical_pnl_total_usdc._value.get() != 0


async def test_resolver_metrics_zero_when_empty(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Gauges zerados quando não há trades."""
    metrics = make_metrics(registry=CollectorRegistry())

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        async with db_session_factory() as session:
            yield SqlAlchemyMarketResolutionRepository(session)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=_StubGamma(),
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    assert metrics.hypothetical_pnl_total_usdc._value.get() == 0
    assert metrics.hypothetical_trades_resolved._value.get() == 0


async def test_resolver_metrics_query_failure_logs_warning(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Se get_pnl_summary falha, gauges ficam stale (não zerados), log warning."""
    metrics = make_metrics(registry=CollectorRegistry())

    # Pre-popula gauge com valor conhecido
    metrics.hypothetical_pnl_total_usdc.set(99.99)

    class _FailingRepo:
        async def insert(self, resolution: MarketResolution) -> bool:
            return True

        async def get_unresolved_condition_ids(self, *, limit: int):
            return []

        async def get_pnl_summary(self):
            raise RuntimeError("simulated DB failure")

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        yield _FailingRepo()

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=_StubGamma(),
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    # Gauge stale (não foi resetado)
    assert metrics.hypothetical_pnl_total_usdc._value.get() == 99.99
```

- [ ] **Step 4.9: Verifications + STOP**

```bash
docker compose stop resolver  # evita interferência
uv run pytest tests/integration/test_resolver_pnl_metrics.py -v 2>&1 | tail -15
uv run pytest tests/unit/infrastructure/test_metrics.py -v 2>&1 | tail -15
docker compose start resolver
uv run mypy src/polycopy
uv run ruff check src/polycopy/domain/pnl.py src/polycopy/ports/market_resolution_repository.py src/polycopy/infrastructure/persistence/market_resolution_repository.py src/polycopy/infrastructure/observability/metrics.py src/polycopy/agents/resolver.py tests/unit/test_ports_typecheck.py tests/unit/infrastructure/test_metrics.py tests/integration/test_resolver_pnl_metrics.py
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -5
```

Commit:
```bash
git add src/polycopy/domain/pnl.py src/polycopy/ports/market_resolution_repository.py src/polycopy/infrastructure/persistence/market_resolution_repository.py src/polycopy/infrastructure/observability/metrics.py src/polycopy/agents/resolver.py tests/unit/test_ports_typecheck.py tests/unit/infrastructure/test_metrics.py tests/integration/test_resolver_pnl_metrics.py
git commit -m "feat(resolver): expose 5 hypothetical PnL gauges via get_pnl_summary"
```

---

## Task 5: CLI script `backtest.py`

**Files:**
- Create: `src/polycopy/scripts/backtest.py`

**Reviewer:** opcional.

---

- [ ] **Step 5.1: Create CLI script**

Create `src/polycopy/scripts/backtest.py`:

```python
"""Backtest CLI — consulta hypothetical_pnl view e formata summary.

Uso:
    uv run python -m polycopy.scripts.backtest [--since 7d] [--by wallet|none] [--format table|json]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import text

from polycopy.config import Settings
from polycopy.infrastructure.persistence.database import (
    make_engine,
    make_session_factory,
)


@dataclass(frozen=True)
class Trade:
    trade_event_id: str
    wallet: str
    condition_id: str
    token_id: str
    side: str
    final_size_usdc: Decimal
    expected_avg_price: Decimal | None
    decided_at: datetime
    resolved_outcome: str | None
    pnl_usdc: Decimal | None
    status: str


def _parse_since(value: str) -> timedelta:
    """Parse '7d', '24h', '1w' format into timedelta."""
    match = re.fullmatch(r"(\d+)([dhwm])", value)
    if not match:
        raise ValueError(f"invalid --since format: {value!r}, expected like '7d' or '24h'")
    n, unit = int(match.group(1)), match.group(2)
    return {
        "h": timedelta(hours=n),
        "d": timedelta(days=n),
        "w": timedelta(weeks=n),
        "m": timedelta(days=n * 30),  # approx
    }[unit]


async def _query_trades(*, since: timedelta) -> list[Trade]:
    settings = Settings()  # type: ignore[call-arg]
    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    cutoff = datetime.now(tz=UTC) - since

    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT trade_event_id, wallet, condition_id, token_id, side,
                       final_size_usdc, expected_avg_price, decided_at,
                       resolved_outcome, pnl_usdc, status
                FROM hypothetical_pnl
                WHERE decided_at > :cutoff
                ORDER BY decided_at DESC
            """),
            {"cutoff": cutoff},
        )
        rows = result.all()

    await engine.dispose()
    return [
        Trade(
            trade_event_id=str(r.trade_event_id),
            wallet=r.wallet,
            condition_id=r.condition_id,
            token_id=r.token_id,
            side=r.side,
            final_size_usdc=r.final_size_usdc,
            expected_avg_price=r.expected_avg_price,
            decided_at=r.decided_at,
            resolved_outcome=r.resolved_outcome,
            pnl_usdc=r.pnl_usdc,
            status=r.status,
        )
        for r in rows
    ]


def _format_table(trades: list[Trade], *, since: timedelta, by: str) -> str:
    """Formata summary + tabela top trades em texto plano."""
    if not trades:
        return f"=== Backtest Summary ===\nPeriod: últimos {since}\nNo trades found in period.\n"

    n = len(trades)
    by_status: dict[str, int] = defaultdict(int)
    for t in trades:
        by_status[t.status] += 1

    resolved_pnls = [t.pnl_usdc for t in trades if t.pnl_usdc is not None]
    total_pnl = sum(resolved_pnls, Decimal(0))

    win_count = by_status.get("win", 0)
    lose_count = by_status.get("lose", 0)
    decided = win_count + lose_count
    winrate = (win_count / decided * 100) if decided > 0 else 0.0

    lines = [
        "=== Backtest Summary ===",
        f"Period:        últimos {since}",
        f"Trades total:  {n}",
        f"  - Resolved:  {by_status.get('win', 0) + by_status.get('lose', 0) + by_status.get('invalid', 0)} "
        f"(win {win_count}, lose {lose_count}, invalid {by_status.get('invalid', 0)})",
        f"  - Pending:    {by_status.get('pending', 0)}",
        f"  - Excluded:   {by_status.get('sell_excluded', 0) + by_status.get('no_expected_price', 0)} "
        f"(sell {by_status.get('sell_excluded', 0)}, no_price {by_status.get('no_expected_price', 0)})",
        "",
        f"PnL hipotético:  ${total_pnl:+.2f} USDC",
        f"Winrate:         {winrate:.1f}% ({win_count} / {decided} resolved excluindo invalid)",
        "",
    ]

    if by == "wallet":
        lines.append("=== By wallet ===")
        by_wallet: dict[str, list[Trade]] = defaultdict(list)
        for t in trades:
            by_wallet[t.wallet].append(t)
        for wallet, wt in by_wallet.items():
            wpnl = sum(
                (t.pnl_usdc for t in wt if t.pnl_usdc is not None), Decimal(0)
            )
            ww = sum(1 for t in wt if t.status == "win")
            wd = sum(1 for t in wt if t.status in ("win", "lose"))
            wrate = (ww / wd * 100) if wd > 0 else 0.0
            lines.append(
                f"  {wallet[:10]}...:  ${wpnl:+.2f}  ({ww} wins / {wd} decided = {wrate:.1f}%)"
            )
        lines.append("")

    lines.append("=== Top 10 trades ===")
    lines.append(f"{'wallet':<14} {'side':<5} {'size':>8} {'expected':>10} {'status':<18} {'pnl':>10}")
    lines.append("-" * 70)
    for t in trades[:10]:
        wallet_short = t.wallet[:12] + ".."
        size_str = f"{t.final_size_usdc:.2f}"
        exp_str = f"{t.expected_avg_price:.4f}" if t.expected_avg_price else "N/A"
        pnl_str = f"{t.pnl_usdc:+.2f}" if t.pnl_usdc is not None else "N/A"
        lines.append(
            f"{wallet_short:<14} {t.side:<5} {size_str:>8} {exp_str:>10} {t.status:<18} {pnl_str:>10}"
        )

    return "\n".join(lines)


def _format_json(trades: list[Trade], *, since: timedelta, by: str) -> str:
    return json.dumps(
        [
            {
                "trade_event_id": t.trade_event_id,
                "wallet": t.wallet,
                "condition_id": t.condition_id,
                "token_id": t.token_id,
                "side": t.side,
                "final_size_usdc": str(t.final_size_usdc),
                "expected_avg_price": str(t.expected_avg_price) if t.expected_avg_price else None,
                "decided_at": t.decided_at.isoformat(),
                "resolved_outcome": t.resolved_outcome,
                "pnl_usdc": str(t.pnl_usdc) if t.pnl_usdc is not None else None,
                "status": t.status,
            }
            for t in trades
        ],
        indent=2,
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Polycopy backtest CLI")
    p.add_argument("--since", default="7d", help="Período (ex: 7d, 24h, 1w). Default: 7d.")
    p.add_argument("--by", default="none", choices=["none", "wallet"], help="Group-by")
    p.add_argument("--format", default="table", choices=["table", "json"], dest="format_")
    return p


async def main_async(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        since = _parse_since(args.since)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    trades = await _query_trades(since=since)

    if args.format_ == "json":
        print(_format_json(trades, since=since, by=args.by))
    else:
        print(_format_table(trades, since=since, by=args.by))
    return 0


def main() -> None:
    sys.exit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
```

- [ ] **Step 5.2: Smoke run**

```bash
uv run python -m polycopy.scripts.backtest --since 30d
```
Expected: prints "No trades found in period." (production prod is empty in current DB) ou tabela se houver dados. Exit code 0.

- [ ] **Step 5.3: Verifications + STOP**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/scripts/backtest.py
uv run ruff format --check src/polycopy/scripts/backtest.py
uv run pytest tests/ 2>&1 | tail -5
```

Commit:
```bash
git add src/polycopy/scripts/backtest.py
git commit -m "feat(scripts): add backtest CLI consuming hypothetical_pnl view"
```

---

## Task 6: Unit tests do CLI

**Files:**
- Create: `tests/unit/scripts/test_backtest.py`

**Reviewer:** opcional.

---

- [ ] **Step 6.1: Create unit test file**

Create `tests/unit/scripts/test_backtest.py`:

```python
"""Unit tests do CLI backtest — testa parsing + formatação com inputs sintéticos."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polycopy.scripts.backtest import (
    Trade,
    _format_json,
    _format_table,
    _parse_since,
)


def _trade(
    *,
    status: str = "win",
    side: str = "BUY",
    pnl: Decimal | None = Decimal("5"),
    wallet: str = "0xabc",
) -> Trade:
    return Trade(
        trade_event_id="00000000-0000-0000-0000-000000000001",
        wallet=wallet,
        condition_id="0x" + "ab" * 32,
        token_id="111",
        side=side,
        final_size_usdc=Decimal("10"),
        expected_avg_price=Decimal("0.5"),
        decided_at=datetime.now(tz=UTC),
        resolved_outcome="YES",
        pnl_usdc=pnl,
        status=status,
    )


def test_parse_since_days() -> None:
    assert _parse_since("7d") == timedelta(days=7)


def test_parse_since_hours() -> None:
    assert _parse_since("24h") == timedelta(hours=24)


def test_parse_since_weeks() -> None:
    assert _parse_since("2w") == timedelta(weeks=2)


def test_parse_since_invalid_raises() -> None:
    with pytest.raises(ValueError, match="invalid --since format"):
        _parse_since("xyz")


def test_format_table_empty_trades_shows_message() -> None:
    out = _format_table([], since=timedelta(days=7), by="none")
    assert "No trades found" in out


def test_format_table_all_wins() -> None:
    trades = [_trade(status="win", pnl=Decimal("5"))] * 3
    out = _format_table(trades, since=timedelta(days=7), by="none")
    assert "Trades total:  3" in out
    assert "win 3" in out
    assert "+15.00" in out  # 3 * 5
    assert "100.0%" in out


def test_format_table_mixed_outcomes() -> None:
    trades = [
        _trade(status="win", pnl=Decimal("5")),
        _trade(status="lose", pnl=Decimal("-10")),
        _trade(status="invalid", pnl=Decimal("-2")),
        _trade(status="pending", pnl=None),
        _trade(status="sell_excluded", pnl=None),
    ]
    out = _format_table(trades, since=timedelta(days=7), by="none")
    assert "Trades total:  5" in out
    assert "win 1" in out
    assert "lose 1" in out
    assert "invalid 1" in out
    assert "Pending:    1" in out
    assert "sell 1" in out
    assert "50.0%" in out  # 1 win / 2 decided


def test_format_table_by_wallet_groups() -> None:
    trades = [
        _trade(wallet="0xa", status="win", pnl=Decimal("5")),
        _trade(wallet="0xa", status="lose", pnl=Decimal("-3")),
        _trade(wallet="0xb", status="win", pnl=Decimal("10")),
    ]
    out = _format_table(trades, since=timedelta(days=7), by="wallet")
    assert "By wallet" in out
    assert "0xa" in out
    assert "0xb" in out


def test_format_json_serializes_trades() -> None:
    import json

    trades = [_trade(status="win", pnl=Decimal("5"))]
    out = _format_json(trades, since=timedelta(days=7), by="none")
    parsed = json.loads(out)
    assert len(parsed) == 1
    assert parsed[0]["status"] == "win"
    assert parsed[0]["pnl_usdc"] == "5"


def test_format_json_handles_null_pnl() -> None:
    import json

    trades = [_trade(status="pending", pnl=None)]
    out = _format_json(trades, since=timedelta(days=7), by="none")
    parsed = json.loads(out)
    assert parsed[0]["pnl_usdc"] is None
```

- [ ] **Step 6.2: Run tests + STOP**

```bash
mkdir -p tests/unit/scripts
uv run pytest tests/unit/scripts/test_backtest.py -v 2>&1 | tail -15
uv run mypy tests/unit/scripts/test_backtest.py
uv run ruff check tests/unit/scripts/test_backtest.py
uv run ruff format --check tests/unit/scripts/test_backtest.py
```

Esperado: 10 PASS.

Commit:
```bash
git add tests/unit/scripts/test_backtest.py
git commit -m "test(scripts): add unit tests for backtest CLI formatting"
```

---

## Task 7: ARCHITECTURE.md docs

**Files:**
- Modify: `ARCHITECTURE.md`

**Reviewer:** opcional.

---

- [ ] **Step 7.1: Locate insertion point**

LEIA `ARCHITECTURE.md`. Adicionar nova seção após "Running tests" ou no fim antes de "Decisões registradas".

- [ ] **Step 7.2: Add "Backtest" section**

Adicionar:

```markdown
## Backtest

Após resoluções de mercados (Plano 5A) e captura de `expected_avg_price` (Plano 5B), o sistema computa **PnL hipotético** — quanto teria sido ganho/perdido se real-mode estivesse ativo.

### View `hypothetical_pnl`

SQL view (não materializada) cruza `order_executions` (mode='dry_run') com `market_resolutions` via `condition_id`. Colunas computadas:
- `qty_tokens` = `final_size_usdc / expected_avg_price` (ou NULL).
- `payout_per_token` = 1.0 se win, 0.0 se lose, 0.5 se INVALID, NULL se pending/sell_excluded.
- `pnl_usdc` = qty * payout - size (ou NULL).
- `status` = `'win'`, `'lose'`, `'invalid'`, `'pending'`, `'sell_excluded'`, `'no_expected_price'`.

V1 cobre apenas trades BUY (Polymarket primário). SELL hipotéticos ficam como follow-up.

### CLI `backtest.py`

```bash
uv run python -m polycopy.scripts.backtest --since 7d --by wallet --format table
```

Args:
- `--since`: período (ex: `7d`, `24h`, `1w`). Default `7d`.
- `--by`: agrupa por `wallet` ou `none`. Default `none`.
- `--format`: `table` (default) ou `json`.

Output exemplo:
```
=== Backtest Summary ===
Period:        últimos 7 days, 0:00:00
Trades total:  42
  - Resolved:  35 (win 18, lose 14, invalid 3)
  - Pending:    5
  - Excluded:   2 (sell 1, no_price 1)

PnL hipotético:  $-3.45 USDC
Winrate:         51.4% (18 / 35 resolved excluindo invalid)
```

### Métricas Prometheus

ResolverAgent expõe 5 gauges (atualizadas a cada loop, intervalo `RESOLVER_SYNC_INTERVAL_SECONDS`, default 1h):
- `polycopy_hypothetical_pnl_total_usdc` — soma cumulativa.
- `polycopy_hypothetical_pnl_24h_usdc` — soma últimas 24h.
- `polycopy_hypothetical_winrate` — 0..1 (exclui invalid do denominador).
- `polycopy_hypothetical_trades_resolved` — count.
- `polycopy_hypothetical_trades_pending` — count.

Endpoint: `http://127.0.0.1:9107/metrics` (porta do ResolverAgent).
```

- [ ] **Step 7.3: Verifications + STOP**

```bash
uv run pytest tests/ 2>&1 | tail -5
```
(ruff doesn't lint markdown)

Commit:
```bash
git add ARCHITECTURE.md
git commit -m "docs(architecture): document hypothetical_pnl view, backtest CLI and gauges"
```

---

## Self-Review (autor do plano)

**Spec coverage:**

| Spec § | Coberto em |
|---|---|
| §3.1 Migration 0009 — `side` + view | T1 |
| §3.2 Domain `OrderExecution.side` + ORM + repo + executor | T2 |
| §3.3 CLI `backtest.py` | T5 |
| §3.4 5 gauges + `_compute_pnl_metrics` + Repository helper | T4 |
| §4 Edge cases (10 cenários) | T3 (10 testes) + T6 (formatação edge cases) |
| §5.1 Integration tests view | T3 |
| §5.2 Unit tests CLI | T6 |
| §5.3 Integration tests resolver gauges | T4 |
| §5.4 Migration round-trip | T1 (alembic upgrade/downgrade) |
| §6.1 Settings — sem novos campos | implícito |
| §6.2 5 métricas | T4 |
| §6.3 Logs estruturados | T4 (`pnl_metrics_compute_failed`) |
| §10 Sucesso (CLI <2s, gauges plausíveis) | T5 (smoke run) + T4 (integration tests) |

Coverage completa.

**Placeholder scan:** sem TBD/TODO/"add appropriate handling". Todos os snippets têm código completo.

**Type consistency:**
- `OrderExecution.side: Literal["BUY", "SELL"]` em T2 (def), propagado em T2 (ORM/repo/executor).
- `PnlSummary` dataclass em T4.1 (def), usada em T4.2 (Protocol), T4.3 (impl), T4.4 (fake), T4.7 (resolver).
- `MarketResolutionRepository.get_pnl_summary() -> PnlSummary` consistente em T4.2 (Protocol), T4.3 (impl), T4.4 (fake), T4.7 (caller).
- View columns `pnl_usdc`, `qty_tokens`, `payout_per_token`, `status` consistentes em T1 (def), T3 (assertions), T4.3 (query), T5 (CLI Trade dataclass), T6 (test fixtures).

**Bite-sized check:** maior task é T4 (~9 steps) por ser cross-cutting. Cada step é isolado. T1, T5, T6, T7 são curtas.

**Reviewer:** nenhum obrigatório (sem money flow novo). Cadência: checkpoint humano por task.

**Notas operacionais:**
- Testes integration: pre-existing safe (test-db-isolation completo).
- Container `polycopy-resolver` rodando — parar antes de rodar `test_resolver_pnl_metrics.py` pra evitar interferência.
- Sem dep extra (`rich`/`tabulate`) — formatação plain Python no CLI.
