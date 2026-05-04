# Fase 5B — Slippage Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. **Cadência: checkpoint humano por task** (mesma das fases anteriores).

**Goal:** Capturar `expected_avg_price` (preço médio esperado calculado do orderbook) em `order_executions` pra cada trade DRY-RUN ou real-mode — input crítico do backtest (Plano 5C).

**Architecture:** Função pura `calculate_expected_avg_price` em `domain/slippage.py` percorre asks (BUY) ou bids (SELL) acumulando até `final_size_usdc`. `DryRunExecutor` e `Web3CLOBExecutor` recebem `PolymarketCLOBPort`, chamam `get_book` antes de simular/executar e propagam o resultado via novo campo `ExecutionResult.expected_avg_price`. `ExecutorAgent` persiste em coluna nova (migration 0008).

**Tech Stack:** Python 3.12, pydantic v2, SQLAlchemy 2 async, alembic, Decimal, prometheus_client, pytest + respx + pytest-asyncio.

**Predecessor:** Plano 5A (head `563c2f6`) + test-db-isolation (head `77bea2f`) + spec 5B (`5c01864`).

**Spec:** `docs/superpowers/specs/2026-05-04-fase-5b-slippage-snapshot-design.md`.

---

## File Structure

**Novos arquivos (3):**
- `src/polycopy/domain/slippage.py` — função pura `calculate_expected_avg_price`.
- `tests/unit/domain/test_slippage.py` — 10 testes unit.
- `alembic/versions/0008_add_expected_avg_price.py` — migration.

**Modificados (~10):**
- `src/polycopy/domain/execution.py` — `ExecutionResult` + `OrderExecution` ganham `expected_avg_price: Decimal | None`.
- `src/polycopy/infrastructure/persistence/models.py` — `OrderExecutionRow` adiciona coluna.
- `src/polycopy/infrastructure/persistence/order_execution_repository.py` — propaga campo no insert.
- `src/polycopy/infrastructure/execution/dry_run_executor.py` — recebe CLOB + métricas + chama get_book.
- `src/polycopy/infrastructure/execution/web3_clob_executor.py` — recebe CLOB (mesma lógica).
- `src/polycopy/infrastructure/observability/metrics.py` — +1 counter.
- `src/polycopy/agents/executor.py` — `_handle_message` propaga campo + `main()` injeta CLOB nos executors.
- `tests/unit/infrastructure/test_dry_run_executor.py` — atualiza testes existentes + 4 novos.
- `tests/unit/infrastructure/test_web3_clob_executor.py` — atualiza testes (CLOB stub).
- `tests/unit/infrastructure/test_metrics.py` — +1 teste.
- `tests/integration/test_executor_e2e.py` — +1 cenário E2E.

---

## Task 1: Domain — `calculate_expected_avg_price` + 10 testes

**Files:**
- Create: `src/polycopy/domain/slippage.py`
- Create: `tests/unit/domain/test_slippage.py`

**Reviewer:** opcional.

---

- [ ] **Step 1.1: Escrever 10 testes unit (RED)**

Create `tests/unit/domain/test_slippage.py`:

```python
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
    result = calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("60")
    )
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
    result = calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("18")
    )
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
    result = calculate_expected_avg_price(
        book=book, side=Side.SELL, target_usdc=Decimal("18")
    )
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
    result = calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("10")
    )
    assert result is not None
    # Confere com tolerância de 8 casas (Numeric(20,8))
    expected = Decimal("10") / (Decimal("10") + Decimal("5") / Decimal("0.6"))
    assert abs(result - expected) < Decimal("0.00000001")


def test_returns_none_when_book_empty() -> None:
    """Asks vazios pra BUY → None."""
    book = _book(asks=[], bids=[_level("0.5", "10")])
    assert calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("10")
    ) is None


def test_returns_none_when_insufficient_volume() -> None:
    """Total liquidez < target → None."""
    book = _book(
        asks=[_level("0.5", "10")],  # apenas 5 USDC disponível
        bids=[],
    )
    assert calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("100")
    ) is None


def test_buy_single_ask_partial() -> None:
    """1 ask com volume excedente; preenche fração e retorna preço daquele nível."""
    book = _book(
        asks=[_level("0.5", "100")],  # 50 USDC disponível
        bids=[],
    )
    # target = 10 USDC. qty = 20. avg = 10/20 = 0.5
    result = calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("10")
    )
    assert result == Decimal("0.5")


def test_returns_none_for_sell_with_empty_bids() -> None:
    """SELL com bids vazios → None."""
    book = _book(asks=[_level("0.6", "100")], bids=[])
    assert calculate_expected_avg_price(
        book=book, side=Side.SELL, target_usdc=Decimal("10")
    ) is None


def test_decimal_precision_8_places() -> None:
    """Confirma precisão preservada até 8 casas decimais."""
    book = _book(
        asks=[_level("0.33333333", "1000")],
        bids=[],
    )
    # target = 33.333333 USDC. qty = 100. avg = 0.33333333
    result = calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("33.333333")
    )
    assert result == Decimal("0.33333333")


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
    result = calculate_expected_avg_price(
        book=book, side=Side.BUY, target_usdc=Decimal("6")
    )
    assert result == Decimal("0.6")
```

Run RED:
```bash
mkdir -p tests/unit/domain
uv run pytest tests/unit/domain/test_slippage.py -v 2>&1 | tail -10
```
Expected: ImportError (`calculate_expected_avg_price` não existe).

- [ ] **Step 1.2: Implementar `src/polycopy/domain/slippage.py`**

```python
"""Slippage / expected price calculation.

Função pura que percorre orderbook acumulando até target_usdc e retorna
weighted avg price. Usada pelos executors (DryRun + Web3CLOB) pra
gravar expected_avg_price em order_executions (Plano 5B).
"""

from __future__ import annotations

from decimal import Decimal

from polycopy.domain.market import OrderBook, OrderBookLevel
from polycopy.domain.models import Side


def calculate_expected_avg_price(
    *, book: OrderBook, side: Side, target_usdc: Decimal
) -> Decimal | None:
    """Weighted avg price pra preencher target_usdc.

    BUY: percorre asks ascendente, acumula custo até target.
    SELL: percorre bids descendente, acumula receita até target.

    Retorna None se liquidez total < target_usdc (book vazio ou insuficiente).
    """
    levels: list[OrderBookLevel] = book.asks if side == Side.BUY else book.bids

    if not levels:
        return None

    accumulated_usdc = Decimal("0")
    accumulated_qty = Decimal("0")

    for level in levels:
        price = level.price.value
        size = level.size.amount
        if size <= 0:
            continue

        slice_usdc = price * size
        if accumulated_usdc + slice_usdc >= target_usdc:
            # Fração final do nível — pega só o que falta
            remaining_usdc = target_usdc - accumulated_usdc
            slice_qty = remaining_usdc / price
            accumulated_qty += slice_qty
            return target_usdc / accumulated_qty

        # Consome nível inteiro e segue
        accumulated_usdc += slice_usdc
        accumulated_qty += size

    # Esgotou book sem atingir target
    return None
```

- [ ] **Step 1.3: GREEN**

```bash
uv run pytest tests/unit/domain/test_slippage.py -v 2>&1 | tail -20
```
Expected: 10 PASS.

- [ ] **Step 1.4: Verificações + STOP**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/domain/slippage.py tests/unit/domain/test_slippage.py
uv run ruff format --check src/polycopy/domain/slippage.py tests/unit/domain/test_slippage.py
uv run pytest tests/ 2>&1 | tail -5
```

**Não rodar `pytest tests/` é seguro agora** (test-db-isolation completo). Esperado: ~407 + 10 = 417 passed, 11 falhas pré-existentes.

Implementer NÃO commita. Controller pede confirmação humana, depois:

```bash
git add src/polycopy/domain/slippage.py tests/unit/domain/test_slippage.py
git commit -m "feat(domain): add calculate_expected_avg_price for slippage snapshot (Fase 5B)"
```

---

## Task 2: `ExecutionResult` + `OrderExecution` ganham `expected_avg_price`

**Files:**
- Modify: `src/polycopy/domain/execution.py`

**Reviewer:** opcional.

---

- [ ] **Step 2.1: Adicionar campo em `ExecutionResult`**

LEIA `src/polycopy/domain/execution.py` primeiro pra confirmar pattern. Adicionar campo:

```python
@dataclass(frozen=True)
class ExecutionResult:
    """Retorno de OrderExecutor.execute(). Convertido em OrderExecution pelo agente."""

    mode: ExecutionMode
    success: bool
    tx_hash: str | None = None
    gas_wei: int | None = None
    failure_reason: FailureReason | None = None
    error_message: str | None = None
    expected_avg_price: Decimal | None = None  # NOVO — Plano 5B
```

Default `None` mantém retrocompatibilidade com callers existentes (eles continuam compilando até serem atualizados em T4/T5).

- [ ] **Step 2.2: Adicionar campo em `OrderExecution`**

```python
@dataclass(frozen=True)
class OrderExecution:
    """..."""

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
    expected_avg_price: Decimal | None = None  # NOVO — Plano 5B
```

`expected_avg_price` é **opcional** — não tem invariante (pode ser None se book insuficiente ou row antiga).

- [ ] **Step 2.3: Verificações + STOP**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/domain/execution.py
uv run ruff format --check src/polycopy/domain/execution.py
uv run pytest tests/ 2>&1 | tail -5
```

Esperado: tudo limpo. Os testes existentes do `OrderExecution` continuam passando (default None).

Commit:
```bash
git add src/polycopy/domain/execution.py
git commit -m "feat(domain): add expected_avg_price to ExecutionResult and OrderExecution"
```

---

## Task 3: Migration 0008 + ORM column

**Files:**
- Create: `alembic/versions/0008_add_expected_avg_price.py`
- Modify: `src/polycopy/infrastructure/persistence/models.py`
- Modify: `src/polycopy/infrastructure/persistence/order_execution_repository.py`

**Reviewer:** opcional (DDL puro).

---

- [ ] **Step 3.1: Criar migration `0008_add_expected_avg_price.py`**

LEIA `alembic/versions/0007_add_market_resolutions.py` pra confirmar style (revision tipo, naming).

Create `alembic/versions/0008_add_expected_avg_price.py`:

```python
"""add expected_avg_price column to order_executions

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-04 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "order_executions",
        sa.Column(
            "expected_avg_price",
            sa.Numeric(20, 8),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("order_executions", "expected_avg_price")
```

- [ ] **Step 3.2: Adicionar coluna em `OrderExecutionRow`**

LEIA `src/polycopy/infrastructure/persistence/models.py:192` pra confirmar style. Adicionar logo após `error_message`:

```python
    expected_avg_price: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
```

(Imports `Decimal` e `Numeric` já existem no arquivo.)

- [ ] **Step 3.3: Atualizar `SqlAlchemyOrderExecutionRepository.insert`**

LEIA `src/polycopy/infrastructure/persistence/order_execution_repository.py`. Adicionar `expected_avg_price` no `pg_insert(...).values(...)`:

```python
        stmt = (
            pg_insert(OrderExecutionRow)
            .values(
                # ... campos existentes ...
                expected_avg_price=execution.expected_avg_price,
            )
            .on_conflict_do_nothing(index_elements=["trade_event_id"])
        )
```

- [ ] **Step 3.4: Validar alembic round-trip**

```bash
docker compose ps postgres
uv run alembic upgrade head
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d order_executions"
```
Esperado: `\d` mostra coluna `expected_avg_price` numeric(20,8) nullable.

```bash
uv run alembic downgrade -1
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d order_executions" | grep expected_avg_price || echo "coluna removida (esperado)"
uv run alembic upgrade head
```

- [ ] **Step 3.5: Verificações + STOP**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/infrastructure/persistence/models.py src/polycopy/infrastructure/persistence/order_execution_repository.py alembic/versions/0008_add_expected_avg_price.py
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -5
```

Esperado: tudo limpo. Suite preservada.

Commit:
```bash
git add alembic/versions/0008_add_expected_avg_price.py src/polycopy/infrastructure/persistence/models.py src/polycopy/infrastructure/persistence/order_execution_repository.py
git commit -m "feat(persistence): add expected_avg_price column to order_executions"
```

---

## Task 4: `DryRunExecutor` recebe CLOB + métrica nova + 4 testes

**Files:**
- Modify: `src/polycopy/infrastructure/execution/dry_run_executor.py`
- Modify: `src/polycopy/infrastructure/observability/metrics.py`
- Modify: `tests/unit/infrastructure/test_dry_run_executor.py` (atualizar + adicionar testes)
- Modify: `tests/unit/infrastructure/test_metrics.py` (+1 teste)

**Reviewer:** opcional.

---

- [ ] **Step 4.1: Adicionar métrica em `metrics.py`**

LEIA `src/polycopy/infrastructure/observability/metrics.py` pra confirmar pattern. Adicionar campo em `Metrics`:

```python
    executor_expected_price_unavailable_total: Counter
```

E em `make_metrics()`:

```python
        executor_expected_price_unavailable_total=Counter(
            "polycopy_executor_expected_price_unavailable",
            "Trades onde expected_avg_price não pôde ser calculado.",
            labelnames=["reason"],
            registry=target,
        ),
```

- [ ] **Step 4.2: Adicionar teste de métrica**

Em `tests/unit/infrastructure/test_metrics.py`, adicionar:

```python
def test_metrics_executor_expected_price_unavailable_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_expected_price_unavailable_total.labels(reason="empty_book").inc()
    metrics.executor_expected_price_unavailable_total.labels(reason="insufficient_volume").inc()
    metrics.executor_expected_price_unavailable_total.labels(reason="fetch_failed").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_expected_price_unavailable"]
    assert len(matching) == 1
```

- [ ] **Step 4.3: Atualizar `DryRunExecutor`**

LEIA `src/polycopy/infrastructure/execution/dry_run_executor.py` (versão atual: 32 linhas). Reescrever:

```python
"""DryRunExecutor: implementação MVP de OrderExecutor.

Sempre retorna ExecutionResult(mode=DRY_RUN, success=True). Não chama
blockchain. Calcula expected_avg_price a partir do orderbook (Plano 5B).
"""

from __future__ import annotations

from decimal import Decimal

import structlog

from polycopy.domain.events import ExecutionMode
from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade
from polycopy.domain.slippage import calculate_expected_avg_price
from polycopy.infrastructure.observability.metrics import Metrics
from polycopy.ports import PolymarketCLOBPort


class DryRunExecutor:
    """Executor que apenas simula — não chama blockchain.

    Calcula expected_avg_price via clob.get_book + função pura
    calculate_expected_avg_price. None se book insuficiente.
    """

    def __init__(self, *, clob: PolymarketCLOBPort, metrics: Metrics) -> None:
        self._clob = clob
        self._metrics = metrics
        self._log = structlog.get_logger("dry_run_executor")

    async def execute(
        self,
        trade: Trade,
        final_size_usdc: Decimal,
    ) -> ExecutionResult:
        expected = await self._compute_expected_price(trade, final_size_usdc)
        return ExecutionResult(
            mode=ExecutionMode.DRY_RUN,
            success=True,
            tx_hash=None,
            gas_wei=None,
            failure_reason=None,
            error_message=None,
            expected_avg_price=expected,
        )

    async def _compute_expected_price(
        self, trade: Trade, final_size_usdc: Decimal
    ) -> Decimal | None:
        try:
            book = await self._clob.get_book(trade.token_id)
        except Exception as exc:  # noqa: BLE001 — qualquer falha → None + métrica
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="fetch_failed"
            ).inc()
            self._log.warning(
                "expected_price_fetch_failed",
                error=str(exc),
                error_type=type(exc).__name__,
                token_id=trade.token_id.value,
            )
            return None

        if not book.asks and not book.bids:
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="empty_book"
            ).inc()
            return None

        result = calculate_expected_avg_price(
            book=book, side=trade.side, target_usdc=final_size_usdc
        )
        if result is None:
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="insufficient_volume"
            ).inc()
        return result
```

- [ ] **Step 4.4: Atualizar testes existentes em `test_dry_run_executor.py`**

LEIA `tests/unit/infrastructure/test_dry_run_executor.py`. Atualizar a construção do executor pra passar CLOB + metrics:

Pattern típico (adapte ao real):
```python
from prometheus_client import CollectorRegistry
from polycopy.infrastructure.observability.metrics import make_metrics


class _StubCLOB:
    """Stub que satisfaz PolymarketCLOBPort."""

    def __init__(self, book: OrderBook | None = None) -> None:
        self._book = book

    async def get_book(self, token_id) -> OrderBook:
        if self._book is None:
            raise RuntimeError("book not configured")
        return self._book


def _make_executor(book: OrderBook | None = None) -> DryRunExecutor:
    metrics = make_metrics(registry=CollectorRegistry())
    return DryRunExecutor(clob=_StubCLOB(book), metrics=metrics)
```

Em testes existentes (que tipicamente fazem `DryRunExecutor()` sem args), adaptar pra usar `_make_executor(...)` com book trivial (`OrderBook(token_id=..., asks=[level("0.5", "1000")], bids=[level("0.5", "1000")], captured_at=now)`).

- [ ] **Step 4.5: Adicionar 4 novos testes**

```python
async def test_execute_returns_expected_avg_price_when_book_available() -> None:
    book = _book(asks=[_level("0.6", "100")], bids=[])
    executor = _make_executor(book=book)
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("60"))

    assert result.expected_avg_price == Decimal("0.6")


async def test_execute_returns_none_when_book_empty() -> None:
    book = _book(asks=[], bids=[])
    metrics = make_metrics(registry=CollectorRegistry())
    executor = DryRunExecutor(clob=_StubCLOB(book), metrics=metrics)
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("10"))

    assert result.expected_avg_price is None
    samples = [
        s for m in CollectorRegistry().collect()
        for s in m.samples
        if s.name == "polycopy_executor_expected_price_unavailable_total"
    ]
    # Métrica incrementada com reason=empty_book — verificar via .labels.get_value()
    assert metrics.executor_expected_price_unavailable_total.labels(
        reason="empty_book"
    )._value.get() == 1.0


async def test_execute_returns_none_when_insufficient_volume() -> None:
    book = _book(asks=[_level("0.5", "10")], bids=[])  # apenas 5 USDC
    metrics = make_metrics(registry=CollectorRegistry())
    executor = DryRunExecutor(clob=_StubCLOB(book), metrics=metrics)
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("100"))

    assert result.expected_avg_price is None
    assert metrics.executor_expected_price_unavailable_total.labels(
        reason="insufficient_volume"
    )._value.get() == 1.0


async def test_execute_returns_none_when_get_book_raises() -> None:
    class _RaisingCLOB:
        async def get_book(self, token_id):
            raise RuntimeError("network down")

    metrics = make_metrics(registry=CollectorRegistry())
    executor = DryRunExecutor(clob=_RaisingCLOB(), metrics=metrics)
    trade = _trade(side=Side.BUY)

    result = await executor.execute(trade, Decimal("10"))

    assert result.expected_avg_price is None
    assert result.success is True  # DRY-RUN ainda retorna success
    assert metrics.executor_expected_price_unavailable_total.labels(
        reason="fetch_failed"
    )._value.get() == 1.0
```

`_trade(side=...)` é helper local pra construir um Trade válido — o test file existente já deve ter algo similar; adapte.

- [ ] **Step 4.6: GREEN + verificações + STOP**

```bash
uv run pytest tests/unit/infrastructure/test_dry_run_executor.py -v 2>&1 | tail -20
uv run pytest tests/unit/infrastructure/test_metrics.py -v 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check src/polycopy/infrastructure/execution/dry_run_executor.py src/polycopy/infrastructure/observability/metrics.py tests/unit/infrastructure/test_dry_run_executor.py tests/unit/infrastructure/test_metrics.py
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -5
```

Esperado: testes existentes adaptados passam, 4 novos passam, métrica nova testada.

Commit:
```bash
git add src/polycopy/infrastructure/execution/dry_run_executor.py src/polycopy/infrastructure/observability/metrics.py tests/unit/infrastructure/test_dry_run_executor.py tests/unit/infrastructure/test_metrics.py
git commit -m "feat(executor): DryRunExecutor computes expected_avg_price from CLOB orderbook"
```

---

## Task 5: `Web3CLOBExecutor` recebe CLOB + atualiza testes

**Files:**
- Modify: `src/polycopy/infrastructure/execution/web3_clob_executor.py`
- Modify: `tests/unit/infrastructure/test_web3_clob_executor.py`

**Reviewer:** opcional. Mas: pode aplicar mesmo helper ou duplicar lógica. Pelo spec §3.5, **duplicação local é aceitável** dado que real-mode ainda não roda.

---

- [ ] **Step 5.1: Atualizar `Web3CLOBExecutor.__init__`**

LEIA `src/polycopy/infrastructure/execution/web3_clob_executor.py`. Adicionar `clob: PolymarketCLOBPort` no construtor (junto com os demais params já existentes — `polygon_rpc_url`, `wallet_private_key`, etc.).

Antes de submeter ordem real (em `execute()`, antes do `client.create_and_post_order(...)`), calcular `expected`:

```python
expected = await self._compute_expected_price(trade, final_size_usdc)
```

Usar o mesmo helper `_compute_expected_price` que `DryRunExecutor` (duplicar localmente — sem extrair pra mixin agora).

`ExecutionResult` retornado em qualquer caminho (executed, failed, exception) propaga `expected_avg_price=expected`.

- [ ] **Step 5.2: Atualizar testes existentes**

LEIA `tests/unit/infrastructure/test_web3_clob_executor.py`. Os testes que constroem `Web3CLOBExecutor(...)` precisam passar CLOB stub também. Reusar `_StubCLOB` da T4 (mover pra fixture compartilhada se quiser, ou duplicar).

Adicionar 1-2 novos cenários: book ok → `expected_avg_price` preenchido em ExecutionResult mesmo quando real-mode succeeded. Não adicionar testes de book vazio aqui (cobertos por T4 via `_compute_expected_price` shared logic, e Web3 aqui é mais sobre integration com `py-clob-client`).

- [ ] **Step 5.3: Verificações + STOP**

```bash
uv run pytest tests/unit/infrastructure/test_web3_clob_executor.py -v 2>&1 | tail -15
uv run mypy src/polycopy
uv run ruff check src/polycopy/infrastructure/execution/web3_clob_executor.py tests/unit/infrastructure/test_web3_clob_executor.py
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -5
```

Commit:
```bash
git add src/polycopy/infrastructure/execution/web3_clob_executor.py tests/unit/infrastructure/test_web3_clob_executor.py
git commit -m "feat(executor): Web3CLOBExecutor computes expected_avg_price (parity with DryRun)"
```

---

## Task 6: `ExecutorAgent` — wiring CLOB no main() + propaga campo

**Files:**
- Modify: `src/polycopy/agents/executor.py`

**Reviewer:** opcional.

---

- [ ] **Step 6.1: Atualizar `_handle_message` pra propagar campo**

LEIA `src/polycopy/agents/executor.py` (procure onde `OrderExecution(...)` é montado). Adicionar `expected_avg_price=result.expected_avg_price`:

```python
            execution = OrderExecution(
                trade_event_id=event.event_id,
                wallet=event.trade.wallet.value,
                condition_id=event.trade.condition_id.value,
                token_id=event.trade.token_id.value,
                final_size_usdc=event.final_size_usdc.amount,
                mode=result.mode,
                result=...,  # já existente, depende de result.success
                tx_hash=result.tx_hash,
                gas_wei=result.gas_wei,
                failure_reason=result.failure_reason,
                error_message=result.error_message,
                decided_at=datetime.now(tz=UTC),
                expected_avg_price=result.expected_avg_price,  # NOVO
            )
```

(Os outros campos exatos dependem do código atual — leia e ajuste sem quebrar.)

- [ ] **Step 6.2: Wiring CLOB no `main()`**

Em `agents/executor.py:main()`, encontrar onde `DryRunExecutor()` ou `Web3CLOBExecutor(...)` é instanciado. Antes disso, instanciar CLOB client:

```python
from polycopy.infrastructure.polymarket.clob_client import PolymarketCLOBClient

# ... dentro de main(), após settings + metrics ...

clob = PolymarketCLOBClient(
    base_url=settings.clob_api_base_url,
    metrics=metrics,
)

if settings.executor_dry_run:
    executor: OrderExecutor = DryRunExecutor(clob=clob, metrics=metrics)
else:
    # ... gates de real-mode existentes ...
    executor = Web3CLOBExecutor(
        # ... params existentes ...
        clob=clob,
    )
```

Verifique a assinatura real do `PolymarketCLOBClient.__init__` (provavelmente `(base_url, metrics, timeout_s, max_retries)`).

- [ ] **Step 6.3: Verificações + smoke restart**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/agents/executor.py
uv run ruff format --check src/polycopy/agents/executor.py
uv run pytest tests/ 2>&1 | tail -5
docker compose restart executor
sleep 8
docker compose ps executor
docker compose logs --tail=20 executor
```

Esperado:
- `Up (healthy)` (ou `starting → healthy`).
- Logs mostram `agent_started` sem traceback.
- Se nenhum trade chegar, o executor fica idle — ok.

- [ ] **Step 6.4: STOP — commit**

Commit:
```bash
git add src/polycopy/agents/executor.py
git commit -m "feat(executor): wire CLOB client into ExecutorAgent main() and persist expected_avg_price"
```

---

## Task 7: Integration E2E — confirma coluna populada via NATS flow

**Files:**
- Modify: `tests/integration/test_executor_e2e.py` (ou criar se não existir)

**Reviewer:** opcional.

---

- [ ] **Step 7.1: Identificar test file**

```bash
ls tests/integration/test_executor*
```
Existem prováveis:
- `tests/integration/test_executor_e2e.py` — fluxo NATS → executor → DB.

LEIA o arquivo pra ver pattern (NATS publish, durable consumer, espera assert no DB).

- [ ] **Step 7.2: Adicionar 1 teste E2E**

```python
async def test_e2e_executor_persists_expected_avg_price(
    db_session_factory: async_sessionmaker[AsyncSession],
    nats_test_bus,  # adapte ao nome real da fixture
) -> None:
    """E2E: order.sized → executor calcula expected_avg_price → row em order_executions."""
    # Stub CLOB com book conhecido
    book = OrderBook(
        token_id=TokenId(value="111"),
        asks=[
            OrderBookLevel(price=Price(value=Decimal("0.6")), size=Money(amount=Decimal("100"))),
        ],
        bids=[],
        captured_at=datetime.now(tz=UTC),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    clob = _StubCLOB(book)
    executor_strategy = DryRunExecutor(clob=clob, metrics=metrics)

    # ... montar ExecutorAgent com nats_test_bus + repo_factory + executor_strategy ...
    # ... iniciar agent.start(), publicar OrderSized, esperar processamento ...

    # Verificar row no DB
    async with db_session_factory() as session:
        result = await session.execute(
            select(OrderExecutionRow).where(OrderExecutionRow.trade_event_id == event_id)
        )
        row = result.scalar_one()

    assert row.expected_avg_price == Decimal("0.6")
```

A construção do agent + NATS test bus depende muito do pattern existente — copie de outro teste E2E na mesma file.

- [ ] **Step 7.3: Executar teste isolado**

```bash
docker compose stop executor  # evita interferência
uv run pytest tests/integration/test_executor_e2e.py::test_e2e_executor_persists_expected_avg_price -v 2>&1 | tail -10
docker compose start executor
```

Esperado: PASS.

- [ ] **Step 7.4: Suite completa + verificações**

```bash
uv run pytest tests/ 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check tests/integration/test_executor_e2e.py
uv run ruff format --check tests/integration/test_executor_e2e.py
```

Esperado: 1 teste novo passa. Baseline preservado (~417+ passed após T1).

- [ ] **Step 7.5: STOP — commit**

```bash
git add tests/integration/test_executor_e2e.py
git commit -m "test(executor): add E2E asserting expected_avg_price persisted via NATS flow"
```

---

## Self-Review (autor do plano)

**Spec coverage:**

| Spec § | Coberto em |
|---|---|
| §3.1 `calculate_expected_avg_price` (função pura) | T1 |
| §3.2 `ExecutionResult.expected_avg_price` field | T2 |
| §3.2 `OrderExecution` field | T2 |
| §3.3 OrderExecutor Protocol sem mudança | implícito (signature inalterada) |
| §3.4 `DryRunExecutor` com CLOB + métrica + log | T4 |
| §3.5 `Web3CLOBExecutor` com CLOB | T5 |
| §3.6 Migration 0008 + ORM column | T3 |
| §3.7 ExecutorAgent — persistência | T6 |
| §3.8 Métrica `polycopy_executor_expected_price_unavailable` | T4 |
| §3.9 Wiring no main() | T6 |
| §4 Schema final | T3 |
| §5 Edge cases (10 cenários) | T1 + T4 |
| §6 Tratamento de falhas | T4 (CLOB raise → fetch_failed) |
| §7.1 Unit slippage | T1 |
| §7.2 Unit DryRunExecutor | T4 |
| §7.3 Unit Web3CLOBExecutor | T5 |
| §7.4 Unit metrics | T4 (Step 4.2) |
| §7.5 Integration E2E | T7 |
| §7.6 Migration round-trip | implícito (db_engine fixture) |

Coverage completa.

**Placeholder scan:** sem TBD/TODO/"add appropriate handling".

**Type consistency:**
- `Side` (não `TradeSide`) — usado em T1 (def), T4 (testes/agent), corrigido em relação ao spec original que usava `TradeSide`.
- `OrderBook(asks=, bids=, token_id=, captured_at=)` — pydantic model, consistente em T1, T4, T7.
- `OrderBookLevel(price: Price, size: Money)` — `price.value: Decimal`, `size.amount: Decimal`. Consistente.
- `expected_avg_price: Decimal | None` em T2 (def), T3 (column type), T4 (return), T6 (propagation), T7 (assertion).
- `calculate_expected_avg_price(book=, side=, target_usdc=)` — keyword-only args.
- `polycopy_executor_expected_price_unavailable` (counter name, sem `_total` no Prometheus name; field `executor_expected_price_unavailable_total`) — alinhado com pattern do projeto.

**Bite-sized check:** cada step é 2-5 minutos. Maior task é T4 (~6 steps, mas cada um isolado). Implementer copia snippets, roda testes.

**Reviewer:** nenhum obrigatório (spec § decisão: "sem money flow novo — só measurement"). Cadência: checkpoint humano por task.

**Notas operacionais herdadas:**
- Test-db-isolation completo — `pytest tests/` é seguro pra rodar.
- Container `polycopy-executor` rodando em produção — pra testes E2E, parar antes (`docker compose stop executor`) pra evitar interferência (mesmo padrão das fases anteriores).
