# Spec — Fase 5B: Slippage Snapshot

**Data:** 2026-05-04
**Status:** Aprovado
**Predecessor:** Plano 5A completo (head `563c2f6`) + test-db-isolation completo (head `77bea2f`).

## 1. Objetivo

Capturar o **preço médio esperado** que cada trade pagaria/receberia se executasse `final_size_usdc` no orderbook do momento da decisão. Esse valor — `expected_avg_price` — é input necessário pro Plano 5C (PnL view) calcular ganho hipotético de cada trade DRY-RUN cruzando com a resolução do mercado (já capturada em 5A).

Em real-mode (futuro), o mesmo campo serve pra calcular slippage real (`actual_avg_price - expected_avg_price`) — daí o nome "Slippage Snapshot".

## 2. Decisões fixadas

1. **Cálculo no executor (DryRunExecutor / Web3CLOBExecutor)** — não no SizingAgent. Slippage é responsabilidade conceitual do executor (quem vê orderbook).
2. **Função pura `calculate_expected_avg_price`** em `src/polycopy/domain/slippage.py` — testável isoladamente sem CLOB.
3. **Storage:** coluna nova `order_executions.expected_avg_price` (`NUMERIC(20, 8)`, nullable). Migration 0008.
4. **Liquidez insuficiente → NULL** — métrica `polycopy_executor_expected_price_unavailable_total{reason}` com reason em `{empty_book, insufficient_volume, fetch_failed}`. Backtest filtra `WHERE expected_avg_price IS NOT NULL`.
5. **Sem `actual_avg_price` agora** — YAGNI. Adicionar quando real-mode estiver ativo.
6. **Sem cache de orderbook** — extra HTTP call per trade é OK no volume atual. Otimizar se virar gargalo.

## 3. Componentes

### 3.1 `src/polycopy/domain/slippage.py` — função pura

```python
from decimal import Decimal
from polycopy.domain.orderbook import OrderBook
from polycopy.domain.value_objects import TradeSide


def calculate_expected_avg_price(
    *, book: OrderBook, side: TradeSide, target_usdc: Decimal
) -> Decimal | None:
    """Weighted avg price pra preencher target_usdc.

    BUY: percorre asks ascendente, acumula custo até atingir target_usdc.
    SELL: percorre bids descendente, acumula receita até atingir target_usdc.

    Retorna None se liquidez total < target_usdc (book vazio ou insuficiente).
    """
```

Algoritmo:
- BUY: itera `book.asks` em ordem crescente de preço.
- SELL: itera `book.bids` em ordem decrescente de preço.
- Para cada nível `(price, size)`: `slice_qty = min(size, remaining_qty_at_price)`. `slice_usdc = price * slice_qty`. Acumula `total_usdc += slice_usdc`, `total_qty += slice_qty`.
- Quando `total_usdc >= target_usdc`: calcula fração final do nível (parcial), recalcula `total_qty` ajustada.
- Weighted avg = `target_usdc / total_qty`.
- Esgotou book sem atingir target → `None`.

### 3.2 `src/polycopy/domain/execution.py` — `ExecutionResult` ganha campo

```python
@dataclass(frozen=True)
class ExecutionResult:
    mode: ExecutionMode
    success: bool
    tx_hash: str | None
    gas_wei: int | None
    failure_reason: FailureReason | None
    error_message: str | None
    expected_avg_price: Decimal | None  # NOVO
```

`OrderExecution` (entity persistida) também ganha o mesmo campo.

### 3.3 `OrderExecutor` Protocol — sem mudança de assinatura

`async def execute(trade, final_size_usdc) -> ExecutionResult` permanece. Mudança é apenas no shape do retorno.

### 3.4 `DryRunExecutor` — recebe CLOB + métricas

```python
class DryRunExecutor:
    def __init__(self, *, clob: PolymarketCLOBPort, metrics: Metrics) -> None:
        self._clob = clob
        self._metrics = metrics
        self._log = structlog.get_logger("dry_run_executor")

    async def execute(self, trade: Trade, final_size_usdc: Decimal) -> ExecutionResult:
        expected = await self._compute_expected_price(trade, final_size_usdc)
        return ExecutionResult(
            mode=ExecutionMode.DRY_RUN,
            success=True,
            tx_hash=None, gas_wei=None,
            failure_reason=None, error_message=None,
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
                error=str(exc), error_type=type(exc).__name__
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

### 3.5 `Web3CLOBExecutor` — mesma lógica

Recebe CLOB no construtor. Calcula `expected_avg_price` ANTES de submeter ordem real. Mesmo helper `_compute_expected_price` (refator opcional pra mixin/helper compartilhado, mas YAGNI — duplicação local é aceitável dado que real-mode ainda não roda).

Em real-mode futuro: `actual_avg_price` viria do fill (campo separate, fora do escopo 5B).

### 3.6 Migration 0008 + ORM

```sql
ALTER TABLE order_executions
ADD COLUMN expected_avg_price NUMERIC(20, 8) NULL;
```

Sem CHECK constraint (NULL é válido pra rows pré-fix + casos de book insuficiente).

`OrderExecutionRow`:
```python
expected_avg_price: Mapped[Decimal | None] = mapped_column(
    Numeric(20, 8), nullable=True
)
```

### 3.7 ExecutorAgent — persistência

Em `_handle_message`, ao montar `OrderExecution` pra persistir, propaga `result.expected_avg_price`. Edit trivial.

`SqlAlchemyOrderExecutionRepository.insert` — adicionar `expected_avg_price` ao `pg_insert(...).values(...)`.

### 3.8 Métrica nova

```python
executor_expected_price_unavailable_total: Counter
```

Em `make_metrics()`:
```python
executor_expected_price_unavailable_total=Counter(
    "polycopy_executor_expected_price_unavailable",
    "Trades onde expected_avg_price não pôde ser calculado.",
    labelnames=["reason"],
    registry=target,
),
```

Reasons: `empty_book`, `insufficient_volume`, `fetch_failed`.

### 3.9 Wiring em `main()` do executor

Atualizar `agents/executor.py:main()`:
- Adicionar instanciação de `PolymarketCLOBClient` (já existe da Fase 2A).
- Passar `clob=clob_client` no construtor de `DryRunExecutor` e `Web3CLOBExecutor`.

## 4. Schema final `order_executions`

```sql
CREATE TABLE order_executions (
    -- ... colunas existentes ...
    expected_avg_price NUMERIC(20, 8) NULL  -- Plano 5B
);
```

Range válido: `0 < expected_avg_price < 1` (Polymarket é binário, preços ∈ (0, 1)). Sem CHECK constraint — NULL precisa ser permitido. Validação opcional via app layer se quiser.

## 5. Edge cases

| Cenário | Comportamento |
|---|---|
| `book.asks` e `book.bids` ambos vazios | `None`, métrica `reason=empty_book` |
| BUY com asks vazios mas bids cheios | `None` (BUY usa asks), métrica `reason=empty_book` |
| Liquidez total nos asks < `final_size_usdc` | `None`, métrica `reason=insufficient_volume` |
| `clob.get_book` lança `httpx.TimeoutException` | `None`, métrica `reason=fetch_failed`, log warning |
| `clob.get_book` lança 5xx | mesma classe, mesmo handling |
| `final_size_usdc` <= 0 | invariante upstream em Sizing — não chega no executor |
| Preço de algum nível = 0 ou 1 (extremos terminais) | função aceita; se calc resultar 0 ou 1 exato, retorna esse valor |
| Decimal precision | `Numeric(20, 8)` cobre Polymarket com folga |
| Trade já executado antes do fix (rows antigas) | `expected_avg_price = NULL`. 5C filtra. |

## 6. Tratamento de falhas

| Falha | Ação |
|---|---|
| CLOB timeout/5xx | `expected_avg_price = None`, trade prossegue (DRY-RUN ou real-mode) |
| `calculate_expected_avg_price` raise (bug) | Captura no `_compute_expected_price`, retorna None com `reason=fetch_failed` (categoria genérica) |
| Migration 0008 falha em ambiente sem prerequisite | alembic mostra erro claro; ops investiga |
| Coluna nova lê em código antigo (compatibilidade) | nullable, `None` retornado; não quebra nada |

## 7. Testes

### 7.1 Unit — `tests/unit/domain/test_slippage.py` (~10 testes)

1. `test_buy_single_level_exact_fill` — 1 ask cobre exatamente target.
2. `test_buy_multi_level_fill` — 3 asks combinados.
3. `test_sell_multi_level_fill` — bids descendente.
4. `test_buy_partial_last_level` — último ask preenche fração.
5. `test_returns_none_when_book_empty` — asks vazios.
6. `test_returns_none_when_insufficient_volume` — total < target.
7. `test_buy_single_ask_partial` — 1 ask, parcial.
8. `test_returns_none_for_sell_with_empty_bids`.
9. `test_decimal_precision_8_places` — input gera result com 8 decimais.
10. `test_zero_price_level_handled` — preço extremo (raro mas possível).

### 7.2 Unit — `tests/unit/infrastructure/test_dry_run_executor.py` (atualizar)

- Atualizar testes existentes pra construir `DryRunExecutor(clob=stub, metrics=...)`.
- 3 novos cenários: book ok → expected_avg_price preenchido; book vazio → None + métrica; get_book raise → None + métrica.

### 7.3 Unit — `tests/unit/infrastructure/test_web3_clob_executor.py` (atualizar)

- Construir com CLOB stub. Mesma cobertura mas para real-mode (com mocks de `py-clob-client`).

### 7.4 Unit — `tests/unit/infrastructure/test_metrics.py` (+1 teste)

- `test_metrics_executor_expected_price_unavailable_counter` — registra labels e samples.

### 7.5 Integration — `tests/integration/test_executor_e2e.py` (atualizar/adicionar 1 teste)

- Cenário: trade flui via NATS → executor → row em `order_executions` com `expected_avg_price` preenchido. Mock CLOB com book conhecido, assertar valor exato.

### 7.6 Migration round-trip

Coberto por `tests/conftest.py` (`db_engine` faz `upgrade(head)` + `downgrade(base)` por session).

## 8. Settings + Métricas + Logs

### 8.1 Settings — sem novos campos

ExecutorAgent já tem `clob_api_base_url`. Reusar.

### 8.2 Métrica nova (1)

- `polycopy_executor_expected_price_unavailable_total{reason}` — counter.

### 8.3 Logs estruturados

- `expected_price_fetch_failed` (warning, com `error`, `error_type`, `token_id`, `final_size_usdc`).
- (Opcional) Log info `expected_price_calculated` por trade no nível DEBUG — defer pra hardening se debugging precisar.

## 9. Observabilidade

`expected_avg_price` é o input crítico do backtest (Plano 5C). Dashboard sugerido:
- Distribuição de `expected_avg_price` por outcome side (asks vs bids).
- Taxa de NULL — se passar de 5%, investigar por liquidez/RPC.
- Métrica `polycopy_executor_expected_price_unavailable{reason}` por reason.

Out of scope: dashboard real, fica pro 5C.

## 10. Open questions / non-goals

- **Caching de orderbook:** YAGNI agora. Se gargalo, adicionar TTL curto (5s).
- **`actual_avg_price` para slippage real:** YAGNI. Adicionar quando real-mode estiver ativo.
- **Spread metric:** seria útil capturar `bid_ask_spread` no momento da decisão. Future scope se 5C precisar.
- **Multi-token markets:** Polymarket só tem binários (YES/NO). Sem mudança.

## 11. Roadmap de implementação

7 tasks bite-sized (~2-3 dias subagent-driven):

- **T1:** Domain — `calculate_expected_avg_price` + 10 unit tests.
- **T2:** `ExecutionResult.expected_avg_price` field + atualizar `OrderExecution` entity.
- **T3:** Migration 0008 + `OrderExecutionRow` ORM update.
- **T4:** `DryRunExecutor` recebe CLOB + métrica nova + 4 unit tests.
- **T5:** `Web3CLOBExecutor` recebe CLOB + atualizar testes existentes.
- **T6:** `ExecutorAgent.main()` wiring + persistência via repo.
- **T7:** Integration E2E — confirmar coluna populada via NATS flow.

Sem reviewer obrigatório (sem money flow novo — só measurement).

## 12. Sucesso

- Migration 0008 aplica; downgrade reverte sem perda.
- Após deploy: novos rows em `order_executions` têm `expected_avg_price` preenchido (exceto casos com book insuficiente, que ficam NULL).
- Métrica `polycopy_executor_expected_price_unavailable{reason}` aparece em `/metrics` do executor.
- Suite continua passando (baseline preservado).
- Plano 5C pode `SELECT trade_event_id, condition_id, final_size_usdc, expected_avg_price FROM order_executions WHERE expected_avg_price IS NOT NULL` pra começar PnL view.
