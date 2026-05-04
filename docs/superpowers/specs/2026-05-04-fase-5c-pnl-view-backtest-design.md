# Spec — Fase 5C: PnL View + Backtest Tooling

**Data:** 2026-05-04
**Status:** Aprovado
**Predecessor:** Fase 5A (head `563c2f6`) + test-db-isolation (head `77bea2f`) + Fase 5B (head `6d9215a`).

## 1. Objetivo

Cruzar `order_executions` (decisões DRY-RUN com `expected_avg_price` do 5B) com `market_resolutions` (5A) pra computar **PnL hipotético** — quanto teria sido ganho/perdido se real-mode estivesse ativo. Última peça da Fase 5 (backtest infrastructure).

3 saídas:
1. **SQL view `hypothetical_pnl`** — fonte de verdade derivada, JOIN entre as 2 tabelas + colunas computadas (qty, payout, pnl, status).
2. **CLI script `backtest.py`** — query ad-hoc por período, formata tabela rica + summary.
3. **Métricas Prometheus no `ResolverAgent`** — 5 gauges atualizadas a cada loop (1h).

## 2. Decisões fixadas

1. **Híbrido: SQL view + CLI + Prometheus** (não materializada — query on-demand).
2. **Coluna `side` adicionada em `order_executions`** (migration 0009 com DEFAULT 'BUY' retroativo, depois drop default).
3. **PnL semantics (BUY apenas):**
   - Win (winning_token_id == token_id): `pnl = qty - size` onde `qty = size / expected_avg_price`.
   - Lose: `pnl = -size`.
   - INVALID (50/50): `pnl = qty * 0.5 - size`.
   - Pending (sem resolution): `pnl = NULL`, status `pending`.
   - SELL: `pnl = NULL`, status `sell_excluded` (v1 limitação).
   - `expected_avg_price IS NULL` ou `= 0`: `pnl = NULL`, status `no_expected_price`.
4. **Métricas no `ResolverAgent`** (não em agente novo, não em executor).
   - Recomputa via SELECT na view a cada loop (1h).
   - Sem labels (cardinalidade controlada).
5. **CLI usa view, não duplica lógica** — single source of truth.

## 3. Componentes

### 3.1 Migration 0009 — `side` column + view

**Files:** `alembic/versions/0009_add_side_and_hypothetical_pnl_view.py`

```python
def upgrade() -> None:
    # Add side column with default for existing rows
    op.add_column(
        "order_executions",
        sa.Column("side", sa.String(), nullable=False, server_default="BUY"),
    )
    op.create_check_constraint(
        "order_executions_side_enum",
        "order_executions",
        "side IN ('BUY', 'SELL')",
    )
    # Drop default — future inserts must provide side explicitly
    op.alter_column("order_executions", "side", server_default=None)

    # Create view
    op.execute("""
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
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS hypothetical_pnl;")
    op.drop_constraint("order_executions_side_enum", "order_executions")
    op.drop_column("order_executions", "side")
```

### 3.2 Domain & infrastructure changes

**Files:**
- `src/polycopy/domain/execution.py` — `OrderExecution.side: Literal["BUY", "SELL"]`. Sem default (rows novas precisam fornecer).
- `src/polycopy/infrastructure/persistence/models.py` — `OrderExecutionRow.side`.
- `src/polycopy/infrastructure/persistence/order_execution_repository.py` — `pg_insert(...).values(side=execution.side)`.
- `src/polycopy/agents/executor.py:_handle_message` — `side=event.trade.side.value` no construtor de `OrderExecution`.

### 3.3 CLI script `src/polycopy/scripts/backtest.py`

```python
"""Backtest CLI — consulta hypothetical_pnl view e formata summary."""

# Args:
#   --since <duration>     ex: 7d, 24h, 1w (default: 7d)
#   --by <wallet|condition|none>  group-by (default: none)
#   --format <table|json|csv>     output format (default: table)
#   --status <all|resolved|pending|excluded>  filter (default: all)
```

Saída esperada (default):
```
=== Backtest Summary ===
Period:        últimos 7 dias (since 2026-04-27 00:00 UTC)
Trades total:  42
  - Resolved:  35 (win 18, lose 14, invalid 3)
  - Pending:    5
  - Excluded:   2 (sell 1, no_expected_price 1)

PnL hipotético:  -$3.45 USDC
Winrate:         51.4% (18 / 35 resolved excluindo invalid)

=== Top trades ===
| trade_event_id | wallet     | side | size  | expected | outcome | pnl    |
|----------------|------------|------|-------|----------|---------|--------|
| ...            | 0xabc...   | BUY  | 10.00 | 0.6500   | win     | +5.38  |
| ...            | 0xdef...   | BUY  |  5.00 | 0.4200   | lose    | -5.00  |
```

Implementação: `asyncpg` ou SQLAlchemy. Formatação com `rich.table` (já presente? confirmar) ou tabulate.

### 3.4 ResolverAgent — 5 gauges

**Files:**
- `src/polycopy/infrastructure/observability/metrics.py` — adicionar 5 gauges.
- `src/polycopy/agents/resolver.py` — método `_compute_pnl_metrics(session)` chamado ao final de cada `run_once` bem-sucedido.

```python
class Metrics:
    # ... existentes ...
    hypothetical_pnl_total_usdc: Gauge
    hypothetical_pnl_24h_usdc: Gauge
    hypothetical_winrate: Gauge
    hypothetical_trades_resolved: Gauge
    hypothetical_trades_pending: Gauge
```

`make_metrics()`:
```python
hypothetical_pnl_total_usdc=Gauge(
    "polycopy_hypothetical_pnl_total_usdc",
    "PnL hipotético acumulado em USDC (todos os trades resolvidos).",
    registry=target,
),
# ... análogo para os outros 4
```

ResolverAgent helper:
```python
async def _compute_pnl_metrics(self) -> None:
    """Recomputa e seta gauges Prometheus a cada ciclo. Best-effort."""
    try:
        async with self._engine.begin() as conn:
            row = (await conn.execute(text("""
                SELECT
                    COUNT(*) FILTER (WHERE status IN ('win','lose','invalid')) as resolved,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COALESCE(SUM(pnl_usdc), 0) as total_pnl,
                    COALESCE(SUM(pnl_usdc) FILTER (
                        WHERE decided_at > now() - interval '24 hours'
                    ), 0) as pnl_24h,
                    COUNT(*) FILTER (WHERE status = 'win')::float
                        / NULLIF(COUNT(*) FILTER (WHERE status IN ('win','lose')), 0) as winrate
                FROM hypothetical_pnl
            """))).one()
        self._metrics.hypothetical_pnl_total_usdc.set(float(row.total_pnl))
        self._metrics.hypothetical_pnl_24h_usdc.set(float(row.pnl_24h))
        self._metrics.hypothetical_winrate.set(float(row.winrate or 0))
        self._metrics.hypothetical_trades_resolved.set(int(row.resolved))
        self._metrics.hypothetical_trades_pending.set(int(row.pending))
    except Exception as exc:
        self._log.warning("pnl_metrics_compute_failed", error=str(exc))
        # Métricas ficam stale (não zeradas) — próximo ciclo retenta
```

**Decisão de injeção:** ResolverAgent atualmente recebe `repo_factory` (context manager retornando `MarketResolutionRepository`). Pra `_compute_pnl_metrics` precisamos de raw SQL access, não do repo. **Solução:** estender `MarketResolutionRepository` com método `get_pnl_summary() -> PnlSummary` (dataclass com 5 campos) que executa a query e retorna valores tipados. Mantém o agent agnóstico de SQL e respeita o pattern hexagonal existente. Implementação concreta `SqlAlchemyMarketResolutionRepository.get_pnl_summary` faz o `SELECT ... FROM hypothetical_pnl`.

## 4. Edge cases

| Cenário | Comportamento |
|---|---|
| Row com `expected_avg_price IS NULL` | `pnl_usdc = NULL`, status `no_expected_price` |
| Row com `expected_avg_price = 0` (defensivo) | mesma classe, evita divisão zero |
| Row sem resolution (mercado aberto) | `pnl_usdc = NULL`, status `pending`, conta em `pending` count |
| INVALID resolution | `pnl = qty * 0.5 - size` |
| SELL trade | `pnl_usdc = NULL`, status `sell_excluded` |
| Sem trades no período do CLI | summary mostra zeros + mensagem "no trades in period" |
| ResolverAgent query falha | log warning, métricas stale, próximo ciclo retenta |
| Migration aplicada com 1 row pré-existente | DEFAULT 'BUY' aplicado retroativamente; aceito |
| View precisa ser recriada após mudança de colunas | downgrade dropa view antes de coluna; upgrade recria |

## 5. Testes

### 5.1 Integration — `tests/integration/test_hypothetical_pnl_view.py` (~10 cenários)

1. BUY win — payout 1.0, pnl positivo.
2. BUY lose — payout 0, pnl = -size.
3. BUY invalid — payout 0.5, pnl = qty*0.5 - size.
4. BUY pending (sem resolution) — pnl NULL.
5. BUY com expected_avg_price NULL — pnl NULL, status no_expected_price.
6. SELL — pnl NULL, status sell_excluded.
7. expected_avg_price = 0 — pnl NULL.
8. Múltiplos trades mesmo condition — todos retornam.
9. JOIN sem match (condition_id sem resolution) — pending.
10. Status enum — todos 5 valores aparecem corretamente.

### 5.2 Unit — `tests/unit/scripts/test_backtest.py`

Testes de formatação CLI com SQL output mockado. 4-5 cenários: empty period, all wins, mixed, only pending, only excluded.

### 5.3 Integration — `tests/integration/test_resolver_pnl_metrics.py` (~3 cenários)

1. Após resolver loop, gauges populados com valores corretos.
2. Métricas em estado vazio (zero trades) → 0/0/0/0/0.
3. Query falha → métricas ficam no último valor conhecido (não zeradas).

### 5.4 Migration round-trip

Coberto por `db_engine` fixture (já valida upgrade/downgrade por session).

## 6. Settings + Métricas + Logs

### 6.1 Settings — sem novos campos

ResolverAgent já tem `resolver_sync_interval_seconds`. Reusar.

### 6.2 Métricas (5 gauges)

- `polycopy_hypothetical_pnl_total_usdc`
- `polycopy_hypothetical_pnl_24h_usdc`
- `polycopy_hypothetical_winrate`
- `polycopy_hypothetical_trades_resolved`
- `polycopy_hypothetical_trades_pending`

### 6.3 Logs estruturados

- `pnl_metrics_compute_failed` (warning, com `error`, `error_type`).
- (Opcional) `pnl_metrics_computed` (debug, com totais).

## 7. Observabilidade — dashboard sugerido

Out of scope (Plano 5C entrega métricas; dashboard Grafana fica pra hardening). Painel sugerido:
- Single stat: PnL Total USDC + 24h + Winrate.
- Counter cumulativo: trades resolved + pending ao longo do tempo.

## 8. Open questions / non-goals

- **Materialized view**: YAGNI agora. Se dataset crescer >100k rows, refatorar.
- **PnL real-mode**: quando real-mode estiver ativo, a mesma view funciona — basta `WHERE mode = 'real'`. CLI ganharia flag `--mode real|dry_run|all`.
- **Métricas por wallet**: cardinalidade alta. CLI cobre group-by quando necessário.
- **Streaming PnL atualizado em tempo real**: out of scope. ResolverAgent tick de 1h é suficiente.
- **Sharpe/drawdown/avg_holding**: out of scope MVP. Adicionar como SQL views auxiliares se 5C revelar necessidade.

## 9. Roadmap de implementação

7 tasks bite-sized (~2 dias subagent-driven):

- **T1:** Migration 0009 — add `side` column + create view `hypothetical_pnl` (sem ORM/repo ainda).
- **T2:** Domain `OrderExecution.side` + ORM column + repository propagate + executor agent propagate.
- **T3:** Integration test `tests/integration/test_hypothetical_pnl_view.py` (10 cenários).
- **T4:** Estender `MarketResolutionRepository` Protocol com `get_pnl_summary() -> PnlSummary`; impl SQL na adapter; ResolverAgent — 5 gauges + `_compute_pnl_metrics` chamando o repo + integration test.
- **T5:** CLI script `src/polycopy/scripts/backtest.py` — args + query + formatação.
- **T6:** Unit tests do CLI — formatação com mocks.
- **T7:** Documentação — atualizar ARCHITECTURE.md (seção Backtest + comandos úteis).

Sem reviewer obrigatório (sem money flow novo).

## 10. Sucesso

- Migration 0009 aplica + downgrade reverte sem perda.
- View `hypothetical_pnl` retorna 1 row por trade com pnl + status corretos pros 10 cenários.
- ResolverAgent: 5 gauges aparecem em `/metrics` da porta 9107 com valores plausíveis após cada ciclo.
- CLI: `uv run python -m polycopy.scripts.backtest --since 7d` imprime summary + tabela rica em <2s pra dataset de teste.
- Suite continua no baseline (~427 + ~25 novos = ~452 passed).
