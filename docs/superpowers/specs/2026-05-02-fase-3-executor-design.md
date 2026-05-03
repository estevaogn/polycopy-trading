# Plano 3 — Executor agent (DRY-RUN MVP)

**Data:** 2026-05-02
**Status:** spec aprovada autonomamente pelo executor (usuário delegou Fase 3 completa nesta sessão)
**Predecessor:** Plano 2C (Sizing) — entregou `OrderSized` event, `polycopy-sizing:9105`, stream JetStream `SIZING_DECISIONS`
**Sucessor:** Fase 4 (real on-chain execution via wallet — fora do escopo Fase 3)

---

## 1. Contexto

Fase 2 entregou o pipeline de decisão completo (detect → notify → risk → size). A Fase 3 fecha o ciclo de copy trading: consome `order.sized` (publicado pelo Sizing 2C) e **simula** a execução on-chain de cada ordem.

**Decisão arquitetural crítica:** **MVP é DRY-RUN apenas.** Razão: execução real on-chain via Polymarket CLOB requer wallet com private key + saldo USDC, EIP-712 signing, gestão de gas em Polygon, e supervisão humana ativa pra cada decisão de risco real. Esse escopo merece sua própria fase com brainstorm dedicado e validação contra testnet (Polygon Mumbai/Amoy) antes de mainnet. Fase 3 entrega toda a estrutura (agente, persistência, eventos, container, métricas, testes) operando em modo dry-run; ativação real fica pra Fase 4 com flag `EXECUTOR_DRY_RUN=false` + integração `Web3CLOBExecutor`.

## 2. Motivação

Sem o Executor, ordens dimensionadas pelo Sizing ficam acumuladas no JetStream sem destino — pipeline E2E não fecha. Mesmo em dry-run, ter o agente provê:

- **Validação end-to-end do pipeline**: trade detectado pelo watcher chega até a "última milha" e é registrado como teria sido executado.
- **Audit trail de "trades que teríamos feito"**: tabela `order_executions` em modo dry-run vira fonte de verdade pra backtesting de PnL hipotético.
- **Métricas de viabilidade**: latência fim-a-fim, distribuição de gas estimado, taxa de "skip" causada por edge cases.
- **Estrutura pronta pra Fase 4**: substituir `DryRunExecutor` por `Web3CLOBExecutor` é mudança contida (mesmo Port `OrderExecutor`).

## 3. Escopo

### 3.1 Dentro de Fase 3 (MVP dry-run)

- 3 eventos novos: `OrderExecuted` (subject `order.executed`), `OrderFailed` (subject `order.failed`), `OrderDryRun` (subject `order.dry_run`).
- Enum `ExecutionMode` (`REAL`, `DRY_RUN`) e `FailureReason` (estrutura aberta — MVP: `INVALID_TRADE_PARAMS`, `EXECUTOR_DISABLED`).
- `OrderExecution` value object + `OrderExecutionRepository` Protocol.
- `OrderExecutor` Protocol (interface pra strategies de execução). Implementação MVP: `DryRunExecutor` (não chama blockchain).
- Tabela `order_executions` + migration alembic + ORM.
- Adapter `SqlAlchemyOrderExecutionRepository` (insert idempotente via PK).
- Extensão `MessagingPort` com 3 métodos publish + implementação no `NatsMessagingBus`.
- Stream JetStream novo `EXECUTION_RESULTS` com subjects literais `["order.executed", "order.failed", "order.dry_run"]`.
- Agente novo `ExecutorAgent` consumindo `order.sized` via durable consumer (`executor-1`).
- Algoritmo: chama `executor.execute(trade, final_size)`. Em dry-run, retorna `ExecutionResult(mode=DRY_RUN, success=True, simulated_gas_wei=None, tx_hash=None)`. Persiste + publica `order.dry_run`.
- Containerização (`polycopy-executor:9106`), scrape Prometheus, atualização `ARCHITECTURE.md`.
- 3 métricas Prometheus + 5 settings novos.
- Testes unit + integration E2E.

### 3.2 Fora de Fase 3 (entra em Fase 4)

- **Real-mode execution via Web3.py + EIP-712 signing**. Requer:
  - Wallet management (private key/mnemonic via secrets manager — Vault, AWS Secrets Manager, ou similar)
  - Implementação `Web3CLOBExecutor` chamando Polymarket smart contracts (Exchange.sol)
  - Gas estimation + gas price strategy (EIP-1559 fees)
  - Nonce management (concorrência se múltiplos executors)
  - Tratamento de erros de rede/RPC (retry, fallback RPC)
  - Validação de saldo USDC + approvals on-chain
- **Tracking de execução parcial** (partial fills): `order.partial_fill`, agregação de fills.
- **Cancelamento de ordens** (timeout, cancel signal).
- **Submissão a múltiplos pools** (otimização de slippage cross-pool).
- **Rate limiting global** entre múltiplos executors.
- **Replay de ordens DRY_RUN como REAL** (pra "promoção" de backtest pra produção).
- **Audit UI / dashboard de PnL hipotético**. Tabela `order_executions` é audit; dashboards entram em fase de observabilidade dedicada.

## 4. Componentes

```
src/polycopy/
├── domain/
│   ├── events.py                                    # + OrderExecuted, OrderFailed, OrderDryRun, ExecutionMode, FailureReason
│   └── execution.py                                 # NEW — OrderExecution value object + ExecutionResult
├── ports/
│   ├── messaging.py                                 # + 3 publish methods
│   ├── order_execution_repository.py                # NEW — OrderExecutionRepository Protocol
│   └── order_executor.py                            # NEW — OrderExecutor Protocol (strategy pra execução)
├── infrastructure/
│   ├── messaging/nats_bus.py                       # + stream EXECUTION_RESULTS + 3 publish methods
│   ├── persistence/
│   │   ├── models.py                               # + OrderExecutionRow ORM
│   │   └── order_execution_repository.py           # NEW — SqlAlchemyOrderExecutionRepository
│   └── execution/
│       ├── __init__.py                             # NEW
│       └── dry_run_executor.py                     # NEW — DryRunExecutor (no-op blockchain)
└── agents/
    └── executor.py                                 # NEW — ExecutorAgent

alembic/versions/
└── 0005_add_order_executions.py                    # NEW

tests/
├── unit/
│   ├── agents/test_executor.py                     # NEW
│   ├── domain/test_execution_events.py             # NEW
│   ├── infrastructure/
│   │   ├── test_dry_run_executor.py                # NEW
│   │   └── test_metrics.py                         # + 3 testes
│   └── test_ports_typecheck.py                     # + stubs
└── integration/
    ├── test_order_execution_repository.py          # NEW
    ├── test_jetstream_bus.py                       # + 6 testes (3 publishes × 2: received + dedup)
    └── test_executor_e2e.py                        # NEW
```

**Componentes-chave:**

- **`ExecutionMode`** — Enum (`REAL`, `DRY_RUN`). Identifica se o agent executou de verdade ou só simulou.
- **`FailureReason`** — Enum aberto (MVP: `INVALID_TRADE_PARAMS`, `EXECUTOR_DISABLED`). Pra Fase 4: `WALLET_NO_BALANCE`, `GAS_EXCEEDED`, `RPC_FAILURE`, `SLIPPAGE_EXCEEDED`, etc.
- **`OrderExecuted` / `OrderFailed` / `OrderDryRun`** — eventos pydantic frozen+strict. Cada um tem `event_id` UUID (mesmo do trade detectado original — idempotência cross-agent), `occurred_at` (timestamp do trade original), `decided_at` (timestamp em que Executor decidiu/simulou), `trade: Trade`, `final_size_usdc: Money`. `OrderExecuted` adiciona `tx_hash: str` + `gas_wei: int` (Fase 4 popula). `OrderFailed` adiciona `reason: FailureReason` + `error_message: str`. `OrderDryRun` é "snapshot do que teria sido feito" — sem dados de tx real.
- **`OrderExecution`** — value object (frozen dataclass) com `trade_event_id`, `wallet`, `condition_id`, `token_id`, `final_size_usdc: Decimal`, `mode: ExecutionMode`, `result: Literal["executed", "failed", "dry_run"]`, `tx_hash: str | None`, `gas_wei: int | None`, `failure_reason: FailureReason | None`, `error_message: str | None`, `decided_at: datetime`. `__post_init__` valida invariantes:
  - `mode == REAL ↔ result ∈ {executed, failed}` (real-mode não produz dry_run)
  - `mode == DRY_RUN ↔ result == "dry_run"`
  - `result == "executed" → tx_hash IS NOT NULL`
  - `result == "failed" → failure_reason IS NOT NULL AND error_message IS NOT NULL`
  - `result == "dry_run" → tx_hash IS NULL AND gas_wei IS NULL AND failure_reason IS NULL`
- **`ExecutionResult`** — dataclass intermediário retornado por `OrderExecutor.execute()`. Carrega `mode`, `success: bool`, `tx_hash`, `gas_wei`, `failure_reason`, `error_message`. Convertido em `OrderExecution` pelo agente antes de persistir.
- **`OrderExecutionRepository`** — Protocol com `async insert(execution) -> bool`.
- **`OrderExecutor`** — Protocol com `async execute(trade, final_size_usdc) -> ExecutionResult`. Strategy pattern — implementações: `DryRunExecutor` (MVP), `Web3CLOBExecutor` (Fase 4).
- **`DryRunExecutor`** — implementação MVP. Sempre retorna `ExecutionResult(mode=DRY_RUN, success=True, tx_hash=None, gas_wei=None, failure_reason=None, error_message=None)`. Sem chamadas externas.
- **Extensão `MessagingPort`** — `async publish_order_executed`, `async publish_order_failed`, `async publish_order_dry_run`. NatsMessagingBus implementa com `Nats-Msg-Id = str(event.event_id)`.
- **`ExecutorAgent`** — herda `AgentBase`. Durable consumer `executor-1` em `order.sized`. Callback `_handle_message`: parse `OrderSized` → chama `executor.execute(trade, final_size)` → constrói `OrderExecution` → persist (idempotente PK) → publish.

## 5. Schema da tabela `order_executions`

```sql
CREATE TABLE order_executions (
    trade_event_id     UUID            PRIMARY KEY,        -- = OrderSized.event_id
    wallet             TEXT            NOT NULL,
    condition_id       TEXT            NOT NULL,
    token_id           TEXT            NOT NULL,
    final_size_usdc    NUMERIC(20, 6)  NOT NULL,
    mode               TEXT            NOT NULL,            -- 'real' | 'dry_run'
    result             TEXT            NOT NULL,            -- 'executed' | 'failed' | 'dry_run'
    tx_hash            TEXT,                                  -- NULL pra dry_run e failed
    gas_wei            NUMERIC(40, 0),                        -- NULL pra dry_run e failed; uint256 cabe em NUMERIC(78,0) mas (40,0) cobre Polygon real
    failure_reason     TEXT,                                  -- NULL exceto failed
    error_message      TEXT,                                  -- NULL exceto failed
    decided_at         TIMESTAMPTZ     NOT NULL,
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT order_executions_mode_enum
        CHECK (mode IN ('real', 'dry_run')),
    CONSTRAINT order_executions_result_enum
        CHECK (result IN ('executed', 'failed', 'dry_run')),
    CONSTRAINT order_executions_mode_result_consistency
        CHECK (
            (mode = 'real' AND result IN ('executed', 'failed'))
            OR (mode = 'dry_run' AND result = 'dry_run')
        ),
    CONSTRAINT order_executions_executed_has_tx
        CHECK (
            (result = 'executed' AND tx_hash IS NOT NULL)
            OR result IN ('failed', 'dry_run')
        ),
    CONSTRAINT order_executions_failed_has_reason
        CHECK (
            (result = 'failed' AND failure_reason IS NOT NULL AND error_message IS NOT NULL)
            OR result IN ('executed', 'dry_run')
        ),
    CONSTRAINT order_executions_dry_run_no_tx
        CHECK (
            (result = 'dry_run' AND tx_hash IS NULL AND gas_wei IS NULL AND failure_reason IS NULL)
            OR result IN ('executed', 'failed')
        ),
    CONSTRAINT order_executions_size_positive
        CHECK (final_size_usdc > 0)
);

CREATE INDEX idx_order_executions_wallet_decided_at
    ON order_executions (wallet, decided_at DESC);

CREATE INDEX idx_order_executions_failed_decided_at
    ON order_executions (decided_at DESC)
    WHERE result = 'failed';

CREATE INDEX idx_order_executions_real_executed
    ON order_executions (decided_at DESC)
    WHERE mode = 'real' AND result = 'executed';
```

**Origem de cada campo:**

| Campo | Origem | Propósito |
|---|---|---|
| `trade_event_id` (PK) | `OrderSized.event_id` (= `WalletTradeDetected.event_id`) | Idempotência cross-agent |
| `wallet`, `condition_id`, `token_id` | `Trade` original | Audit |
| `final_size_usdc` | `OrderSized.final_size_usdc.amount` | Tamanho que iria/foi pro broker |
| `mode` | `ExecutionMode` (config-driven) | `'real'` ou `'dry_run'` |
| `result` | `ExecutionResult.success` + mode | `'executed'`/`'failed'`/`'dry_run'` |
| `tx_hash`, `gas_wei` | retornados por executor real | NULL em dry_run |
| `failure_reason`, `error_message` | retornados por executor em falha | NULL exceto se failed |
| `decided_at` | `datetime.now(tz=UTC)` no agent | Timestamp da decisão |
| `created_at` | server default | Timestamp de gravação no DB |

**Decisões do schema:**
- **PK = `trade_event_id`**: idempotência grátis. Padrão consistente com `risk_decisions` e `order_sizings`.
- **`gas_wei NUMERIC(40, 0)`**: gas em wei é uint256 on-chain (até ~10^77), mas valores reais em Polygon cabem confortavelmente em 40 dígitos.
- **5 CHECK constraints** garantindo invariantes mode↔result e result↔dados específicos.
- **3 indexes** (composite + 2 parciais): debug por wallet, dashboard de falhas, dashboard de execuções reais bem-sucedidas.
- **Sem FK** pra `order_sizings` (audit retroativo).

## 6. Fluxos

### 6.1 Decisão de execução (caminho principal — MVP dry-run)

```
JetStream durable consumer "executor-1" entrega `order.sized` payload
└─→ ExecutorAgent._handle_message(payload, num_delivered)
    ├─→ OrderSized.model_validate_json(payload)
    │   └─ ValidationError → métrica + ack silencioso
    ├─→ executor.execute(trade, final_size_usdc) -> ExecutionResult
    │   └─ DryRunExecutor (MVP): sempre retorna mode=DRY_RUN, success=True
    ├─→ OrderExecution(...) construída a partir de ExecutionResult
    ├─→ repo.insert(execution) -> bool
    │   ├─ True (nova) → segue
    │   └─ False (PK conflict, redelivery) → métrica duplicate_skip + ack silencioso
    ├─→ publish_order_dry_run / publish_order_executed / publish_order_failed
    │   (escolha por mode + result)
    └─→ métricas executor_orders_total, executor_decision_duration_seconds, executor_gas_wei (real-only)
```

### 6.2 Real-mode (Fase 4 — fora do escopo)

```
DryRunExecutor → Web3CLOBExecutor:
  - Carrega wallet (private key via secrets manager)
  - Estima gas (eth.estimate_gas)
  - Constrói EIP-712 typed data pra Polymarket Exchange
  - Sinaliza com wallet
  - Submete via web3.eth.send_raw_transaction
  - Aguarda confirmação (1 bloco em Polygon ~2s)
  - Retorna ExecutionResult com tx_hash + gas_wei reais
```

## 7. Tratamento de falhas

| Falha | Comportamento | Métrica/Log |
|---|---|---|
| Payload malformado | Ack silencioso | `executor_orders_total{result="failed", reason="invalid_payload", mode="*"}` |
| Executor levanta exceção | Captura → `OrderExecution(result="failed", failure_reason=..., error_message=str(exc))` → persiste + publish `order.failed` | `executor_orders_total{result="failed", reason=...}` |
| DB indisponível | Exceção propaga → JetStream redelivery (até 5x) | log error |
| Bus indisponível | Exceção propaga; 2º handler vê `is_new=False` → skip | log warning |
| Re-delivery (duplicate) | `is_new=False` → ack sem re-publish | `executor_orders_total{result="duplicate_skip", reason=...}` |

## 8. Observabilidade

### 8.1 Settings novas (Settings flat — débito conhecido)

```python
executor_metrics_port: int = Field(9106, alias="EXECUTOR_METRICS_PORT")
executor_max_deliver: int = Field(5, alias="EXECUTOR_MAX_DELIVER")
executor_durable_name: str = Field("executor-1", alias="EXECUTOR_DURABLE_NAME")
executor_dry_run: bool = Field(True, alias="EXECUTOR_DRY_RUN")
"""DRY-RUN by default — Fase 3 MVP. Set to false ONLY after Fase 4 real-mode is implemented + tested on testnet."""
```

(Apenas 4 settings — não há real-mode config nesta fase. Fase 4 adicionará `WALLET_PRIVATE_KEY_SECRET_REF`, `POLYGON_RPC_URL`, `EXECUTOR_GAS_LIMIT_WEI`, etc.)

Settings finais propostas: 4 (executor_*).

### 8.2 Métricas Prometheus

| Métrica | Tipo | Labels | Propósito |
|---|---|---|---|
| `polycopy_executor_orders_total` | Counter | `result` (executed\|failed\|dry_run\|duplicate_skip), `mode` (real\|dry_run), `reason` | Contagem por outcome + modo |
| `polycopy_executor_decision_duration_seconds` | Histogram | (none) | Latência fim-a-fim |
| `polycopy_executor_gas_wei` | Histogram | (none) | Gas usado em wei (só observado em real-mode com result="executed"; vazio em dry_run) |

### 8.3 Logs estruturados

`ExecutorAgent`: `event="executor_decision"` com `trade_event_id`, `wallet`, `mode`, `result`, `final_size_usdc`, `tx_hash`, `gas_wei`, `failure_reason`, `error_message`.

## 9. Testes

### 9.1 Unit

| File | Testes |
|---|---|
| `tests/unit/agents/test_executor.py` | (1) happy path dry_run; (2) executor lança exception → persist failed; (3) idempotency duplicate_skip; (4) invalid payload silent ack; (5) bus publish failure propagates after persist; (6) `_select_publish` cobre 3 paths (executed/failed/dry_run) — provavelmente via testes do happy path. |
| `tests/unit/infrastructure/test_dry_run_executor.py` | (1) execute returns DRY_RUN result com success=True; (2) sempre returns mode=DRY_RUN |
| `tests/unit/infrastructure/test_metrics.py` | +3 testes (1 por métrica) |
| `tests/unit/domain/test_execution_events.py` | OrderExecuted/Failed/DryRun shape + tz-aware + subjects + OrderExecution invariants (5 invariants × happy + 5 raise paths) |

### 9.2 Integration

| File | Testes |
|---|---|
| `tests/integration/test_order_execution_repository.py` | insert new/duplicate/persists data + 3 CHECK violations + Protocol typecheck (~6 testes) |
| `tests/integration/test_jetstream_bus.py` | +6 testes pros 3 publishes (received + dedup pra cada) |
| `tests/integration/test_executor_e2e.py` | (1) E2E dry_run flow — publica OrderSized → DB tem dry_run + bus tem order.dry_run; (2) E2E redelivery idempotent. (3 não-trivial pra testar failure path em E2E sem mockar executor — fica unit-only) |

### 9.3 Smoke opt-in

Não há. Executor é puramente lógica + DB + bus. Real-mode (Fase 4) terá smoke opt-in contra testnet Polygon.

## 10. Roadmap (8 tasks)

| Task | Escopo | Reviewer |
|---|---|---|
| **T1** | Domain — events + enums + `OrderExecution` value object + `ExecutionResult` | opcional |
| **T2** | Ports — `OrderExecutionRepository` + `OrderExecutor` + extensão `MessagingPort` + impl mínima 3 publishes em `NatsMessagingBus` | opcional |
| **T3** | Tabela `order_executions` + migration `0005` + ORM | opcional (DDL puro) |
| **T4** | `SqlAlchemyOrderExecutionRepository` + integration tests | opcional |
| **T5** | Stream `EXECUTION_RESULTS` + `DryRunExecutor` impl + 6 integration tests pros publishes | obrigatório (mensageria + nova execution lib) |
| **T6** | `ExecutorAgent` + 4 settings + 3 métricas + .env.example + 12 unit tests | obrigatório (lógica principal) |
| **T7** | Container `polycopy-executor:9106` + scrape Prometheus + ARCHITECTURE.md | opcional |
| **T8** | Integration E2E `test_executor_e2e.py` | opcional |

## 11. Open questions / known debt

- **Real-mode é Fase 4 inteira.** Sem clareza ainda de: (a) qual rede (Polygon mainnet vs testnet pra dev), (b) como gerenciar wallet/seeds (Vault, AWS Secrets Manager, env var encriptada), (c) gas strategy (EIP-1559 com tip dynamic vs static), (d) RPC provider (Alchemy, Infura, public node), (e) retry strategy pra falhas de rede vs falhas on-chain.
- **`DryRunExecutor` sempre returns success=True** — não simula falhas. Se valer testar caminho `OrderFailed` em E2E, criar `FlakyDryRunExecutor` ou adicionar seed de aleatoriedade. MVP não inclui pra manter simplicidade.
- **Settings flat continua dívida** — +4 vars (executor_*) na Fase 3, +N na Fase 4. Refator pra `ExecutorSettings` nested fica em hardening.
- **Persist→publish gap herdado** — mesmo caveat das Fases 2B/2C: se Executor crash entre `repo.insert` e `bus.publish_*`, evento perdido. Solução: transactional outbox.
- **`gas_wei NUMERIC(40, 0)`** — uint256 cabe até NUMERIC(78,0) mas Polygon real raramente excede 10 dígitos. (40,0) é folga generosa.
- **`mode` field é deduzido do executor injetado** — se mudar `EXECUTOR_DRY_RUN=false` sem reinstanciar executor real, agente vai gravar mode='dry_run' enquanto pretende real. Mitigação: settings change requer container restart (já é o padrão de Settings frozen).

## 12. Self-review (executor autônomo)

- **Placeholder scan:** sem TBD/TODO/"implement later" no código entregue. Real-mode marcado claramente como Fase 4 fora de escopo.
- **Internal consistency:** 3 events ↔ 3 subjects ↔ 3 result values. Schema CHECK constraints refletem invariants do `OrderExecution.__post_init__`.
- **Scope check:** focado em dry-run MVP. Real-mode explicitamente fora.
- **Ambiguity check:** "DRY-RUN apenas" enfatizado em spec, plano, código (default `executor_dry_run=True`), docstring, log.
- **Decisão crítica:** dry-run-only é a abordagem responsável quando executor autônomo não tem credenciais de wallet — entrega valor (estrutura completa + audit trail) sem risco financeiro.
