# Plano 4 — Web3CLOBExecutor (real on-chain execution)

**Data:** 2026-05-02
**Status:** spec aprovada (brainstorm humano supervisionado, 6 perguntas críticas decididas explicitamente)
**Predecessor:** Plano 3 (Executor DRY-RUN MVP) — entregou `OrderExecutor` Protocol, `ExecutorAgent`, `polycopy-executor:9106`, stream `EXECUTION_RESULTS`.
**Sucessor:** Fase 5 (hardening — partial fills, cancel orders, order books WebSocket, audit dashboards).

---

## 1. Contexto

Fase 3 entregou `ExecutorAgent` em modo DRY-RUN apenas (default `EXECUTOR_DRY_RUN=true`). Agora a Fase 4 substitui o `DryRunExecutor` por `Web3CLOBExecutor` — implementação real que submete ordens no Polymarket CLOB on-chain (Polygon mainnet).

O Strategy Pattern (`OrderExecutor` Protocol) desenhado na Fase 3 era exatamente pra isso. `ExecutorAgent` não muda — apenas o executor injetado no `main()` muda quando `EXECUTOR_DRY_RUN=false` + `EXECUTOR_REAL_MODE_CONFIRMED=true` (double opt-in).

**Decisão crítica do brainstorm:** Polymarket NÃO tem deploy em testnet (Amoy/Mumbai). Toda Fase 4 é executada contra Polygon mainnet com **5 camadas de kill-switches** + **tamanhos minúsculos** (`MAX_SIZE_USDC=$2`, `DAILY_MAX_USDC=$20`) durante validação inicial.

**Escolha técnica fundamental:** usar `py-clob-client` (biblioteca oficial Polymarket) que encapsula EIP-712 signing + submissão pro operator off-chain + settlement on-chain. Reduz superfície de bug — não estamos escrevendo cripto de baixo nível, só usando lib testada.

## 2. Motivação

Sem `Web3CLOBExecutor`, copy trading nunca acontece de verdade — Fase 3 gera audit de "trades que teríamos feito" mas não move dinheiro. Fase 4 fecha o ciclo:

- Operador roda `setup_wallet` script uma vez → wallet aprovada pra Exchange contract gastar até $100 USDC.
- Container `polycopy-executor` rodando com `EXECUTOR_DRY_RUN=false` + `EXECUTOR_REAL_MODE_CONFIRMED=true` chama `Web3CLOBExecutor` em vez de `DryRunExecutor`.
- Cada `order.sized` recebido do Sizing vira tentativa real de execução: kill-switches → CLOB submission → tx_hash on-chain → publish `order.executed` ou `order.failed`.

Real-mode **não é mandatory** — toda infra DRY-RUN da Fase 3 continua funcionando. Operador alterna entre modos via env var sem refator.

## 3. Escopo

### 3.1 Dentro de Fase 4

- Estender `FailureReason` enum com 10 razões novas (5 kill-switch + 5 erros on-chain).
- 10 settings novas em `config.py` (wallet, RPC, contracts, real-mode flags, kill-switches).
- `KillSwitch` class — state in-memory + 5 camadas de checagens + métricas.
- `order_mapper.py` — função pura `Trade + final_size_usdc → OrderArgs` (py-clob-client format).
- `Web3CLOBExecutor` — implementação real do `OrderExecutor` Protocol via `py-clob-client`.
- Factory `build_clob_client()` — monta `ClobClient` a partir de Settings.
- `_verify_allowance()` — leitura on-chain pra falhar rápido se setup_wallet não rodou.
- Atualização do `main()` em `agents/executor.py` — DI condicional (DryRunExecutor vs Web3CLOBExecutor) + safety gates duplos.
- Script standalone `polycopy/scripts/setup_wallet.py` — approve USDC pra Exchange, one-shot manual.
- 4 métricas Prometheus novas (kill-switch blocks, CLOB request duration, wallet balance, consecutive failures).
- Smoke opt-in `tests/integration/test_polymarket_smoke_executor.py` (read-only — nunca submete order).
- Runbook `docs/runbooks/fase-4-first-real-trade.md` — checklist humano pro primeiro real trade.
- Dependência nova: `py-clob-client` (biblioteca oficial Polymarket).

### 3.2 Fora de Fase 4 (entra em Fase 5+)

- **Partial fills tracking** (`order.partial_fill` event, agregação de fills).
- **Cancel orders** (timeout, sinal externo).
- **WebSocket subscriptions** ao CLOB pra updates real-time de ordem (status, fills).
- **Múltiplos executors em paralelo** (nonce management, distributed locking).
- **Replay de DRY-RUN como REAL** (promoção de backtest pra produção).
- **Telegram alerts** quando kill-switches ativam (reuso do notifier client — cross-cutting deferido).
- **Circuit breaker por failure rate %** (por enquanto é só contagem absoluta).
- **Audit dashboard** de PnL real on-chain.
- **Stop-loss / Take-profit** pós-execução (precisa monitor de outcome resolution).
- **Multi-wallet** (1 wallet por estratégia, distribuição de risco).
- **Polymarket Proxy account** (`SIGNATURE_TYPE=1`) — fica deferido por causa do bug ativo issue #336 do `py-clob-client`.

## 4. Componentes

```
src/polycopy/
├── domain/events.py                                    # + 10 razões em FailureReason
├── config.py                                           # + 10 settings (Fase 4 — DANGER ZONE)
├── infrastructure/
│   ├── execution/
│   │   ├── kill_switch.py                              # NEW — 5 camadas + state in-memory
│   │   ├── order_mapper.py                             # NEW — Trade → OrderArgs
│   │   ├── web3_clob_executor.py                       # NEW — implementação real
│   │   └── (dry_run_executor.py, __init__.py — Fase 3, intactos)
│   └── observability/metrics.py                        # + 4 métricas
└── agents/executor.py                                  # main() DI condicional + safety gates

src/polycopy/scripts/
├── __init__.py                                         # NEW (vazio)
└── setup_wallet.py                                     # NEW — CLI manual one-shot

tests/
├── unit/
│   ├── domain/test_execution_events.py                 # + atualização test_failure_reason_values
│   ├── infrastructure/
│   │   ├── test_kill_switch.py                         # NEW — 12 testes
│   │   ├── test_order_mapper.py                        # NEW — 6 testes
│   │   ├── test_web3_clob_executor.py                  # NEW — 15 testes (CLOB mockado)
│   │   └── test_metrics.py                             # + 4 testes (1 por métrica nova)
│   ├── agents/test_executor.py                         # + 3 testes (main() safety gates)
│   └── scripts/
│       ├── __init__.py                                 # NEW
│       └── test_setup_wallet.py                        # NEW — 4 testes
└── integration/test_polymarket_smoke_executor.py       # NEW — opt-in PYTEST_LIVE_POLYGON

docs/runbooks/
└── fase-4-first-real-trade.md                          # NEW — checklist humano
```

**Componentes-chave:**

- **`KillSwitch`** — instância long-lived no agent. Mantém `deque[(timestamp, size)]` com janela rolante 24h pra contagem de trades, contador `consecutive_failures` resetado em sucesso. Método `check(size_usdc) -> FailureReason | None` retorna razão de bloqueio ou None. Métodos `record_success(size)` / `record_failure()` mutam state. Pause file checado a cada `check()` via `path.exists()`.
- **`order_mapper.to_order_args(trade, final_size_usdc) -> OrderArgs`** — função pura. Calcula `shares = final_size_usdc / trade.price.value` (Polymarket CLOB trabalha em shares, não USDC). Mapeia `Side` enum pra `BUY`/`SELL` literal do py-clob-client.
- **`Web3CLOBExecutor`** — recebe `clob_client: ClobClient`, `kill_switch: KillSwitch`, `max_size_usdc: Decimal`, `metrics: Metrics`. `execute()` faz: (1) kill_switch.check, (2) order_mapper.to_order_args, (3) `await asyncio.to_thread(clob.create_and_post_order, ...)` (py-clob-client é sync), (4) try/except mapeando exceptions específicas pra `FailureReason`, (5) record_success/failure no kill_switch, (6) `ExecutionResult(mode=REAL, ...)`.
- **`build_clob_client(settings) -> ClobClient`** — factory que monta `ClobClient(host=settings.polymarket_clob_api_url, key=settings.wallet_private_key.get_secret_value(), chain_id=137, signature_type=0)` + `client.create_or_derive_api_creds()`.
- **`_verify_allowance(clob_client, settings)` async** — chama `clob_client.get_allowance()` (ou Web3.py `usdc.allowance(wallet, exchange)`); raise `RuntimeError("USDC allowance insufficient — run setup_wallet script")` se baixa.
- **`setup_wallet.py`** — script CLI (não roda no agent). Reads Settings → imprime address + balances + allowance → pergunta confirmação → faz `usdc.approve(exchange, MAX_APPROVAL_USDC * 10**6)` via Web3.py direto (não py-clob-client — operação atômica).
- **`main()` updated** — DI condicional baseada em `executor_dry_run` + `executor_real_mode_confirmed`. Triple safety gates: (1) `dry_run=False` (default true), (2) `real_mode_confirmed=True` (default false), (3) `wallet_private_key` não-None. Qualquer um falhar → `RuntimeError` clara.

## 5. Fluxos

### 5.1 Setup inicial (humano, one-shot)

```
1. User cria EOA: gera private key + address (ex: via foundry cast wallet new ou metamask).
2. User funda wallet:
   - $5 MATIC (gas, via bridge ou exchange)
   - $20-50 USDC (collateral, via Polygon Bridge ou Coinbase)
3. User configura .env:
   WALLET_PRIVATE_KEY=0x...
   POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/<KEY>
   MAX_APPROVAL_USDC=100
4. User roda: uv run python -m polycopy.scripts.setup_wallet
   → mostra balances + allowance atual
   → pede confirmação
   → faz approve(exchange, $100 USDC)
   → imprime tx_hash + URL Polygonscan
5. User aguarda 1 bloco (~2s em Polygon).
6. Setup completo. Wallet pronta pra real-mode.
```

### 5.2 Ativação de real-mode (humano via env vars)

```
1. EXECUTOR_DRY_RUN=true (default seguro), container já rodando em dry-run há tempo suficiente.
2. User valida pipeline DRY-RUN saudável (logs, métricas, audit em order_executions).
3. User edita .env:
   EXECUTOR_DRY_RUN=false
   EXECUTOR_REAL_MODE_CONFIRMED=true
4. docker compose restart executor
5. Container restarta. main() detecta real-mode → instancia Web3CLOBExecutor.
6. _verify_allowance() roda no startup → falha rápido se setup_wallet não rodou.
7. Agent começa a consumir order.sized do Sizing → executa real.
```

### 5.3 Execução real de uma ordem

```
JetStream entrega order.sized payload
└─→ ExecutorAgent._handle_message
    ├─→ OrderSized.model_validate_json(payload)
    └─→ Web3CLOBExecutor.execute(trade, final_size_usdc)
        ├─→ KillSwitch.check(final_size_usdc) -> FailureReason | None
        │   ├─ pause file existe → MANUALLY_PAUSED
        │   ├─ consecutive_failures ≥ 3 → CIRCUIT_BREAKER
        │   ├─ trades_24h ≥ 10 → DAILY_TRADES_EXCEEDED
        │   ├─ sum(usdc_24h) + size > 20 → DAILY_USDC_EXCEEDED
        │   └─ size > 2 → SIZE_EXCEEDS_EXECUTOR_CAP
        ├─→ Se bloqueado: ExecutionResult(mode=REAL, success=False, reason=...) + métrica
        ├─→ Senão: order_mapper.to_order_args(trade, final_size_usdc)
        ├─→ clob_client.create_and_post_order(args)  # py-clob-client encapsula EIP-712 + POST
        │   ├─ Sucesso: response com tx_hash + gas_used
        │   │   └─→ kill_switch.record_success(final_size_usdc)
        │   │   └─→ ExecutionResult(mode=REAL, success=True, tx_hash=..., gas_wei=...)
        │   └─ Erro: classifica via _classify_clob_error(exc):
        │       ├─ InsufficientBalanceError → INSUFFICIENT_USDC_BALANCE
        │       ├─ InsufficientAllowanceError → INSUFFICIENT_USDC_ALLOWANCE
        │       ├─ ClobApiError genérica → CLOB_REJECTED_ORDER
        │       ├─ RpcError (rede/timeout) → RPC_ERROR
        │       └─ SignatureError → SIGNATURE_ERROR
        │       └─→ kill_switch.record_failure() + ExecutionResult(success=False, reason=...)
        └─→ Métricas: kill_switch_blocks_total{reason}, clob_request_duration_seconds{result},
                      wallet_balance_usdc, consecutive_failures, executor_orders_total{result, mode, reason}
```

`ExecutorAgent` faz a parte downstream (persiste em `order_executions` + publica `order.executed`/`order.failed`). Nenhuma mudança nele.

## 6. Tratamento de falhas

| Falha | Comportamento | Métrica/Log |
|---|---|---|
| Pause file `/tmp/polycopy/executor.pause` existe | Skip execução, ExecutionResult(failed, MANUALLY_PAUSED). NÃO incrementa consecutive_failures. | `kill_switch_blocks_total{reason="manually_paused"}` |
| Daily trades cap atingido | Skip, DAILY_TRADES_EXCEEDED. Reset quando trade > 24h sai da janela. | idem |
| Daily USDC cap atingido | Skip, DAILY_USDC_EXCEEDED. Reset idem. | idem |
| 3+ failures consecutivas | Skip todas até reset. Reset em **success** (não automático no tempo). Operador pode forçar via container restart. | `consecutive_failures` Gauge ≥ 3 |
| Size > MAX_SIZE_USDC | Skip, SIZE_EXCEEDS_EXECUTOR_CAP. Indica bug no Sizing (deveria ter capado antes). | idem + log error |
| CLOB API rejeita ordem | ExecutionResult(failed, CLOB_REJECTED_ORDER), record_failure(). Detalhes do erro vão pro `error_message`. | `executor_orders_total{result=failed, reason=clob_rejected_order}` |
| USDC saldo insuficiente | INSUFFICIENT_USDC_BALANCE. Operador precisa fundar mais. | `wallet_balance_usdc` Gauge baixo |
| USDC allowance insuficiente | INSUFFICIENT_USDC_ALLOWANCE. Operador roda setup_wallet de novo (incrementa cap). | idem |
| RPC Alchemy timeout/erro | RPC_ERROR + record_failure. Retry implícito do `py-clob-client` ou JetStream redelivery. | `clob_request_duration_seconds{result=error}` alta |
| EIP-712 signature error | SIGNATURE_ERROR. Bug grave — chave inválida ou clock skew. | log error + alerta operacional |
| Real-mode sem WALLET_PRIVATE_KEY | `main()` raise RuntimeError no startup. Container falha em criar/restart. | health check falha |
| Real-mode sem REAL_MODE_CONFIRMED=true | idem | idem |
| Allowance insuficiente no startup | `_verify_allowance` raise RuntimeError. Container falha. | idem |

**Persist→publish gap herdado** (mesmo caveat das fases anteriores): se Executor crash entre `repo.insert` e `bus.publish_*`, evento perdido. NATS dedup só impede duplicate. Solução real: transactional outbox (Fase 5+).

## 7. Observabilidade

### 7.1 Settings novas (10 — Settings flat dívida acumulada continua)

```python
# Wallet (real-mode only — None default fail-fast)
wallet_private_key: SecretStr | None = Field(None, alias="WALLET_PRIVATE_KEY")

# Polygon network
polygon_rpc_url: str = Field(
    "https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY", alias="POLYGON_RPC_URL"
)
polygon_chain_id: int = Field(137, alias="POLYGON_CHAIN_ID")

# Polymarket contracts
polymarket_exchange_address: str = Field(
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", alias="POLYMARKET_EXCHANGE_ADDRESS"
)
polymarket_clob_api_url: str = Field(
    "https://clob.polymarket.com", alias="POLYMARKET_CLOB_API_URL"
)

# Executor real-mode safety gates
executor_real_mode_confirmed: bool = Field(False, alias="EXECUTOR_REAL_MODE_CONFIRMED")

# Approval cap
max_approval_usdc: int = Field(100, alias="MAX_APPROVAL_USDC")

# Kill-switches (5 camadas)
executor_max_size_usdc: Decimal = Field(Decimal("2"), alias="EXECUTOR_MAX_SIZE_USDC")
executor_daily_max_usdc: Decimal = Field(Decimal("20"), alias="EXECUTOR_DAILY_MAX_USDC")
executor_daily_max_trades: int = Field(10, alias="EXECUTOR_DAILY_MAX_TRADES")
executor_circuit_breaker_failures: int = Field(3, alias="EXECUTOR_CIRCUIT_BREAKER_FAILURES")
executor_pause_file: Path = Field(
    Path("/tmp/polycopy/executor.pause"), alias="EXECUTOR_PAUSE_FILE"
)
```

### 7.2 Métricas Prometheus (4 novas)

| Métrica | Tipo | Labels | Propósito |
|---|---|---|---|
| `polycopy_executor_kill_switch_blocks_total` | Counter | `reason` | Quantas vezes cada camada bloqueou |
| `polycopy_executor_clob_request_duration_seconds` | Histogram | `result` (success/error) | Latência da chamada ao CLOB API |
| `polycopy_executor_wallet_balance_usdc` | Gauge | (none) | Saldo USDC atual (atualizado pós-trade success) |
| `polycopy_executor_consecutive_failures` | Gauge | (none) | Contador atual circuit breaker |

**Reuso da Fase 3 (sem mudança):**
- `polycopy_executor_orders_total{result, mode, reason}` — agora popula `mode='real'` + `result='executed'/'failed'` + 10 razões novas.
- `polycopy_executor_decision_duration_seconds` — fim-a-fim incluindo kill-switch + CLOB.
- `polycopy_executor_gas_wei` — agora populado de verdade (era vazio em dry-run).

### 7.3 Logs estruturados

`Web3CLOBExecutor`: `event="real_execution_attempt"` com `trade_event_id`, `wallet`, `size_usdc`, `kill_switch_passed`, `clob_response_ms`, `tx_hash`, `gas_wei`, `failure_reason`, `error_message`.

## 8. Testes

### 8.1 Unit (sem infra)

| File | Testes |
|---|---|
| `tests/unit/infrastructure/test_kill_switch.py` | 12 testes: cada camada bloqueia/passa, eviction 24h window, ordem das checagens, record_success reseta failures |
| `tests/unit/infrastructure/test_order_mapper.py` | 6 testes: BUY/SELL × diferentes prices/sizes, edge case fracionário, side enum mapping |
| `tests/unit/infrastructure/test_web3_clob_executor.py` | 15 testes: happy path, kill-switch bloqueia (CLOB nunca chamado), classificação de cada exception → FailureReason, métricas observadas, mode sempre REAL, tx_hash propagado |
| `tests/unit/infrastructure/test_metrics.py` | +4 testes (1 por métrica nova) |
| `tests/unit/agents/test_executor.py` | +3 testes: main() raise sem REAL_MODE_CONFIRMED, sem WALLET_PRIVATE_KEY, allowance insuficiente |
| `tests/unit/scripts/test_setup_wallet.py` | 4 testes: balances mostrados, "no" não aprova, "yes" aprova com cap correto, imprime tx_hash |
| `tests/unit/domain/test_execution_events.py` | +1 teste atualizado pra novos valores de FailureReason enum |

### 8.2 Smoke opt-in (read-only — NUNCA submete order)

`tests/integration/test_polymarket_smoke_executor.py` com `pytestmark = [pytest.mark.live, pytest.mark.skipif(PYTEST_LIVE_POLYGON != "1", ...)]`:

- `test_clob_client_can_authenticate` — verifica L1 auth via Alchemy + lê markets.
- `test_wallet_has_funds_and_allowance` — confirma saldos MATIC/USDC + allowance ≥ threshold (falha rápido se setup_wallet não rodou).

Pra rodar: `PYTEST_LIVE_POLYGON=1 uv run pytest tests/integration/test_polymarket_smoke_executor.py`. Por default skipam.

### 8.3 Manual acceptance (humano após deploy)

`docs/runbooks/fase-4-first-real-trade.md` com checklist de 10 passos. Operador valida primeiro real trade manualmente (verifica tx_hash no Polygonscan, confirma saldo USDC pós-trade). NÃO automatizado — humano-in-the-loop pra primeira execução.

## 9. Roadmap (8 tasks)

| Task | Escopo | Reviewer |
|---|---|---|
| **T1** | Domain — estender `FailureReason` enum (+10 razões), atualizar test | opcional |
| **T2** | Settings — 10 vars novas, atualização `.env.example` com seção "DANGER ZONE" | opcional |
| **T3** | `KillSwitch` class + 12 unit tests | opcional |
| **T4** | `order_mapper.py` + 6 unit tests | opcional |
| **T5** | `Web3CLOBExecutor` + factory + `_verify_allowance` + 15 unit tests | **obrigatório** (lógica real-mode + dependência externa nova) |
| **T6** | `setup_wallet.py` script + 4 unit tests | opcional |
| **T7** | 4 métricas + atualização `metrics.py` + 4 unit tests + `main()` em `executor.py` (DI condicional + safety gates) + 3 unit tests | **obrigatório** (mexe em `main()` que carrega secrets + DI crítico) |
| **T8** | Smoke opt-in + runbook humano | opcional |

**Cadência:** subagent-driven com **checkpoint humano por task** (não autônomo). T5 + T7 com code reviewer obrigatório. Implementer NÃO commita.

**Estimativa:** ~700 linhas de produção + ~500 de testes. Comparável com Fase 3.

**Dependência nova:** `py-clob-client` adicionada via `uv add py-clob-client` em T5.

**Sem novos containers, migrations, streams JetStream, ou tabelas.** Toda Fase 4 é DI + lógica + scripts.

## 10. Open questions / known debt

- **Settings flat dívida** continua — +10 vars. Total Fase 1-4: ~40 vars novas em Settings flat. Refator pra `<Agent>Settings` nested (e separar `WalletSettings`) entra em hardening dedicado.
- **Persist→publish gap** herdado das fases 2B/2C/3.
- **Daily caps in-memory** resetam em restart do container. Operador que reinicia container "burla" o cap diário. Mitigação aceita pra MVP — em hardening: persistir contador em Redis ou tabela.
- **Circuit breaker reseta em sucesso** (não em tempo). Decisão consciente — em sistema saudável, success único reseta. Em sistema degradando, mais conservador seria reset por janela temporal (ex: 30min sem failures). Deferido.
- **Allowance check no startup** não revalida durante runtime. Se operador revogar allowance manualmente (cenário improvável), agent vai falhar em runtime com `INSUFFICIENT_USDC_ALLOWANCE` até reiniciar — não graceful, mas explícito (operador vai ver nas métricas).
- **`py-clob-client` é dependência síncrona** (não async). Wrappamos em `asyncio.to_thread()` — adequado mas não otimal. Em hardening: avaliar `httpx async` direto + EIP-712 signing manual via `eth_account.Account.sign_typed_data`.
- **Issue #336 do `py-clob-client`** sobre `order_version_mismatch` em `SIGNATURE_TYPE=1` não nos afeta (vamos com `SIGNATURE_TYPE=0`). Se Polymarket fixar e quisermos features de Proxy account, hardening futuro.
- **Sem WebSocket subscription** ao CLOB pra updates real-time de ordem. Fase 4 fire-and-forget — ordem submetida, agent assume que settle eventualmente. Em hardening: subscriber pra `order.matched`/`order.filled` events do Polymarket.
- **Sem stop-loss / take-profit** — Fase 4 só executa entrada. Saída fica como Fase 5 inteira (precisa monitoring de outcome resolution + segundo agente).
- **Sem multi-wallet** — uma única EOA pra todos os trades. Distribuição de risco entre wallets fica deferida.
- **Telegram alerts pra kill-switches** deferido — cross-cutting que requer reuso do notifier client.

## 11. Self-review (autor da spec)

- **Placeholder scan:** sem TBD/TODO. 6 decisões fechadas no brainstorm.
- **Internal consistency:** `FailureReason` enum estendido em §3 cobre todas as razões mencionadas em §6 (tratamento de falhas) e §5 (fluxos). 10 settings em §7.1 batem 1:1 com componentes em §4. 4 métricas em §7.2 batem com instrumentação descrita em §5.3.
- **Scope check:** focado num único plano de Fase 4. T1-T8 implementáveis sequencialmente.
- **Ambiguity check:** "5 camadas de kill-switch" tem ordem explícita em §5.3 (pause → circuit_breaker → daily_trades → daily_usdc → size_cap). "Triple safety gates" do `main()` enumerado em §4 (componente `main() updated`). Real-mode requer (dry_run=false) AND (real_mode_confirmed=true) AND (private_key não-None) — nenhuma combinação parcial ativa.

**Decisões críticas tomadas explicitamente pelo usuário (brainstorm humano):**
1. Polygon mainnet (sem testnet pra Polymarket).
2. EOA SIGNATURE_TYPE=0.
3. Private key em `.env` via `SecretStr`.
4. Alchemy free tier RPC.
5. Approval manual via script com cap explícito.
6. 5 kill-switches médios (mainstream defesa em profundidade).
