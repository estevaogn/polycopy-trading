# Plano 2C — Sizing agent

**Data:** 2026-05-02
**Status:** spec aprovada (autonomamente pelo executor — usuário delegou execução completa do 2C nesta sessão), aguarda plano de implementação
**Predecessor:** Plano 2B (Risk agent) — entregou `OrderApproved` event, `polycopy-risk:9104`, stream JetStream `RISK_DECISIONS`
**Sucessor:** Fase 3 (execução real on-chain via wallet — fora do escopo Fase 2)

---

## 1. Contexto

Fase 2 do `PROMPT_POLYCOPY_v2.md` é decomposta em 2A (market data, ✅), 2B (risk, ✅), 2C (sizing, **este documento**). Sizing é a última peça da Fase 2: pega trades já aprovados pelo Risk e calcula o **tamanho final** que o sistema enviaria pro broker (Polymarket CLOB) na Fase 3.

## 2. Motivação

`RiskAgent` (2B) aprovou um trade detectado, mas o tamanho aprovado é o que a wallet observada operou — não necessariamente o que **a gente** quer copiar. Razões pra escalar:

- **Capital próprio menor**: copiar 100% do trade de uma whale com $1M nem cabe no capital da copy.
- **Risk budgeting por trade**: não dá pra apostar todo o capital num único trade.
- **Floor de poeira**: trades < $1 USDC de tamanho final desperdiçam fees (gas + market maker spread) — melhor pular.

Sizing aplica uma **proporcionalidade hardcoded** (default 10%) com cap absoluto (`MAX_SIZE_USDC=50`) e floor (`MIN_SIZE_USDC=1`). Trades abaixo do floor publicam `order.skipped` (audit) em vez de `order.sized`.

## 3. Escopo

### 3.1 Dentro de 2C

- 2 eventos novos: `OrderSized` (subject `order.sized`) e `OrderSkipped` (subject `order.skipped`).
- Enum `SkipReason` com 1 razão (MVP): `BELOW_MIN_SIZE`. Estrutura aberta pra mais razões depois (ex: `INSUFFICIENT_CAPITAL`, `RATE_LIMIT_EXCEEDED`).
- `OrderSizing` value object + `OrderSizingRepository` Protocol.
- Tabela `order_sizings` + migration alembic + ORM.
- Adapter `SqlAlchemyOrderSizingRepository` (insert idempotente via PK).
- Extensão `MessagingPort` com 2 métodos publish novos + implementação no `NatsMessagingBus` (junto pra evitar quebra mypy do Protocol como aprendido em 2B-T2).
- Stream JetStream novo `SIZING_DECISIONS` com subjects literais `["order.sized", "order.skipped"]` (decisão arquitetural fixada em 2B-T5: 1 stream por agente que decide).
- Agente novo `SizingAgent` consumindo `order.approved` via durable consumer (`sizing-1`).
- Algoritmo de sizing: `final_size = min(MAX_SIZE_USDC, original_size * PROPORTION_RATIO)`. Se `final_size < MIN_SIZE_USDC` → skip.
- Containerização (`polycopy-sizing:9105`), scrape Prometheus, atualização `ARCHITECTURE.md`.
- 3 métricas Prometheus + 6 settings novos.
- Testes unit + integration E2E.

### 3.2 Fora de 2C

- **Slippage check via orderbook** (CLOB do 2A T4). Razão: requer cálculo de market impact + retry quando slippage exceder budget. Merece sua própria fase de hardening.
- **Capital tracking**: validação contra saldo em wallet própria. Requer wallet integration (Fase 3).
- **Rate limiting global** entre agente e broker. Fase 3.
- **Sizing dinâmico baseado em conviction/PnL histórico da wallet copiada**. Refinement futuro.
- **Execução on-chain**. Fase 3.
- **Audit UI / dashboard**. Tabela `order_sizings` é audit; dashboards entram em fase de observabilidade dedicada.

## 4. Componentes

```
src/polycopy/
├── domain/
│   ├── events.py                                    # + OrderSized, OrderSkipped, SkipReason
│   └── sizing.py                                    # NEW — OrderSizing value object
├── ports/
│   ├── messaging.py                                 # + publish_order_sized, publish_order_skipped
│   └── order_sizing_repository.py                   # NEW — OrderSizingRepository Protocol
├── infrastructure/
│   ├── messaging/nats_bus.py                       # + 2 publish methods + stream SIZING_DECISIONS
│   └── persistence/
│       ├── models.py                               # + OrderSizingRow ORM
│       └── order_sizing_repository.py              # NEW — SqlAlchemyOrderSizingRepository
└── agents/
    └── sizing.py                                   # NEW — SizingAgent

alembic/versions/
└── 0004_add_order_sizings.py                       # NEW

tests/
├── unit/
│   ├── agents/test_sizing.py                       # NEW
│   └── infrastructure/test_metrics.py              # + 3 testes (1 por métrica nova)
└── integration/
    ├── test_order_sizing_repository.py             # NEW
    └── test_sizing_e2e.py                          # NEW
```

**Componentes-chave:**

- **`SkipReason`** — Enum string com 1 razão MVP: `BELOW_MIN_SIZE` (final_size < `SIZING_MIN_SIZE_USDC`). Aberto pra extensão.
- **`OrderSized` / `OrderSkipped`** — eventos pydantic frozen+strict. Ambos têm `event_id` UUID (mesmo do trade detectado original — idempotência cross-agent), `occurred_at` (timestamp do trade original — preservado pra Sizing/Risk lag), `decided_at` (timestamp em que Sizing decidiu). `OrderSized` adiciona `final_size_usdc: Money` + `original_size_usdc: Money` (audit transparente do que foi escalado). `OrderSkipped` adiciona `reason: SkipReason`.
- **`OrderSizing`** — value object (frozen dataclass) com `trade_event_id: UUID`, `wallet`, `condition_id`, `token_id` (str), `original_size_usdc: Decimal`, `final_size_usdc: Decimal | None` (None se skipped), `decision: Literal["sized", "skipped"]`, `reason: SkipReason | None`, `decided_at: datetime`. `__post_init__` valida invariante: `decision=="sized"` ↔ `reason is None and final_size_usdc is not None`.
- **`OrderSizingRepository`** — Protocol com `async insert(sizing) -> bool`.
- **Extensão `MessagingPort`** — `async publish_order_sized` + `async publish_order_skipped`. NatsMessagingBus implementa com `Nats-Msg-Id = str(event.event_id)` (mesmo pattern do 2B).
- **`SizingAgent`** — herda `AgentBase`. Durable consumer `sizing-1` em `order.approved`. Callback `_handle_message`: parse `OrderApproved` → calcula `_size(trade)` → persiste `OrderSizing` (idempotente PK) → publica `order.sized` ou `order.skipped`.

## 5. Schema da tabela `order_sizings`

```sql
CREATE TABLE order_sizings (
    trade_event_id     UUID            PRIMARY KEY,        -- = OrderApproved.event_id
    wallet             TEXT            NOT NULL,
    condition_id       TEXT            NOT NULL,
    token_id           TEXT            NOT NULL,
    original_size_usdc NUMERIC(20, 6)  NOT NULL,            -- size do trade detectado original
    final_size_usdc    NUMERIC(20, 6),                       -- NULL se skipped
    decision           TEXT            NOT NULL,            -- 'sized' | 'skipped'
    reason             TEXT,                                  -- SkipReason; NULL se sized
    decided_at         TIMESTAMPTZ     NOT NULL,
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT order_sizings_decision_enum
        CHECK (decision IN ('sized', 'skipped')),
    CONSTRAINT order_sizings_consistency
        CHECK (
            (decision = 'sized' AND final_size_usdc IS NOT NULL AND reason IS NULL)
            OR (decision = 'skipped' AND final_size_usdc IS NULL AND reason IS NOT NULL)
        ),
    CONSTRAINT order_sizings_size_positive
        CHECK (original_size_usdc > 0 AND (final_size_usdc IS NULL OR final_size_usdc > 0))
);

CREATE INDEX idx_order_sizings_wallet_decided_at
    ON order_sizings (wallet, decided_at DESC);

CREATE INDEX idx_order_sizings_skipped_decided_at
    ON order_sizings (decided_at DESC)
    WHERE decision = 'skipped';
```

**Origem de cada campo:**

| Campo | Origem | Propósito |
|---|---|---|
| `trade_event_id` (PK) | `OrderApproved.event_id` (= `WalletTradeDetected.event_id`) | Idempotência cross-agent |
| `wallet`, `condition_id`, `token_id` | `Trade` original | Audit query |
| `original_size_usdc` | `OrderApproved.trade.size_usdc.amount` | Audit do tamanho da whale antes do scale-down |
| `final_size_usdc` | calculado: `min(MAX, original * RATIO)` se ≥ MIN, senão NULL | Tamanho que iria pro broker |
| `decision` | resultado do `_size()` | `'sized'` ou `'skipped'` |
| `reason` | `SkipReason` enum | Por que pulou (NULL se sized) |
| `decided_at` | `datetime.now(tz=UTC)` no agent | Timestamp da decisão |
| `created_at` | server default | Timestamp de gravação no DB |

**Decisões do schema:**
- **PK = `trade_event_id`**: idempotência grátis, mesmo padrão de `risk_decisions` (2B).
- **`final_size_usdc NUMERIC(20, 6)`**: mesma precisão de `markets.volume_24h_usdc` e `wallet_trades.size_usdc`. Coerência entre tabelas.
- **3 CHECK constraints**: enum, consistency (sized→size+no-reason; skipped→no-size+reason), e positividade (size > 0).
- **2 indexes parciais**: `wallet+decided_at DESC` (debug por wallet); `decided_at DESC WHERE skipped` (dashboards de skip rate).
- **Sem FK** pra `risk_decisions` ou `wallet_trades`: audit retroativo após archive de qualquer outra tabela.

## 6. Fluxos

### 6.1 Decisão de sizing (caminho principal)

```
JetStream durable consumer "sizing-1" entrega `order.approved` payload
└─→ SizingAgent._handle_message(payload, num_delivered)
    ├─→ OrderApproved.model_validate_json(payload)
    │   └─ ValidationError → métrica + ack silencioso
    ├─→ _size(trade) -> SizingResult (final_size, decision, reason)
    │   ├─ scaled = trade.size_usdc.amount * PROPORTION_RATIO
    │   ├─ capped = min(scaled, MAX_SIZE_USDC)
    │   ├─ se capped < MIN_SIZE_USDC → SizingResult("skipped", None, BELOW_MIN_SIZE)
    │   └─ senão → SizingResult("sized", capped, None)
    ├─→ OrderSizing(...) construída
    ├─→ repo.insert(sizing) -> bool
    │   ├─ True (nova) → segue
    │   └─ False (PK conflict, redelivery) → métrica duplicate_skip + ack silencioso
    ├─→ publish_order_sized OR publish_order_skipped (Nats-Msg-Id = event_id)
    └─→ métricas sizing_decisions_total, sizing_decision_duration_seconds, size_ratio_observed
```

**Ordem persist→publish:** mesmo invariante e mesmo gap conhecido do 2B (Risk crash entre persist e publish → evento perdido). Documentado abaixo na seção 11.

### 6.2 Cálculo do `_size`

```python
def _size(trade: Trade) -> SizingResult:
    original = trade.size_usdc.amount  # Decimal
    scaled = original * self._proportion_ratio  # Decimal mul Decimal
    capped = min(scaled, self._max_size_usdc)
    if capped < self._min_size_usdc:
        return SizingResult(
            final_size=None,
            decision="skipped",
            reason=SkipReason.BELOW_MIN_SIZE,
        )
    return SizingResult(
        final_size=capped.quantize(Decimal("0.000001")),  # USDC quantum
        decision="sized",
        reason=None,
    )
```

Sem dependências externas. Puro cálculo local.

## 7. Tratamento de falhas

| Falha | Comportamento | Métrica/Log |
|---|---|---|
| Payload malformado | Ack silencioso | `sizing_decisions_total{result="skipped", reason="invalid_payload"}` |
| DB indisponível | Exceção propaga → JetStream redelivery (até 5x) | log error |
| Bus indisponível | Exceção propaga; 2º handler vê `is_new=False` → skip publish | log warning |
| Re-delivery (duplicate) | `is_new=False` → ack sem re-publish | `sizing_decisions_total{result="duplicate_skip", reason=...}` |

## 8. Observabilidade

### 8.1 Settings novas (Settings flat — débito conhecido)

```python
sizing_metrics_port: int = Field(9105, alias="SIZING_METRICS_PORT")
sizing_max_deliver: int = Field(5, alias="SIZING_MAX_DELIVER")
sizing_durable_name: str = Field("sizing-1", alias="SIZING_DURABLE_NAME")
sizing_proportion_ratio: Decimal = Field(Decimal("0.1"), alias="SIZING_PROPORTION_RATIO")
sizing_max_size_usdc: Decimal = Field(Decimal("50"), alias="SIZING_MAX_SIZE_USDC")
sizing_min_size_usdc: Decimal = Field(Decimal("1"), alias="SIZING_MIN_SIZE_USDC")
```

### 8.2 Métricas Prometheus

| Métrica | Tipo | Labels | Propósito |
|---|---|---|---|
| `polycopy_sizing_decisions_total` | Counter | `result` (sized\|skipped\|duplicate_skip), `reason` | Decisões por outcome e razão |
| `polycopy_sizing_decision_duration_seconds` | Histogram | (none) | Latência fim-a-fim. SLO P95 < 100ms (sem fetch externo) |
| `polycopy_sizing_size_ratio_observed` | Histogram | (none) | Razão `final_size / original_size`. Buckets 0.0-0.5 esperados (default ratio 0.1, max-cap diminui ratio em trades grandes) |

### 8.3 Logs estruturados

`SizingAgent`: `event="sizing_decision"` com `trade_event_id`, `wallet`, `decision`, `original_size_usdc`, `final_size_usdc`, `reason`, `duration_ms`.

## 9. Testes

### 9.1 Unit

| File | Testes |
|---|---|
| `tests/unit/agents/test_sizing.py` | (1) `_size` happy path (trade 100 → final 10); (2) `_size` capped (trade 10000 * 0.1 = 1000 → final 50 cap); (3) `_size` skipped (trade 1 * 0.1 = 0.1 < min 1); (4) `_handle_message` happy path sized + skipped; (5) idempotency (duplicate `is_new=False`); (6) invalid payload silent ack |
| `tests/unit/infrastructure/test_metrics.py` | +3 testes (1 por métrica nova) |
| `tests/unit/domain/test_sizing_events.py` | (1) OrderSized requires tz-aware decided_at + occurred_at; (2) OrderSkipped requires reason; (3) OrderSizing invariant (sized↔size+no-reason; skipped↔no-size+reason); (4) subject constants |

### 9.2 Integration

| File | Testes |
|---|---|
| `tests/integration/test_order_sizing_repository.py` | (1) insert new returns True; (2) insert duplicate returns False; (3) insert sized persists final_size; (4) constraint consistency violation; (5) constraint size_positive violation; (6) Protocol typecheck |
| `tests/integration/test_sizing_e2e.py` | (1) E2E sized — publica OrderApproved → DB tem decision=sized + bus tem OrderSized; (2) E2E skipped — publica OrderApproved com size minúsculo → DB tem decision=skipped + bus tem OrderSkipped; (3) E2E redelivery idempotent |

### 9.3 Smoke opt-in

Não há. Sizing é puramente lógica + DB + bus. 2A já cobre Polymarket schema.

## 10. Roadmap (8 tasks)

| Task | Escopo | Reviewer |
|---|---|---|
| **T1** | Domain — `SkipReason`, `OrderSized`, `OrderSkipped`, `OrderSizing` value object | opcional |
| **T2** | Ports — `OrderSizingRepository` + extensão `MessagingPort` + impl mínima dos 2 publishes em `NatsMessagingBus` (mesmo padrão 2B-T2) | opcional |
| **T3** | Tabela `order_sizings` + migration `0004` + ORM `OrderSizingRow` | opcional (DDL puro) |
| **T4** | `SqlAlchemyOrderSizingRepository` + integration tests | opcional |
| **T5** | Stream `SIZING_DECISIONS` + 4 integration tests pros publishes | obrigatório (mexe em mensageria em produção) |
| **T6** | `SizingAgent` + 6 settings + 3 métricas + .env.example + unit tests | obrigatório (lógica principal) |
| **T7** | Container `polycopy-sizing:9105` + scrape Prometheus + ARCHITECTURE.md | opcional |
| **T8** | Integration E2E `test_sizing_e2e.py` | opcional |

## 11. Open questions / known debt

- **Settings flat continua dívida** — +6 campos novos. Refator pra `SizingSettings` nested em hardening futuro.
- **Sem slippage check via orderbook** — escopo Fase 3 (execução real). Hoje, o final_size pode ser submetido a um orderbook que não absorva sem slippage > budget. Aceito porque ainda não há submissão real.
- **`PROPORTION_RATIO=0.1` hardcoded** — sem versionamento de configuração; mudança requer redeploy + perda de auditoria histórica do "qual ratio estava ativo na decisão X". Aceito MVP.
- **Persist→publish gap** — herdado do 2B. Mesmo caveat: se Sizing crash entre `repo.insert` e `bus.publish_*`, evento nunca é publicado (NATS dedup só impede duplicate). Solução real: transactional outbox em hardening futuro.
- **Sem teste opt-in live** — Sizing não bate em internet.
- **`SkipReason` com 1 valor (`BELOW_MIN_SIZE`)** — estrutura aberta pra crescer. Adicionar `INSUFFICIENT_CAPITAL`, `RATE_LIMIT_EXCEEDED`, `MARKET_CLOSED_BETWEEN_RISK_AND_SIZE` quando relevante.

## 12. Self-review (executor autônomo)

- **Placeholder scan:** sem TBD/TODO. 7 decisões fechadas autonomamente (todas no "recomendado").
- **Internal consistency:** SkipReason em `_size` corresponde 1:1 ao enum (1 razão MVP). Schema CHECK reflete invariante "sized↔size+no-reason". Eventos publicados batem com nomes na seção 6.
- **Scope check:** focado num único plano. T1-T8 implementáveis sequencialmente.
- **Ambiguity check:** "fail-safe brando" não se aplica (Sizing não chama serviços externos). Cálculo `_size` matematicamente determinístico. Idempotência cross-attempt explicitada.
- **Decisões autônomas tomadas pelo executor:** algoritmo médio (proporcionalidade hardcoded), 2 eventos separados, tabela durable, agente próprio, stream JetStream separado, 3 métricas, defaults sensatos (ratio 0.1, max 50, min 1).
