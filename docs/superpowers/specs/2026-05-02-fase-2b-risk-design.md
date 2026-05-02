# Plano 2B — Risk agent

**Data:** 2026-05-02
**Status:** spec aprovada, aguarda plano de implementação
**Predecessor:** Plano 2A (CLOB + market data) — entregou `MarketRepository` com flag `is_stale`, `PolymarketGammaPort`, container `polycopy-marketdata`
**Sucessor:** Plano 2C (Sizing agent)

---

## 1. Contexto

Fase 2 do `PROMPT_POLYCOPY_v2.md` é decomposta em 2A (market data, ✅ concluído em 2026-05-02), 2B (risk, este documento) e 2C (sizing). Este spec define o **Risk agent** — gate fail-safe entre detecção (1B) e sizing (2C).

## 2. Motivação

O `WatcherAgent` (1B) detecta trades de wallets observadas e publica `wallet.trade.detected` no JetStream. Sem um filtro entre detecção e execução, qualquer trade seria copiado: trades enormes, em mercados quase resolvidos, com liquidez baixa, ou em mercados arquivados podem queimar capital. **Risk** é o gate que aplica regras hardcoded antes do trade chegar no sizing.

Risk também é o consumer natural do trabalho do 2A: a tabela `markets` + flag `is_stale` foram desenhadas pra Risk consumir com lazy fallback.

## 3. Escopo

### 3.1 Dentro de 2B

- 2 eventos novos: `OrderApproved` (subject `order.approved`) e `TradeRejected` (subject `trade.rejected`).
- Enum `RejectionReason` com 5 razões.
- `RiskDecision` value object + `RiskDecisionRepository` Protocol.
- Tabela `risk_decisions` + migration alembic + ORM.
- Adapter `SqlAlchemyRiskDecisionRepository` (insert idempotente via PK).
- Extensão do `JetStreamMessagingAdapter` (1C) com 2 métodos publish novos.
- Agente novo `RiskAgent` com 5 regras de decisão hardcoded + lazy fetch via Gamma quando cache miss/stale.
- Containerização (`polycopy-risk:9104`), scrape Prometheus, atualização `ARCHITECTURE.md`.
- 4 métricas Prometheus específicas + 8 settings novos.
- Testes unit + integration E2E.

### 3.2 Fora de 2B

- Sizing — entra no 2C.
- Regras dinâmicas (ex: limite por wallet, por mercado, por hora). MVP é 5 regras hardcoded; refinement entra na Fase 3.
- Slippage check com orderbook ao vivo via CLOB. 2A já tem o adapter; Risk não usa. Razão: 5 regras com Gamma cobrem 90% dos casos sem custo de latência por trade. Slippage entra na 2C (sizing já vai consultar orderbook pra dimensionar).
- Audit UI / dashboard. Tabela `risk_decisions` é audit; dashboards entram em uma fase de observabilidade dedicada.
- ML feedback loop. `trade.rejected` é publicado num subject separado pra permitir consumers futuros, mas ML não está em escopo agora.

## 4. Componentes

```
src/polycopy/
├── domain/
│   ├── events.py                                    # + OrderApproved, TradeRejected, RejectionReason
│   └── risk.py                                      # NEW — RiskDecision value object
├── ports/
│   ├── messaging.py                                 # + publish_order_approved, publish_trade_rejected
│   └── risk_decision_repository.py                  # NEW — RiskDecisionRepository Protocol
├── infrastructure/
│   ├── messaging/jetstream.py                      # + 2 publish methods (reusam _publish_with_msg_id)
│   └── persistence/
│       ├── models.py                               # + RiskDecisionRow ORM
│       └── risk_decision_repository.py             # NEW — SqlAlchemyRiskDecisionRepository
└── agents/
    └── risk.py                                     # NEW — RiskAgent

alembic/versions/
└── 0003_add_risk_decisions.py                      # NEW

tests/
├── unit/
│   ├── agents/test_risk.py                         # NEW
│   └── infrastructure/test_metrics.py              # + 4 testes (1 por métrica nova)
└── integration/
    ├── test_risk_decision_repository.py            # NEW
    └── test_risk_e2e.py                            # NEW
```

**Componentes-chave:**

- **`RejectionReason`** — Enum string com 5 razões: `SIZE_EXCEEDED`, `MARKET_NOT_CACHED`, `MARKET_INACTIVE`, `PRICE_OUT_OF_RANGE`, `INSUFFICIENT_LIQUIDITY`.
- **`OrderApproved` / `TradeRejected`** — eventos pydantic frozen+strict, mesmo shape do `WalletTradeDetected` (`event_id` UUID + `occurred_at` tz-aware + `trade: Trade`). `TradeRejected` tem campo extra `reason: RejectionReason`.
- **`RiskDecision`** — value object (frozen dataclass) com `trade_event_id: UUID`, `wallet`, `condition_id`, `token_id` (str), `decision: Literal["approved","rejected"]`, `reason: RejectionReason | None`, `decided_at: datetime`.
- **`RiskDecisionRepository`** — Protocol com `async insert(decision) -> bool` (True se nova, False se duplicate por PK — idempotência).
- **Extensão `MessagingPort`** — 2 métodos `async publish_order_approved(event)` / `async publish_trade_rejected(event)`. JetStream adapter reusa `_publish_with_msg_id(subject, event_id)` que `publish_wallet_trade_detected` já usa.
- **`RiskAgent`** — herda `AgentBase`, registra durable consumer (`risk-1`) em `wallet.trade.detected` no `start()`, callback `_handle_message` faz fetch + evaluate + persist + publish.

## 5. Schema da tabela `risk_decisions`

```sql
CREATE TABLE risk_decisions (
    trade_event_id    UUID        PRIMARY KEY,        -- = WalletTradeDetected.event_id
    wallet            TEXT        NOT NULL,
    condition_id      TEXT        NOT NULL,
    token_id          TEXT        NOT NULL,
    decision          TEXT        NOT NULL,           -- 'approved' | 'rejected'
    reason            TEXT,                            -- RejectionReason; NULL se approved
    decided_at        TIMESTAMPTZ NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT risk_decisions_decision_enum
        CHECK (decision IN ('approved', 'rejected')),
    CONSTRAINT risk_decisions_reason_consistency
        CHECK (
            (decision = 'approved' AND reason IS NULL)
            OR (decision = 'rejected' AND reason IS NOT NULL)
        )
);

CREATE INDEX idx_risk_decisions_wallet_decided_at
    ON risk_decisions (wallet, decided_at DESC);

CREATE INDEX idx_risk_decisions_rejected_decided_at
    ON risk_decisions (decided_at DESC)
    WHERE decision = 'rejected';
```

**Origem de cada campo:**

| Campo | Origem | Propósito |
|---|---|---|
| `trade_event_id` (PK) | `WalletTradeDetected.event_id` | Idempotência — re-delivery vê duplicate key |
| `wallet`, `condition_id`, `token_id` | `Trade` original | Audit query (sem FK pra `markets` — markets podem ser arquivados) |
| `decision` | Resultado do `_evaluate` | `'approved'` ou `'rejected'` |
| `reason` | `RejectionReason` enum | Por que rejeitou (NULL se aprovou) |
| `decided_at` | `datetime.now(tz=UTC)` no agent | Quando Risk decidiu (não quando gravou) |
| `created_at` | Server default `now()` | Quando gravou no DB (debug de gap entre decided/created) |

**Decisões do schema:**
- **PK = `trade_event_id`** dá idempotência grátis. `INSERT ... ON CONFLICT DO NOTHING` no repository → `rowcount` indica se foi nova.
- **`decision TEXT + CHECK`** em vez de Postgres ENUM type — mesmo padrão de `wallet_trades.side` e `markets.outcome`. Evita `ALTER TYPE` em migrations futuras.
- **`reason_consistency` CHECK** garante invariante: aprovado nunca tem reason; rejeitado sempre tem.
- **2 indexes parciais:** `wallet + decided_at DESC` (debug por wallet); `decided_at DESC WHERE rejected` (dashboards de rejeição).
- **Sem FK pra `markets`** — `condition_id`/`token_id` são duplicados do `Trade` que decidiu, intencional pra audit retroativo.

## 6. Fluxos

### 6.1 Decisão de um trade (caminho principal)

```
JetStream durable consumer "risk-1" entrega `wallet.trade.detected` payload
└─→ RiskAgent._handle_message(payload, num_delivered)
    ├─→ WalletTradeDetected.model_validate_json(payload)
    │   └─ ValidationError → métrica + ack silencioso (sem retry infinito)
    ├─→ _fetch_market(trade.token_id) -> (Market | None, cache_result)
    │   ├─ MarketRepository.get_market → CachedMarket | None
    │   ├─ se cache hit fresh → retorna (market, "hit_fresh")
    │   ├─ se cache hit stale OU miss → tenta lazy fetch via Gamma
    │   │   ├─ Gamma success → upsert via repo → retorna (fresh_market, "hit_stale" | "miss")
    │   │   ├─ Gamma fail (PolymarketUnavailableError) → fallback brando:
    │   │   │   ├─ se cache stale: aceita stale → retorna (cached.market, "hit_stale")
    │   │   │   └─ se miss: retorna (None, "miss")
    │   └─ métrica market_cache_hits_total{result=cache_result}
    ├─→ _evaluate(trade, market) -> RejectionReason | None
    │   ├─ size > MAX → SIZE_EXCEEDED
    │   ├─ market is None → MARKET_NOT_CACHED
    │   ├─ not market.is_active or market.is_archived → MARKET_INACTIVE
    │   ├─ price ∉ [MIN, MAX] → PRICE_OUT_OF_RANGE
    │   ├─ market.liquidity < MIN_LIQ → INSUFFICIENT_LIQUIDITY
    │   └─ todas passam → None (approved)
    ├─→ RiskDecision(...) construída
    ├─→ repo.insert(decision) -> bool
    │   ├─ True (nova) → segue
    │   └─ False (PK conflict, redelivery) → métrica duplicate_skip + ack silencioso
    ├─→ publish_order_approved OR publish_trade_rejected (Nats-Msg-Id = event_id)
    └─→ métricas decisions_total, decision_duration_seconds
```

**Ordem crítica `persist → publish`:** se publish falhar após DB OK, JetStream re-delivers → 2º handler vê `is_new=False` → skip. NATS dedup nativo (`Nats-Msg-Id = event_id`) protege contra publish duplicado caso Risk crash entre persist e publish (raríssimo).

### 6.2 Lazy fetch via Gamma

Risk consome `MarketRepository.get_market` que **não faz fetch externo** (decisão do 2A). Quando o resultado é stale ou None, Risk delega ao `PolymarketGammaPort.get_market(token_id)` injetado:

- Sucesso → atualiza cache via `repo.upsert_many([fresh])` (próximas decisões pegam fresh sem novo fetch).
- Falha (`PolymarketUnavailableError`) → fail-safe brando: aceita stale se houver, senão decide com `market=None` (vira `MARKET_NOT_CACHED`).

Métrica `polycopy_risk_lazy_fetch_total{result}` rastreia quanto Risk depende desse fallback.

## 7. Tratamento de falhas

| Falha | Comportamento | Métrica/Log |
|---|---|---|
| Payload malformado | Ack silencioso (sem retry infinito) | `decisions_total{result="rejected", reason="invalid_payload"}` + log warning |
| Cache miss + Gamma down | `MARKET_NOT_CACHED` (rejeição fail-safe) | `lazy_fetch_total{result="fail"}` + `decisions_total{result="rejected", reason="market_not_cached"}` |
| Cache stale + Gamma down | Aceita stale, decide normal | `lazy_fetch_total{result="fail"}` + `cache_hits_total{result="hit_stale"}` |
| DB indisponível durante `repo.insert` | Exceção propaga → JetStream redelivery (até `RISK_MAX_DELIVER=5`) | métrica não incrementada (erro fatal); log error |
| Bus indisponível durante publish | Exceção propaga → mesma redelivery; 2º handler vê `is_new=False` → skip | log warning ("publish failed but persisted") |
| Re-delivery (duplicate) | `is_new=False` → ack sem re-publish | `decisions_total{result="duplicate_skip", reason=...}` |

**Idempotência cross-attempt:** PK `trade_event_id` + NATS `Nats-Msg-Id` = garantia dupla. Mesmo trade entregue 5 vezes resulta em 1 decisão persistida e 1 evento publicado.

## 8. Observabilidade

### 8.1 Settings novas (Settings flat — débito conhecido)

```python
risk_metrics_port: int = Field(9104, alias="RISK_METRICS_PORT")
risk_max_deliver: int = Field(5, alias="RISK_MAX_DELIVER")
risk_durable_name: str = Field("risk-1", alias="RISK_DURABLE_NAME")
risk_max_trade_usdc: Decimal = Field(Decimal("100"), alias="RISK_MAX_TRADE_USDC")
risk_min_price: Decimal = Field(Decimal("0.05"), alias="RISK_MIN_PRICE")
risk_max_price: Decimal = Field(Decimal("0.95"), alias="RISK_MAX_PRICE")
risk_min_liquidity_usdc: Decimal = Field(Decimal("1000"), alias="RISK_MIN_LIQUIDITY_USDC")
risk_gamma_fetch_timeout_s: float = Field(5.0, alias="RISK_GAMMA_FETCH_TIMEOUT_S")
```

Risk consome `MARKET_CACHE_TTL_SECONDS` (já existente do 2A) pra construir o `MarketRepository`.

### 8.2 Métricas Prometheus

| Métrica | Tipo | Labels | Propósito |
|---|---|---|---|
| `polycopy_risk_decisions_total` | Counter | `result` (approved\|rejected\|duplicate_skip), `reason` | Decisões totais por outcome e razão |
| `polycopy_risk_decision_duration_seconds` | Histogram | (none) | Latência fim-a-fim. SLO alvo: P95 < 200ms (cache hit), P95 < 5s (lazy fetch) |
| `polycopy_market_cache_hits_total` | Counter | `result` (hit_fresh\|hit_stale\|miss) | Hit rate do cache do MarketRepository (adiada do 2A) |
| `polycopy_risk_lazy_fetch_total` | Counter | `result` (success\|fail) | Quanto Risk depende de Gamma. Alta contagem `success` = MarketDataAgent atrasado; `fail` = Gamma down |

### 8.3 Logs estruturados

`RiskAgent`: `event="risk_decision"` com `trade_event_id`, `wallet`, `decision`, `reason`, `cache_result`, `duration_ms`.

## 9. Testes

### 9.1 Unit (sem infra)

| File | Testes |
|---|---|
| `tests/unit/agents/test_risk.py` | (1) `_evaluate` cobre cada das 5 regras + caminho aprovado; (2) `_fetch_market` cobre 4 cenários: hit fresh, hit stale + Gamma success, hit stale + Gamma fail, miss + Gamma success, miss + Gamma fail; (3) `_handle_message` happy path approved + rejected; (4) idempotência (duplicate `is_new=False`); (5) payload malformado |
| `tests/unit/infrastructure/test_metrics.py` | +4 testes (1 por métrica nova, padrão do arquivo) |

### 9.2 Integration (Postgres + NATS reais)

| File | Testes |
|---|---|
| `tests/integration/test_risk_decision_repository.py` | (1) insert nova retorna True; (2) insert duplicate retorna False (PK conflict); (3) constraint `reason_consistency` violada quando inconsistente |
| `tests/integration/test_risk_e2e.py` | (1) publish trade no bus → DB tem decisão approved + bus tem `order.approved`; (2) trade que viola `size_exceeded` → DB tem decisão rejected + bus tem `trade.rejected`; (3) re-delivery não duplica decisão nem evento |

### 9.3 Smoke opt-in

Não há. 2A já cobre Gamma/CLOB real schema; Risk é puramente local (regras + DB + bus).

## 10. Roadmap (8 tasks)

| Task | Escopo | Reviewer |
|---|---|---|
| **T1** | Domain — `RejectionReason`, `OrderApproved`, `TradeRejected`, `RiskDecision` value object | opcional |
| **T2** | Ports — `RiskDecisionRepository` + extensão `MessagingPort` | opcional |
| **T3** | Tabela `risk_decisions` + migration `0003` + ORM `RiskDecisionRow` | opcional (DDL puro) |
| **T4** | `SqlAlchemyRiskDecisionRepository` + integration tests | obrigatório |
| **T5** | Estender `JetStreamMessagingAdapter` com 2 publish methods + tests | **obrigatório** (mexe em código de mensageria em produção) |
| **T6** | `RiskAgent` + 8 settings + 4 métricas + .env.example + unit tests | **obrigatório** (lógica principal) |
| **T7** | Container `polycopy-risk:9104` + scrape Prometheus + ARCHITECTURE.md | opcional |
| **T8** | Integration E2E `test_risk_e2e.py` | opcional |

## 11. Open questions / known debt

- **Settings flat continua dívida** — +8 campos novos. Refator pra `RiskSettings` nested ainda fora de escopo (esperando momento de consolidar todos os 4 agentes).
- **Sem dashboard de audit** — `risk_decisions` table existe mas não há UI/CLI pra consultar. Queries SQL diretas por enquanto. Dashboard entra em fase de observabilidade dedicada.
- **5 regras hardcoded sem versionamento** — se o limite mudar (ex: `MAX_TRADE_USDC` de 100 pra 500), histórico de decisões não vai dizer qual limite estava ativo. Aceitável pra MVP; versionamento entra se limites começarem a mudar com frequência.
- **Lazy fetch não tem retry interno** — uma chamada falha = `result=fail` direto. Razão: Gamma client já tem tenacity (3 tentativas) por baixo. Adicionar retry no Risk seria double-retry. Deixar como está.
- **Sem teste opt-in live** — Risk não bate em internet. Suficiente.
- **Métrica de "trades aguardando decisão"** (gauge) não incluída — JetStream já expõe lag do durable consumer via NATS metrics (não polycopy_*). Adicionar gauge próprio seria duplicação.
- **Persist→publish gap (at-least-once delivery não garantido):** se Risk crash entre `repo.insert()` (committed) e `bus.publish()`, o evento NUNCA é publicado — próxima redelivery vê `is_new=False` e skipa publish. NATS `Nats-Msg-Id` dedup protege contra duplicate, não contra missing. Aceito como caveat MVP. Solução real: transactional outbox pattern (gravar evento numa `outbox` table na mesma transação do `risk_decisions`, processo separado lê da outbox e publica com retry). Entra em hardening futuro se downstream Sizing reportar gaps.

## 12. Self-review (autor da spec)

- **Placeholder scan:** sem TBD/TODO/"implement later". Todas as 6 decisões fechadas no brainstorm.
- **Internal consistency:** os 5 reasons em `_evaluate` correspondem 1:1 ao enum `RejectionReason` da seção 4. Schema CHECK constraint reflete a invariante "approved ↔ reason NULL". Eventos publicados (`order.approved`, `trade.rejected`) batem com nomes referenciados na motivação.
- **Scope check:** focado num único plano. T1-T8 implementáveis em sequência sem decomposição adicional.
- **Ambiguity check:** "fail-safe brando" na seção 6.2 está explícito (aceita stale se houver, senão `MARKET_NOT_CACHED`). Idempotência cross-attempt explicitada na seção 7. Ordem persist→publish destacada com motivação.
