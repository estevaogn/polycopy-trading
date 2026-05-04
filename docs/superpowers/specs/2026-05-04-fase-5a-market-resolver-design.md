# Plano 5A — Market Resolution Tracker

**Data:** 2026-05-04
**Status:** spec aprovada (brainstorm humano supervisionado, 5 perguntas críticas decididas)
**Predecessor:** Fase 4 (Web3CLOBExecutor) — entregou estrutura real-mode + DRY-RUN coletando dados há ~1 dia
**Sucessor:** Plano 5B (Slippage Snapshot — preço médio esperado via orderbook), Plano 5C (PnL view + backtest tooling)

---

## 1. Contexto

Fase 4 completa entregou a estrutura de execução real-mode + pipeline DRY-RUN coletando dados 24/7 (5 tabelas: `wallet_trades`, `markets`, `risk_decisions`, `order_sizings`, `order_executions`). User pivotou prioridade pra **backtest infrastructure** após descobrir que real-mode é bloqueado por Cloudflare em IPs datacenter.

Plano 5A é a **primeira peça do backtest**: detectar quando markets do Polymarket resolvem (YES, NO, INVALID) e gravar resultado. Sem isso, `order_executions` (com `mode='dry_run'`) é uma lista de hipóteses sem PnL real — não dá pra calcular "quanto teria ganhado".

## 2. Motivação

Backtest histórico precisa de 3 dados pra cada trade hipotético:
1. **Tamanho final** (`order_executions.final_size_usdc`) ✓ já temos
2. **Preço de entrada** (`wallet_trades.price`) ✓ já temos (com hipótese de execução perfeita)
3. **Outcome final do market** (resolved YES vs NO vs INVALID) ✗ **falta — Plano 5A entrega**

Cálculo PnL hipotético por trade:
```
Se market resolveu YES e trade era BUY YES @ price 0.4 com $10 USDC:
    shares = 10 / 0.4 = 25 shares
    valor final = 25 * 1.0 (YES vence) = $25
    PnL = $25 - $10 = +$15

Se mesmo trade mas market resolveu NO:
    shares = 25
    valor final = 25 * 0.0 (YES perde) = $0
    PnL = $0 - $10 = -$10

Se market INVALID (cancelled/disputed):
    PnL = $0 (capital devolvido) — neutralizado no agregado
```

5A entrega **só a tabela `market_resolutions`**. Cálculo PnL fica em 5C (após 5B agregar slippage snapshot pra precisão de preço de entrada).

## 3. Escopo

### 3.1 Dentro de 5A

- 1 evento de domain (sem JetStream pub): `ResolvedOutcome` StrEnum (YES/NO/INVALID).
- `MarketResolution` value object + `MarketResolutionRepository` Protocol.
- `ResolvedMarketDTO` (mapper-only) — extensão DTO pro mapper Gamma capturar `closed`, `outcomePrices`, `umaResolutionStatuses` raw.
- Tabela `market_resolutions` + migration alembic 0007 + ORM.
- Adapter `SqlAlchemyMarketResolutionRepository` (insert idempotente via PK + `get_unresolved_condition_ids` LEFT JOIN).
- Extensão `PolymarketGammaPort` com `list_markets_by_condition_ids_closed`.
- Implementação no `PolymarketGammaClient` (parâmetro `condition_ids` + `closed=true`).
- Agente novo `ResolverAgent` consumindo nada do JetStream (loop polling-driven, mesma natureza do `MarketDataAgent`).
- Algoritmo de classificação: parse `outcomePrices` + tolerâncias (≥0.99/≤0.01 pra terminais; 0.45-0.55 split pra INVALID; senão pending → skip).
- Containerização `polycopy-resolver:9107`, scrape Prometheus, atualização `ARCHITECTURE.md`.
- 4 métricas Prometheus + 3 settings novos.
- Testes unit + integration + E2E.

### 3.2 Fora de 5A (entra em 5B+ ou hardening futuro)

- **Slippage snapshot**: preço médio esperado de execução via orderbook → Plano 5B.
- **Cálculo PnL hipotético**: SQL view + tooling de backtest → Plano 5C.
- **Audit dashboard**: visualização de PnL/win rate/drawdown → Fase 6 (Observability UI).
- **Real-time WebSocket subscriptions** ao Polymarket pra resolução instantânea: lag de 1h é aceito pra batch backtest. Real-time merece sua própria fase quando real-mode estiver ativo.
- **Resoluções revertidas** (UMA dispute pós-settle): mercado raríssimo. Aceito como debt — row continua YES/NO antigo até alguém revisar manualmente.
- **Backfill histórico de markets** que sumiram da `wallet_trades` antes do Resolver entrar em ação: se trade já está em wallet_trades, condition_id estará em `get_unresolved_condition_ids()` query — coberto naturalmente.
- **Polling adaptativo** (frequência maior perto de `endDate`): YAGNI pra MVP — 1h fixo basta.

## 4. Componentes

```
src/polycopy/
├── domain/
│   ├── events.py                                    # + ResolvedOutcome StrEnum
│   └── resolution.py                                # NEW — MarketResolution value object + ResolvedMarketDTO
├── ports/
│   ├── polymarket_gamma.py                          # + list_markets_by_condition_ids_closed
│   └── market_resolution_repository.py              # NEW — Protocol
├── infrastructure/
│   ├── polymarket/gamma_client.py                   # + impl list_markets_by_condition_ids_closed
│   └── persistence/
│       ├── models.py                                # + MarketResolutionRow ORM
│       └── market_resolution_repository.py          # NEW — SqlAlchemyMarketResolutionRepository
└── agents/
    └── resolver.py                                  # NEW — ResolverAgent

alembic/versions/
└── 0007_add_market_resolutions.py                   # NEW

tests/
├── unit/
│   ├── agents/test_resolver.py                      # NEW
│   ├── domain/test_resolution.py                    # NEW
│   ├── infrastructure/
│   │   ├── test_metrics.py                          # + 4 testes (1 por métrica)
│   │   └── test_gamma_client.py                     # + testes pra novo método
│   └── test_ports_typecheck.py                      # + stubs
└── integration/
    ├── test_market_resolution_repository.py         # NEW
    └── test_resolver_e2e.py                         # NEW
```

**Componentes-chave:**

- **`ResolvedOutcome`** — Enum string com 3 valores: `YES`, `NO`, `INVALID`.
- **`MarketResolution`** — value object (frozen dataclass) com `condition_id`, `resolved_outcome`, `winning_token_id` (`None` se INVALID), `closed_time`, `resolved_at`, `outcome_prices_raw`, `uma_resolution_statuses_raw`. `__post_init__` valida invariantes.
- **`ResolvedMarketDTO`** — DTO interno usado SOMENTE pelo mapper Gamma → Resolver. Carrega campos brutos extras (`closed`, `outcome_prices_raw`, `uma_resolution_statuses_raw`, `yes_token_id`, `no_token_id`, `closed_time`) que não pertencem ao `Market` value object canonical (que é só pra markets ativos).
- **`MarketResolutionRepository`** — Protocol com `async insert(resolution) -> bool` (idempotente via PK) + `async get_unresolved_condition_ids(*, limit) -> list[str]` (LEFT JOIN wallet_trades vs market_resolutions).
- **Extensão `PolymarketGammaPort`** — `async list_markets_by_condition_ids_closed(*, condition_ids: list[str], limit: int) -> list[ResolvedMarketDTO]`. Filtra Gamma com `condition_ids=...&closed=true`.
- **`ResolverAgent`** — herda `AgentBase`. Sem durable consumer (não consome JetStream). Loop a cada `RESOLVER_SYNC_INTERVAL_SECONDS=3600` chama `run_once`: lê unresolved → query Gamma → classify → insert.

## 5. Schema da tabela `market_resolutions`

```sql
CREATE TABLE market_resolutions (
    condition_id      TEXT        PRIMARY KEY,
    resolved_outcome  TEXT        NOT NULL,            -- 'YES' | 'NO' | 'INVALID'
    winning_token_id  TEXT,                             -- NULL se INVALID
    closed_time       TIMESTAMPTZ,                      -- closedTime do Gamma (when Polymarket fechou)
    resolved_at       TIMESTAMPTZ NOT NULL,             -- quando Resolver detectou
    outcome_prices_raw TEXT       NOT NULL,             -- JSON original do Gamma (audit)
    uma_resolution_statuses_raw TEXT,                   -- JSON original (pode ser '[]' ou NULL)
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT market_resolutions_outcome_enum
        CHECK (resolved_outcome IN ('YES', 'NO', 'INVALID')),
    CONSTRAINT market_resolutions_winning_token_consistency
        CHECK (
            (resolved_outcome IN ('YES', 'NO') AND winning_token_id IS NOT NULL)
            OR (resolved_outcome = 'INVALID' AND winning_token_id IS NULL)
        )
);

CREATE INDEX idx_market_resolutions_resolved_at
    ON market_resolutions (resolved_at DESC);
CREATE INDEX idx_market_resolutions_outcome
    ON market_resolutions (resolved_outcome);
```

**Origem de cada campo:**

| Campo | Origem | Propósito |
|---|---|---|
| `condition_id` (PK) | filtro de `wallet_trades.condition_id` distinct | 1 row por mercado, idempotência grátis |
| `resolved_outcome` | parser de `outcomePrices` no Gamma | YES/NO/INVALID |
| `winning_token_id` | derivado de `Market.token_id` (lado que ganhou) | Audit retroativo + acelera JOIN PnL no 5C |
| `closed_time` | Gamma `closedTime` | Quando Polymarket fechou (input pra análise lag detection→close) |
| `resolved_at` | `datetime.now(tz=UTC)` | Quando Resolver detectou |
| `outcome_prices_raw` | Gamma `outcomePrices` (string JSON) | Re-processamento se parser tiver bug |
| `uma_resolution_statuses_raw` | Gamma `umaResolutionStatuses` (string JSON, pode ser `'[]'` ou null) | Audit do estado UMA na detecção |
| `created_at` | server default `now()` | Timestamp de gravação |

**Decisões do schema:**
- **PK = `condition_id`** (TEXT, não UUID — Polymarket usa hex address `0x...`). 1 row por mercado. Idempotência grátis.
- **`winning_token_id` redundante** com JOIN em `markets`, mas guardado pra robustez (markets podem ser arquivados/deletados, perdendo o link).
- **Audit raw JSON** em ambas colunas — estratégia "preserve what we got" pra debug futuro.
- **2 indexes não-parciais**: `resolved_at DESC` (queries cronológicas) e `resolved_outcome` (dashboards de viés global).
- **Sem FK** com `markets` — `markets` pode ser arquivado/deletado entre runs (TTL). Audit retroativo continuará funcionando.

## 6. Fluxos

### 6.1 Loop principal (a cada 1h)

```
ResolverAgent.run_once()
├─→ async with repo_factory() as repo:
│       unresolved = await repo.get_unresolved_condition_ids(limit=100)
│   (LEFT JOIN wallet_trades vs market_resolutions, retorna até 100 condition_ids únicos
│    que ainda não temos resolução)
├─→ se unresolved vazio: log "no_unresolved" + return early
├─→ markets = await gamma.list_markets_by_condition_ids_closed(
│       condition_ids=unresolved, limit=len(unresolved)
│   )
│   (Gamma /markets?condition_ids=...&closed=true — retorna 0..N markets fechados)
├─→ pra cada market_dto:
│       resolution = self._classify_resolution(market_dto)
│       se resolution is None (pending UMA): skip silencioso
│       senão: repo.insert(resolution)
├─→ Métricas: sync_total{result=ok}, resolutions_detected_total{outcome}, unresolved_pending Gauge
├─→ Log estruturado: resolver_sync_completed (unresolved_checked, resolutions_detected, duration_ms)
```

### 6.2 Algoritmo de classificação `_classify_resolution`

Lógica determinística com tolerâncias:

```python
def _classify_resolution(market: ResolvedMarketDTO) -> MarketResolution | None:
    if not market.closed:
        return None  # nunca devia chegar (query filtrou closed=true) — defensivo

    prices = json.loads(market.outcome_prices_raw)  # ex: '["0.0", "1.0"]'
    yes_price, no_price = Decimal(prices[0]), Decimal(prices[1])

    # Settled YES (tolerância 0.01 pra rounding)
    if yes_price >= Decimal("0.99") and no_price <= Decimal("0.01"):
        return MarketResolution(
            ..., resolved_outcome=YES, winning_token_id=market.yes_token_id, ...
        )

    # Settled NO (tolerância idem)
    if no_price >= Decimal("0.99") and yes_price <= Decimal("0.01"):
        return MarketResolution(
            ..., resolved_outcome=NO, winning_token_id=market.no_token_id, ...
        )

    # INVALID (split 50/50 com tolerância 0.05)
    if (Decimal("0.45") <= yes_price <= Decimal("0.55")
        and Decimal("0.45") <= no_price <= Decimal("0.55")):
        return MarketResolution(
            ..., resolved_outcome=INVALID, winning_token_id=None, ...
        )

    # Preços não-terminais (ex: ["0.7", "0.3"]) — UMA ainda processando
    return None  # skip — próximo polling em 1h re-checa
```

**Tolerâncias decididas no brainstorm:**
- 0.01 pra terminais (cobre rounding floats Polymarket).
- 0.05 pra INVALID (cobre `["0.49","0.51"]`).
- Faixa cinzenta (ex: `["0.7","0.3"]`) → pending intencional.

## 7. Tratamento de falhas

| Falha | Comportamento | Métrica/Log |
|---|---|---|
| `wallet_trades` vazio | `unresolved=[]` → early return | log "resolver_sync_no_unresolved" (info, não warning) |
| Gamma timeout/5xx/429 | Tenacity retry interno do `PolymarketGammaClient` (3x exponential). Falha após retry → exception propaga | `resolver_sync_total{result=fail}` + log warning |
| Gamma retorna 0 markets pra batch (todos pending) | Loop completa normal — só não insere nada | `resolutions_detected_total` não incrementa |
| `repo.insert()` retorna False (race duplicate) | Skip silencioso. Idempotência preserva integridade | métrica `resolutions_detected_total` não conta duplicate |
| Parse JSON `outcomePrices` falha (formato inesperado) | Exception → captura no try/except do `run_once` → métrica fail + log | warning com payload preview |
| `condition_id` muito longo pra batch URL | Trunca em `BATCH_SIZE=100`. Próximo polling pega o resto | OK — convergência em 2-3 polls |
| Postgres indisponível | Exception → captura → próximo polling em 1h | warning |
| `ResolvedMarketDTO` vem incompleto (Gamma omite `umaResolutionStatuses`) | `uma_resolution_statuses_raw=None` no insert (coluna nullable) | sem ação especial |

## 8. Observabilidade

### 8.1 Settings novas (3 — Settings flat continua dívida)

```python
# Resolver agent (Plano 5A)
resolver_metrics_port: int = Field(9107, alias="RESOLVER_METRICS_PORT")
resolver_sync_interval_s: float = Field(3600.0, alias="RESOLVER_SYNC_INTERVAL_SECONDS")
resolver_batch_size: int = Field(100, alias="RESOLVER_BATCH_SIZE")
```

Reuso de Settings da Fase 2A: `gamma_api_base_url`. Nenhum setting novo de wallet/RPC (Resolver não toca chain — só Gamma + Postgres).

### 8.2 Métricas Prometheus (4 novas)

| Métrica | Tipo | Labels | Propósito |
|---|---|---|---|
| `polycopy_resolver_sync_total` | Counter | `result` (ok\|fail) | Iterações de sync |
| `polycopy_resolver_sync_duration_seconds` | Histogram | (none) | Latência fim-a-fim |
| `polycopy_resolver_resolutions_detected_total` | Counter | `outcome` (yes\|no\|invalid) | Resoluções gravadas |
| `polycopy_resolver_unresolved_pending` | Gauge | (none) | Backlog atual de condition_ids unresolved |

### 8.3 Logs estruturados

`ResolverAgent`:
- `event="resolver_sync_completed"` — info — `unresolved_checked`, `resolutions_detected`, `duration_ms`.
- `event="resolver_sync_no_unresolved"` — info — sem dados pra processar (esperado em runs após backlog drenado).
- `event="resolver_sync_failed"` — warning — `error`, `error_type`, `unresolved_checked` (até onde chegou antes de falhar).

## 9. Testes

### 9.1 Unit (sem infra)

| File | Testes |
|---|---|
| `tests/unit/domain/test_resolution.py` | (1) `MarketResolution` invariantes (YES/NO ↔ winning_token_id; INVALID ↔ no winning_token); (2) tz-aware `resolved_at`/`closed_time`; (3) `ResolvedOutcome` enum values; ~14 testes total. |
| `tests/unit/agents/test_resolver.py` | (1) `_classify_resolution` 6 cenários (settled YES, NO, INVALID, pending non-terminal, edge rounding 0.999/0.001, edge INVALID 0.49/0.51); (2) `run_once` happy path; (3) `run_once` empty unresolved; (4) `run_once` Gamma exception → record_failure; (5) idempotência (duplicate insert returns False); ~12 testes. |
| `tests/unit/infrastructure/test_metrics.py` | +4 testes (1 por métrica nova) |
| `tests/unit/infrastructure/test_gamma_client.py` | +3 testes pra `list_markets_by_condition_ids_closed` (params corretos, parse `closed=true`, tolerância erro) |
| `tests/unit/test_ports_typecheck.py` | +stub `_FakeMarketResolutionRepo` + helper |

### 9.2 Integration (Postgres real)

| File | Testes |
|---|---|
| `tests/integration/test_market_resolution_repository.py` | (1) insert new returns True; (2) insert duplicate returns False; (3) `get_unresolved_condition_ids` LEFT JOIN works; (4) CHECK `outcome_enum` violation (raw SQL); (5) CHECK `winning_token_consistency` violation; (6) Protocol typecheck. ~7 testes. |
| `tests/integration/test_resolver_e2e.py` | (1) E2E YES detection — fixture wallet_trade + Gamma fake response settled YES → DB tem row YES; (2) E2E INVALID detection — Gamma fake 50/50 → DB tem row INVALID; (3) E2E pending skipped — Gamma fake 0.7/0.3 → DB sem row, próximo poll re-tenta. ~3 testes. |

### 9.3 Smoke opt-in

Não há. Resolver é puramente lógica + DB + Gamma read. 2A já tem smoke pra Gamma real.

## 10. Roadmap (8 tasks)

| Task | Escopo | Reviewer |
|---|---|---|
| **T1** | Domain — `ResolvedOutcome` enum, `MarketResolution`, `ResolvedMarketDTO` + 14 testes unit | opcional |
| **T2** | Ports — `MarketResolutionRepository` Protocol + extensão `PolymarketGammaPort` + atualização `test_ports_typecheck.py` | opcional |
| **T3** | Tabela `market_resolutions` + migration `0007` + ORM `MarketResolutionRow` | opcional (DDL puro) |
| **T4** | `SqlAlchemyMarketResolutionRepository` + 7 integration tests | opcional |
| **T5** | Estensão `PolymarketGammaClient` com `list_markets_by_condition_ids_closed` + 3 testes respx | opcional |
| **T6** | `ResolverAgent` + 3 settings + 4 métricas + .env.example + 12 unit tests + função `_classify_resolution` | **obrigatório** (lógica principal — sensível a edge cases de pricing/UMA) |
| **T7** | Container `polycopy-resolver:9107` + scrape Prometheus + ARCHITECTURE.md | opcional |
| **T8** | Integration E2E `test_resolver_e2e.py` (3 testes) | opcional |

**Estimativa total:** ~600-800 linhas de produção + ~500-600 linhas de testes. Comparable com Plano 2A ou 2B.

**Cadência:** subagent-driven com checkpoint humano por task (mesma das fases anteriores). T6 com code reviewer obrigatório (lógica de classification tem múltiplos edge cases sensíveis). Implementer NÃO commita; user aprova cada commit.

## 11. Open questions / known debt

- **Settings flat continua dívida** — +3 vars novas. Refator pra `<Agent>Settings` nested fica em hardening.
- **Persist→publish gap** — não aplica (Resolver não publica eventos JetStream).
- **`BATCH_SIZE=100` fixo**: se `wallet_trades` cobrir muitos markets nicho (>1000 condition_ids), backlog leva 10+ polls (10h) pra drenar. Aceito pra MVP (volume esperado 100-300 condition_ids únicos). Hardening: adaptive batch size ou paginação multi-call.
- **Markets `closed=true` mas Polymarket nunca settled** (UMA disputed permanente): rows ficam pra sempre em pending state. Aceito como debt — raríssimo em produção.
- **Tolerâncias de pricing** (0.99, 0.01, 0.45-0.55): hardcoded no código. Se Polymarket mudar formato, tolerâncias podem precisar ajuste. Mitigação: testes unit cobrem boundary cases; ajuste é 1 linha.
- **Fronteira gray (0.7/0.3 não-terminal)**: marcamos pending, mas na prática alguns markets ficam stuck nesse estado. Sem mecanismo de timeout — se UMA não settle em meses, condition_id fica em `unresolved` indefinidamente. Aceito.
- **`get_unresolved_condition_ids` query**: full table scan pequeno (< 1000 rows típico). Sem índice em `wallet_trades.condition_id` ainda — adicionar se virar gargalo (improvável em horizonte de 1 ano).
- **Sem WebSocket subscription** ao Polymarket pra resolução em tempo real: lag de 1h aceitável pra backtest. Real-time merece fase própria quando real-mode estiver ativo.
- **Backfill via reprocessamento**: se Resolver tiver bug e gravar resolução errada, manualmente `DELETE FROM market_resolutions WHERE condition_id IN (...)` e próximo polling re-detecta. Audit raw JSON cobre re-classificação.

## 12. Self-review (autor da spec)

- **Placeholder scan**: sem TBD/TODO. 5 decisões fechadas no brainstorm.
- **Internal consistency**: schema CHECK constraints (§5) batem com invariantes do `MarketResolution.__post_init__` (§4). Tolerâncias de classificação (§6.2) batem com tabela de edge cases (§7). 4 métricas (§8.2) batem com instrumentação descrita em §6.1 (`run_once` flow).
- **Scope check**: focado num único plano. T1-T8 implementáveis sequencialmente. Próximos planos (5B, 5C) explicitamente fora.
- **Ambiguity check**: tolerâncias `≥0.99/≤0.01/0.45-0.55` cravadas. Pending = "non-terminal price OR fora das tolerâncias terminais" — documentado em código + testes cobrem boundary. PK `condition_id` (não composite) cravada. Filtro Gamma `closed=true` (não `archived=true`) cravado.

**Decisões críticas tomadas explicitamente pelo usuário (brainstorm humano):**
1. Escopo: condition_ids de `wallet_trades` (cobertura full).
2. Topologia: agente novo `polycopy-resolver:9107`.
3. Frequência: 1h.
4. Pending: ignorar até UMA settled (`market_resolutions` puramente append-only).
5. Schema: PK `condition_id` + audit raw JSON.
