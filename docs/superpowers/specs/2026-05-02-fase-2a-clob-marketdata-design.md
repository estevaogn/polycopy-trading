# Plano 2A — CLOB client + Market Data sync

**Data:** 2026-05-02
**Status:** spec aprovada, aguarda plano de implementação
**Predecessor:** Fase 1 (Planos 1A/1B/1C) — fundação, adapters, agentes watcher/notifier
**Sucessor:** Plano 2B (Risk agent), Plano 2C (Sizing agent)

---

## 1. Contexto

A Fase 2 do roadmap em `PROMPT_POLYCOPY_v2.md` reúne três subsistemas — WebSocket CLOB, Risk e Sizing — que são suficientemente independentes pra justificar specs e planos separados. Decomposição acordada: **2A (CLOB + market data) → 2B (Risk) → 2C (Sizing)**, cada um seguindo o ciclo brainstorm → spec → plano → execução do superpowers.

Este documento é a spec do **2A**. Entrega a infraestrutura de dados de mercado que o Risk (2B) vai consumir pra decidir aprovar/rejeitar trades detectados.

## 2. Motivação

O agente `risk` (2B) precisa, ao receber `wallet.trade.detected`, responder em segundos com aprovação ou rejeição. Pra isso ele consulta:

- **Orderbook ao vivo** do token alvo — pra calcular preço esperado e checar slippage máximo (200 bps).
- **Metadata do mercado** — status (ativo/fechado), data de resolução (limite > 72h), volume 24h (limite ≥ $50k), liquidez.

Orderbook é dado volátil; metadata é dado morno (muda em escala de minutos). 2A separa essas duas naturezas: orderbook é sempre fresh (REST por chamada), metadata é cacheada em DB (`markets`) com background sync + lazy fallback.

## 3. Escopo

### 3.1 Dentro de 2A

- `OrderBook` e `Market` como value objects no domain.
- Ports: `PolymarketClobPort`, `PolymarketGammaPort`, `MarketRepository`.
- Adapters REST: `PolymarketClobClient` (orderbook), `PolymarketGammaClient` (metadata).
- Tabela `markets` + migration alembic.
- Adapter de persistência: `SqlAlchemyMarketRepository` com lazy fallback baseado em TTL.
- Agente novo `MarketDataAgent` com loop de sync periódico via Gamma.
- Containerização do agente, scrape Prometheus, atualização do `ARCHITECTURE.md`.
- Métricas Prometheus específicas (sync, cache hits, latência HTTP por client).
- Testes unit, integration e smoke opt-in (`@pytest.mark.live`).

### 3.2 Fora de 2A

- WebSocket do CLOB (`wss://ws-subscriptions-clob.polymarket.com/ws/`). Decisão: REST sob demanda atende o alvo de latência da Fase 2 (detection → submission < 5s); WS entra como otimização futura quando latência REST virar gargalo medido.
- Lógica de risco propriamente dita — entra no 2B.
- Sizing — entra no 2C.
- Background daemon de sync com critério dinâmico (ex: "sincroniza só mercados em que wallets observadas tradem"). Entra mais tarde se o critério "top N por volume" se mostrar inadequado.
- Suporte a mercados n-ários. Assume todos binários (Yes/No) — válido pro escopo atual da Polymarket.
- Cliente Data API novo. O cliente Data já existe (Plano 1B/T3) e continua dedicado a `/activity`/`/positions` de wallets, sem sobreposição com Gamma/CLOB.

## 4. Componentes

```
src/polycopy/
  domain/
    market.py                            # OrderBook, Market (value objects)
  ports/
    polymarket_clob.py                   # PolymarketClobPort       (Protocol)
    polymarket_gamma.py                  # PolymarketGammaPort      (Protocol)
    market_repository.py                 # MarketRepository         (Protocol)
  infrastructure/
    polymarket/
      clob_client.py                     # PolymarketClobClient     (httpx + tenacity + métricas)
      gamma_client.py                    # PolymarketGammaClient    (httpx + tenacity + métricas)
    persistence/
      models.py                          # + MarketRow ORM
      market_repository.py               # SqlAlchemyMarketRepository
  agents/
    marketdata.py                        # MarketDataAgent (AgentBase)

migrations/
  versions/<rev>_add_markets.py          # alembic
```

### 4.1 Responsabilidades

- **`PolymarketClobClient`** — wrapper REST CLOB (`https://clob.polymarket.com`). Foco em orderbook ao vivo. Sem cache em DB. Sempre fresh. Retorna `OrderBook` em memória.
- **`PolymarketGammaClient`** — wrapper REST Gamma (`https://gamma-api.polymarket.com`). Foco em metadata de mercados. É a fonte primária dos campos persistidos em `markets`. Métodos esperados: `get_market(token_id)`, `list_active_markets(limit, sort_by_volume_24h_desc)`.
- **`MarketRepository`** — leitura/escrita da tabela `markets`. Read-through cache pra metadata, com TTL. `upsert_many` idempotente pro sync periódico.
- **`MarketDataAgent`** — agente novo, herda de `AgentBase`. Loop: dorme `SYNC_INTERVAL`, chama `gamma.list_active_markets`, hidrata `markets` via repo. Heartbeat, métricas, graceful shutdown — mesmo padrão do watcher/notifier.
- **`OrderBook`, `Market`** — value objects no domain. `OrderBook` não persiste; `Market` é serializado pra/de `MarketRow`. `MarketRow` carrega metadados de cache (`last_synced_at`, `created_at`, `updated_at`) que `Market` não conhece.

## 5. Schema da tabela `markets`

```sql
CREATE TABLE markets (
    token_id          TEXT        PRIMARY KEY,
    condition_id      TEXT        NOT NULL,
    question          TEXT        NOT NULL,
    slug              TEXT,
    outcome           TEXT        NOT NULL,           -- "Yes" / "No"
    end_date          TIMESTAMPTZ,
    is_active         BOOLEAN     NOT NULL,
    is_archived       BOOLEAN     NOT NULL DEFAULT false,
    volume_24h_usdc   NUMERIC(20, 6),
    liquidity_usdc    NUMERIC(20, 6),
    last_synced_at    TIMESTAMPTZ NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_markets_condition_id    ON markets (condition_id);
CREATE INDEX idx_markets_active_end_date ON markets (end_date)            WHERE is_active = true;
CREATE INDEX idx_markets_volume_24h      ON markets (volume_24h_usdc DESC NULLS LAST) WHERE is_active = true;
```

**Origem de cada campo:**

| Campo | Origem | Consumidor (no 2B) |
|---|---|---|
| `token_id` (PK) | Gamma `/markets` | identificador do book CLOB |
| `condition_id` | Gamma | pareamento Yes/No no mesmo mercado |
| `question`, `slug`, `outcome` | Gamma | mensagem humana / notificação |
| `end_date` | Gamma | limite "> 72h até resolução" |
| `is_active`, `is_archived` | Gamma | limite "mercado ativo" |
| `volume_24h_usdc` | Gamma | limite "$50k volume 24h" |
| `liquidity_usdc` | Gamma | sinal complementar de liquidez |
| `last_synced_at` | escrita local | TTL pro lazy fallback |

**Decisões:**

- `token_id` é PK (não `condition_id`) porque cada mercado tem 2 tokens (Yes/No), e o Risk avalia uma posição por token.
- Tabela regular, não hypertable (cresce devagar, ~milhares de linhas).
- Sem `ON DELETE CASCADE` — consistência com a regra de auditoria.
- TTL default 30 minutos (`MARKET_CACHE_TTL_SECONDS=1800`), configurável.

## 6. Fluxo de dados

### 6.1 Sync periódico (background)

```
MarketDataAgent (loop a cada SYNC_INTERVAL, default 300s)
    └─→ PolymarketGammaClient.list_active_markets(limit=N, sort=volume_24h_desc)
        └─→ MarketRepository.upsert_many(markets)   -- ON CONFLICT (token_id) DO UPDATE
```

### 6.2 Leitura sob demanda (Risk no 2B)

```
Risk Agent (on wallet.trade.detected)
    ├─→ MarketRepository.get_market(token_id)
    │       ├─ row fresh (last_synced_at + TTL > now)? → retorna do DB
    │       └─ row stale ou ausente?
    │              └─→ PolymarketGammaClient.get_market(token_id)
    │                     └─→ MarketRepository.upsert(market) → retorna fresh
    │
    └─→ PolymarketClobClient.get_book(token_id)   -- sempre fresh, sem cache
```

### 6.3 Decisões do fluxo

- **Sync periódico não bloqueia leitura.** Se o agente `marketdata` cair, Risk continua via lazy fallback (degradação graciosa). Heartbeat atrasado dispara alerta no watchdog (Fase 7) mas não derruba o copy trading.
- **Top N do sync:** default `MARKETDATA_TOP_N=200`, `MARKETDATA_SYNC_INTERVAL_SECONDS=300`. Critério: `is_active=true`, ordenado por `volume_24h_usdc DESC`.
- **Concorrência no upsert lazy:** `ON CONFLICT (token_id) DO UPDATE` resolve corrida entre sync periódico e fetch lazy. Last writer wins — aceitável (ambos têm dados ~frescos da Gamma).
- **OrderBook stateless:** sem cache, sempre HTTP GET. Métricas de latência permitem detectar regressão depois.
- **Idempotência do sync:** se o agente reinicia no meio de uma iteração, próxima refaz o batch inteiro. Sem checkpoint.

## 7. Error handling e retry

Mesma estratégia do 1B (data client):

- `tenacity` com `stop_after_attempt(3)`, `wait_exponential(multiplier=0.2, max=2.0)`.
- Retry em: `httpx.TransportError`, HTTP 429, HTTP 5xx.
- Não retry em: 4xx (exceto 429), parsing errors.
- Timeout: `httpx.Timeout(connect=2.0, read=5.0, write=2.0, pool=5.0)`.
- Após 3 tentativas → exceção tipada `PolymarketUnavailableError`. Quem chama decide.

### 7.1 Comportamento sob falha

| Cenário | `MarketDataAgent` | `MarketRepository.get_market` (caminho lazy) |
|---|---|---|
| Gamma 5xx persistente | log error, métrica `polycopy_marketdata_sync_failed_total`, dorme `SYNC_INTERVAL`, retenta | propaga `PolymarketUnavailableError`; Risk decide (provavelmente rejeita — fail-safe) |
| Linha não existe e Gamma fora | n/a | propaga `PolymarketUnavailableError` |
| Linha stale e Gamma fora | n/a | retorna a linha stale com flag `is_stale=True`; Risk decide se aceita stale (default no 2B: rejeita — fail-safe) |
| CLOB book fora | n/a | `PolymarketClobClient.get_book` levanta; Risk rejeita |

## 8. Observabilidade

### 8.1 Métricas Prometheus

```
polycopy_http_request_duration_seconds{client="clob",  endpoint, status}   # histogram (estende a métrica do 1B)
polycopy_http_request_duration_seconds{client="gamma", endpoint, status}   # histogram (estende)
polycopy_marketdata_sync_total{result="ok"|"fail"}                         # counter
polycopy_marketdata_sync_duration_seconds                                  # histogram
polycopy_marketdata_markets_tracked                                        # gauge
polycopy_clob_book_fetch_total{result="ok"|"fail"}                         # counter
```

**Métrica de cache (`polycopy_market_cache_hits_total{result="hit"|"stale"|"miss"}`) fica fora do 2A.** O `MarketRepository` apenas lê do DB e expõe a flag `is_stale` no `CachedMarket`; quem decide "aceita stale / refaz fetch" é o caller (Risk no Plano 2B). A métrica nasce no consumer junto da decisão. Será especificada no 2B.

A métrica `polycopy_http_request_duration_seconds` já existe (Plano 1B/T3). Estendemos com novos labels `client="clob"|"gamma"` ao invés de criar métricas paralelas — single source of truth.

### 8.2 Logs estruturados

- `MarketDataAgent`: `event="sync.started"`, `event="sync.completed"` com `markets_synced`, `duration_ms`.
- Clients HTTP: `event="http.request"` com `client`, `endpoint`, `status`, `duration_ms`. Sem body (privacidade + ruído).

### 8.3 Settings novas

```python
GAMMA_API_BASE_URL: str = "https://gamma-api.polymarket.com"
CLOB_API_BASE_URL:  str = "https://clob.polymarket.com"
MARKETDATA_TOP_N: int = 200
MARKETDATA_SYNC_INTERVAL_SECONDS: int = 300
MARKET_CACHE_TTL_SECONDS: int = 1800
```

`Settings` flat ganha 5 campos novos. O split em sub-models (`PostgresConfig`, `NotifierConfig`, `PolymarketConfig`, `MarketDataConfig`) continua sendo dívida técnica conhecida do 1C — fica fora do escopo do 2A.

## 9. Estratégia de testes

```
tests/unit/
  domain/test_market.py                         # invariantes de OrderBook e Market
  infrastructure/test_clob_client.py            # respx: parse, retry 5xx, timeout, 4xx propaga
  infrastructure/test_gamma_client.py           # respx: parse, list_active_markets, paginação se houver
  infrastructure/test_market_repository_unit.py # opcional, só se houver lógica de TTL fora do SQL
  agents/test_marketdata.py                     # AgentBase: loop, upsert, métricas, shutdown gracioso
                                                # com Protocols mockados

tests/integration/
  test_market_repository.py                     # SqlAlchemyMarketRepository contra Postgres real:
                                                #   upsert_many idempotente; get_market fresh/stale/miss;
                                                #   lazy fallback dispara fetch + upsert
  test_marketdata_e2e.py                        # MarketDataAgent + Postgres + Gamma fake (respx):
                                                #   1 ciclo popula, 2º ciclo idempotente, shutdown limpo
  test_polymarket_smoke.py                      # opt-in @pytest.mark.live, PYTEST_LIVE=1:
                                                #   bate Gamma e CLOB reais; valida schema atual
```

### 9.1 Decisões

- **Unit usa respx + Protocols mockados.** Pra agente, mock dos ports (não do client concreto). Testes rápidos e isolados.
- **Integration de Gamma/CLOB usa httpx fake** (respx no integration também), não rede real. Determinismo > realismo aqui.
- **Smoke `@pytest.mark.live` é opt-in.** Default `pytest` não bate em rede externa (CI estável). Roda manualmente antes de release ou quando suspeitar de quebra de schema da Polymarket.
- **Fixtures de respostas reais.** Lição aprendida do 1C (commit `860b264`): testes com schema imaginário passaram em CI mas quebraram em prod. Pra clob_client e gamma_client, fixtures dos testes são **respostas reais capturadas** da API (1 amostra por endpoint, salva em `tests/fixtures/polymarket/`), não JSON inventado. **A captura é pré-requisito da T3** (ver §10).
- **Coverage:** alvo geral ≥ 75% (threshold do CI). `domain/market.py` e portas devem dar 100% trivialmente.
- **Suíte continua passando após 2A:** total esperado ~125-135 testes (113 atuais + ~15-20 novos).

## 10. Sequência de tasks

| # | Task | Entregável | Verificação |
|---|---|---|---|
| **T1** | Domain `OrderBook` + `Market` | `domain/market.py` + `tests/unit/domain/test_market.py` | unit verde, mypy strict, ≥ 90% cov no arquivo |
| **T2** | Ports `PolymarketClobPort`, `PolymarketGammaPort`, `MarketRepository` | 3 Protocols + `test_ports_typecheck.py` estendido | mypy strict, suíte verde |
| **T3** | `PolymarketGammaClient` (REST + tenacity + métricas) | adapter + unit (respx) + fixture real | unit verde, métricas registradas |
| **T4** | `PolymarketClobClient` (REST + tenacity + métricas) | adapter + unit (respx) + fixture real | idem |
| **T5** | Tabela `markets` + migration alembic + `MarketRow` ORM | `migrations/versions/<rev>_add_markets.py` + atualização em `infrastructure/persistence/models.py` | `alembic upgrade head` + `downgrade -1` ok |
| **T6** | `SqlAlchemyMarketRepository` com `upsert_many`, `get_market` (lazy + TTL) | adapter + integration tests Postgres real | integration verde, fresh/stale/miss cobertos |
| **T7** | `MarketDataAgent` (loop sync, AgentBase, settings, métricas) | `agents/marketdata.py` + unit + integration E2E | E2E verde, heartbeat + métricas observáveis |
| **T8** | Containerização: serviço `marketdata` no compose; scrape Prometheus; `ARCHITECTURE.md` atualizado | compose entry + docs | `docker compose up -d marketdata` healthy; `/metrics` responde |
| **T9** | Smoke E2E real (opt-in) | `tests/integration/test_polymarket_smoke.py` | passa com `PYTEST_LIVE=1` em ambiente com internet |

**Cadência:** mesma do 1C — implementer subagent NÃO commita; controller mostra working tree, pede confirmação humana, commita. Pausa antes de `git add`/`git commit`.

**Dependências:** T1 → T2 → (T3, T4, T5 paralelos) → T6 → T7 → T8 → T9. T3 e T4 podem rodar em paralelo se dispatchar 2 implementers; default sequencial pra checkpoint humano por task.

**Pré-requisito de T3 (e T4):** capturar uma resposta real de cada endpoint (`gamma-api.polymarket.com/markets?limit=2` e `clob.polymarket.com/book?token_id=<token>`) e salvar como fixture antes de escrever o parser. Sem isso, repete-se o erro do 1C de testar contra schema imaginário.

## 11. Riscos conhecidos e dúvidas abertas

- **Schema real da Gamma e CLOB.** Os endpoints, parâmetros e shapes do JSON precisam ser validados contra resposta real antes de codar. T3/T4 começam capturando fixtures.
- **Mercados n-ários.** Schema atual assume binário (Yes/No). Se Polymarket lançar n-ário, vira problema futuro — provavelmente Fase 6+ (Discovery).
- **Settings flat.** `Settings` cresce mais 5 campos. Split em sub-models continua dívida técnica conhecida desde o 1C; não tratar agora.
- **WebSocket no futuro.** Quando latência REST virar gargalo medido (ainda não medido), migrar pra WS no path de Risk. Spec separada futura.
- **Wallet-driven sync.** Default top-N por volume pode deixar Risk pagando lazy fetch frequente se as wallets observadas tradem em mercados frios. Se isso acontecer, evoluir critério pra "mercados onde wallets observadas tradem" — mudança incremental no `MarketDataAgent`, sem trocar arquitetura.
- **Rate limiting da Gamma.** Não documentado aqui — vamos descobrir na T3 capturando fixtures. Se for restritivo, pode obrigar a aumentar `SYNC_INTERVAL` ou diminuir `TOP_N`.

---

**Próximo passo:** após review humana desta spec, invocar `superpowers:writing-plans` pra detalhar o plano de implementação task a task, com steps bite-sized, comandos de verificação e estrutura compatível com `superpowers:subagent-driven-development`.
