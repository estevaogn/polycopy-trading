# Plano 1C — Design dos Agentes (Watcher + Notifier)

**Data:** 2026-05-01
**Escopo:** Fase 1 do PROMPT_POLYCOPY_v2.md, passos 1.11–1.14
**Status:** Aprovado para implementação

## 1. Objetivo

Implementar os dois primeiros agentes concretos do sistema:

- `agents/watcher` — faz polling da Polymarket Data API por wallet, detecta trades novos, persiste no Postgres com dedup, e publica `WalletTradeDetected` num stream JetStream.
- `agents/notifier` — consome o stream com durable consumer, formata a notificação e envia via Telegram.

Ambos rodam como containers Docker isolados (`polycopy-watcher`, `polycopy-notifier`), gerenciados pelo `docker-compose.yml` ao lado da infra existente.

Este plano também migra o `NatsMessagingBus` de core NATS para JetStream (necessário pra durable consumer no notifier).

## 2. Decisões-chave (com motivação)

| # | Decisão | Motivação |
|---|---------|-----------|
| 2.1 | Plano 1C cobre 1.11–1.14 num único arco de 6 tasks | Continuidade; checkpoints granulares por task |
| 2.2 | YAML `config/wallets_seed.yaml` como fonte das wallets monitoradas; passo 1.11 começa com env var `WATCH_WALLETS`, 1.12 migra pro YAML | Espelha esboço do prompt; valida polling antes de adicionar parser |
| 2.3 | Lib Telegram: `aiogram>=3.13` | Async-first, menor que `python-telegram-bot`, futuro-prova pra Commander (Fase 3) |
| 2.4 | Containers Docker separados, `/metrics` em portas dedicadas (9101/9102) | Isolamento de falhas; bate com convenção `polycopy-*` |
| 2.5 | Cursor `since` por wallet vem de `repo.latest_occurred_at`; bootstrap = `now - 24h` se cursor for `None` | Reaproveita repositório existente; sem nova tabela; resiliente a restart |
| 2.6 | Watcher após esgotar retries do client: loga + métrica + continua loop (W1) | Absorve instabilidade transitória da Data API sem ciclos de restart |
| 2.7 | Notifier com JetStream durable consumer (N2): substitui `NatsMessagingBus` core por JetStream agora (M1) | Garante entrega via ack/redelivery; migrar depois seria refazer mesmo trabalho |
| 2.8 | JetStream stream `WALLET_TRADES` (subject `wallet.trade.>`, max_age 7d, replicas 1, file storage); consumer `notifier-1` push, ack-explicit, ack_wait 30s, max_deliver 5 | Configuração mínima para a fase; janela de 7 dias é folgada |
| 2.9 | Dedup duas camadas: PK no Postgres + `Nats-Msg-Id` JetStream por `tx_hash:log_index` | Defesa em profundidade |
| 2.10 | Métricas dos agentes adicionadas ao `Metrics` dataclass existente | Centralização do registry |

## 3. Arquitetura (data flow)

```
                    Polymarket Data API
                            │
                  (httpx + tenacity retry 5xx)
                            │
                  ┌─────────▼──────────┐
                  │  agents/watcher    │  (container polycopy-watcher)
                  │  AgentBase loop    │     /metrics:9101
                  │  WATCHER_INTERVAL_S │
                  └─────────┬──────────┘
                            │
              insert_if_absent (PK dedup)
                            │
                  ┌─────────▼──────────┐         ┌─────────────────────┐
                  │  Postgres          │◄────────┤ latest_occurred_at  │
                  │  wallet_trades     │  cursor │   (próxima iter)    │
                  └────────────────────┘         └─────────────────────┘
                            │
                só novos (insert_if_absent → True)
                            │
                  ┌─────────▼──────────┐
                  │  NATS JetStream    │   stream: WALLET_TRADES
                  │  publish(...)      │   subject: wallet.trade.detected
                  │  Nats-Msg-Id dedup │   max_age: 7d, file storage
                  └─────────┬──────────┘
                            │
                push durable consumer "notifier-1"
                ack-explicit, ack-wait 30s, max-deliver 5
                            │
                  ┌─────────▼──────────┐
                  │  agents/notifier   │  (container polycopy-notifier)
                  │  durable consumer  │     /metrics:9102
                  │  format → send     │
                  └─────────┬──────────┘
                            │
                       aiogram.Bot
                            │
                            ▼
                      Telegram chat
```

## 4. Componentes e arquivos

### 4.1 Modificações em arquivos existentes

- **`src/polycopy/infrastructure/messaging/nats_bus.py`** — substituir core NATS por JetStream:
  - `connect()`: conecta NATS, obtém JetStream context, cria stream `WALLET_TRADES` idempotentemente.
  - `publish_wallet_trade_detected(event)`: `js.publish(subject, payload, headers={"Nats-Msg-Id": f"{tx_hash}:{log_index}"})`.
  - `subscribe(subject, handler, *, durable: str | None = None)`: assina ephemeral (core) se `durable=None`, durable JetStream caso contrário; `ConsumerConfig(ack_policy=EXPLICIT, ack_wait=30, max_deliver=5)`.
  - `close()`: drain (idempotente).
- **`src/polycopy/ports/messaging.py`** — `MessagingPort.subscribe` ganha kwarg `durable: str | None = None`.
- **`src/polycopy/config.py`** — campos novos:
  - `telegram_bot_token: SecretStr`
  - `telegram_chat_id: int`
  - `watcher_interval_s: float = 5.0`
  - `watcher_bootstrap_hours: int = 24`
  - `watcher_metrics_port: int = 9101`
  - `notifier_metrics_port: int = 9102`
  - `wallets_seed_path: Path = Path("config/wallets_seed.yaml")`
  - `polymarket_base_url: str = "https://data-api.polymarket.com"`
  - `postgres_host: str = "localhost"` (extrair do DSN atual)
- **`src/polycopy/infrastructure/observability/metrics.py`** — adicionar ao dataclass `Metrics`:
  - `watcher_iterations_total: Counter` (labels: `wallet`, `outcome` em `ok|empty|error`)
  - `watcher_trades_inserted_total: Counter` (label: `wallet`)
  - `watcher_iteration_duration_seconds: Histogram` (label: `wallet`)
  - `notifier_messages_total: Counter` (label: `outcome` em `sent|telegram_error|dropped_max_deliver`)
  - `notifier_send_duration_seconds: Histogram`
- **`docker-compose.yml`** — serviços `polycopy-watcher` e `polycopy-notifier`.
- **`prometheus.yml`** (config existente) — 2 scrape jobs.
- **`.env.example`** — entries das vars novas (sem valores reais).
- **`pyproject.toml`** — deps novas: `aiogram>=3.13`, `pyyaml>=6.0`.

### 4.2 Arquivos novos

- `config/wallets_seed.yaml` — lista de wallets (commitado).
- `src/polycopy/infrastructure/wallets_seed.py` — parser YAML + validação (`WalletAddress`).
- `src/polycopy/infrastructure/observability/http_metrics.py` — helper `start_metrics_server(port)` que envolve `prometheus_client.start_http_server`.
- `src/polycopy/infrastructure/telegram/__init__.py` — vazio.
- `src/polycopy/infrastructure/telegram/notifier_client.py` — `TelegramNotifier` (formata MarkdownV2, manda via `aiogram.Bot`).
- `src/polycopy/agents/watcher.py` — `WatcherAgent(AgentBase)` + `main()` async (entrypoint do container).
- `src/polycopy/agents/notifier.py` — `NotifierAgent(AgentBase)` + `main()` async.
- `Dockerfile.agent` — multi-stage, entrypoint parametrizado por `AGENT_MODULE`.
- `tests/unit/agents/test_watcher.py`
- `tests/unit/agents/test_notifier.py`
- `tests/unit/infrastructure/test_wallets_seed.py`
- `tests/unit/infrastructure/test_telegram_notifier.py`
- `tests/integration/test_jetstream_bus.py` (substitui `test_nats_bus.py`)
- `tests/integration/test_watcher_e2e.py`
- `tests/integration/test_notifier_e2e.py`
- `ARCHITECTURE.md` — overview do sistema com diagrama Mermaid.

### 4.3 Schema do `wallets_seed.yaml`

```yaml
wallets:
  - address: "0x1234567890abcdef1234567890abcdef12345678"
    label: "Whale 1"
  - address: "0x..."
    label: "Whale 2"
```

`address` validado por `WalletAddress` (regex existente). `label` aparece na mensagem Telegram.

## 5. Componentes — detalhamento

### 5.1 `WatcherAgent`

Subclasse de `AgentBase`. `run_once()` itera sobre todas as wallets monitoradas e chama `_poll_wallet`.

```python
class WatcherAgent(AgentBase):
    name = "watcher"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        interval_s: float,
        wallets: list[TrackedWallet],
        data_client: PolymarketDataPort,
        repo_factory: Callable[[], AsyncContextManager[WalletTradeRepository]],
        bus: MessagingPort,
        metrics: Metrics,
        bootstrap_hours: int,
    ) -> None: ...

    async def run_once(self) -> None:
        for wallet in self._wallets:
            await self._poll_wallet(wallet)

    async def _poll_wallet(self, wallet: TrackedWallet) -> None:
        # ver Seção 3 do design (transação curta no banco; publish fora dela)
```

**Pontos importantes:**
- `repo_factory()` retorna async-context que abre `AsyncSession`, faz commit no exit normal, rollback em exception. Evita transação longa atravessando todo o `run_once`.
- Publish acontece **fora** do bloco `async with repo_factory()`. Se publish falhar após insert ter sucedido, próxima iteração não republica (já dedupado pelo PK). Métrica `watcher_trades_inserted_total` ≠ contagem de publishes sinaliza divergência. Outbox-pattern entra em fase posterior (roadmap fase 4).
- Em exception (após retries internos do `PolymarketDataClient` esgotarem): loga, incrementa `watcher_iterations_total{outcome="error"}`, **não** propaga (W1). Métrica `iteration_duration_seconds` observada no `finally`.

### 5.2 `NotifierAgent`

Subclasse de `AgentBase`, mas com `run_once()` no-op (sleep curto). O trabalho real está no callback do durable consumer registrado em `start()` antes do `run()`.

```python
class NotifierAgent(AgentBase):
    name = "notifier"

    async def start(self) -> None:
        await self._bus.subscribe(
            WalletTradeDetected.SUBJECT,
            self._handle_message,
            durable="notifier-1",
        )

    async def run_once(self) -> None:
        await asyncio.sleep(1.0)  # heartbeat sem trabalho

    async def _handle_message(self, payload: bytes, *, num_delivered: int) -> None:
        # parse → format → telegram.send → métricas
        # exception → não acka; JetStream redelivera; max_deliver → drop
        # se num_delivered == max_deliver e envio falhar:
        #   métrica dropped_max_deliver antes de propagar
```

`_label_for(address)` usa um `dict[str, TrackedWallet]` carregado em `main()`. Wallet desconhecida (não está no YAML mas chegou um evento no stream): log warning + label = `address[:8]+"…"`.

### 5.3 `TelegramNotifier`

Wrapper fino sobre `aiogram.Bot.send_message` em `infrastructure/telegram/notifier_client.py`.

**Formato MarkdownV2:**
```
🟢 *BUY* — *Whale 1*
$10\.00 @ 0\.55 \(token 12345\)
2026\-05\-01 12:00:00 UTC
[tx](https://polygonscan.com/tx/0xcd...cd)
```

Caracteres especiais MarkdownV2 (`_*[]()~``>#+-=|{}.!`) são escapados em campos dinâmicos via `_escape_md()`. Emoji por side: 🟢 BUY / 🔴 SELL.

`send_trade_notification` captura `aiogram.exceptions.TelegramAPIError` e `TelegramNetworkError`, levanta uma exception interna `TelegramError` definida no módulo (handler do notifier captura essa exception e não acka).

### 5.4 `wallets_seed.py`

```python
@dataclass(frozen=True)
class TrackedWallet:
    address: WalletAddress
    label: str

def load_wallets_seed(path: Path) -> list[TrackedWallet]:
    """Lê YAML, valida endereços via WalletAddress, retorna lista imutável."""
```

Erros de schema (label faltando, address inválido) levantam `ValueError` com mensagem específica — o `main()` do watcher loga e sai com exit code 1.

## 6. Estratégia de testes

### 6.1 Unit (sem infra)

- `tests/unit/agents/test_watcher.py` — fakes pra `PolymarketDataPort`, `WalletTradeRepository` (in-memory), `MessagingPort` (in-memory). Cobre:
  - bootstrap quando `latest_occurred_at` retorna `None` (passa `now - 24h` ao client).
  - cursor reuso quando retorna datetime.
  - dedup: client retorna 2 trades; repo aceita 1 e rejeita outro; só 1 publish.
  - W1: client levanta `httpx.HTTPStatusError`, agente loga, métrica `error`++, próxima iter roda normal.
  - métricas `iteration_duration_seconds` observada mesmo em erro.
- `tests/unit/agents/test_notifier.py` — fake bus; injeta payload bytes; verifica formato Telegram, métricas `sent`/`telegram_error`, e que exception não-acka.
- `tests/unit/infrastructure/test_wallets_seed.py` — schema válido, address inválido, label faltando, arquivo inexistente.
- `tests/unit/infrastructure/test_telegram_notifier.py` — mock `aiogram.Bot`; verifica `chat_id`, `parse_mode="MarkdownV2"`, e escape correto de caracteres especiais.

### 6.2 Integration (NATS+Postgres reais; Polymarket+Telegram mockados)

- `tests/integration/test_jetstream_bus.py` (substitui `test_nats_bus.py`):
  - publish + durable subscribe entrega payload.
  - dedup por `Nats-Msg-Id`: publish duas vezes do mesmo `tx_hash:log_index` → consumer recebe 1.
  - exception no handler → redelivery (recebe N vezes até ack).
  - close idempotente.
  - adapter satisfaz `MessagingPort`.
- `tests/integration/test_watcher_e2e.py` (passo 1.12):
  - mocka Polymarket Data API com `respx`.
  - sobe agent task asyncio com `interval_s=0.05`.
  - valida row em `wallet_trades`, mensagem no JetStream stream, métricas incrementadas.
  - segunda iteração com mesmo trade não duplica row nem mensagem.
- `tests/integration/test_notifier_e2e.py`:
  - sobe notifier; publica `WalletTradeDetected` no JetStream.
  - `aiogram.Bot.send_message` mockado via `unittest.mock.AsyncMock`.
  - verifica chamada com payload esperado.

### 6.3 Cobertura alvo

- `domain/` ≥ 90% (já satisfeita)
- `agents/` ≥ 85%
- `infrastructure/messaging/` ≥ 85%

Total esperado pós-1C: 80 atuais + ~31 novos = ~111 testes.

## 7. Containerização

### 7.1 `Dockerfile.agent`

Multi-stage; base `python:3.12-slim`; instala deps via `uv sync --frozen --no-dev`; usuário `polycopy` (uid 1000); ENTRYPOINT parametrizado por `AGENT_MODULE` (env var ou build arg).

### 7.2 Entries no `docker-compose.yml`

```yaml
  polycopy-watcher:
    build:
      context: .
      dockerfile: Dockerfile.agent
      args: { AGENT_MODULE: watcher }
    container_name: polycopy-watcher
    restart: unless-stopped
    depends_on:
      polycopy-postgres: { condition: service_healthy }
      polycopy-nats:     { condition: service_healthy }
    environment:
      ENV: ${ENV}
      LOG_LEVEL: ${LOG_LEVEL}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_PORT: 5432
      POSTGRES_HOST: polycopy-postgres
      NATS_URL: nats://polycopy-nats:4222
      REDIS_URL: redis://polycopy-redis:6379
      WATCHER_INTERVAL_S: "5"
      WATCHER_BOOTSTRAP_HOURS: "24"
      WATCHER_METRICS_PORT: "9101"
      WALLETS_SEED_PATH: /app/config/wallets_seed.yaml
      POLYMARKET_BASE_URL: https://data-api.polymarket.com
    ports: ["9101:9101"]

  polycopy-notifier:
    # idêntico, AGENT_MODULE: notifier; porta 9102
    # vars adicionais: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
```

### 7.3 Prometheus

Adicionar em `prometheus.yml`:
```yaml
  - job_name: polycopy-watcher
    static_configs: [{ targets: ['polycopy-watcher:9101'] }]
  - job_name: polycopy-notifier
    static_configs: [{ targets: ['polycopy-notifier:9102'] }]
```

## 8. Plano de tasks (6 tasks; cadência one-task-per-confirmation)

1. **Task 1** — Migrar `NatsMessagingBus` para JetStream (refaz testes da T4 do 1B).
2. **Task 2** — Configs novas + métricas adicionadas em `metrics.py` + `http_metrics.py` (sem parser YAML ainda).
3. **Task 3** — `agents/watcher.py` esqueleto: wallets via env `WATCH_WALLETS` (lista CSV), sem dedup ainda, com testes unit. (passo 1.11)
4. **Task 4** — `wallets_seed.py` parser + watcher migra pro YAML + dedup E2E + test integration `test_watcher_e2e.py`. (passo 1.12)
5. **Task 5** — `agents/notifier.py` + `TelegramNotifier` + testes unit/integration. (passo 1.13)
6. **Task 6** — `Dockerfile.agent` + compose entries + Prometheus jobs + `ARCHITECTURE.md` + READMEs. (passo 1.14)

## 9. Definição de pronto do Plano 1C

- Watcher rodando via `docker compose up -d polycopy-watcher` faz polling de wallets do `wallets_seed.yaml` em intervalo configurável.
- Notifier rodando via `docker compose up -d polycopy-notifier` recebe eventos do JetStream e manda mensagens no Telegram.
- `/metrics` acessível em `:9101` (watcher) e `:9102` (notifier); Prometheus faz scrape.
- Suíte completa verde (~111 testes, integration incluído).
- `ARCHITECTURE.md` no repo com diagrama Mermaid do data flow.
- Docstrings de módulo nos dois agentes explicando como rodar local sem Docker (`uv run python -m polycopy.agents.watcher`).

## 10. Fora do escopo (entram em fases posteriores)

- Outbox pattern para garantir consistência entre `wallet_trades` e JetStream (Fase 4).
- Múltiplos chats no Telegram (Fase 3 — Commander).
- Discovery automática de wallets (Fase 6).
- Watchdog de agentes parados (Fase 7).
- Tabela `tracked_wallets` no Postgres (Fase 6+).
- Métricas business (P&L, hit rate, etc) — só infra/operacional agora.
