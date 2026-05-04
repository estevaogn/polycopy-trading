# Spec — Fase 6: Multi-Wallet Discovery (Leaderboard CLI)

**Data:** 2026-05-04
**Status:** Aprovado (pendente revisão escrita)
**Predecessor:** Fase 5C (head `0de022a`).

## 1. Objetivo

Desbloquear o backtest fornecendo **wallets candidatas data-driven** pro `wallets_seed.yaml`, hoje populado a dedo (2 entries). O backtest atual coletou 88 trades de uma wallet dummy (`0x1111…`) — sinal nulo. Sem critério objetivo de "smart money", qualquer melhoria downstream (PnL view, métricas) fica analisando ruído.

Entrega: **CLI one-shot** que consulta o leaderboard oficial da Polymarket, filtra por critérios mínimos, e gera dois artefatos pra revisão manual:

1. `config/wallets_candidates.yaml` — entries no schema do `wallets_seed.yaml`, prontas pra copiar.
2. `docs/discover_wallets_report.md` — tabela rica com metadados do run pra auditoria.

A **promoção** de candidato pra wallet observada continua **manual** (operador edita `wallets_seed.yaml` e reinicia watcher). Sem auto-apply nesta fase.

## 2. Contexto

- WatcherAgent já suporta múltiplas wallets (`watcher.py:69` itera `list[TrackedWallet]`).
- `wallets_seed.yaml` carregado por `infrastructure/wallets_seed.py:load_wallets_seed`.
- Polymarket expõe endpoint oficial `GET https://data-api.polymarket.com/v1/leaderboard` (mesmo host da Data API que `PolymarketDataClient` já usa). Sem auth.
- Resposta inclui: `rank, proxyWallet, userName, vol, pnl, profileImage, xUsername, verifiedBadge`.
- Parâmetros: `category` (10 valores), `timePeriod` (DAY/WEEK/MONTH/ALL), `orderBy` (PNL/VOL), `limit` 1–50, `offset` 0–1000.

## 3. Decisões fixadas

1. **CLI one-shot**, não agente periódico. (Revisitar em Fase 7+ se virar workflow recorrente.)
2. **Critério: PnL absoluto com filtros mínimos.** API ordena por PNL desc; filtro pós-fetch por volume mínimo + exclusão de wallets já no seed.
3. **Defaults:** `timePeriod=MONTH`, `category=OVERALL`, `orderBy=PNL`, `top=50`, `min_volume_usdc=5000`. Todos override-able via flags.
4. **Output dual:** YAML pra colar + Markdown report. Re-run sobrescreve sem perguntar (idempotente).
5. **Estrutura alinhada com padrões existentes** — Port + Adapter + domain puro + script thin. Permite reuso futuro (agente periódico vira wrapping).
6. **Sem auto-promoção.** Usuário sempre revisa antes de mover entries pro `wallets_seed.yaml`.
7. **Cap em `top=1050`** (limite da API: offset máx 1000 + page de 50).
8. **Sem persistência em DB, sem JetStream, sem container.**

## 4. Componentes

### 4.1 Port — `src/polycopy/ports/polymarket_leaderboard.py`

```python
from typing import Protocol
from polycopy.domain.discovery import LeaderboardEntry, TimePeriod, Category, OrderBy

class PolymarketLeaderboardPort(Protocol):
    async def fetch_leaderboard(
        self,
        *,
        time_period: TimePeriod,
        category: Category,
        order_by: OrderBy = OrderBy.PNL,
        limit: int = 50,
        offset: int = 0,
    ) -> list[LeaderboardEntry]: ...
```

### 4.2 Domain — `src/polycopy/domain/discovery.py`

```python
class TimePeriod(str, Enum):
    DAY = "DAY"; WEEK = "WEEK"; MONTH = "MONTH"; ALL = "ALL"

class Category(str, Enum):
    OVERALL = "OVERALL"; POLITICS = "POLITICS"; SPORTS = "SPORTS"
    CRYPTO = "CRYPTO"; CULTURE = "CULTURE"; MENTIONS = "MENTIONS"
    WEATHER = "WEATHER"; ECONOMICS = "ECONOMICS"; TECH = "TECH"; FINANCE = "FINANCE"

class OrderBy(str, Enum):
    PNL = "PNL"; VOL = "VOL"

@dataclass(frozen=True)
class LeaderboardEntry:
    rank: int
    address: WalletAddress
    user_name: str | None
    volume_usdc: Decimal
    pnl_usdc: Decimal
    verified_badge: bool

@dataclass(frozen=True)
class CandidateWallet:
    address: WalletAddress
    label: str
    rank: int
    volume_usdc: Decimal
    pnl_usdc: Decimal
    verified_badge: bool
```

Funções puras:

- `derive_label(entry: LeaderboardEntry) -> str` — retorna `userName` sanitizado (trim, whitespace→`_`, drop não-printable, max 32 chars) ou fallback `f"{address[:10]}…"` quando vazio/None.
- `filter_and_rank(entries, *, min_volume_usdc, exclude, top_n) -> list[CandidateWallet]` — filtra por volume mínimo, exclui addresses do seed, dedup por endereço, mantém ordem (PNL desc da API), trunca em `top_n`.
- `render_candidates_yaml(candidates) -> str` — emite `wallets:\n  - address: …\n    label: …` com aspas dobradas em address, no schema do `wallets_seed.yaml`.
- `render_report_md(candidates, *, time_period, category, min_volume_usdc, run_at) -> str` — frontmatter YAML com metadados do run + tabela markdown com colunas `Rank | userName | Address | Volume (USDC) | PnL (USDC) | Verified | Polymarket`.

### 4.3 Adapter — `src/polycopy/infrastructure/polymarket/leaderboard_client.py`

```python
class PolymarketLeaderboardClient:
    """Implementa PolymarketLeaderboardPort. httpx + tenacity + métricas."""

    def __init__(
        self, *, base_url: str, metrics: Metrics,
        timeout_s: float = 10.0, max_retries: int = 3,
    ) -> None: ...

    async def fetch_leaderboard(self, *, time_period, category, order_by=OrderBy.PNL,
                                 limit=50, offset=0) -> list[LeaderboardEntry]: ...
```

- `GET {base_url}/v1/leaderboard` com query params dos enums (`.value`).
- Reusa `_is_retryable` (5xx + `httpx.RequestError`) seguindo padrão `DataClient`.
- Parse: rows JSON → `LeaderboardEntry` via `_row_to_entry` (`userName` ausente/None → `None`; `verifiedBadge` ausente → `False`; `vol`/`pnl` via `Decimal(str(...))`).
- Métricas: `polycopy_leaderboard_requests_total{status}` + `polycopy_leaderboard_request_duration_seconds`.

### 4.4 CLI — `src/polycopy/scripts/discover_wallets.py`

Argparse:

| Flag | Default | Validação |
|---|---|---|
| `--time-period` | `MONTH` | `TimePeriod` enum |
| `--category` | `OVERALL` | `Category` enum |
| `--top N` | `50` | `1 <= N <= 1050` (clamp + warning se exceder) |
| `--min-volume USDC` | `5000` | `Decimal >= 0` |
| `--seed-path PATH` | `config/wallets_seed.yaml` | path existente |
| `--candidates-out PATH` | `config/wallets_candidates.yaml` | parent dir existente |
| `--report-out PATH` | `docs/discover_wallets_report.md` | parent dir existente |
| `--dry-run` | `False` | flag booleana |

Fluxo (`async def main`):

1. Parse args.
2. `seed_addresses = {w.address for w in load_wallets_seed(args.seed_path)}`.
3. Loop paginado: `offset=0,50,100,…` enquanto `len(rows_acumulado) < args.top` e última page veio `>=limit`. Cap em `offset=1000`.
4. `candidates = filter_and_rank(rows, min_volume_usdc=args.min_volume, exclude=seed_addresses, top_n=args.top)`.
5. Se `not candidates`: stderr explica motivo (`no rows from API` ou `all filtered out`); exit 2; **sem escrever arquivos**.
6. Print tabela compacta no stdout.
7. Se `not args.dry_run`: escreve `args.candidates_out` (YAML) + `args.report_out` (MD).
8. Exit 0.

### 4.5 Reuso

- `infrastructure/wallets_seed.py:load_wallets_seed` (já existe).
- `infrastructure/observability/metrics.py:make_metrics` — adicionar 2 métricas novas no factory existente.
- `infrastructure/observability/logging.py:configure_logging` — reusa.
- `domain/value_objects.py:WalletAddress` — checksum/validação.

## 5. Schemas de output

### 5.1 `config/wallets_candidates.yaml`

Idêntico ao `wallets_seed.yaml` (exemplo com endereços fictícios — wallets já presentes no seed nunca aparecem aqui):

```yaml
wallets:
  - address: "0xcafef00dba5eba11deadbeef00000000000000aa"
    label: "whale_alpha"
  - address: "0xcafef00dba5eba11deadbeef00000000000000bb"
    label: "0xcafef00d…"
```

### 5.2 `docs/discover_wallets_report.md`

```markdown
---
generated_at: 2026-05-04T20:30:00Z
time_period: MONTH
category: OVERALL
order_by: PNL
min_volume_usdc: 5000.00
top: 50
seed_path: config/wallets_seed.yaml
seed_size: 2
total_fetched: 50
total_excluded_existing: 1
total_excluded_min_volume: 4
total_candidates: 45
---

# Wallet candidates — MONTH/OVERALL (run 2026-05-04 20:30 UTC)

| Rank | userName | Address | Volume (USDC) | PnL (USDC) | Verified | Polymarket |
|-----:|----------|---------|--------------:|-----------:|:--------:|------------|
|    1 | whale_alpha | 0xcafef00d…aa | 1,234,567.89 | +98,765.43 | yes | [link](https://polymarket.com/profile/0xcafef00d…) |
| … | … | … | … | … | … | … |
```

## 6. Errors e edge cases

| Cenário | Comportamento |
|---|---|
| API 5xx | Tenacity retry: exponential backoff, max 3 tentativas |
| API 4xx | Erro imediato; CLI imprime stderr + exit 1 |
| API retorna 0 rows na page 0 | Exit 2 (sem arquivos), stderr explica params |
| Após filtros, 0 candidatos | Exit 2 (sem arquivos), stderr informa quantos foram excluídos por cada filtro |
| Page retorna `<limit` rows | Fim natural da paginação |
| `top > 1050` | Clamp em 1050, warning em stderr |
| `--seed-path` inexistente | `FileNotFoundError` propaga; CLI converte em mensagem clara + exit 1 |
| `--seed-path` válido mas vazio | OK, `exclude=set()`, processa normal |
| `userName` vazio/None | Fallback `derive_label` → `0x123abc…` |
| `userName` com caracteres exóticos | Sanitização (trim, whitespace→`_`, drop não-printable, max 32 chars) |
| Endereço duplicado em pages | Dedup em `filter_and_rank` |
| `--dry-run` | Print no stdout; **não escreve YAML nem MD** |
| Output paths em diretório inexistente | Erro claro + exit 1 (não cria parent dirs automaticamente) |

## 7. Testes

### 7.1 Unit — `tests/unit/domain/test_discovery.py`

- `derive_label`: userName presente, vazio, com whitespace, com caracteres não-printable, exceeding max length, fallback.
- `filter_and_rank`: ordenação preservada, exclusão por seed, filtro de min_volume, top_n cap, dedup endereços, lista vazia.
- `render_candidates_yaml`: roundtrip — gerar YAML, re-parsear via `load_wallets_seed`, comparar entries.
- `render_report_md`: presença de campos no frontmatter, contagem de linhas da tabela igual a `len(candidates) + cabeçalho`, escape de pipes em userName.

### 7.2 Unit — `tests/unit/infrastructure/test_leaderboard_client.py`

`httpx.MockTransport`:

- 200 happy path: parse de payload representativo (incluindo userName None e verifiedBadge ausente).
- 500 → 500 → 200: retry sucede após 2 falhas.
- 5xx persistente: erra após max_retries.
- 4xx (400/404): erra imediato sem retry.
- Métricas incrementadas em sucesso e falha.

### 7.3 Unit — `tests/unit/scripts/test_discover_wallets_cli.py`

- Defaults aplicados quando flags omitidas.
- `--top 2000` → clamp em 1050 + warning.
- `--dry-run` não escreve arquivos (`tmp_path` para outputs).
- Exit codes: 0 sucesso, 2 sem candidatos, 1 erro de API/IO.
- Argparse rejeita enums inválidos (`--time-period FOO`).

### 7.4 Integration live — `tests/integration/test_leaderboard_live.py`

Gated por `PYTEST_LIVE_POLYMARKET=1` (segue padrão `test_polymarket_smoke_executor.py`):

- 1 request real ao endpoint contra `MONTH/OVERALL/limit=5`.
- Valida shape: `len(rows) <= 5`, cada row tem campos requeridos, parse não levanta.

**Sem testes E2E** — sem container, sem JetStream, sem DB.

## 8. Observability

### 8.1 Métricas (Prometheus)

Adicionar em `make_metrics()`:

- `polycopy_leaderboard_requests_total{endpoint="leaderboard",status="200|4xx|5xx"}` — `Counter`.
- `polycopy_leaderboard_request_duration_seconds{endpoint="leaderboard"}` — `Histogram`.

CLI **não sobe HTTP `/metrics` server**. Métricas existem só pra reuso da fixture `Metrics` no client (consistência com `DataClient`/`GammaClient`/`ClobClient`).

### 8.2 Logs estruturados

Via `configure_logging` existente:

- `discover_run_started` — params (time_period, category, top, min_volume_usdc, seed_size).
- `leaderboard_page_fetched` — offset, count.
- `discover_run_filtered` — total_fetched, excluded_existing, excluded_min_volume, total_candidates.
- `discover_run_completed` — paths_written (ou `dry_run=True`).

## 9. Fora de escopo (explícito)

- Auto-promoção de candidatos pro `wallets_seed.yaml`.
- Cache em disco de runs anteriores; histórico cross-run.
- Score multidimensional (Sharpe, winrate, profit factor) — escolha foi PnL+filtros, não Sharpe-like.
- Discovery via co-trading / on-chain / Subgraph (The Graph).
- Container/agente periódico — Ports prontos pra wrapping futuro, mas não construído agora.
- Sincronização do deploy Hetzner com `wallets_seed.yaml` local — task de ops separada.
- Filtros adicionais (`pnl/vol ratio`, `verifiedBadge=true`).

## 10. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Endpoint não-documentado mudar / virar privado | Spec já reconhece — fallback é Subgraph (deferido); detecção por integration live test |
| Top-50 do leaderboard ainda gera ruído | Revisão manual antes de promover é o gate principal; min_volume default ajustável |
| Wallets ranqueadas por sorte de período curto | Default `MONTH` reduz ruído vs DAY; usuário pode override pra `ALL` |
| Rate limit (60 req/min reportado, não documentado oficialmente) | Sequencial sem paralelismo; top=1050 = 21 requests = bem abaixo do limite |
| `userName` vir com markdown injection | Sanitização em `derive_label` + escape de pipes em `render_report_md` |

## 11. Critérios de aceite

- `uv run python -m polycopy.scripts.discover_wallets` completa em <30s contra API real, gera `config/wallets_candidates.yaml` + `docs/discover_wallets_report.md` com pelo menos 1 candidato.
- `wallets_candidates.yaml` é válido pelo schema do `load_wallets_seed` (parser do watcher aceita sem erro).
- Re-run sobrescreve outputs sem prompt; conteúdo determinístico dado mesma resposta da API.
- `--dry-run` exibe tabela e não cria/modifica arquivos de output.
- Wallets já no `wallets_seed.yaml` nunca aparecem em `wallets_candidates.yaml`.
- Suite de testes nova passa em `uv run pytest tests/unit/domain/test_discovery.py tests/unit/infrastructure/test_leaderboard_client.py tests/unit/scripts/test_discover_wallets_cli.py`.
- Mypy strict limpo nos novos arquivos.

## 12. Estimativa

~400 LOC distribuídas: ~80 (port + adapter + métricas), ~150 (domain puro), ~100 (CLI), ~250 (testes).
