# PolyCopy — Prompt Mestre v2

Sistema multi-agente de copy trading na Polymarket com controle mobile. MVP estruturado, operação 24/7 sem supervisão, controle pelo celular via Telegram.

---

## 0. Como este documento é usado

Este prompt **não é executado de uma vez**. É um catálogo de passos que você invoca um por um. Cada passo tem escopo mínimo, definição de pronto verificável, e termina com um commit.

**Fluxo de trabalho com Claude Code:**

1. Eu (humano) abro o Claude Code na pasta `~/projects/polycopy` no servidor
2. Na primeira sessão, colo este documento inteiro como contexto
3. Em seguida, peço **um passo específico** (ex: "execute o passo 0.1")
4. Claude Code responde com o plano daquele passo (arquivos que vai criar, decisões que precisa tomar)
5. Eu aprovo ou corrijo
6. Claude Code implementa, roda os comandos da definição de pronto, faz o commit
7. Confirmo que está verde e peço o próximo passo

**Regras absolutas para o Claude Code:**

- Nunca avance pro próximo passo sem confirmação explícita do humano
- Nunca crie arquivos fora do escopo declarado do passo atual
- Nunca instale dependência fora da lista do passo atual sem perguntar
- Sempre rode os comandos da definição de pronto antes de declarar passo terminado
- Sempre faça commit conventional (`feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`) ao fim de cada passo
- Se descobrir que o passo precisa ser dividido (escopo maior do que parecia), pare e proponha a subdivisão antes de continuar
- Se descobrir que o passo está errado ou impossível como descrito, pare e exponha o problema antes de improvisar

---

## 1. Filosofia e princípios não-negociáveis

**Filosofia:**
- MVP estruturado: simplicidade onde não importa, rigor onde importa (dinheiro, segurança, dados)
- Operação autônoma 24/7 com self-healing
- Aprendizado contínuo: toda decisão é logada e analisável
- Mobile-first ops: gerenciamento e alertas pelo Telegram, depois PWA
- Refatorável, não reescritível: cada decisão considera caminho de upgrade futuro

**Princípios não-negociáveis (valem em todos os passos):**

1. **Idempotência em tudo que toca dinheiro.** Chave única `(wallet, tx_hash, log_index)` impede ordem duplicada mesmo após crash + reprocessamento.
2. **Fail-safe, não fail-open.** Em qualquer dúvida, sistema NÃO opera. Kill switch tem precedência sobre tudo.
3. **Auditável.** Toda decisão (executar, skipar, falhar, motivo) é evento imutável reconstrutível dos logs.
4. **Domain isolado.** Lógica de negócio (sizing, risco, scoring) não conhece HTTP, banco, blockchain. Testável puro.
5. **Observável desde dia 1.** Logs JSON estruturados com correlation IDs. Métricas Prometheus básicas.
6. **Self-healing.** Worker que crasha reinicia sozinho. Conexão que cai reconecta com backoff. Evento que falha vai pra DLQ.

---

## 2. Contexto operacional (referência rápida)

| Item | Valor |
|---|---|
| Capital inicial alvo | até $1.000 USDC em Polygon |
| Latência alvo MVP | detecção → ordem submetida em < 5s |
| Latência alvo futura | < 1s via subscriber on-chain (Fase 8) |
| Disponibilidade alvo | 99% (~7h downtime/mês aceitável) |
| Servidor | Hetzner CX33 Nuremberg, Ubuntu 24.04, 4 vCPU / 8GB / 80GB SSD |
| Acesso dev | SSH `polycopy@178.105.46.37`, chave Ed25519 |
| Pasta projeto | `/home/polycopy/projects/polycopy` |
| Ambiente dev local | Windows 11 + PowerShell + SSH (sem Docker local) |

**Nota sobre nomes:** o usuário Linux do servidor se chama `polycopy` e o pacote Python também se chama `polycopy`. São coisas distintas — usuário Linux gerencia o filesystem e roda containers, pacote Python é o código fonte do sistema.

---

## 3. Decisões técnicas fixas

Esta é a fonte única de verdade pra qualquer decisão técnica que apareça em mais de um lugar do projeto. Se você ver conflito entre esta seção e qualquer outra, esta seção ganha.

**Linguagem e tooling:**

| Item | Decisão |
|---|---|
| Python | 3.12.7 (fixar em `.python-version`) |
| Gerenciador de pacotes | `uv` (não Poetry, não pip-tools) |
| Layout do projeto | src-layout (`src/polycopy/`) |
| Pacote raiz | `polycopy` |
| Linter + formatter | `ruff` (substitui black, isort, flake8) |
| Type checker | `mypy --strict` |
| Test runner | `pytest` + `pytest-asyncio` + `pytest-cov` + `respx` + `hypothesis` |
| Pre-commit | `pre-commit` (instalado APENAS no último passo da Fase 0, depois do CI verde uma vez) |
| Conventional commits | `commitizen` (instalado junto do pre-commit, último passo da Fase 0) |
| Coverage mínimo | domain/ ≥ 90%, geral ≥ 75% (enforce no CI a partir da Fase 1) |

**Infraestrutura (docker-compose):**

| Serviço | Imagem |
|---|---|
| PostgreSQL + TimescaleDB | `timescale/timescaledb:2.17.2-pg16` |
| NATS com JetStream | `nats:2.10-alpine` (com flag `-js`) |
| Redis (cache, não fila) | `redis:7.4-alpine` |
| Prometheus | `prom/prometheus:v2.55.1` |

**TimescaleDB:** a extensão é habilitada via init script SQL em `infra/postgres/init.sql` montado em `/docker-entrypoint-initdb.d/`. Esse script roda só na primeira criação do volume.

**Compose strategy:** um único `docker-compose.yml` na Fase 0. Compose separado pra produção entra na Fase 7 (deploy automatizado). Manter um arquivo só evita divergência boba no início.

**Convenção de nomes de containers:** prefixo `polycopy-` em todos. Ex: `polycopy-postgres`, `polycopy-nats`, `polycopy-redis`, `polycopy-prometheus`. Agentes seguirão o mesmo padrão (ex: `polycopy-watcher`) quando entrarem na Fase 1.

**Política de .env:**

- Um único `.env` no servidor, em `~/projects/polycopy/.env`
- Permissão `600`, dono `polycopy:polycopy`
- `.env` no `.gitignore` desde o commit zero
- `.env.example` versionado, documenta todas as variáveis com comentários
- Chave privada da carteira Polygon entra só na Fase 5 (executor real). Antes disso, `.env` só tem credenciais não-sensíveis (URLs, tokens de API pública, configs)
- Migração pra `age`/`sops` (criptografia em rest) entra na Fase 5

**Logging:**

- `structlog` configurado em `src/polycopy/infrastructure/observability/logging.py` (criado no passo correspondente da Fase 1)
- Output: JSON em produção, console colorido em dev (controlado por `ENV=dev|prod`)
- Filtro de secrets: lista de chaves redatadas em código, nunca logar campos `private_key`, `api_secret`, `passphrase`, `mnemonic`

**Naming de eventos NATS:**

- Convenção: `domain.entity.action` em snake_case lowercase
- Exemplos: `wallet.trade.detected`, `order.approved`, `order.rejected`, `system.pause.requested`
- Subjects são imutáveis depois de produção. Versionamento futuro via sufixo (`v2`) se necessário

---

## 4. Arquitetura — visão alto nível

11 agentes Python independentes, cada um num container Docker próprio, comunicando via NATS JetStream. Cada agente tem responsabilidade única, isolamento de falha, e healthcheck próprio.

**Padrão arquitetural:** Hexagonal (Ports & Adapters). Domain isolado, ports são interfaces (Protocol/ABC), infrastructure são adapters concretos, agents são processos executáveis que compõem domain + adapters.

**Os 11 agentes** (descritos em detalhe nos passos das fases que os criam):

1. `watcher` — observa carteiras alvo (Fase 1)
2. `notifier` — canal pro celular via Telegram e PWA (Fase 1, expandido na Fase 3)
3. `risk` — guarda do capital, aplica limites (Fase 2)
4. `sizing` — calcula tamanho proporcional (Fase 2)
5. `commander` — recebe comandos do Telegram (Fase 3)
6. `executor` — assina e envia ordens, único que toca chave privada (Fase 4 DRY_RUN, Fase 5 real)
7. `reconciler` — confere consistência on-chain vs estado local (Fase 5)
8. `discovery` — descobre e ranqueia carteiras lucrativas (Fase 6)
9. `scanner` — caça oportunidades independentes (arbitragem, spreads anormais) (Fase 6)
10. `analyst` — aprendizado contínuo, relatório semanal (Fase 6)
11. `watchdog` — monitora os outros agentes, expõe `/health` (Fase 7)

---

## 5. Limites de risco hardcoded

Estes limites entram em código na Fase 2 (agente `risk`). Estão aqui pra referência:

- `RISK_MAX_CAPITAL_USDC` — capital total alocado (env var, default $1000)
- Exposição máxima por mercado: 5% do capital
- Exposição máxima por wallet copiada: 10% do capital
- Posições copiadas abertas simultaneamente: máx 20
- Drawdown diário: 8% → kill switch automático
- Slippage máximo aceito: 200 bps (configurável por wallet)
- Tamanho mínimo de ordem: $5
- Liquidez mínima do mercado: $50k volume 24h
- Tempo até resolução do mercado: > 72h
- Blacklist/whitelist de tags configurável (`config/risk.yaml`)

---

## 6. Estrutura do projeto na Fase 0

A estrutura **completa** do projeto (com todos os 11 agentes) está no Apêndice A como referência. **Não criar essa estrutura toda agora.** A Fase 0 entrega só o esqueleto mínimo abaixo:

```
polycopy/
├── pyproject.toml
├── uv.lock
├── .python-version
├── .gitignore
├── .env.example
├── .editorconfig
├── README.md
├── docker-compose.yml
├── infra/
│   ├── postgres/
│   │   └── init.sql
│   └── prometheus/
│       └── prometheus.yml
├── scripts/
│   └── bootstrap-env.sh
├── src/
│   └── polycopy/
│       └── __init__.py
├── tests/
│   ├── __init__.py
│   └── test_smoke.py
└── .github/
    └── workflows/
        └── ci.yml
```

Tudo que estiver fora dessa lista **não entra na Fase 0**.

---

## 7. Catálogo de passos

Cada fase é uma sequência numerada de passos. Cada passo tem objetivo, arquivos tocados, definição de pronto, e o que NÃO inclui.

---

### Fase 0 — Fundação

**Objetivo da fase:** ter um projeto Python esqueleto, infraestrutura Docker rodando, CI verde, e tooling de qualidade. Zero código de domínio. Zero agente. Zero cliente HTTP. Só o chão.

**Definição de pronto da Fase 0** (todos os comandos retornam 0):

```bash
cd ~/projects/polycopy
docker compose up -d --wait     # bloqueia até todos healthy ou falha rápido (Compose ≥ v2.18)
docker compose ps               # confirma status
uv sync
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
git log --oneline               # mostra commits convencionais dos passos 0.1 a 0.5
```

E o GitHub Actions deve estar verde no PR de bootstrap (ou na branch `main` se você optar por commitar direto).

---

#### Passo 0.1 — Bootstrap do projeto Python

**Objetivo:** ter `pyproject.toml` válido com `uv`, `.python-version` fixado, `.gitignore` correto, primeiro commit.

**Arquivos criados:**
- `pyproject.toml` (com seções `[project]`, `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`)
- `.python-version` contendo `3.12.7`
- `.gitignore` (Python padrão + `.env` + `.env.local` + `.venv` + `__pycache__` + `.mypy_cache` + `.ruff_cache` + `.pytest_cache` + `htmlcov/` + `*.pyc`)
- `.editorconfig` (utf-8, LF, indent 4 pra Python, indent 2 pra YAML/JSON)
- `src/polycopy/__init__.py` com `__version__ = "0.1.0"`
- `tests/__init__.py` vazio
- `tests/test_smoke.py` com um teste único: `def test_package_imports(): import polycopy; assert polycopy.__version__ == "0.1.0"`

**Configurações fixas no `pyproject.toml`:**
- Python requires `>=3.12,<3.13`
- Ruff: line-length 100, target-version py312, regras `["E", "F", "I", "B", "UP", "N", "S", "C4", "PIE", "RET", "SIM", "ARG"]` (com excludes razoáveis pra testes)
- Mypy: `strict = true`, `python_version = "3.12"`, `plugins = ["pydantic.mypy"]`
- Pytest: `testpaths = ["tests"]`, `asyncio_mode = "auto"`, `addopts = ["--tb=short", "--strict-markers", "--strict-config"]`
- License: `license = {text = "Proprietary"}` no `[project]` (projeto privado, sem arquivo `LICENSE` versionado)

**Dependências instaladas (e SOMENTE estas):**
- `[project.dependencies]`: `pydantic>=2.9` (runtime; necessária pro plugin `pydantic.mypy` funcionar honestamente desde já — será amplamente usada como runtime na Fase 1). **Exceção deliberada à filosofia "zero código de domínio na Fase 0":** preferimos uma dep declarada e não-importada do que um plugin mypy referenciando lib que não está em `[project.dependencies]`.
- `[dependency-groups.dev]` (uv): `pytest>=8`, `pytest-asyncio>=0.24`, `pytest-cov>=5`, `mypy>=1.13`, `ruff>=0.7`

**Comandos de validação:**
```bash
uv sync
uv run python -c "import polycopy; print(polycopy.__version__)"
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

**Definição de pronto:** todos os comandos acima retornam 0.

**Commit:** `chore: bootstrap pyproject and project skeleton`

**NÃO inclui:** nenhuma config de docker, nenhuma pasta `infra/`, nenhum README, nenhum CI, nenhum pre-commit. Só Python skeleton.

---

#### Passo 0.2 — README, .env.example, bootstrap-env.sh

**Objetivo:** documentar o básico de "o que é" e "como começa", e dar o script idempotente que prepara o `.env` no servidor.

**Arquivos criados:**
- `README.md` com seções: visão geral em 3 parágrafos, requisitos (Python 3.12.7, Docker, uv), comandos pra rodar localmente (`uv sync`, `uv run pytest`), instrução de bootstrap do `.env` (`bash scripts/bootstrap-env.sh`), nota dizendo que o desenvolvimento é feito no servidor via SSH, link pro Apêndice A do prompt mestre como referência da estrutura completa
- `.env.example` com placeholders documentados pra: `ENV` (dev/prod), `LOG_LEVEL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`, `NATS_URL`, `REDIS_URL`, `PROMETHEUS_PORT`. Cada variável com comentário explicando.
- `scripts/bootstrap-env.sh` — script bash enxuto (≤ 25 linhas, sem flags/getopts/cores/logging) que:
  - `set -euo pipefail` no topo
  - Aborta com erro claro se `.env` já existe (NÃO sobrescreve — comportamento "fail-on-existing", não idempotente no sentido estrito)
  - Copia `.env.example` → `.env`
  - Gera `POSTGRES_PASSWORD` com `openssl rand -hex 32` e injeta no `.env` via `sed -i` (GNU sed, servidor é Ubuntu Linux)
  - Aplica `chmod 600` no `.env`
  - Antes do `git add` no passo de commit, rodar `chmod +x scripts/bootstrap-env.sh` pra que o bit executável fique versionado no git

**Comandos de validação:**
```bash
test -f README.md
test -f .env.example
test -x scripts/bootstrap-env.sh
grep -q "POSTGRES_PASSWORD" .env.example
bash -n scripts/bootstrap-env.sh   # syntax check
```

**Definição de pronto:** os 3 arquivos existem, script tem permissão de execução e passa em syntax check.

**Commit:** `docs: add README, env example, and bootstrap script`

**NÃO inclui:** documentação de arquitetura (entra no ARCHITECTURE.md na Fase 1), runbooks (entram na Fase 7), execução do script (acontece no passo 0.3 antes de subir o compose).

---

#### Passo 0.3 — Infra Docker (postgres+timescale, nats, redis, prometheus)

**Objetivo:** criar os arquivos de configuração da infra e subir os 4 serviços de uma vez. Validação real é "containers up + healthy + queries respondendo" (não "arquivo existe").

**Pré-requisito:** `.env` criado no servidor via `bash scripts/bootstrap-env.sh` (passo 0.2). Confirmar antes com `ls -l .env` (esperado: `-rw-------` dono `polycopy:polycopy`).

**Arquivos criados:**

- `infra/postgres/init.sql`:
  ```sql
  CREATE EXTENSION IF NOT EXISTS timescaledb;
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
  ```

- `infra/prometheus/prometheus.yml`:
  ```yaml
  global:
    scrape_interval: 15s
    evaluation_interval: 15s
  scrape_configs:
    - job_name: prometheus
      static_configs:
        - targets: ['localhost:9090']
  ```

- `docker-compose.yml` com 4 serviços.

  **Convenção de porta (todos os serviços):** string short form `"127.0.0.1:<host>:<container>"` pra bindar só em loopback. Ex: `"127.0.0.1:${POSTGRES_PORT:-5432}:5432"`.

  **Convenção de env vars:** usar `environment:` explícito por serviço (NÃO usar `env_file: .env`). Princípio: least-privilege — cada container só recebe as vars que precisa, evita vazar futuras secrets (ex: `TELEGRAM_TOKEN` na Fase 3) pro postgres.

  Serviços:

  - `postgres` — imagem `timescale/timescaledb:2.17.2-pg16`, volume nomeado `polycopy_postgres_data`, monta `./infra/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro`, healthcheck via `pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}` (no `$${VAR}` o `$$` escapa pra `$` em parse-time do compose; o container resolve `${VAR}` em runtime via shell), porta `"127.0.0.1:${POSTGRES_PORT:-5432}:5432"`. Bloco `environment:` explícito (LHS = nome da var dentro do container, RHS = interpolação do `.env` do host):
    ```yaml
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ```
  - `nats` — imagem `nats:2.10-alpine`, comando `["-js", "-m", "8222"]`, volume nomeado `polycopy_nats_data`, healthcheck via `wget -qO- http://localhost:8222/healthz`, portas `"127.0.0.1:4222:4222"` e `"127.0.0.1:8222:8222"`. Sem `environment:` (NATS não precisa de credenciais nesta fase).
  - `redis` — imagem `redis:7.4-alpine`, comando `redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru --save ""` (cache puro, sem persistência), healthcheck via `redis-cli ping`, porta `"127.0.0.1:6379:6379"`. Sem `environment:`.
  - `prometheus` — imagem `prom/prometheus:v2.55.1`, monta `./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro`, volume nomeado `polycopy_prometheus_data`, porta `"127.0.0.1:9090:9090"`. Sem `environment:`.

  Todos com `restart: unless-stopped` e label `com.polycopy.role=infra`.

**Configuração esperada em `.env`** (gerada pelo `bootstrap-env.sh` no passo 0.2):
```
ENV=dev
LOG_LEVEL=DEBUG
POSTGRES_USER=polycopy
POSTGRES_PASSWORD=<gerado por openssl rand -hex 32>
POSTGRES_DB=polycopy
POSTGRES_PORT=5432
NATS_URL=nats://localhost:4222
REDIS_URL=redis://localhost:6379/0
PROMETHEUS_PORT=9090
```

**Comandos de validação:**
```bash
docker compose up -d --wait                     # bloqueia até todos healthy ou falha rápido
docker compose ps                               # confirma "running" e "healthy"
docker compose logs postgres | grep -i error    # deve estar vazio
docker compose logs nats | grep -i error        # deve estar vazio
docker compose exec postgres psql -U polycopy -d polycopy -c "SELECT extname FROM pg_extension WHERE extname='timescaledb';"
curl -sf http://localhost:9090/-/ready          # Prometheus pronto
docker compose exec redis redis-cli ping        # PONG
```

**Definição de pronto:** todos os comandos acima passam, todos os 4 containers `healthy`.

**Commit:** `feat: add docker-compose infra with postgres timescale nats redis prometheus`

**NÃO inclui:** containers dos agentes Python, nginx, backup, secrets manager, smoke test de conectividade Python→serviços (vai como passo 1.0 da Fase 1).

---

#### Passo 0.4 — CI no GitHub Actions

**Objetivo:** ter pipeline que roda em todo push e PR, executando lint + type + test.

**Arquivos criados:**
- `.github/workflows/ci.yml` com um job `quality` rodando em `ubuntu-latest`:
  - Checkout (`actions/checkout@v4`)
  - Setup Python 3.12.7 (`actions/setup-python@v5`)
  - Install uv via `astral-sh/setup-uv@v5` com `enable-cache: true` (cacheia `~/.cache/uv` entre runs — primeiro push é lento, demais voam)
  - `uv sync --frozen`
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run mypy src`
  - `uv run pytest --cov=src/polycopy --cov-report=term`

  Trigger: `push` em `main` e `pull_request` pra `main`.

  Bloco `concurrency:` no topo do workflow:
  ```yaml
  concurrency:
    group: ci-${{ github.ref }}
    cancel-in-progress: true
  ```
  Cancela runs anteriores quando vc faz push rápido em sequência na mesma ref (evita fila inútil).

**Comandos de validação:**
```bash
test -f .github/workflows/ci.yml
yamllint .github/workflows/ci.yml 2>/dev/null || python -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"
```

E após o push: GitHub Actions verde.

**Definição de pronto:** arquivo válido + Actions verde no primeiro push.

**Commit:** `ci: add github actions pipeline for lint type test`

**NÃO inclui:** deploy automatizado (Fase 7), security scan (`bandit`, `pip-audit` entram na Fase 7), build de imagens Docker (Fase 7).

---

#### Passo 0.5 — Pre-commit + commitizen (último passo da Fase 0)

**Objetivo:** instalar pre-commit hooks e commitizen DEPOIS que o resto está funcionando. Antes deste passo, commits são feitos manualmente com mensagens conventional escritas à mão.

**Por que último:** pre-commit precisa de `uv run` funcionando, `pyproject.toml` estável, e CI verde. Tentar instalar antes gera fricção.

**Arquivos criados:**
- `.pre-commit-config.yaml` com hooks:
  - `pre-commit-hooks` (trailing-whitespace, end-of-file-fixer, check-yaml, check-added-large-files, check-merge-conflict)
  - `ruff` (lint + format)
  - `mypy` (executado via `uv run mypy`)
  - `commitizen` (commit-msg hook)
- Atualização de `pyproject.toml`: adicionar `commitizen` ao dev group e seção `[tool.commitizen]` com `name = "cz_conventional_commits"`, `version = "0.1.0"`, `tag_format = "v$version"`

**Comandos de validação:**
```bash
uv sync
uv run pre-commit install
uv run pre-commit install --hook-type commit-msg
uv run pre-commit run --all-files     # pode reformatar; rodar de novo deve passar limpo
```

**Definição de pronto:** pre-commit roda em todos os arquivos sem erro na segunda execução.

**Commit:** `chore: add pre-commit hooks and commitizen`

**NÃO inclui:** hook de testes no pre-push (lento demais, deixa pro CI), `bandit` security hook (Fase 7).

---

### ✅ Fim da Fase 0

Neste ponto: projeto Python esqueleto, infraestrutura Docker rodando 4 serviços healthy, CI verde, pre-commit ativo, 5 commits conventional. Zero código de negócio.

**Antes de começar a Fase 1**, valide manualmente:
- `docker compose ps` mostra tudo healthy
- GitHub Actions verde no último push
- `git log --oneline` mostra os 5 commits da Fase 0

---

### Fase 1 — Domínio + Watcher mínimo + Notifier mínimo

**Objetivo da fase:** ter o domínio puro testado, cliente Data API funcional, repositórios SQLAlchemy + migrations alembic, agente `watcher` em produção fazendo polling de wallets do `wallets_seed.yaml` e publicando eventos NATS, agente `notifier` recebendo esses eventos e mandando mensagem no Telegram.

Os passos detalhados da Fase 1 serão expandidos quando você terminar a Fase 0. Razão: muitas decisões da Fase 1 dependem do que aprendermos durante a Fase 0 (versões finais das libs, comportamento real do TimescaleDB no servidor, etc). Catalogar Fase 1 agora seria especulação.

**Esboço da Fase 1** (será detalhado depois, ~11-13 passos):
- 1.0: Smoke test de conectividade Python→infra (psycopg connect, NATS ping, Redis ping) — primeiro passo pra validar que o ambiente da Fase 0 funciona de fato pelo lado do Python. Roda via `uv run pytest tests/integration/test_infra_connectivity.py` com containers up.
- 1.1: Domain value objects (`Money`, `Price`, `Bps`, `WalletAddress`, `ConditionId`, `TokenId`) + testes
- 1.2: Domain models (`Wallet`, `Trade`, `Position`) + testes
- 1.3: Domain events (`WalletTradeDetected` etc, dataclasses imutáveis) + testes
- 1.4: Ports (`PolymarketDataPort`, `MessagingPort`, `WalletRepository`) — interfaces puras
- 1.5: Settings com pydantic-settings + logging com structlog
- 1.6: SQLAlchemy setup + primeira migration alembic (tabela `wallet_trades` com índice único `(tx_hash, log_index)`)
- 1.7: Repositório concreto `SqlAlchemyWalletTradeRepository` + testes de integração
- 1.8: Cliente Data API (`PolymarketDataClient`) com httpx + tenacity + métricas + testes com respx
- 1.9: NATS bus adapter + testes
- 1.10: `agents/_base.py` com heartbeat, graceful shutdown, signal handling + testes
- 1.11: `agents/watcher` esqueleto (polling de 1 wallet hardcoded, sem dedup ainda) + commit
- 1.12: Watcher com dedup via repositório + integração end-to-end no servidor
- 1.13: `agents/notifier` mínimo (consome `wallet.trade.detected`, manda Telegram via `python-telegram-bot`)
- 1.14: ARCHITECTURE.md + diagrama Mermaid + README dos dois agentes

**Definição de pronto da Fase 1** (preview, será refinada):
- Watcher rodando 24/7 no servidor detectando trades de pelo menos 1 wallet conhecida
- Você recebe notificação no Telegram em < 30s do trade real acontecer
- Coverage do `domain/` ≥ 90%
- Métricas Prometheus expostas pelo watcher
- Documentação completa dos dois agentes

---

### Fases 2 a 8 — esboço de alto nível

Não detalho por enquanto. Cada fase será expandida em passos quando a anterior estiver concluída e tivermos aprendizado suficiente.

- **Fase 2** — WebSocket CLOB + Risk + Sizing
- **Fase 3** — Telegram completo + Commander
- **Fase 4** — Executor em DRY_RUN + event sourcing + outbox
- **Fase 5** — Executor real + Reconciler + API REST + capital começa em $50
- **Fase 6** — Discovery + Scanner + Analyst
- **Fase 7** — Hardening + Watchdog + Deploy automatizado + PWA
- **Fase 8** — Otimização baseada em dados (incluindo possível subscriber on-chain pra latência <1s)

---

## 8. Apêndice A — Estrutura completa do projeto (referência futura)

Esta é a estrutura final que o projeto terá ao fim da Fase 7. **Não criar agora.** Cada arquivo aparece no passo da fase correspondente.

```
polycopy/
├── pyproject.toml
├── uv.lock
├── .python-version
├── .pre-commit-config.yaml
├── .editorconfig
├── .gitignore
├── .env.example
├── README.md
├── ARCHITECTURE.md            # Fase 1
├── DEPLOY.md                  # Fase 7
├── SECURITY.md                # Fase 5
├── OPERATIONS.md              # Fase 7
├── CHANGELOG.md               # gerado por commitizen
├── docker-compose.yml         # Fase 0
├── docker-compose.prod.yml    # Fase 7
├── Dockerfile                 # Fase 1 (multi-stage)
├── infra/
│   ├── postgres/init.sql      # Fase 0
│   ├── prometheus/prometheus.yml  # Fase 0
│   ├── nginx/                 # Fase 7
│   └── backup/                # Fase 7
├── src/polycopy/
│   ├── __init__.py
│   ├── config.py              # Fase 1
│   ├── domain/                # Fase 1
│   ├── ports/                 # Fase 1
│   ├── infrastructure/        # Fase 1+
│   ├── agents/                # Fase 1+
│   └── api/                   # Fase 5
├── migrations/                # Fase 1 (alembic)
├── scripts/                   # Fase 5+
├── config/                    # Fase 1+
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── pwa/                       # Fase 7 (Next.js)
└── .github/workflows/
    ├── ci.yml                 # Fase 0
    └── deploy.yml             # Fase 7
```

---

## 9. Apêndice B — Eventos NATS (catálogo, referência futura)

Catálogo de subjects que serão usados ao longo das fases. Documentado aqui pra evitar inventar nomes inconsistentes depois.

| Subject | Publisher | Consumer | Fase |
|---|---|---|---|
| `wallet.trade.detected` | watcher | risk, notifier | 1 |
| `wallet.added` | discovery | watcher | 6 |
| `wallet.removed` | discovery | watcher | 6 |
| `wallet.score.updated` | discovery | analyst | 6 |
| `opportunity.detected` | scanner | risk, notifier | 6 |
| `order.approved` | risk | sizing | 2 |
| `order.rejected` | risk, sizing | notifier | 2 |
| `order.sized` | sizing | executor | 2 |
| `order.submitted` | executor | reconciler, notifier | 4 |
| `order.filled` | executor | reconciler, notifier | 4 |
| `order.partial` | executor | reconciler, notifier | 4 |
| `order.cancelled` | executor | reconciler, notifier | 4 |
| `order.failed` | executor | notifier | 4 |
| `reconciliation.mismatch` | reconciler | notifier | 5 |
| `system.pause.requested` | commander, reconciler | todos | 3 |
| `system.resume.requested` | commander | todos | 3 |
| `system.kill.requested` | commander | todos | 3 |
| `wallet.disable.requested` | commander | watcher | 3 |
| `wallet.enable.requested` | commander | watcher | 3 |
| `agent.down.<nome>` | watchdog | notifier | 7 |
| `report.weekly` | analyst | notifier | 6 |
| `heartbeat.<nome>` | todos | watchdog | 1+ |

---

## 10. Apêndice C — Catálogo de tabelas (referência futura)

| Tabela | Fase | Tipo |
|---|---|---|
| `wallet_trades` | 1 | hypertable |
| `tracked_wallets` | 6 | regular |
| `wallet_score_history` | 6 | hypertable |
| `markets` | 2 | regular |
| `risk_decisions` | 2 | hypertable |
| `copy_orders` | 4 | regular |
| `copy_order_events` | 4 | regular (event log) |
| `positions` | 5 | regular (read model) |
| `position_snapshots` | 5 | hypertable |
| `pnl_snapshots` | 5 | hypertable |
| `opportunities` | 6 | hypertable |
| `outbox` | 4 | regular |
| `agent_heartbeats` | 1 | regular |

Sem `ON DELETE CASCADE` em tabelas de auditoria. Migrations alembic versionadas.

---

## 11. Apêndice D — Métricas Prometheus (referência futura)

Catálogo de métricas que serão emitidas. Cada uma é instanciada no passo da fase que cria o agente correspondente.

- `polycopy_wallet_trades_detected_total{wallet, source}` — Fase 1
- `polycopy_detection_to_decision_seconds` (histogram) — Fase 2
- `polycopy_decision_to_submission_seconds` (histogram) — Fase 4
- `polycopy_e2e_seconds` (histogram, alvo p99 < 5s) — Fase 4
- `polycopy_orders_total{status, wallet, reason}` — Fase 4
- `polycopy_slippage_bps` (histogram) — Fase 5
- `polycopy_pnl_usdc{window}` — Fase 5
- `polycopy_capital_deployed_usdc` (gauge) — Fase 5
- `polycopy_open_positions` (gauge) — Fase 5
- `polycopy_kill_switch_active` (gauge 0/1) — Fase 3
- `polycopy_agent_heartbeat_age_seconds{agent}` — Fase 1+
- `polycopy_http_request_duration_seconds{client, endpoint, status}` (histogram) — Fase 1

---

## 12. Apêndice E — Fontes de dados Polymarket (referência futura)

**REST APIs públicas:**
- Gamma: `https://gamma-api.polymarket.com` — descoberta de mercados/eventos
- Data API: `https://data-api.polymarket.com` — `/activity`, `/positions`, `/value`, leaderboard, top holders

**REST CLOB:**
- `https://clob.polymarket.com` — orderbook, preços, execução (autenticado pra escrita)

**WebSocket (Fase 2+):**
- `wss://ws-subscriptions-clob.polymarket.com/ws/` — canais `market` e `user`

**On-chain (Fase 8):**
- WSS Polygon (Alchemy free tier no MVP)
- Endereços de contratos (CTF Exchange, NegRisk CTF Exchange, Conditional Tokens) — pesquisar em `docs.polymarket.com/contracts` no momento da Fase 8, podem ter mudado

---

## 13. Regras finais pro Claude Code (resumo executivo)

1. **Antes de cada passo:** responda com plano (arquivos que vai criar, decisões que precisa tomar, riscos que vê). Espere aprovação. Não crie nada antes.
2. **Trabalhe um passo por vez.** Não comece o passo N+1 sem confirmação humana de que N está pronto.
3. **Conventional commits obrigatórios** ao fim de cada passo. Exatamente uma das prefixos: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, `ci:`, `style:`, `perf:`.
4. **Type safety:** mypy `--strict`. Sem `Any` exceto fronteiras justificadas com comentário `# type: ignore[<rule>] # <razão>`.
5. **Sem código morto, sem TODO solto, sem `# type: ignore` sem razão escrita.**
6. **Segurança:** chave privada, mnemônico, API secrets — nunca em código, nunca em log (filtros no logger), nunca em commit. `.env` no `.gitignore` desde init.
7. **Pergunte antes de adicionar dependência fora da lista do passo atual.**
8. **Pergunte antes de mudar a arquitetura proposta.** Se discordar, traga argumentos e me deixe decidir.
9. **Mensure latência desde a Fase 1.** Toda chamada externa tem métrica de duração.
10. **Pesquise na web quando precisar.** APIs e libs mudam. Não confie em memória pra versões e endpoints.
11. **Pense em falha pra cada feature.** O que acontece se isso falhar? Como o sistema se recupera? O usuário fica sabendo?
12. **Documente lições.** Se descobrir bug ou má decisão, registre em `docs/lessons-learned.md` com data, contexto, decisão original, problema, correção.

---

**FIM DO PROMPT MESTRE v2.**

Quando estiver pronto pra começar, abra o Claude Code em `~/projects/polycopy`, cole este documento inteiro como contexto inicial, e em seguida peça: **"Execute o passo 0.1."**
