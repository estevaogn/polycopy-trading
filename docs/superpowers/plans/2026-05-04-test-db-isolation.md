# Test Database Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. **Cadência: checkpoint humano por task** (mesma das fases anteriores).

**Goal:** Isolar `pytest tests/` do DB de produção criando DB lógico separado `polycopy_test` no mesmo container Postgres, criado on-demand pelo conftest.

**Architecture:** Conftest cria `polycopy_test` se não existir via psycopg admin connection ao DB `postgres`, monkeypatcha `POSTGRES_DB` na sessão. Settings e alembic/env.py inalterados — DSN resolve transparentemente via env var.

**Tech Stack:** Python 3.12, pytest + pytest-asyncio, psycopg v3 (sync, dev dep), alembic, sqlalchemy 2.

**Predecessor:** Plano 5A completo (head `563c2f6`) + spec test-db-isolation (`ee3e846`).

**Spec:** `docs/superpowers/specs/2026-05-04-test-db-isolation-design.md`.

---

## File Structure

**Modificados (3):**
- `pyproject.toml` — `psycopg` em `[dependency-groups].dev`.
- `tests/conftest.py` — adiciona fixtures `monkeypatch_session` + `_ensure_test_db`; ajusta `settings` e `db_engine` pra declarar dependência.
- `ARCHITECTURE.md` (ou `README.md`, depende de onde já tem seção de testes) — seção "Running tests" mencionando `polycopy_test` + privilege CREATEDB.

**Novos (1):**
- `tests/integration/test_db_isolation.py` — teste canário (1 test).

**Lock file:**
- `uv.lock` — atualizado pelo `uv lock` ou `uv sync`.

---

## Task 1: Adicionar dep `psycopg` (v3 sync) em `pyproject.toml`

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock` (gerado automaticamente)

---

- [ ] **Step 1.1: Inspecionar `pyproject.toml`**

LEIA `pyproject.toml` primeiro pra identificar:
- Onde estão as dev dependencies (provavelmente `[tool.uv.dev-dependencies]` ou `[dependency-groups].dev` ou `[project.optional-dependencies].dev`).
- Se `psycopg` ou `psycopg2` já está listado (improvável; o projeto usa asyncpg).

- [ ] **Step 1.2: Adicionar `psycopg` ao grupo dev**

Adicionar ao bloco de dev dependencies. Formato típico (adapte ao bloco existente):

```toml
[dependency-groups]
dev = [
    # ... deps existentes ...
    "psycopg[binary]>=3.2",
]
```

`[binary]` evita compilação local de C bindings.

- [ ] **Step 1.3: Sync lock**

```bash
uv sync --all-groups
```

Esperado: `psycopg` instalado, `uv.lock` atualizado.

- [ ] **Step 1.4: Smoke import**

```bash
uv run python -c "import psycopg; print(psycopg.__version__)"
```

Esperado: imprime versão (≥ 3.2).

- [ ] **Step 1.5: Verificações + STOP — commit**

```bash
uv run mypy src/polycopy
uv run pytest tests/ 2>&1 | tail -5
```

Esperado: nenhuma regressão. Suite continua nos mesmos números (411 passed + 11 falhas pré-existentes — se rodar isso destrói prod por enquanto, é OK uma última vez).

Implementer NÃO commita. Controller pede confirmação humana, depois:

```bash
git add pyproject.toml uv.lock
git commit -m "build(deps): add psycopg v3 dev dependency for test db isolation"
```

---

## Task 2: Adicionar fixtures de isolamento em `tests/conftest.py`

**Files:**
- Modify: `tests/conftest.py`

**Esta é a task crítica do plano.**

---

- [ ] **Step 2.1: Ler `tests/conftest.py` atual**

```bash
cat tests/conftest.py
```

Identifique:
- Imports existentes (`pytest`, `Settings`, `Config`, `command`, sqlalchemy stuff).
- A fixture `settings` (session-scoped).
- A fixture `db_engine` (session-scoped, sync, faz `command.upgrade(head)` + teardown `command.downgrade(base)`).
- Outras fixtures existentes a preservar.

- [ ] **Step 2.2: Adicionar imports**

No topo do arquivo, adicionar (logo após imports de sqlalchemy/alembic):

```python
import psycopg
```

Também importar `MonkeyPatch` dentro da fixture pra evitar import-level cost (no Step 2.3).

- [ ] **Step 2.3: Adicionar fixture `monkeypatch_session`**

Adicionar **antes** das fixtures `settings` e `db_engine` (ordem importa pra leitura, não pra resolução):

```python
@pytest.fixture(scope="session")
def monkeypatch_session() -> Iterator["MonkeyPatch"]:
    """Monkeypatch com escopo session (pytest built-in é function-scoped)."""
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()
```

Adicionar `MonkeyPatch` ao import condicional ou usar string-quoted annotation se preferir mypy strict.

- [ ] **Step 2.4: Adicionar fixture `_ensure_test_db`**

Logo após `monkeypatch_session`:

```python
@pytest.fixture(scope="session", autouse=True)
def _ensure_test_db(monkeypatch_session: "MonkeyPatch") -> None:
    """Cria polycopy_test se não existir; override POSTGRES_DB pra esta sessão."""
    test_db = "polycopy_test"
    base_settings = Settings()  # type: ignore[call-arg]

    admin_dsn = (
        f"postgresql://{base_settings.postgres_user}:"
        f"{base_settings.postgres_password.get_secret_value()}@"
        f"{base_settings.postgres_host}:{base_settings.postgres_port}/postgres"
    )

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (test_db,)
            )
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{test_db}"')

    monkeypatch_session.setenv("POSTGRES_DB", test_db)
```

**Atenção segurança:** `f-string` em SQL é geralmente perigoso, mas `test_db` é constante hardcoded literal `"polycopy_test"` — sem injection possível. Se o linter `bandit` reclamar (S608 ou similar), adicione `# noqa: S608` na linha do CREATE com comentário explicativo.

- [ ] **Step 2.5: Ajustar fixture `settings` pra declarar dependência**

LEIA a fixture atual:

```python
@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
```

Substituir por:

```python
@pytest.fixture(scope="session")
def settings(_ensure_test_db: None) -> Settings:
    return Settings()  # type: ignore[call-arg]
```

Razão: `Settings()` lê env vars no init. Se for instanciado antes do monkeypatch, pega `POSTGRES_DB=polycopy` (prod) em vez de `polycopy_test`.

- [ ] **Step 2.6: Ajustar fixture `db_engine` pra declarar dependência**

LEIA a assinatura atual:

```python
@pytest.fixture(scope="session")
def db_engine(settings: Settings, alembic_config: Config) -> Iterator[AsyncEngine]:
    ...
```

Substituir por:

```python
@pytest.fixture(scope="session")
def db_engine(
    settings: Settings,
    alembic_config: Config,
    _ensure_test_db: None,  # noqa: PT019 — força ordem antes do upgrade
) -> Iterator[AsyncEngine]:
    ...
```

Resto da fixture **sem mudança**. `command.upgrade(head)` agora roda contra `polycopy_test` transparentemente.

- [ ] **Step 2.7: Verificações intermediárias**

```bash
uv run mypy src/polycopy tests
uv run ruff check tests/conftest.py
uv run ruff format --check tests/conftest.py
```

Esperado: tudo limpo. Se `ruff format` flagar, rode `uv run ruff format tests/conftest.py`.

**NÃO rode `pytest tests/` ainda — o teste canário (T3) é o que valida ponta-a-ponta.**

- [ ] **Step 2.8: STOP — commit**

Implementer NÃO commita. Controller pede confirmação humana, depois:

```bash
git add tests/conftest.py
git commit -m "test(conftest): isolate integration tests on polycopy_test database"
```

---

## Task 3: Adicionar teste canário

**Files:**
- Create: `tests/integration/test_db_isolation.py`

---

- [ ] **Step 3.1: Criar arquivo do canário**

```python
"""Canário: confirma que testes integration rodam contra polycopy_test.

Falha imediatamente se algum dia o monkeypatch quebrar — protege contra
regressão silenciosa que destrói dados de produção.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.integration


async def test_isolation_uses_test_database(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Garante que current_database() == polycopy_test (não polycopy)."""
    async with db_session_factory() as session:
        result = await session.execute(text("SELECT current_database()"))
        assert result.scalar() == "polycopy_test"
```

- [ ] **Step 3.2: Rodar o teste canário isoladamente**

```bash
uv run pytest tests/integration/test_db_isolation.py -v 2>&1 | tail -10
```

Esperado: **1 PASS**. O teste retorna `"polycopy_test"`, confirmando que monkeypatch + Settings + alembic estão alinhados.

Se falhar com:
- `"polycopy"` retornado: monkeypatch não pegou. Verifique ordem das fixtures (Step 2.5/2.6).
- `OperationalError`: postgres não tá rodando ou DB não existe. Verifique `docker compose ps postgres`.
- `InsufficientPrivilege`: user `polycopy` sem CREATEDB. Conceder via psql admin: `ALTER USER polycopy CREATEDB;`.

- [ ] **Step 3.3: Verificações estáticas**

```bash
uv run mypy tests/integration/test_db_isolation.py
uv run ruff check tests/integration/test_db_isolation.py
uv run ruff format --check tests/integration/test_db_isolation.py
```

Esperado: limpo.

- [ ] **Step 3.4: STOP — commit**

```bash
git add tests/integration/test_db_isolation.py
git commit -m "test(integration): add canary asserting tests run on polycopy_test"
```

---

## Task 4: Validação ponta-a-ponta — proteção real da prod

**Files:** nenhum (validação operacional pura)

---

- [ ] **Step 4.1: Capturar estado pré-teste da prod**

```bash
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\dt" > /tmp/prod_schema_before.txt
docker compose exec -T postgres psql -U polycopy -d polycopy -c "SELECT count(*) FROM wallet_trades" >> /tmp/prod_schema_before.txt 2>/dev/null || echo "wallet_trades count unavailable" >> /tmp/prod_schema_before.txt
cat /tmp/prod_schema_before.txt
```

Esperado: lista de tabelas (markets, wallet_trades, risk_decisions, order_sizings, order_executions, market_resolutions, alembic_version) + count atual de wallet_trades.

- [ ] **Step 4.2: Rodar suite completa**

```bash
docker compose stop resolver  # evita interferência do agente em produção
uv run pytest tests/ 2>&1 | tail -10
```

Esperado: 412 passed + 11 falhas pré-existentes + 1 NOVO teste canário (= 413 passed total).

- [ ] **Step 4.3: Capturar estado pós-teste da prod**

```bash
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\dt" > /tmp/prod_schema_after.txt
docker compose exec -T postgres psql -U polycopy -d polycopy -c "SELECT count(*) FROM wallet_trades" >> /tmp/prod_schema_after.txt 2>/dev/null || echo "wallet_trades count unavailable" >> /tmp/prod_schema_after.txt
diff /tmp/prod_schema_before.txt /tmp/prod_schema_after.txt
```

Esperado: **diff vazio**. Schema de produção e count de wallet_trades idênticos antes/depois. Esta é **a prova** de que o fix funciona.

Se diff mostrar mudança: fix não funcionou. Investigue qual fixture rodou contra prod.

- [ ] **Step 4.4: Confirmar test DB existe e está vazio**

```bash
docker compose exec -T postgres psql -U polycopy -d polycopy_test -c "\dt"
```

Esperado: zero tabelas (downgrade rodou no test DB), apenas `alembic_version` ou nada.

- [ ] **Step 4.5: Restart resolver + recheck health**

```bash
docker compose start resolver
sleep 8
docker compose ps resolver
```

Esperado: container `Up (healthy)`. Continua rodando contra `polycopy` (produção).

- [ ] **Step 4.6: STOP — sem commit**

T4 é validação operacional, não modifica nenhum arquivo. Se passou, prossiga pra T5. Se falhou, volte e investigue antes de seguir.

---

## Task 5: Documentar em `ARCHITECTURE.md`

**Files:**
- Modify: `ARCHITECTURE.md`

---

- [ ] **Step 5.1: Identificar local pra inserir**

LEIA `ARCHITECTURE.md` e procure seção que fala de testes ou desenvolvimento local. Se não houver, adicione no final ou após a seção de containers.

- [ ] **Step 5.2: Adicionar seção "Running tests"**

Adicionar:

```markdown
## Running tests

A suite `pytest tests/` roda contra um DB lógico separado (`polycopy_test`) no mesmo container Postgres. Produção (`polycopy`) permanece intocada.

**Setup automático:** `tests/conftest.py` cria `polycopy_test` on-demand na primeira execução. Schema é re-aplicado via alembic a cada session. Teardown dropa as tabelas (mas mantém o DB pra próximas sessions).

**Pré-requisito:** o user Postgres precisa de privilege `CREATEDB`. Default no compose local. Pra ambientes que não permitem (ex: managed Postgres em CI futuro), criar `polycopy_test` manualmente antes:

```sql
CREATE DATABASE polycopy_test;
```

**Canário de isolação:** `tests/integration/test_db_isolation.py` confirma que cada session de pytest roda contra `polycopy_test`. Se este teste falhar, **pare imediatamente** — algum monkeypatch quebrou e os testes podem estar tocando produção.

**Comandos úteis:**

```bash
# Estado da prod
docker compose exec postgres psql -U polycopy -d polycopy -c "\dt"

# Estado do test DB
docker compose exec postgres psql -U polycopy -d polycopy_test -c "\dt"

# Drop test DB (raro — só pra reset completo)
docker compose exec postgres psql -U polycopy -d postgres -c "DROP DATABASE polycopy_test"
```
```

- [ ] **Step 5.3: Verificações**

```bash
uv run pytest tests/ 2>&1 | tail -5
```

Esperado: ainda 413 passed (sem regressão).

- [ ] **Step 5.4: STOP — commit**

```bash
git add ARCHITECTURE.md
git commit -m "docs(architecture): document polycopy_test isolation in test setup"
```

---

## Self-Review (autor do plano)

**Spec coverage:**

| Spec § | Coberto em |
|---|---|
| §3.1 DB lógico separado polycopy_test | T2 (fixture _ensure_test_db) |
| §3.2 Criação on-demand | T2 (psycopg admin connect + CREATE) |
| §3.3 Override transparente via POSTGRES_DB | T2 (monkeypatch.setenv) |
| §3.4 Teardown atual mantido | T2 (db_engine sem mudança no teardown) |
| §3.5 Test DB persiste entre sessões | T2 (não há DROP DATABASE no teardown) |
| §3.6 Privilege CREATEDB requerida | T5 (documentação) |
| §4.1 _ensure_test_db fixture | T2 |
| §4.2 db_engine declara dependência | T2 |
| §4.3 alembic/env.py sem mudança | implícito (não há task) |
| §4.4 settings declara dependência | T2 |
| §4.5 psycopg dev dep | T1 |
| §5 Teste canário | T3 |
| §6 Edge cases | T3 step 3.2 (mensagens de erro listadas) |
| §7 Migration de execuções existentes | T4 (validação E2E confirma) |
| §8 Documentação | T5 |
| §11 Sucesso (diff vazio prod schema) | T4 step 4.3 |

Coverage completa.

**Placeholder scan:** sem TBD/TODO/"add appropriate handling".

**Type consistency:**
- `_ensure_test_db: None` em T2 (def), T2 step 2.5 (settings), T2 step 2.6 (db_engine).
- `monkeypatch_session: "MonkeyPatch"` consistente.
- `polycopy_test` (literal) em T2, T3, T4, T5.
- `psycopg` (não `psycopg2`) em T1 e T2.

**Bite-sized check:** cada step é 2-5 minutos. Task mais pesada é T2 (~7 steps mas todos simples). Implementer copia snippets, roda comandos.

**Reviewer:** nenhum obrigatório (spec § decisão: "mudança restrita a infra de testes"). Cadência: checkpoint humano por task.
