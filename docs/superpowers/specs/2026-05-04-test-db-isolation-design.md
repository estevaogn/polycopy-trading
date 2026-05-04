# Spec — Test Database Isolation

**Data:** 2026-05-04
**Status:** Aprovado

## 1. Problema

A suite `pytest tests/` destrói tabelas do DB de produção. `tests/conftest.py:75` (fixture `db_engine`, scope=session) faz no teardown:

```python
finally:
    command.downgrade(alembic_config, "base")
```

Como `Settings().postgres_async_dsn` aponta pra `polycopy` (DB único compartilhado entre dev local + DRY-RUN em produção no Hetzner), o downgrade dropa **todas** as tabelas operacionais. Cada execução completa de `pytest tests/` zera dados acumulados (incluindo `wallet_trades` coletados pelo watcher).

**Histórico recente:** sessão de 2026-05-04 perdeu 1+ dia de `wallet_trades` em DRY-RUN em duas ocasiões (T7 e T8 do Plano 5A) por execuções de pytest durante validação. Schema teve que ser recriado manualmente via `alembic upgrade head`.

## 2. Solução adotada

Criar DB lógico separado `polycopy_test` no mesmo container Postgres. Tests rodam contra `polycopy_test`. Produção (`polycopy`) intocada.

### Por que DB separado e não:

- **Container postgres separado:** overhead operacional (+200MB RAM, +healthcheck, +depends_on em outros services se quisermos paridade) sem ganho de isolamento sobre DB lógico separado.
- **Schema separado dentro do mesmo DB:** alembic precisaria entender schemas, complexidade extra desnecessária.
- **TRUNCATE em vez de DROP:** ainda destrói dados de produção; só acelera teardown, não resolve.
- **SAVEPOINT/transaction rollback:** já existe em `db_session` fixture; mas testes que usam `db_session_factory` fazem commit real (necessário pra E2E) — rollback não cobre eles.

## 3. Decisões fixadas

1. **DB lógico separado** (`polycopy_test`) no mesmo container Postgres da produção.
2. **Criação on-demand pelo conftest** (`CREATE DATABASE IF NOT EXISTS`-equivalent via `pg_database` lookup). Zero setup pra novo dev — checkout + `pytest` funciona.
3. **Override transparente via `POSTGRES_DB` env var** monkeypatched no escopo da sessão. Settings não muda — fonte única de verdade.
4. **Teardown atual mantido** (`command.downgrade(alembic_config, "base")` continua dropando tables, mas agora **do test DB**).
5. **Test DB persiste entre sessões** (não é dropado no teardown). Próxima session faz upgrade no DB existente vazio.
6. **Privilege CREATEDB requerida** no Postgres user — default no compose local. Documentar em runbook pra eventual migração CI.

## 4. Componentes

### 4.1 `tests/conftest.py` — nova fixture autouse `_ensure_test_db`

Fixture session-scoped, autouse, ordem antes de `db_engine`:

```python
import psycopg

@pytest.fixture(scope="session")
def monkeypatch_session():
    """Monkeypatch com escopo session (built-in monkeypatch é function-scoped)."""
    from _pytest.monkeypatch import MonkeyPatch
    mp = MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="session", autouse=True)
def _ensure_test_db(monkeypatch_session) -> None:
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

### 4.2 `db_engine` fixture — declarar dependência explícita

Adicionar `_ensure_test_db` como parâmetro pra forçar ordem (autouse session-scoped não garante ordem com outras fixtures do mesmo escopo se não houver dependência declarada):

```python
@pytest.fixture(scope="session")
def db_engine(
    settings: Settings,
    alembic_config: Config,
    _ensure_test_db: None,  # noqa: PT019 — força ordem
) -> Iterator[AsyncEngine]:
    ...
```

Resto da fixture sem mudança. `command.upgrade(head)` e `command.downgrade(base)` operam no test DB transparentemente (Settings agora resolve `polycopy_test`).

**`settings` fixture** também precisa declarar `_ensure_test_db` como dependência, pois `Settings()` lê env vars no init — se for instanciado antes do monkeypatch, pega valor antigo:

```python
@pytest.fixture(scope="session")
def settings(_ensure_test_db: None) -> Settings:
    return Settings()  # type: ignore[call-arg]
```

### 4.3 `alembic/env.py` — sem mudança

Já lê via `Settings().postgres_async_dsn` (linha 25). Monkeypatch transparente.

### 4.4 `settings` fixture — declarar dependência (ver §4.2)

Conforme detalhado em §4.2, fixture `settings` recebe `_ensure_test_db: None` como parâmetro pra forçar ordem antes do `Settings()` ser instanciado.

### 4.5 `pyproject.toml` — adicionar dependência

`psycopg` (v3, sync) como dev dependency. Mais simples que asyncpg pra DDL admin one-shot. ~150KB.

## 5. Teste canário

Adicionar 1 teste em `tests/integration/test_db_isolation.py`:

```python
"""Canário: confirma que testes integration rodam contra polycopy_test."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.integration


async def test_isolation_uses_test_database(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Falha imediatamente se algum dia o monkeypatch quebrar."""
    async with db_session_factory() as session:
        result = await session.execute(text("SELECT current_database()"))
        assert result.scalar() == "polycopy_test"
```

Se este teste passar, garantimos que **nenhum** outro teste tocou produção (pq todos compartilham o `db_engine` session-scoped).

## 6. Edge cases

| Cenário | Comportamento |
|---|---|
| Postgres parado | `psycopg.connect` falha → pytest aborta com `OperationalError` claro |
| User sem privilege CREATEDB | `psycopg` lança `InsufficientPrivilege` — mensagem do Postgres já é clara |
| Test DB já existe (corrente) | `pg_database` lookup retorna 1 row → skip CREATE |
| Test DB existe mas com schema antigo | `command.upgrade(head)` aplica diff; alembic reporta inconsistências |
| Race entre dois pytest concurrent | `CREATE DATABASE` é serializado pelo Postgres; segundo recebe "already exists" benigno (mas autocommit=True + IF EXISTS lookup evita) |
| `psycopg` não instalado | Erro de import na coleta — `pip install` resolve |

## 7. Migration de execuções existentes

Primeira execução pós-deploy do fix:
1. Pytest detecta que `polycopy_test` não existe.
2. Cria.
3. Aplica migrations 0001..0007.
4. Rodam todos os testes.
5. Teardown drop tables (mas DB persiste).
6. Próxima session: `polycopy_test` existe vazio; alembic re-aplica.

Sem migração de dados — DB de produção fica como está.

## 8. Documentação

Atualizar `ARCHITECTURE.md` ou `README.md` com seção "Running tests" mencionando:
- Tests usam `polycopy_test` (separado de `polycopy`).
- Conftest cria automaticamente.
- Postgres user precisa de privilege CREATEDB.

## 9. Open questions / non-goals

- **CI futuro sem privilege CREATEDB:** se algum dia rodar testes em ambiente sem CREATEDB (ex: managed Postgres em CI), refator pra criar DB via fixture init script no docker-compose. Out of scope por enquanto.
- **Paralelização de testes (`pytest-xdist`):** spec atual assume 1 worker. Múltiplos workers compartilhando o mesmo `polycopy_test` causa race em downgrade/upgrade. Out of scope — projeto não usa xdist.
- **TRUNCATE per-test em vez de transaction rollback:** spec mantém o pattern atual (sessions com rollback em `db_session`, sessions com commit em `db_session_factory`). Sem mudança.

## 10. Roadmap de implementação

Tarefas estimadas (subagent-driven, ~1 dia):

1. **T1:** Adicionar dep `psycopg` em pyproject + uv lock.
2. **T2:** Adicionar fixture `monkeypatch_session` + `_ensure_test_db` em `tests/conftest.py`.
3. **T3:** Adicionar teste canário `tests/integration/test_db_isolation.py`.
4. **T4:** Validar end-to-end: rodar `pytest tests/` 2x, confirmar que produção `polycopy` mantém schema/dados intactos.
5. **T5:** Documentar em `ARCHITECTURE.md` (seção "Running tests"). Mencionar privilege CREATEDB como pré-requisito.

Reviewer obrigatório: nenhum (mudança restrita a infra de testes).

## 11. Sucesso

- `psql -U polycopy -d polycopy -c "\dt"` antes e depois de `pytest tests/` produz **mesmo output** (schema preservado).
- `psql -U polycopy -d polycopy_test -c "\dt"` após `pytest tests/` mostra zero tabelas (downgrade rodou).
- `polycopy_test` existe entre sessions (lookup em `pg_database` confirma).
- Container `polycopy-resolver:9107` continua healthy ininterruptamente durante `pytest tests/`.
- Suite continua em 412 passed + 11 falhas pré-existentes (sem regressão).
