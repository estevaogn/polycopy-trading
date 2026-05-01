# Fase 1A — Fundação (domain, ports, config, logging): Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pavimentar a base da Fase 1 do PolyCopy — smoke test de conectividade Python→infra, domínio puro testado (`value objects`, `models`, `events`), ports (interfaces), `Settings` com pydantic-settings, e logging com `structlog`. Zero adapter concreto, zero migration, zero agente — esses entram nos Planos 1B e 1C.

**Architecture:** Hexagonal/Ports & Adapters. Domínio é Pydantic v2 `frozen` models com validators (sem dependência de I/O). Ports são `typing.Protocol` (duck-typed, fácil de mockar). Stack I/O é async em todo lugar. `Settings` lê `.env` na inicialização, sem fallbacks mágicos. `structlog` configurado uma vez no boot do processo, JSON em prod / console colorido em dev.

**Tech Stack adicionado nesta fase:**
- `pydantic-settings>=2.5` (config via env)
- `structlog>=24.4` (logging estruturado)
- `asyncpg>=0.30` (driver Postgres async — já entra agora pra suportar smoke test e Plano 1B)
- `nats-py>=2.7` (cliente NATS async)
- `redis>=5.1` (cliente Redis com `redis.asyncio`)
- `python-dotenv>=1.0` (dev only — carrega `.env` em testes antes de Settings existir)

**Source spec:** `PROMPT_POLYCOPY_v2.md` (Fase 1, esboço passos 1.0 a 1.5). Decisões técnicas adicionais (async stack, Protocol vs ABC, etc) estão documentadas na seção "Decisões técnicas" deste plano.

**Execution model:** Usuário pede uma Task por vez (ex: "execute Task 1"). Implementador segue os steps, valida, commita. **NÃO avança pra Task N+1 sem confirmação explícita do usuário.**

---

## Pre-flight checklist (uma vez, antes da Task 1)

- [ ] **Step P.1: Working directory correto**

Run: `pwd`
Expected: `/home/polycopy/projects/polycopy`

- [ ] **Step P.2: Fase 0 completa (5 commits convencionais)**

Run: `git log --oneline | head -10`
Expected: pelo menos os 5 commits da Fase 0:
```
414a906 chore: add pre-commit hooks and commitizen
30adf09 ci: add github actions pipeline for lint type test
396be15 feat: add docker-compose infra with postgres timescale nats redis prometheus
3ec527c docs: add README, env example, and bootstrap script
c7684ce chore: bootstrap pyproject and project skeleton
```

- [ ] **Step P.3: Working tree limpo (ou só com untracked aceitáveis)**

Run: `git status`
Expected: `On branch main` + working tree clean (ou apenas `PROMPT_POLYCOPY_v2.md` e `docs/` untracked, que são esperados — esses entram em commit separado, fora deste plano).

- [ ] **Step P.4: `.env` existe com perms 600**

Run: `ls -l .env`
Expected: `-rw------- 1 polycopy polycopy ...` (chmod 600, dono polycopy).

Se faltar: `bash scripts/bootstrap-env.sh` antes de prosseguir.

- [ ] **Step P.5: Infra healthy**

Run: `docker compose ps`
Expected: 4 containers `(healthy)` — postgres, nats, redis, prometheus.

Se faltar: `docker compose up -d --wait`.

- [ ] **Step P.6: Suite Python verde no estado atual**

Run:
```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0.

Se falhar: corrija ANTES de Task 1. Não comece a Fase 1A em codebase vermelho.

- [ ] **Step P.7: Pre-commit hooks ativos**

Run: `uv run pre-commit run --all-files`
Expected: tudo passa.

Se algum hook falhar: corrija e commite (commit fora do escopo deste plano).

---

## Decisões técnicas fixadas neste plano

| Item | Decisão | Justificativa |
|---|---|---|
| Stack I/O | Async em todo lugar | NATS, HTTP, SQLAlchemy, Telegram — todos têm libs async maduras. 11 agentes vão ter I/O concorrente. |
| Cliente Postgres | `asyncpg` direto (não `psycopg`) | Driver async nativo, é o que o SQLAlchemy 2.x usa em modo async (Plano 1B). |
| Cliente NATS | `nats-py` (oficial) | Único cliente Python mantido pela equipe NATS. |
| Cliente Redis | `redis[asyncio]` (lib oficial `redis>=5`) | Cliente unificado sync+async. |
| Settings | `pydantic-settings>=2.5` | Tipado, lê `.env` automático, integra com pydantic. |
| Logging | `structlog` JSON em prod, console em dev | Controlado por `ENV=dev|prod`. Filtro de secrets baked-in. |
| Domain types | `pydantic.BaseModel` `frozen=True` | Queremos validators (ex: WalletAddress hex check); dataclass não tem. |
| Domain events | `pydantic.BaseModel` `frozen=True` com `event_id: UUID` + `occurred_at: datetime` | Imutável, serializável, rastreável. |
| Ports | `typing.Protocol` (não ABC) | Duck-typed, `runtime_checkable=False` (mypy-only); permite mock leve em testes. |
| `.env` em testes | `python-dotenv` na Task 1; substituído por Settings na Task 6 | Antes de existir `Settings`, precisamos carregar `.env` pra testes integration. Após Task 6, conftest usa `Settings()`. |
| Markers pytest | `integration` registrado em `pyproject.toml` | `--strict-markers` reclama se não registrar. |
| Coverage | Não enforçamos % no CI ainda | Spec diz "enforce no CI a partir da Fase 1"; vamos enforçar no fim do Plano 1C (depois que tivermos código de domínio significativo). |

---

## Estrutura de arquivos criada nesta fase

```
src/polycopy/
├── __init__.py                            # já existe
├── config.py                              # Task 6
├── domain/
│   ├── __init__.py                        # Task 2
│   ├── value_objects.py                   # Task 2
│   ├── models.py                          # Task 3
│   └── events.py                          # Task 4
├── ports/
│   ├── __init__.py                        # Task 5
│   ├── messaging.py                       # Task 5
│   ├── polymarket_data.py                 # Task 5
│   └── repository.py                      # Task 5
└── infrastructure/
    ├── __init__.py                        # Task 6
    └── observability/
        ├── __init__.py                    # Task 6
        └── logging.py                     # Task 6

tests/
├── __init__.py                            # já existe
├── conftest.py                            # Task 1; refatorado Task 6
├── test_smoke.py                          # já existe
├── unit/
│   ├── __init__.py                        # Task 2
│   └── domain/
│       ├── __init__.py                    # Task 2
│       ├── test_value_objects.py          # Task 2
│       ├── test_models.py                 # Task 3
│       └── test_events.py                 # Task 4
└── integration/
    ├── __init__.py                        # Task 1
    └── test_infra_connectivity.py         # Task 1
```

Ports não têm teste próprio — são interfaces; o teste é "código que importa o port type-checa". A validação real vem nos adapters (Plano 1B).

---

## Task 1: Passo 1.0 — Smoke test conectividade Python→infra

**Objetivo:** ter testes integration que conectam Python aos 3 backends (postgres, nats, redis) e confirmam que respondem. Marcar como `integration` para separar de unit tests. Configurar conftest.py mínima com `python-dotenv` carregando `.env` da raiz.

**Files:**
- Modify: `pyproject.toml` (deps `asyncpg`, `nats-py`, `redis`, `python-dotenv` dev; `markers` em `[tool.pytest.ini_options]`)
- Create: `tests/conftest.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/test_infra_connectivity.py`

---

- [ ] **Step 1.1: Modify `pyproject.toml` — adicionar deps runtime**

Localizar `[project] dependencies` e atualizar:

```toml
dependencies = [
    "pydantic>=2.9",
    "asyncpg>=0.30",
    "nats-py>=2.7",
    "redis>=5.1",
]
```

(`asyncpg`, `nats-py`, `redis` entram como runtime porque vão ser usadas pelos adapters do Plano 1B/1C. `python-dotenv` é dev-only.)

- [ ] **Step 1.2: Modify `pyproject.toml` — adicionar `python-dotenv` ao dev group**

```toml
[dependency-groups]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.24",
    "pytest-cov>=5",
    "mypy>=1.13",
    "ruff>=0.7",
    "pre-commit>=4",
    "commitizen>=4",
    "python-dotenv>=1.0",
]
```

- [ ] **Step 1.3: Modify `pyproject.toml` — registrar marker `integration`**

Em `[tool.pytest.ini_options]`, adicionar `markers`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
addopts = ["--tb=short", "--strict-markers", "--strict-config"]
markers = [
    "integration: tests that require running infrastructure (postgres, nats, redis)",
]
```

- [ ] **Step 1.4: Re-sync deps**

Run: `uv sync`
Expected: `uv.lock` atualizado, novos pacotes instalados.

- [ ] **Step 1.5: Create `tests/conftest.py`**

```python
"""Shared test fixtures and bootstrap.

Carrega `.env` da raiz do repo se existir, antes dos testes coletarem env vars.
Em Task 6 essa lógica passa a usar `polycopy.config.Settings`; por ora, dotenv direto.
"""
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _REPO_ROOT / ".env"

if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)
```

- [ ] **Step 1.6: Create `tests/integration/__init__.py`** (vazio)

```python
```

- [ ] **Step 1.7: Write the failing test — `tests/integration/test_infra_connectivity.py`**

```python
"""Smoke tests de conectividade Python -> infra (postgres, nats, redis).

Pré-requisito: `docker compose up -d --wait` rodando, `.env` populado.
Marcador: `integration`. Rodar com `uv run pytest -m integration`.
"""
from __future__ import annotations

import os

import asyncpg
import nats
import pytest
import redis.asyncio as aioredis

pytestmark = pytest.mark.integration


def _postgres_dsn() -> str:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    port = os.environ.get("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@127.0.0.1:{port}/{db}"


async def test_postgres_connect_and_select_one() -> None:
    conn = await asyncpg.connect(_postgres_dsn())
    try:
        result = await conn.fetchval("SELECT 1")
        assert result == 1
    finally:
        await conn.close()


async def test_postgres_timescale_extension_loaded() -> None:
    conn = await asyncpg.connect(_postgres_dsn())
    try:
        ext = await conn.fetchval(
            "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'"
        )
        assert ext == "timescaledb"
    finally:
        await conn.close()


async def test_nats_connect_and_close() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])
    try:
        assert nc.is_connected is True
    finally:
        await nc.close()


async def test_redis_ping() -> None:
    r = aioredis.from_url(os.environ["REDIS_URL"])
    try:
        pong = await r.ping()
        assert pong is True
    finally:
        await r.aclose()
```

- [ ] **Step 1.8: Run integration tests — esperado PASS (infra está up)**

Run: `uv run pytest -m integration -v`
Expected: 4 tests passed.

Se falhar `POSTGRES_USER` keyerror: confira que `.env` existe e tem as vars. `cat .env | grep POSTGRES_`.
Se falhar `connection refused`: confira `docker compose ps` — todos healthy.
Se falhar `password authentication failed`: o `.env` tem password antigo de quando o volume `polycopy_postgres_data` foi criado. Solução: `docker compose down -v` (DESTRUTIVO — apaga dados) + `docker compose up -d --wait`.

- [ ] **Step 1.9: Confirmar que unit tests não rodam por default**

Por enquanto não há unit tests novos, mas vamos confirmar a separação: `uv run pytest -m "not integration"`
Expected: passa o `test_smoke.py` (1 test).

E `uv run pytest` (sem filtro) deve rodar TODOS — 1 + 4 = 5 tests.

- [ ] **Step 1.10: Run full quality gate**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0.

(Se ruff/format reclamar do conftest.py ou do test novo: `uv run ruff format .` e re-rode `--check`.)

- [ ] **Step 1.11: Stage e commit**

Run:
```bash
git add pyproject.toml uv.lock tests/conftest.py tests/integration/
git status
```

Confirme que NADA fora do escopo está staged. Então:

```bash
git commit -m "test: add infra connectivity smoke tests"
```

- [ ] **Step 1.12: STOP — esperar confirmação humana antes de Task 2**

---

## Task 2: Passo 1.1 — Domain value objects

**Objetivo:** ter os 6 value objects (`Money`, `Price`, `Bps`, `WalletAddress`, `ConditionId`, `TokenId`) implementados como Pydantic frozen models com validators, e testados unitariamente. Coverage do `src/polycopy/domain/value_objects.py` ≥ 95%.

**Files:**
- Create: `src/polycopy/domain/__init__.py`
- Create: `src/polycopy/domain/value_objects.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/unit/domain/__init__.py`
- Create: `tests/unit/domain/test_value_objects.py`

---

- [ ] **Step 2.1: Create `src/polycopy/domain/__init__.py`** (vazio)

```python
```

- [ ] **Step 2.2: Create `tests/unit/__init__.py`** (vazio)

```python
```

- [ ] **Step 2.3: Create `tests/unit/domain/__init__.py`** (vazio)

```python
```

- [ ] **Step 2.4: Write the failing tests — `tests/unit/domain/test_value_objects.py`**

```python
"""Unit tests for domain value objects."""
from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from polycopy.domain.value_objects import (
    Bps,
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)


class TestMoney:
    def test_construct_quantizes_to_six_decimals(self) -> None:
        m = Money(amount=Decimal("1.123456789"))
        assert m.amount == Decimal("1.123457")  # ROUND_HALF_EVEN

    def test_zero_factory(self) -> None:
        assert Money.zero().amount == Decimal("0.000000")

    def test_from_usdc_int(self) -> None:
        assert Money.from_usdc(100).amount == Decimal("100.000000")

    def test_from_usdc_str(self) -> None:
        assert Money.from_usdc("1.5").amount == Decimal("1.500000")

    def test_addition(self) -> None:
        a = Money.from_usdc("1.50")
        b = Money.from_usdc("2.25")
        assert (a + b).amount == Decimal("3.750000")

    def test_subtraction(self) -> None:
        a = Money.from_usdc("5.00")
        b = Money.from_usdc("1.50")
        assert (a - b).amount == Decimal("3.500000")

    def test_lt(self) -> None:
        assert Money.from_usdc("1") < Money.from_usdc("2")
        assert not (Money.from_usdc("2") < Money.from_usdc("1"))

    def test_frozen(self) -> None:
        m = Money.from_usdc("1")
        with pytest.raises(ValidationError):
            m.amount = Decimal("99")  # type: ignore[misc]

    def test_negative_allowed(self) -> None:
        # Money pode ser negativo (PnL drawdown). Sem validador de >= 0.
        assert Money.from_usdc("-1.5").amount == Decimal("-1.500000")


class TestPrice:
    def test_zero_and_one_are_valid(self) -> None:
        assert Price(value=Decimal("0")).value == Decimal("0.0000")
        assert Price(value=Decimal("1")).value == Decimal("1.0000")

    def test_quantize_to_four_decimals(self) -> None:
        p = Price(value=Decimal("0.12345678"))
        assert p.value == Decimal("0.1235")

    def test_below_zero_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Price(value=Decimal("-0.0001"))

    def test_above_one_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Price(value=Decimal("1.0001"))


class TestBps:
    def test_construct_from_int(self) -> None:
        assert Bps(value=200).value == 200

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Bps(value=-1)

    def test_above_10000_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Bps(value=10001)

    def test_to_decimal_fraction(self) -> None:
        # 200 bps = 2% = 0.02
        assert Bps(value=200).as_fraction() == Decimal("0.02")


class TestWalletAddress:
    VALID = "0x1234567890abcdef1234567890abcdef12345678"

    def test_valid_lowercase(self) -> None:
        w = WalletAddress(value=self.VALID)
        assert w.value == self.VALID

    def test_normalized_to_lowercase(self) -> None:
        upper = "0x" + "A" * 40
        w = WalletAddress(value=upper)
        assert w.value == upper.lower()

    def test_missing_0x_prefix_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletAddress(value=self.VALID[2:])

    def test_wrong_length_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletAddress(value="0x1234")

    def test_non_hex_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletAddress(value="0x" + "z" * 40)


class TestConditionId:
    VALID = "0x" + "ab" * 32  # 64 hex chars

    def test_valid(self) -> None:
        c = ConditionId(value=self.VALID)
        assert c.value == self.VALID

    def test_normalized_to_lowercase(self) -> None:
        upper = "0x" + "AB" * 32
        c = ConditionId(value=upper)
        assert c.value == upper.lower()

    def test_wrong_length_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ConditionId(value="0xabcd")


class TestTokenId:
    def test_string_form_accepted(self) -> None:
        t = TokenId(value="123456789012345678901234567890")
        assert t.value == "123456789012345678901234567890"

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TokenId(value="-1")

    def test_non_numeric_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TokenId(value="abc")
```

- [ ] **Step 2.5: Run tests — esperado FAIL (módulo não existe)**

Run: `uv run pytest tests/unit/domain/test_value_objects.py -v`
Expected: ImportError / ModuleNotFoundError em `polycopy.domain.value_objects`.

- [ ] **Step 2.6: Implement `src/polycopy/domain/value_objects.py`**

```python
"""Domain value objects: tipos primitivos imutáveis com validação."""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

_USDC_QUANTUM = Decimal("0.000001")  # USDC tem 6 decimais on-chain
_PRICE_QUANTUM = Decimal("0.0001")  # 4 casas é o que CLOB Polymarket usa
_HEX_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_HEX_CONDITION_ID_RE = re.compile(r"^0x[0-9a-f]{64}$")
_NUMERIC_TOKEN_ID_RE = re.compile(r"^[0-9]+$")


class Money(BaseModel):
    """Valor monetário em USDC. Sempre quantizado para 6 casas decimais."""

    model_config = ConfigDict(frozen=True, strict=True)

    amount: Decimal

    @field_validator("amount", mode="after")
    @classmethod
    def _quantize(cls, v: Decimal) -> Decimal:
        return v.quantize(_USDC_QUANTUM)

    @classmethod
    def zero(cls) -> "Money":
        return cls(amount=Decimal("0"))

    @classmethod
    def from_usdc(cls, value: int | str | Decimal) -> "Money":
        return cls(amount=Decimal(str(value)))

    def __add__(self, other: "Money") -> "Money":
        return Money(amount=self.amount + other.amount)

    def __sub__(self, other: "Money") -> "Money":
        return Money(amount=self.amount - other.amount)

    def __lt__(self, other: "Money") -> bool:
        return self.amount < other.amount

    def __le__(self, other: "Money") -> bool:
        return self.amount <= other.amount


class Price(BaseModel):
    """Preço de outcome no Polymarket: probabilidade implícita ∈ [0, 1]."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: Annotated[Decimal, Field(ge=0, le=1)]

    @field_validator("value", mode="after")
    @classmethod
    def _quantize(cls, v: Decimal) -> Decimal:
        return v.quantize(_PRICE_QUANTUM)


class Bps(BaseModel):
    """Basis points: 1 bp = 0.01%. Inteiro ∈ [0, 10000]."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: Annotated[int, Field(ge=0, le=10_000)]

    def as_fraction(self) -> Decimal:
        return Decimal(self.value) / Decimal(10_000)


class WalletAddress(BaseModel):
    """Endereço Ethereum/Polygon: 0x + 40 hex, normalizado lowercase."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: str

    @field_validator("value", mode="after")
    @classmethod
    def _validate_and_lower(cls, v: str) -> str:
        normalized = v.lower()
        if not _HEX_ADDRESS_RE.match(normalized):
            raise ValueError(f"invalid wallet address: {v!r} (expected 0x + 40 hex chars)")
        return normalized


class ConditionId(BaseModel):
    """Polymarket condition_id: 0x + 64 hex (32 bytes), normalizado lowercase."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: str

    @field_validator("value", mode="after")
    @classmethod
    def _validate_and_lower(cls, v: str) -> str:
        normalized = v.lower()
        if not _HEX_CONDITION_ID_RE.match(normalized):
            raise ValueError(f"invalid condition_id: {v!r} (expected 0x + 64 hex chars)")
        return normalized


class TokenId(BaseModel):
    """Polymarket token_id: uint256 representado como string decimal."""

    model_config = ConfigDict(frozen=True, strict=True)

    value: str

    @field_validator("value", mode="after")
    @classmethod
    def _validate_numeric(cls, v: str) -> str:
        if not _NUMERIC_TOKEN_ID_RE.match(v):
            raise ValueError(f"invalid token_id: {v!r} (expected non-negative integer string)")
        return v
```

- [ ] **Step 2.7: Run tests — esperado PASS**

Run: `uv run pytest tests/unit/domain/test_value_objects.py -v`
Expected: todos os tests passam.

- [ ] **Step 2.8: Run full quality gate**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0.

- [ ] **Step 2.9: Verificar coverage do módulo**

Run: `uv run pytest tests/unit/domain/test_value_objects.py --cov=src/polycopy/domain/value_objects --cov-report=term-missing`
Expected: coverage ≥ 95%. Linhas faltantes (se houver) devem ser branches improváveis.

- [ ] **Step 2.10: Stage e commit**

```bash
git add src/polycopy/domain/ tests/unit/
git commit -m "feat(domain): add value objects (Money, Price, Bps, WalletAddress, ConditionId, TokenId)"
```

- [ ] **Step 2.11: STOP — esperar confirmação humana antes de Task 3**

---

## Task 3: Passo 1.2 — Domain models (`Wallet`, `Trade`, `Position`)

**Objetivo:** ter as 3 entidades principais do domínio modeladas como pydantic frozen models, usando os value objects da Task 2. Testar construção e invariantes.

**Files:**
- Create: `src/polycopy/domain/models.py`
- Create: `tests/unit/domain/test_models.py`

---

- [ ] **Step 3.1: Write the failing tests — `tests/unit/domain/test_models.py`**

```python
"""Unit tests for domain models."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from polycopy.domain.models import Side, Trade, Wallet
from polycopy.domain.value_objects import (
    Bps,
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"
_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN = "12345678901234567890"
_VALID_TX = "0x" + "cd" * 32


class TestWallet:
    def test_construct_minimal(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="alice",
            enabled=True,
        )
        assert w.nickname == "alice"
        assert w.enabled is True
        assert w.max_slippage_bps.value == 200  # default

    def test_disabled_wallet(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="bob",
            enabled=False,
        )
        assert w.enabled is False

    def test_custom_slippage(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="alice",
            enabled=True,
            max_slippage_bps=Bps(value=500),
        )
        assert w.max_slippage_bps.value == 500

    def test_frozen(self) -> None:
        w = Wallet(
            address=WalletAddress(value=_VALID_ADDR),
            nickname="alice",
            enabled=True,
        )
        with pytest.raises(ValidationError):
            w.enabled = False  # type: ignore[misc]


class TestTrade:
    def _trade(self, **overrides: object) -> Trade:
        defaults: dict[str, object] = {
            "tx_hash": _VALID_TX,
            "log_index": 0,
            "wallet": WalletAddress(value=_VALID_ADDR),
            "condition_id": ConditionId(value=_VALID_COND),
            "token_id": TokenId(value=_VALID_TOKEN),
            "side": Side.BUY,
            "price": Price(value=Decimal("0.55")),
            "size_usdc": Money.from_usdc("10"),
            "occurred_at": datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        }
        defaults.update(overrides)
        return Trade(**defaults)  # type: ignore[arg-type]

    def test_construct_buy(self) -> None:
        t = self._trade()
        assert t.side is Side.BUY
        assert t.size_usdc.amount == Decimal("10.000000")

    def test_construct_sell(self) -> None:
        t = self._trade(side=Side.SELL)
        assert t.side is Side.SELL

    def test_dedup_key(self) -> None:
        t = self._trade()
        assert t.dedup_key == (_VALID_TX, 0)

    def test_negative_log_index_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._trade(log_index=-1)

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._trade(occurred_at=datetime(2026, 5, 1, 12, 0))  # sem tzinfo
```

- [ ] **Step 3.2: Run tests — esperado FAIL**

Run: `uv run pytest tests/unit/domain/test_models.py -v`
Expected: ImportError em `polycopy.domain.models`.

- [ ] **Step 3.3: Implement `src/polycopy/domain/models.py`**

```python
"""Domain models: Wallet, Trade, Position e tipos relacionados."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from polycopy.domain.value_objects import (
    Bps,
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Wallet(BaseModel):
    """Carteira observada (copiada). Imutável dentro de uma sessão."""

    model_config = ConfigDict(frozen=True, strict=True)

    address: WalletAddress
    nickname: str
    enabled: bool
    max_slippage_bps: Bps = Bps(value=200)


class Trade(BaseModel):
    """Trade detectado on-chain ou via Data API. Identidade = (tx_hash, log_index)."""

    model_config = ConfigDict(frozen=True, strict=True)

    tx_hash: str
    log_index: Annotated[int, Field(ge=0)]
    wallet: WalletAddress
    condition_id: ConditionId
    token_id: TokenId
    side: Side
    price: Price
    size_usdc: Money
    occurred_at: datetime

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v

    @property
    def dedup_key(self) -> tuple[str, int]:
        return (self.tx_hash, self.log_index)


class Position(BaseModel):
    """Posição agregada por (wallet, condition, token). Read model."""

    model_config = ConfigDict(frozen=True, strict=True)

    wallet: WalletAddress
    condition_id: ConditionId
    token_id: TokenId
    size_usdc: Money  # capital alocado
    avg_price: Price
```

- [ ] **Step 3.4: Run tests — esperado PASS**

Run: `uv run pytest tests/unit/domain/test_models.py -v`
Expected: todos passam.

- [ ] **Step 3.5: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0.

- [ ] **Step 3.6: Stage e commit**

```bash
git add src/polycopy/domain/models.py tests/unit/domain/test_models.py
git commit -m "feat(domain): add Wallet, Trade, Position models with Side enum"
```

- [ ] **Step 3.7: STOP — esperar confirmação humana antes de Task 4**

---

## Task 4: Passo 1.3 — Domain events

**Objetivo:** ter eventos de domínio imutáveis (`WalletTradeDetected` é o único usado na Fase 1) com `event_id` UUID e `occurred_at` timezone-aware. Frozen pydantic model. Serializável pra payload NATS no Plano 1B.

**Files:**
- Create: `src/polycopy/domain/events.py`
- Create: `tests/unit/domain/test_events.py`

---

- [ ] **Step 4.1: Write the failing tests — `tests/unit/domain/test_events.py`**

```python
"""Unit tests for domain events."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)

_VALID_ADDR = "0x1234567890abcdef1234567890abcdef12345678"
_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN = "12345"
_VALID_TX = "0x" + "cd" * 32


def _trade() -> Trade:
    return Trade(
        tx_hash=_VALID_TX,
        log_index=0,
        wallet=WalletAddress(value=_VALID_ADDR),
        condition_id=ConditionId(value=_VALID_COND),
        token_id=TokenId(value=_VALID_TOKEN),
        side=Side.BUY,
        price=Price(value=Decimal("0.55")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
    )


class TestWalletTradeDetected:
    def test_construct(self) -> None:
        ev = WalletTradeDetected(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=timezone.utc),
            trade=_trade(),
        )
        assert isinstance(ev.event_id, UUID)
        assert ev.trade.dedup_key == (_VALID_TX, 0)

    def test_subject(self) -> None:
        assert WalletTradeDetected.SUBJECT == "wallet.trade.detected"

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WalletTradeDetected(
                event_id=uuid4(),
                occurred_at=datetime(2026, 5, 1, 12, 0),  # naive
                trade=_trade(),
            )

    def test_serialization_roundtrip(self) -> None:
        ev = WalletTradeDetected(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=timezone.utc),
            trade=_trade(),
        )
        payload = ev.model_dump_json()
        restored = WalletTradeDetected.model_validate_json(payload)
        assert restored == ev

    def test_frozen(self) -> None:
        ev = WalletTradeDetected(
            event_id=uuid4(),
            occurred_at=datetime.now(tz=timezone.utc),
            trade=_trade(),
        )
        with pytest.raises(ValidationError):
            ev.event_id = uuid4()  # type: ignore[misc]
```

- [ ] **Step 4.2: Run tests — esperado FAIL**

Run: `uv run pytest tests/unit/domain/test_events.py -v`
Expected: ImportError em `polycopy.domain.events`.

- [ ] **Step 4.3: Implement `src/polycopy/domain/events.py`**

```python
"""Domain events: imutáveis, identificáveis (event_id UUID), timezone-aware."""
from __future__ import annotations

from datetime import datetime
from typing import ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from polycopy.domain.models import Trade


class WalletTradeDetected(BaseModel):
    """Evento publicado quando o watcher detecta um trade de uma wallet observada.

    NATS subject: `wallet.trade.detected`.
    """

    SUBJECT: ClassVar[str] = "wallet.trade.detected"

    model_config = ConfigDict(frozen=True, strict=True)

    event_id: UUID
    occurred_at: datetime
    trade: Trade

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_tzaware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return v
```

- [ ] **Step 4.4: Run tests — esperado PASS**

Run: `uv run pytest tests/unit/domain/test_events.py -v`
Expected: todos passam.

- [ ] **Step 4.5: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0.

- [ ] **Step 4.6: Stage e commit**

```bash
git add src/polycopy/domain/events.py tests/unit/domain/test_events.py
git commit -m "feat(domain): add WalletTradeDetected event with NATS subject"
```

- [ ] **Step 4.7: STOP — esperar confirmação humana antes de Task 5**

---

## Task 5: Passo 1.4 — Ports (interfaces)

**Objetivo:** ter as 3 interfaces principais (`MessagingPort`, `PolymarketDataPort`, `WalletTradeRepository`) como `typing.Protocol`. Sem implementação. Apenas contratos. Permite que adapters concretos no Plano 1B implementem por duck-typing e testes mockem facilmente.

**Decisão de design:** ports não têm tests próprios — são interfaces. A garantia é que o mypy bate quando um adapter implementa o Protocol. Validação real vem nos adapters (Plano 1B).

**Files:**
- Create: `src/polycopy/ports/__init__.py`
- Create: `src/polycopy/ports/messaging.py`
- Create: `src/polycopy/ports/polymarket_data.py`
- Create: `src/polycopy/ports/repository.py`
- Create: `tests/unit/test_ports_typecheck.py` (apenas confirma que os Protocols importam e mypy aceita um stub fake)

---

- [ ] **Step 5.1: Create `src/polycopy/ports/__init__.py`**

```python
"""Ports: interfaces tipadas que adapters concretos implementam."""
from polycopy.ports.messaging import MessagingPort
from polycopy.ports.polymarket_data import PolymarketDataPort
from polycopy.ports.repository import WalletTradeRepository

__all__ = ["MessagingPort", "PolymarketDataPort", "WalletTradeRepository"]
```

- [ ] **Step 5.2: Create `src/polycopy/ports/messaging.py`**

```python
"""MessagingPort: contrato para publicar/assinar eventos no bus (NATS no Plano 1B)."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from polycopy.domain.events import WalletTradeDetected

EventHandler = Callable[[bytes], Awaitable[None]]


class MessagingPort(Protocol):
    """Bus de eventos. Implementação concreta: NATS JetStream (Plano 1B)."""

    async def publish_wallet_trade_detected(
        self, event: WalletTradeDetected
    ) -> None:
        """Publica evento no subject `wallet.trade.detected`."""
        ...

    async def subscribe(self, subject: str, handler: EventHandler) -> None:
        """Assina subject; handler recebe payload bruto (bytes JSON)."""
        ...

    async def close(self) -> None:
        """Fecha conexão com graceful drain."""
        ...
```

- [ ] **Step 5.3: Create `src/polycopy/ports/polymarket_data.py`**

```python
"""PolymarketDataPort: contrato para consultar dados de atividade na Polymarket."""
from __future__ import annotations

from datetime import datetime
from typing import Protocol

from polycopy.domain.models import Trade
from polycopy.domain.value_objects import WalletAddress


class PolymarketDataPort(Protocol):
    """Cliente da Polymarket Data API. Implementação concreta: httpx (Plano 1B)."""

    async def fetch_user_activity(
        self,
        wallet: WalletAddress,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[Trade]:
        """Retorna trades da wallet, ordenados por `occurred_at` desc.

        Se `since` for passado, retorna apenas trades com `occurred_at > since`.
        """
        ...
```

- [ ] **Step 5.4: Create `src/polycopy/ports/repository.py`**

```python
"""WalletTradeRepository: contrato para persistência de trades detectados."""
from __future__ import annotations

from datetime import datetime
from typing import Protocol

from polycopy.domain.models import Trade
from polycopy.domain.value_objects import WalletAddress


class WalletTradeRepository(Protocol):
    """Persistência de trades. Implementação concreta: SQLAlchemy + Postgres (Plano 1B)."""

    async def insert_if_absent(self, trade: Trade) -> bool:
        """Insere trade. Retorna True se inseriu, False se já existia (dedup por tx_hash+log_index)."""
        ...

    async def latest_occurred_at(self, wallet: WalletAddress) -> datetime | None:
        """Retorna `occurred_at` do trade mais recente da wallet, ou None."""
        ...
```

- [ ] **Step 5.5: Create `tests/unit/test_ports_typecheck.py`** — confirma imports + um stub mostrando como adapter implementa o Protocol.

```python
"""Smoke tests para confirmar que os ports são importáveis e implementáveis.

NÃO testa comportamento (Protocol não tem comportamento). Mypy faz o trabalho
de validar que adapters concretos no Plano 1B implementam os contratos.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from polycopy.domain.events import WalletTradeDetected
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.ports import (
    MessagingPort,
    PolymarketDataPort,
    WalletTradeRepository,
)
from polycopy.ports.messaging import EventHandler


class _FakeMessaging:
    """Stub que implementa MessagingPort por duck-typing."""

    def __init__(self) -> None:
        self.published: list[WalletTradeDetected] = []

    async def publish_wallet_trade_detected(
        self, event: WalletTradeDetected
    ) -> None:
        self.published.append(event)

    async def subscribe(self, subject: str, handler: EventHandler) -> None:
        return None

    async def close(self) -> None:
        return None


def _addr() -> WalletAddress:
    return WalletAddress(value="0x" + "1" * 40)


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=_addr(),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=timezone.utc),
    )


def _accepts_messaging_port(_: MessagingPort) -> None:
    """Helper: mypy falha aqui se o argumento não satisfizer MessagingPort."""
    return None


async def test_fake_messaging_satisfies_port() -> None:
    fake = _FakeMessaging()
    _accepts_messaging_port(fake)  # mypy strict garante o contrato

    ev = WalletTradeDetected(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=timezone.utc),
        trade=_trade(),
    )
    await fake.publish_wallet_trade_detected(ev)
    assert fake.published == [ev]


def test_ports_importable() -> None:
    assert MessagingPort is not None
    assert PolymarketDataPort is not None
    assert WalletTradeRepository is not None
```

- [ ] **Step 5.6: Run tests — esperado PASS**

Run: `uv run pytest tests/unit/test_ports_typecheck.py -v`
Expected: 2 tests passam.

- [ ] **Step 5.7: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0.

(Nota: se o mypy reclamar de Protocol, confira que `from typing import Protocol` está correto e o Python é 3.12.)

- [ ] **Step 5.8: Stage e commit**

```bash
git add src/polycopy/ports/ tests/unit/test_ports_typecheck.py
git commit -m "feat(ports): add MessagingPort, PolymarketDataPort, WalletTradeRepository protocols"
```

- [ ] **Step 5.9: STOP — esperar confirmação humana antes de Task 6**

---

## Task 6: Passo 1.5 — Settings (pydantic-settings) + logging (structlog)

**Objetivo:** ter `polycopy.config.Settings` lendo `.env` automaticamente, e `polycopy.infrastructure.observability.logging.configure_logging()` configurando structlog em modo JSON (prod) ou console (dev) com filtro de secrets. Refatorar conftest.py para usar `Settings` em vez de `python-dotenv`.

**Files:**
- Modify: `pyproject.toml` (adicionar `pydantic-settings`, `structlog`)
- Create: `src/polycopy/config.py`
- Create: `src/polycopy/infrastructure/__init__.py`
- Create: `src/polycopy/infrastructure/observability/__init__.py`
- Create: `src/polycopy/infrastructure/observability/logging.py`
- Modify: `tests/conftest.py` (usar Settings)
- Create: `tests/unit/infrastructure/__init__.py`
- Create: `tests/unit/infrastructure/test_logging.py`

---

- [ ] **Step 6.1: Modify `pyproject.toml` — adicionar deps**

`[project] dependencies`:

```toml
dependencies = [
    "pydantic>=2.9",
    "pydantic-settings>=2.5",
    "structlog>=24.4",
    "asyncpg>=0.30",
    "nats-py>=2.7",
    "redis>=5.1",
]
```

- [ ] **Step 6.2: Re-sync**

Run: `uv sync`
Expected: `pydantic-settings` e `structlog` instaladas.

- [ ] **Step 6.3: Create `src/polycopy/config.py`**

```python
"""Application settings loaded from environment / .env file.

Uses pydantic-settings: lê variáveis do ambiente, com fallback pra `.env`
na raiz do repo. Sem defaults silenciosos para credenciais — falha rápido
se algo faltar.
"""
from __future__ import annotations

from enum import Enum
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class Environment(str, Enum):
    DEV = "dev"
    PROD = "prod"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Settings(BaseSettings):
    """Configuração aplicacional. Imutável após construção."""

    model_config = SettingsConfigDict(
        env_file=_REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    env: Environment = Field(Environment.DEV, alias="ENV")
    log_level: LogLevel = Field(LogLevel.INFO, alias="LOG_LEVEL")

    postgres_user: str = Field(..., alias="POSTGRES_USER")
    postgres_password: SecretStr = Field(..., alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(..., alias="POSTGRES_DB")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")

    nats_url: str = Field(..., alias="NATS_URL")
    redis_url: str = Field(..., alias="REDIS_URL")
    prometheus_port: int = Field(9090, alias="PROMETHEUS_PORT")

    @property
    def postgres_dsn(self) -> str:
        """DSN sync (psycopg-style). Async DSN é montado no Plano 1B (Task 7)."""
        return (
            f"postgresql://{self.postgres_user}:"
            f"{self.postgres_password.get_secret_value()}@127.0.0.1:"
            f"{self.postgres_port}/{self.postgres_db}"
        )
```

- [ ] **Step 6.4: Create infrastructure package**

```bash
mkdir -p src/polycopy/infrastructure/observability
```

Then create `src/polycopy/infrastructure/__init__.py` (vazio):

```python
```

And `src/polycopy/infrastructure/observability/__init__.py` (vazio):

```python
```

- [ ] **Step 6.5: Write the failing test — `tests/unit/infrastructure/test_logging.py`**

```python
"""Tests for structlog configuration."""
from __future__ import annotations

import json
from io import StringIO

import pytest
import structlog

from polycopy.config import Environment, LogLevel
from polycopy.infrastructure.observability.logging import (
    configure_logging,
    get_logger,
)


@pytest.fixture(autouse=True)
def _reset_structlog() -> None:
    structlog.reset_defaults()


def test_get_logger_returns_bound_logger() -> None:
    configure_logging(env=Environment.DEV, level=LogLevel.DEBUG)
    log = get_logger("test")
    assert log is not None


def test_prod_emits_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(env=Environment.PROD, level=LogLevel.INFO)
    log = get_logger("test")
    log.info("hello", user_id=42)

    captured = capsys.readouterr()
    line = captured.out.strip()
    parsed = json.loads(line)
    assert parsed["event"] == "hello"
    assert parsed["user_id"] == 42
    assert parsed["level"] == "info"


def test_secrets_filtered() -> None:
    """Campos sensíveis (private_key, api_secret, mnemonic, passphrase) são redatados."""
    buf = StringIO()
    configure_logging(env=Environment.PROD, level=LogLevel.INFO, stream=buf)
    log = get_logger("test")
    log.info("login", api_secret="should-not-leak", user="alice")

    line = buf.getvalue().strip()
    assert "should-not-leak" not in line
    assert '"api_secret":"[REDACTED]"' in line or '"api_secret": "[REDACTED]"' in line


def test_log_level_respected(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(env=Environment.PROD, level=LogLevel.WARNING)
    log = get_logger("test")
    log.info("should-be-filtered")
    log.warning("should-pass")

    captured = capsys.readouterr()
    assert "should-be-filtered" not in captured.out
    assert "should-pass" in captured.out


def test_dev_uses_console_renderer(capsys: pytest.CaptureFixture[str]) -> None:
    """Em dev, output não é JSON puro — tem timestamps, cores ANSI possíveis."""
    configure_logging(env=Environment.DEV, level=LogLevel.INFO)
    log = get_logger("test")
    log.info("hello-dev")

    captured = capsys.readouterr()
    # Console renderer: linha contém o evento, mas não é JSON parseable.
    assert "hello-dev" in captured.out
    with pytest.raises(json.JSONDecodeError):
        json.loads(captured.out.strip().splitlines()[-1])
```

- [ ] **Step 6.6: Run tests — esperado FAIL**

Run: `uv run pytest tests/unit/infrastructure/test_logging.py -v`
Expected: ImportError em `polycopy.infrastructure.observability.logging`.

- [ ] **Step 6.7: Implement `src/polycopy/infrastructure/observability/logging.py`**

```python
"""Structlog configuration: JSON em prod, console em dev. Filtro de secrets."""
from __future__ import annotations

import logging
import sys
from typing import IO, Any

import structlog
from structlog.types import EventDict

from polycopy.config import Environment, LogLevel

_REDACTED_KEYS = frozenset(
    {
        "private_key",
        "api_secret",
        "passphrase",
        "mnemonic",
        "telegram_token",
        "postgres_password",
    }
)


def _redact_secrets(_: object, __: str, event_dict: EventDict) -> EventDict:
    """Substitui valores de chaves sensíveis por [REDACTED]."""
    for key in list(event_dict.keys()):
        if key.lower() in _REDACTED_KEYS:
            event_dict[key] = "[REDACTED]"
    return event_dict


def configure_logging(
    *,
    env: Environment,
    level: LogLevel,
    stream: IO[str] | None = None,
) -> None:
    """Configura structlog. Idempotente: chamadas repetidas reconfiguram limpo.

    Não toca em `logging.getLogger()` (stdlib): a integração stdlib<->structlog
    entra no Plano 1B junto com adapters que dependem de libs (asyncpg, nats)
    que logam via stdlib.

    Args:
        env: dev | prod. Em dev usa ConsoleRenderer; em prod, JSONRenderer.
        level: nível mínimo de log.
        stream: stream de saída (default sys.stdout). Útil em testes.
    """
    target_stream = stream if stream is not None else sys.stdout
    log_level = getattr(logging, level.value)

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        timestamper,
        _redact_secrets,
    ]

    if env is Environment.PROD:
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=target_stream),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name) if name else structlog.get_logger()
```

- [ ] **Step 6.8: Run tests — esperado PASS**

Run: `uv run pytest tests/unit/infrastructure/test_logging.py -v`
Expected: todos passam.

(Se algum teste falhar por causa de timestamp/level no JSON: confirme as keys que `JSONRenderer` produz com a config acima — `event`, `level`, `timestamp`, mais campos custom. Ajuste asserts se necessário antes de commitar.)

- [ ] **Step 6.9: Refatorar `tests/conftest.py` — usar Settings**

Substituir conteúdo atual por:

```python
"""Shared test fixtures and bootstrap.

Carrega `.env` via `polycopy.config.Settings` (que usa pydantic-settings).
Settings é construída lazy via fixture, não no import — isso permite testes
unitários que não dependem de `.env` rodarem sem ele.
"""
from __future__ import annotations

import pytest

from polycopy.config import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Singleton Settings carregada do `.env`. Use em testes integration."""
    return Settings()  # type: ignore[call-arg]
```

E atualizar `tests/integration/test_infra_connectivity.py` pra usar a fixture:

```python
"""Smoke tests de conectividade Python -> infra (postgres, nats, redis)."""
from __future__ import annotations

import asyncpg
import nats
import pytest
import redis.asyncio as aioredis

from polycopy.config import Settings

pytestmark = pytest.mark.integration


def _postgres_dsn(settings: Settings) -> str:
    return (
        f"postgresql://{settings.postgres_user}:"
        f"{settings.postgres_password.get_secret_value()}@127.0.0.1:"
        f"{settings.postgres_port}/{settings.postgres_db}"
    )


async def test_postgres_connect_and_select_one(settings: Settings) -> None:
    conn = await asyncpg.connect(_postgres_dsn(settings))
    try:
        result = await conn.fetchval("SELECT 1")
        assert result == 1
    finally:
        await conn.close()


async def test_postgres_timescale_extension_loaded(settings: Settings) -> None:
    conn = await asyncpg.connect(_postgres_dsn(settings))
    try:
        ext = await conn.fetchval(
            "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'"
        )
        assert ext == "timescaledb"
    finally:
        await conn.close()


async def test_nats_connect_and_close(settings: Settings) -> None:
    nc = await nats.connect(settings.nats_url)
    try:
        assert nc.is_connected is True
    finally:
        await nc.close()


async def test_redis_ping(settings: Settings) -> None:
    r = aioredis.from_url(settings.redis_url)
    try:
        pong = await r.ping()
        assert pong is True
    finally:
        await r.aclose()
```

- [ ] **Step 6.10: Remove `python-dotenv` do dev group**

`[dependency-groups] dev`:

```toml
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.24",
    "pytest-cov>=5",
    "mypy>=1.13",
    "ruff>=0.7",
    "pre-commit>=4",
    "commitizen>=4",
]
```

(Settings já carrega `.env` via pydantic-settings; dotenv direto não é mais necessário.)

- [ ] **Step 6.11: Re-sync após remoção**

Run: `uv sync`
Expected: `python-dotenv` removida do venv. `uv.lock` atualizado.

- [ ] **Step 6.12: Run integration tests novamente — confirmar refactor**

Run: `uv run pytest -m integration -v`
Expected: 4 tests passam, agora consumindo Settings.

- [ ] **Step 6.13: Run full quality gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: todos exit 0.

- [ ] **Step 6.14: Stage e commit**

```bash
git add pyproject.toml uv.lock src/polycopy/config.py src/polycopy/infrastructure/ tests/
git commit -m "feat(config): add Settings and structlog logging with secret filter"
```

- [ ] **Step 6.15: STOP — esperar confirmação humana antes do fim do Plano 1A**

---

## Final: Validação completa do Plano 1A

Após as 6 tasks, rode o checklist final:

- [ ] **Step F.1: Working tree limpo + 6 commits novos**

Run:
```bash
git status
git log --oneline | head -15
```
Expected:
- `git status`: working tree clean (exceto untracked esperados como `PROMPT_POLYCOPY_v2.md` e `docs/`)
- `git log`: além dos 5 commits da Fase 0, mais 6 commits desta fase:
  ```
  feat(config): add Settings and structlog logging with secret filter
  feat(ports): add MessagingPort, PolymarketDataPort, WalletTradeRepository protocols
  feat(domain): add WalletTradeDetected event with NATS subject
  feat(domain): add Wallet, Trade, Position models with Side enum
  feat(domain): add value objects (Money, Price, Bps, WalletAddress, ConditionId, TokenId)
  test: add infra connectivity smoke tests
  ```

- [ ] **Step F.2: Suite verde inteira**

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
Expected: tudo exit 0. Test count esperado:
- 1 (smoke) + ~9 (value objects) + ~5 (models) + ~4 (events) + 2 (ports typecheck) + ~5 (logging) + 4 (integration) = ~30 tests.

- [ ] **Step F.3: Coverage do `domain/`**

Run: `uv run pytest tests/unit/domain --cov=src/polycopy/domain --cov-report=term-missing`
Expected: coverage ≥ 90% no `domain/` (alvo da Fase 1).

Se < 90%: identifique linhas faltantes e adicione tests antes de declarar Plano 1A pronto.

- [ ] **Step F.4: Pre-commit verde**

Run: `uv run pre-commit run --all-files`
Expected: tudo passa.

- [ ] **Step F.5: GitHub Actions verde**

Após o último push, verifique manualmente que CI ficou ✅.

Se TODOS os steps F.1 a F.5 passarem: **Plano 1A está completo.** Próximo: criar Plano 1B (passos 1.6 a 1.10 — SQLAlchemy, alembic, repositório, cliente Data API, NATS adapter, agent base).

---

## Notas de execução

**Regras absolutas que valem em todas as Tasks** (do prompt mestre, seção "Regras finais"):

1. NÃO avance pra Task seguinte sem confirmação explícita do humano.
2. NÃO crie arquivos fora do escopo declarado da Task atual.
3. NÃO instale dependência fora da lista da Task atual sem perguntar.
4. SEMPRE rode os comandos de validação ANTES de declarar a Task pronta.
5. SEMPRE faça commit conventional ao fim de cada Task.
6. Se descobrir que a Task precisa ser dividida (escopo maior do que parecia), pare e proponha a subdivisão antes de continuar.
7. Se descobrir que a Task está errada ou impossível como descrita, pare e exponha o problema antes de improvisar.

**Sobre coverage:** o spec diz "domain/ ≥ 90%, geral ≥ 75% (enforce no CI a partir da Fase 1)". Não vamos enforçar % no CI durante o Plano 1A — só no fim do Plano 1C, quando tivermos cobertura suficiente pra que o gate seja realista. Por ora, medimos coverage manualmente como sanity check.

**Sobre commits e a memória de feedback:** mesmo quando você (humano) aprovou um plano que termina com "commit", pause antes de `git add`/`git commit` e me peça confirmação explícita. A memória `feedback_commits.md` está vigente.

**Sobre dependências adicionadas:** este plano adiciona `asyncpg`, `nats-py`, `redis`, `pydantic-settings`, `structlog` ao runtime e (temporariamente) `python-dotenv` ao dev. Se quiser substituir alguma (ex: `psycopg` em vez de `asyncpg`, `loguru` em vez de `structlog`), pare antes da Task correspondente e renegocie.

**Sobre `.env` e secrets:** `Settings` lê `.env`; `SecretStr` previne logging acidental de `POSTGRES_PASSWORD`. O filtro de secrets do `structlog` redacta valores cuja KEY bate com a lista. Se você adicionar uma chave nova (ex: `TELEGRAM_TOKEN` no Plano 1C), também adicione no `_REDACTED_KEYS`.
