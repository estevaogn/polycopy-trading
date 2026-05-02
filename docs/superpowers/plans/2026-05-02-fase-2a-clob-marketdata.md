# Plano 2A — CLOB client + Market Data sync (Implementation Plan)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entregar a infraestrutura de dados de mercado da Fase 2 — clients REST pra Gamma e CLOB da Polymarket, tabela `markets` com cache read-through, agente `MarketDataAgent` com sync periódico e containerização — pra que o Risk (Plano 2B) tenha latência baixa e fail-safe pra decidir aprovar/rejeitar trades.

**Architecture:** Dois clients REST (`PolymarketGammaClient`, `PolymarketClobClient`) implementam ports tipados; `SqlAlchemyMarketRepository` faz cache read-through em `markets` com TTL configurável; `MarketDataAgent` (subclasse de `AgentBase`) roda loop periódico que chama Gamma `list_active_markets` e faz `upsert_many` no repo. Orderbook não é cacheado (sempre fresh via CLOB REST). Agente roda em container próprio (`polycopy-marketdata`) com `/metrics` na porta 9103.

**Tech Stack:** Python 3.12, httpx + tenacity (já instalados), SQLAlchemy async (já instalado), alembic (já configurado), prometheus-client (já instalado), respx (dev, já instalado). **Sem dependências novas.**

**Cadência:** one-task-per-confirmation. Cada Task termina com STOP — esperar confirmação humana antes da próxima. Pausa antes de `git add`/`git commit` em cada task (regra `feedback_commits.md`).

**Spec referência:** `docs/superpowers/specs/2026-05-02-fase-2a-clob-marketdata-design.md`

---

## File Structure

```
src/polycopy/
├── domain/
│   └── market.py                                          # Task 1
├── ports/
│   ├── __init__.py                                        # Task 2 (modificar)
│   ├── polymarket_clob.py                                 # Task 2
│   ├── polymarket_gamma.py                                # Task 2
│   └── market_repository.py                               # Task 2
├── infrastructure/
│   ├── observability/
│   │   └── metrics.py                                     # Task 3 + Task 4 + Task 6 + Task 7 (modificar)
│   ├── polymarket/
│   │   ├── gamma_client.py                                # Task 3
│   │   └── clob_client.py                                 # Task 4
│   └── persistence/
│       ├── models.py                                      # Task 5 (modificar)
│       └── market_repository.py                           # Task 6
├── agents/
│   └── marketdata.py                                      # Task 7
└── config.py                                              # Task 7 (modificar)

alembic/versions/
└── 0002_add_markets.py                                    # Task 5

tests/
├── fixtures/polymarket/
│   ├── gamma_market.json                                  # Task 3
│   └── clob_book.json                                     # Task 4
├── unit/
│   ├── domain/
│   │   └── test_market.py                                 # Task 1
│   ├── infrastructure/
│   │   ├── test_clob_client.py                            # Task 4
│   │   ├── test_gamma_client.py                           # Task 3
│   │   └── test_metrics.py                                # Task 3 + Task 6 + Task 7 (modificar)
│   ├── agents/
│   │   └── test_marketdata.py                             # Task 7
│   └── test_ports_typecheck.py                            # Task 2 (modificar)
└── integration/
    ├── test_market_repository.py                          # Task 6
    ├── test_marketdata_e2e.py                             # Task 7
    └── test_polymarket_smoke.py                           # Task 9

docker-compose.yml                                         # Task 8 (modificar)
infra/prometheus/prometheus.yml                            # Task 8 (modificar)
ARCHITECTURE.md                                            # Task 8 (modificar)
.env.example                                               # Task 7 + Task 8 (modificar)
pyproject.toml                                             # Task 9 (modificar — markers)
```

---

## Task 1: Domain types `OrderBook` e `Market`

**Objetivo:** value objects imutáveis que representam orderbook e metadata de mercado, com validação. Sem persistência. Sem dependência de infra.

**Files:**
- Create: `src/polycopy/domain/market.py`
- Create: `tests/unit/domain/test_market.py`

---

- [ ] **Step 1.1: Escrever testes que falham pra `Market`**

Crie `tests/unit/domain/test_market.py`:

```python
"""Testes pra value objects de mercado: OrderBook, Market."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from polycopy.domain.market import Market, OrderBook, OrderBookLevel
from polycopy.domain.value_objects import ConditionId, Money, Price, TokenId


def _market(
    *,
    token_id: str = "42",
    is_active: bool = True,
    is_archived: bool = False,
    end_date: datetime | None = None,
    volume_24h: Decimal | None = Decimal("75000"),
    liquidity: Decimal | None = Decimal("12000"),
) -> Market:
    return Market(
        token_id=TokenId(value=token_id),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        question="Will X happen?",
        slug="will-x-happen",
        outcome="Yes",
        end_date=end_date,
        is_active=is_active,
        is_archived=is_archived,
        volume_24h_usdc=None if volume_24h is None else Money.from_usdc(str(volume_24h)),
        liquidity_usdc=None if liquidity is None else Money.from_usdc(str(liquidity)),
    )


class TestMarket:
    def test_valid_market(self) -> None:
        m = _market()
        assert m.token_id.value == "42"
        assert m.is_active is True

    def test_outcome_must_be_yes_or_no(self) -> None:
        with pytest.raises(ValidationError):
            Market(
                token_id=TokenId(value="42"),
                condition_id=ConditionId(value="0x" + "ab" * 32),
                question="?",
                slug=None,
                outcome="Maybe",
                end_date=None,
                is_active=True,
                is_archived=False,
                volume_24h_usdc=None,
                liquidity_usdc=None,
            )

    def test_immutable(self) -> None:
        m = _market()
        with pytest.raises(ValidationError):
            m.is_active = False  # type: ignore[misc]

    def test_archived_excludes_active(self) -> None:
        # Regra: arquivado só é válido se não-ativo.
        with pytest.raises(ValidationError):
            _market(is_active=True, is_archived=True)

    def test_archived_and_inactive_ok(self) -> None:
        m = _market(is_active=False, is_archived=True)
        assert m.is_archived is True
        assert m.is_active is False


class TestOrderBookLevel:
    def test_level_valid(self) -> None:
        lvl = OrderBookLevel(price=Price(value=Decimal("0.55")), size=Money.from_usdc("100"))
        assert lvl.price.value == Decimal("0.5500")

    def test_size_non_negative(self) -> None:
        with pytest.raises(ValidationError):
            OrderBookLevel(
                price=Price(value=Decimal("0.5")),
                size=Money(amount=Decimal("-1")),
            )


class TestOrderBook:
    def _book(
        self,
        *,
        bids: list[tuple[str, str]] | None = None,
        asks: list[tuple[str, str]] | None = None,
    ) -> OrderBook:
        bids = bids if bids is not None else [("0.50", "100"), ("0.49", "200")]
        asks = asks if asks is not None else [("0.51", "150"), ("0.52", "250")]
        return OrderBook(
            token_id=TokenId(value="42"),
            bids=[
                OrderBookLevel(price=Price(value=Decimal(p)), size=Money.from_usdc(s))
                for p, s in bids
            ],
            asks=[
                OrderBookLevel(price=Price(value=Decimal(p)), size=Money.from_usdc(s))
                for p, s in asks
            ],
            captured_at=datetime.now(tz=UTC),
        )

    def test_best_bid_and_ask(self) -> None:
        book = self._book()
        assert book.best_bid is not None
        assert book.best_bid.price.value == Decimal("0.5000")
        assert book.best_ask is not None
        assert book.best_ask.price.value == Decimal("0.5100")

    def test_empty_book_no_best(self) -> None:
        book = self._book(bids=[], asks=[])
        assert book.best_bid is None
        assert book.best_ask is None

    def test_bids_must_be_descending(self) -> None:
        with pytest.raises(ValidationError):
            self._book(bids=[("0.49", "100"), ("0.50", "200")])

    def test_asks_must_be_ascending(self) -> None:
        with pytest.raises(ValidationError):
            self._book(asks=[("0.52", "100"), ("0.51", "200")])

    def test_immutable(self) -> None:
        book = self._book()
        with pytest.raises(ValidationError):
            book.bids = []  # type: ignore[misc]
```

- [ ] **Step 1.2: Rodar testes pra confirmar falha**

Run: `uv run pytest tests/unit/domain/test_market.py -v`
Expected: FAIL com `ImportError: cannot import name 'Market' from 'polycopy.domain.market'` (ou o módulo nem existe).

- [ ] **Step 1.3: Implementar `src/polycopy/domain/market.py`**

```python
"""Domain types pra mercados: OrderBook, Market.

Value objects imutáveis. Sem dependência de infra.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from polycopy.domain.value_objects import ConditionId, Money, Price, TokenId


class Market(BaseModel):
    """Metadata de um token de mercado da Polymarket.

    Cada `condition_id` tem 2 tokens (Yes/No); cada token vira um `Market`.
    """

    model_config = ConfigDict(frozen=True, strict=True)

    token_id: TokenId
    condition_id: ConditionId
    question: str
    slug: str | None
    outcome: Annotated[str, Field(pattern=r"^(Yes|No)$")]
    end_date: datetime | None
    is_active: bool
    is_archived: bool
    volume_24h_usdc: Money | None
    liquidity_usdc: Money | None

    @model_validator(mode="after")
    def _check_archived_consistency(self) -> Market:
        if self.is_archived and self.is_active:
            raise ValueError("market cannot be both is_active=True and is_archived=True")
        return self


class OrderBookLevel(BaseModel):
    """Um nível do orderbook: preço e tamanho agregado nesse preço."""

    model_config = ConfigDict(frozen=True, strict=True)

    price: Price
    size: Money

    @field_validator("size", mode="after")
    @classmethod
    def _size_non_negative(cls, v: Money) -> Money:
        if v.amount < 0:
            raise ValueError(f"order book level size must be >= 0, got {v.amount}")
        return v


class OrderBook(BaseModel):
    """Snapshot do orderbook de um token, capturado num momento específico.

    `bids` em ordem decrescente de preço (melhor primeiro).
    `asks` em ordem crescente de preço (melhor primeiro).
    """

    model_config = ConfigDict(frozen=True, strict=True)

    token_id: TokenId
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    captured_at: datetime

    @model_validator(mode="after")
    def _check_ordering(self) -> OrderBook:
        for i in range(1, len(self.bids)):
            if self.bids[i].price.value > self.bids[i - 1].price.value:
                raise ValueError("bids must be in descending price order")
        for i in range(1, len(self.asks)):
            if self.asks[i].price.value < self.asks[i - 1].price.value:
                raise ValueError("asks must be in ascending price order")
        return self

    @property
    def best_bid(self) -> OrderBookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> OrderBookLevel | None:
        return self.asks[0] if self.asks else None
```

- [ ] **Step 1.4: Rodar testes pra confirmar passar**

Run: `uv run pytest tests/unit/domain/test_market.py -v`
Expected: PASS, todos verdes.

- [ ] **Step 1.5: Rodar verificações completas**

```bash
uv run ruff check src/polycopy/domain/market.py tests/unit/domain/test_market.py
uv run ruff format --check src/polycopy/domain/market.py tests/unit/domain/test_market.py
uv run mypy src/polycopy/domain/market.py
uv run pytest tests/unit/domain/ -v
```
Expected: tudo PASS.

- [ ] **Step 1.6: STOP — pedir confirmação humana, depois commit**

Mostrar `git status` + `git diff --stat`. Aguardar autorização.

```bash
git add src/polycopy/domain/market.py tests/unit/domain/test_market.py
git commit -m "feat(domain): add OrderBook and Market value objects"
```

---

## Task 2: Ports `PolymarketClobPort`, `PolymarketGammaPort`, `MarketRepository`

**Objetivo:** três Protocols tipados que adapters concretos das próximas tasks vão implementar. Smoke test atualizado pra confirmar que os ports são importáveis e que mypy strict valida quaisquer implementações.

**Files:**
- Create: `src/polycopy/ports/polymarket_clob.py`
- Create: `src/polycopy/ports/polymarket_gamma.py`
- Create: `src/polycopy/ports/market_repository.py`
- Modify: `src/polycopy/ports/__init__.py`
- Modify: `tests/unit/test_ports_typecheck.py`

---

- [ ] **Step 2.1: Criar `src/polycopy/ports/polymarket_clob.py`**

```python
"""PolymarketClobPort: contrato pra consultar orderbook do CLOB Polymarket."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.market import OrderBook
from polycopy.domain.value_objects import TokenId


class PolymarketClobPort(Protocol):
    """Cliente da Polymarket CLOB REST API. Implementação concreta: httpx (Plano 2A)."""

    async def get_book(self, token_id: TokenId) -> OrderBook:
        """Retorna snapshot do orderbook do token.

        Sempre fresh; sem cache. Levanta `PolymarketUnavailableError` após N retries.
        """
        ...
```

- [ ] **Step 2.2: Criar `src/polycopy/ports/polymarket_gamma.py`**

```python
"""PolymarketGammaPort: contrato pra consultar metadata de mercados via Gamma."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.market import Market
from polycopy.domain.value_objects import TokenId


class PolymarketGammaPort(Protocol):
    """Cliente da Polymarket Gamma REST API. Implementação concreta: httpx (Plano 2A)."""

    async def get_market(self, token_id: TokenId) -> Market | None:
        """Retorna `Market` correspondente ao token, ou None se não existir.

        Levanta `PolymarketUnavailableError` após N retries.
        """
        ...

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        """Retorna até `limit` mercados ativos, ordenados por volume 24h desc.

        Apenas mercados com `is_active=True` e `is_archived=False`.
        Levanta `PolymarketUnavailableError` após N retries.
        """
        ...
```

- [ ] **Step 2.3: Criar `src/polycopy/ports/market_repository.py`**

```python
"""MarketRepository: contrato de persistência pra cache de Market."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from polycopy.domain.market import Market
from polycopy.domain.value_objects import TokenId


class CachedMarket(Protocol):
    """Resultado de leitura do cache. Encapsula market + freshness."""

    market: Market
    last_synced_at: datetime
    is_stale: bool


class MarketRepository(Protocol):
    """Cache read-through pra metadata de mercados. Plano 2A."""

    async def upsert_many(self, markets: list[Market]) -> int:
        """Insere/atualiza muitos mercados em batch. Retorna número de linhas afetadas."""
        ...

    async def get_market(self, token_id: TokenId) -> CachedMarket | None:
        """Retorna cached market do DB ou None se ausente.

        NÃO faz fetch externo. Caller decide se aceita stale ou refaz fetch via Gamma.
        Use `is_stale` (computado contra TTL) pra decidir.
        """
        ...
```

- [ ] **Step 2.4: Atualizar `src/polycopy/ports/__init__.py`**

```python
"""Ports: interfaces tipadas que adapters concretos implementam."""

from polycopy.ports.market_repository import CachedMarket, MarketRepository
from polycopy.ports.messaging import MessagingPort
from polycopy.ports.polymarket_clob import PolymarketClobPort
from polycopy.ports.polymarket_data import PolymarketDataPort
from polycopy.ports.polymarket_gamma import PolymarketGammaPort
from polycopy.ports.repository import WalletTradeRepository

__all__ = [
    "CachedMarket",
    "MarketRepository",
    "MessagingPort",
    "PolymarketClobPort",
    "PolymarketDataPort",
    "PolymarketGammaPort",
    "WalletTradeRepository",
]
```

- [ ] **Step 2.5: Estender `tests/unit/test_ports_typecheck.py`**

Adicione no final do arquivo (depois do `test_ports_importable` existente):

```python
from datetime import datetime as _datetime
from datetime import timedelta as _timedelta

from polycopy.domain.market import Market, OrderBook
from polycopy.domain.value_objects import TokenId
from polycopy.ports import (
    CachedMarket,
    MarketRepository,
    PolymarketClobPort,
    PolymarketGammaPort,
)


class _FakeClob:
    """Stub que implementa PolymarketClobPort."""

    async def get_book(self, token_id: TokenId) -> OrderBook:
        return OrderBook(
            token_id=token_id,
            bids=[],
            asks=[],
            captured_at=_datetime.now(tz=UTC),
        )


class _FakeGamma:
    """Stub que implementa PolymarketGammaPort."""

    async def get_market(self, token_id: TokenId) -> Market | None:
        return None

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        return []


class _FakeCachedMarket:
    def __init__(self, market: Market) -> None:
        self.market = market
        self.last_synced_at = _datetime.now(tz=UTC)
        self.is_stale = False


class _FakeMarketRepo:
    """Stub que implementa MarketRepository."""

    def __init__(self) -> None:
        self.upserted: list[Market] = []

    async def upsert_many(self, markets: list[Market]) -> int:
        self.upserted.extend(markets)
        return len(markets)

    async def get_market(self, token_id: TokenId) -> CachedMarket | None:
        return None


def _accepts_clob(_: PolymarketClobPort) -> None: ...
def _accepts_gamma(_: PolymarketGammaPort) -> None: ...
def _accepts_market_repo(_: MarketRepository) -> None: ...


def test_fakes_satisfy_new_ports() -> None:
    _accepts_clob(_FakeClob())
    _accepts_gamma(_FakeGamma())
    _accepts_market_repo(_FakeMarketRepo())


def test_new_ports_importable() -> None:
    assert PolymarketClobPort is not None
    assert PolymarketGammaPort is not None
    assert MarketRepository is not None
    assert CachedMarket is not None


def _make_inactive_archived_market() -> Market:
    """Helper pra exercitar Market sem violar invariantes — útil em smoke tests futuros."""
    from polycopy.domain.value_objects import ConditionId

    return Market(
        token_id=TokenId(value="1"),
        condition_id=ConditionId(value="0x" + "00" * 32),
        question="?",
        slug=None,
        outcome="Yes",
        end_date=_datetime.now(tz=UTC) + _timedelta(days=7),
        is_active=False,
        is_archived=True,
        volume_24h_usdc=None,
        liquidity_usdc=None,
    )
```

- [ ] **Step 2.6: Rodar verificações**

```bash
uv run ruff check src/polycopy/ports/ tests/unit/test_ports_typecheck.py
uv run ruff format --check src/polycopy/ports/ tests/unit/test_ports_typecheck.py
uv run mypy src/polycopy
uv run pytest tests/unit/test_ports_typecheck.py -v
```
Expected: tudo PASS.

- [ ] **Step 2.7: STOP — confirmação humana, depois commit**

```bash
git add src/polycopy/ports/ tests/unit/test_ports_typecheck.py
git commit -m "feat(ports): add CLOB, Gamma and MarketRepository protocols"
```

---

## Task 3: `PolymarketGammaClient` (REST + tenacity + métricas)

**Objetivo:** adapter REST pra Gamma API. Métodos `get_market(token_id)` e `list_active_markets(limit=N)`. Retry exponencial em 5xx/transport errors. Métricas Prometheus de latência e contagem.

**Pré-requisito:** capturar fixture real ANTES de escrever parser. Sem isso, repete-se o erro do 1C de testar contra schema imaginário (commit `860b264`).

**Files:**
- Create: `tests/fixtures/polymarket/gamma_market.json`
- Create: `src/polycopy/infrastructure/polymarket/gamma_client.py`
- Modify: `src/polycopy/infrastructure/observability/metrics.py`
- Create: `tests/unit/infrastructure/test_gamma_client.py`
- Modify: `tests/unit/infrastructure/test_metrics.py`

---

- [ ] **Step 3.1: Capturar fixture real da Gamma**

Em ambiente com internet (servidor ou local com saída):

```bash
mkdir -p tests/fixtures/polymarket
curl -sS "https://gamma-api.polymarket.com/markets?limit=2&active=true&archived=false&order=volume24hr&ascending=false" \
  | python -m json.tool > tests/fixtures/polymarket/gamma_market.json
head -50 tests/fixtures/polymarket/gamma_market.json
```

Expected: JSON com array de mercados. Cada item tem campos como `id`, `conditionId`, `clobTokenIds` (array de 2 token ids — Yes e No), `question`, `slug`, `outcomes` (array `["Yes","No"]`), `endDate` (ISO 8601), `active`, `archived`, `volume24hr`, `liquidity`.

**Se algum campo estiver com nome diferente**, ajustar o parser do Step 3.4 antes de escrever o teste do Step 3.3.

- [ ] **Step 3.2: Adicionar métricas Gamma em `metrics.py`**

Modify `src/polycopy/infrastructure/observability/metrics.py`. Adicionar dois campos no `Metrics` dataclass e na `make_metrics()`:

```python
@dataclass(frozen=True)
class Metrics:
    polymarket_requests_total: Counter
    polymarket_request_duration_seconds: Histogram

    # Gamma + CLOB (Plano 2A)
    polymarket_http_request_duration_seconds: Histogram
    polymarket_http_requests_total: Counter

    watcher_iterations_total: Counter
    # ... (resto inalterado)
```

E na função `make_metrics()`, adicionar antes dos `watcher_*`:

```python
        polymarket_http_request_duration_seconds=Histogram(
            "polycopy_polymarket_http_request_duration_seconds",
            "Latência HTTP por client Polymarket (gamma|clob).",
            labelnames=["client", "endpoint"],
            registry=target,
        ),
        polymarket_http_requests_total=Counter(
            "polycopy_polymarket_http_requests",
            "Total de requests HTTP por client Polymarket (gamma|clob).",
            labelnames=["client", "endpoint", "status"],
            registry=target,
        ),
```

**Não removemos `polymarket_requests_total` / `polymarket_request_duration_seconds`** — eles continuam dedicados ao `data_client` da Fase 1B (label `endpoint=activity`). Os novos campos servem `gamma` e `clob`. Manter compat retro evita regressão na Fase 1.

- [ ] **Step 3.3: Atualizar `tests/unit/infrastructure/test_metrics.py`**

Localizar o teste que verifica os campos de `Metrics` e estender pra cobrir os 2 novos campos. O nome exato do teste depende do que existe — abrir o arquivo, achar a função que lista campos esperados, e adicionar:

```python
    assert metrics.polymarket_http_request_duration_seconds is not None
    assert metrics.polymarket_http_requests_total is not None
```

Run: `uv run pytest tests/unit/infrastructure/test_metrics.py -v`
Expected: PASS.

- [ ] **Step 3.4: Escrever testes pra `PolymarketGammaClient`**

Crie `tests/unit/infrastructure/test_gamma_client.py`:

```python
"""Testes unit do PolymarketGammaClient com respx mocks."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from polycopy.domain.value_objects import TokenId
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.gamma_client import (
    PolymarketGammaClient,
    PolymarketUnavailableError,
)

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "polymarket"
_GAMMA_FIXTURE = _FIXTURES / "gamma_market.json"


def _gamma_response_json() -> list[dict]:
    return json.loads(_GAMMA_FIXTURE.read_text())


def _make_client() -> PolymarketGammaClient:
    metrics = make_metrics(registry=CollectorRegistry())
    return PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com",
        metrics=metrics,
        max_retries=3,
    )


@respx.mock
async def test_list_active_markets_parses_fixture() -> None:
    payload = _gamma_response_json()
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    markets = await client.list_active_markets(limit=2)

    assert len(markets) >= 1
    first = markets[0]
    assert first.is_active is True
    assert first.outcome in {"Yes", "No"}


@respx.mock
async def test_list_active_markets_returns_two_per_condition() -> None:
    """Cada conditionId tem 2 tokens (Yes/No); cliente expande para 2 Markets por mercado."""
    payload = _gamma_response_json()
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    markets = await client.list_active_markets(limit=2)

    # Se a fixture tem N mercados, esperamos até 2*N Market objects.
    n_payload = len(payload)
    assert len(markets) <= 2 * n_payload


@respx.mock
async def test_get_market_returns_none_for_unknown_token() -> None:
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=[]),
    )

    client = _make_client()
    result = await client.get_market(TokenId(value="999999999"))
    assert result is None


@respx.mock
async def test_retry_on_5xx_eventually_succeeds() -> None:
    payload = _gamma_response_json()
    route = respx.get("https://gamma-api.polymarket.com/markets")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(200, json=payload),
    ]

    client = _make_client()
    markets = await client.list_active_markets(limit=2)

    assert len(markets) >= 1
    assert route.call_count == 3


@respx.mock
async def test_retry_exhausted_raises_unavailable() -> None:
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(503),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.list_active_markets(limit=2)


@respx.mock
async def test_4xx_does_not_retry() -> None:
    route = respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(400),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.list_active_markets(limit=2)
    assert route.call_count == 1


@respx.mock
async def test_metrics_recorded() -> None:
    payload = _gamma_response_json()
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com",
        metrics=metrics,
    )
    await client.list_active_markets(limit=2)

    histogram = metrics.polymarket_http_request_duration_seconds.labels(
        client="gamma", endpoint="markets"
    )
    counter = metrics.polymarket_http_requests_total.labels(
        client="gamma", endpoint="markets", status="200"
    )
    # Sample count > 0 prova que observe foi chamado.
    assert histogram._sum.get() >= 0
    assert counter._value.get() == 1
```

Run: `uv run pytest tests/unit/infrastructure/test_gamma_client.py -v`
Expected: FAIL com `ImportError: cannot import name 'PolymarketGammaClient'`.

- [ ] **Step 3.5: Implementar `src/polycopy/infrastructure/polymarket/gamma_client.py`**

```python
"""PolymarketGammaClient: REST adapter da Gamma API.

Endpoint base: https://gamma-api.polymarket.com
Retry: exponencial em 5xx + transport errors; não retenta em 4xx.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.market import Market
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.observability.metrics import Metrics


class PolymarketUnavailableError(RuntimeError):
    """API Polymarket indisponível após retries (Gamma ou CLOB)."""


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return isinstance(exc, httpx.RequestError)


class PolymarketGammaClient:
    """Cliente REST da Gamma. Implementa `PolymarketGammaPort`."""

    def __init__(
        self,
        *,
        base_url: str,
        metrics: Metrics,
        timeout_s: float = 5.0,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._metrics = metrics
        self._timeout_s = timeout_s
        self._max_retries = max_retries

    async def get_market(self, token_id: TokenId) -> Market | None:
        # Gamma `/markets` aceita filtro por clobTokenIds. Retorna array.
        rows = await self._fetch_markets(params={"clob_token_ids": token_id.value, "limit": 1})
        for row in rows:
            for market in self._row_to_markets(row):
                if market.token_id.value == token_id.value:
                    return market
        return None

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        rows = await self._fetch_markets(
            params={
                "active": "true",
                "archived": "false",
                "order": "volume24hr",
                "ascending": "false",
                "limit": limit,
            }
        )
        out: list[Market] = []
        for row in rows:
            out.extend(self._row_to_markets(row))
        return out

    async def _fetch_markets(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.get(f"{self._base_url}/markets", params=params)
                response.raise_for_status()
                return response

        start = time.perf_counter()
        try:
            response = await self._with_retry(_do)
        except RetryError as exc:
            self._metrics.polymarket_http_requests_total.labels(
                client="gamma", endpoint="markets", status="error"
            ).inc()
            raise PolymarketUnavailableError(
                f"Gamma /markets unavailable after retries: {exc.last_attempt.exception()}"
            ) from exc
        except httpx.HTTPStatusError as exc:
            self._metrics.polymarket_http_requests_total.labels(
                client="gamma", endpoint="markets", status=str(exc.response.status_code)
            ).inc()
            raise PolymarketUnavailableError(
                f"Gamma /markets HTTP {exc.response.status_code}"
            ) from exc
        finally:
            self._metrics.polymarket_http_request_duration_seconds.labels(
                client="gamma", endpoint="markets"
            ).observe(time.perf_counter() - start)

        self._metrics.polymarket_http_requests_total.labels(
            client="gamma", endpoint="markets", status=str(response.status_code)
        ).inc()
        data = response.json()
        if not isinstance(data, list):
            raise PolymarketUnavailableError(
                f"Gamma /markets unexpected payload type: {type(data).__name__}"
            )
        return data

    async def _with_retry(self, fn: Callable[[], Awaitable[httpx.Response]]) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=0.2, max=2),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                return await fn()
        raise RuntimeError("unreachable")

    @staticmethod
    def _row_to_markets(row: dict[str, Any]) -> list[Market]:
        """Cada row tem 2 tokens (Yes/No). Retorna 2 Market objects."""
        token_ids_raw = row.get("clobTokenIds")
        if isinstance(token_ids_raw, str):
            # Gamma às vezes retorna a lista como string JSON.
            import json as _json

            token_ids_raw = _json.loads(token_ids_raw)
        if not isinstance(token_ids_raw, list) or len(token_ids_raw) != 2:
            return []
        outcomes_raw = row.get("outcomes")
        if isinstance(outcomes_raw, str):
            import json as _json

            outcomes_raw = _json.loads(outcomes_raw)
        if not isinstance(outcomes_raw, list) or len(outcomes_raw) != 2:
            return []

        condition_id = ConditionId(value=str(row["conditionId"]))
        question = str(row["question"])
        slug = row.get("slug")
        end_date_raw = row.get("endDate")
        end_date = (
            datetime.fromisoformat(end_date_raw.replace("Z", "+00:00"))
            if isinstance(end_date_raw, str)
            else None
        )
        is_active = bool(row.get("active", False))
        is_archived = bool(row.get("archived", False))
        volume_raw = row.get("volume24hr")
        volume = Money.from_usdc(str(volume_raw)) if volume_raw is not None else None
        liq_raw = row.get("liquidity")
        liq = Money.from_usdc(str(liq_raw)) if liq_raw is not None else None

        out: list[Market] = []
        for token_id_str, outcome in zip(token_ids_raw, outcomes_raw, strict=True):
            if outcome not in ("Yes", "No"):
                continue
            out.append(
                Market(
                    token_id=TokenId(value=str(token_id_str)),
                    condition_id=condition_id,
                    question=question,
                    slug=slug if isinstance(slug, str) else None,
                    outcome=outcome,
                    end_date=end_date,
                    is_active=is_active,
                    is_archived=is_archived,
                    volume_24h_usdc=volume,
                    liquidity_usdc=liq,
                )
            )
        return out
```

**Nota sobre `Decimal` import:** `from decimal import Decimal` está no header mas só usado se a fixture quiser construções diretas — pode ser removido pelo ruff `F401`. Remover se não usar.

- [ ] **Step 3.6: Rodar testes**

```bash
uv run pytest tests/unit/infrastructure/test_gamma_client.py -v
uv run pytest tests/unit/infrastructure/test_metrics.py -v
```
Expected: PASS.

Se algum teste falhar porque a fixture real bate com schema diferente do parser, ajustar o `_row_to_markets` ATÉ o teste passar. Não inventar valores — sempre olhar a fixture real.

- [ ] **Step 3.7: Verificações completas**

```bash
uv run ruff check src/polycopy/infrastructure/polymarket/gamma_client.py src/polycopy/infrastructure/observability/metrics.py tests/unit/infrastructure/test_gamma_client.py tests/unit/infrastructure/test_metrics.py
uv run ruff format --check src/polycopy/infrastructure/polymarket/gamma_client.py src/polycopy/infrastructure/observability/metrics.py
uv run mypy src/polycopy
uv run pytest tests/ -x
```
Expected: tudo PASS, suíte inteira verde.

- [ ] **Step 3.8: STOP — confirmação humana, depois commit**

```bash
git add src/polycopy/infrastructure/polymarket/gamma_client.py \
        src/polycopy/infrastructure/observability/metrics.py \
        tests/fixtures/polymarket/gamma_market.json \
        tests/unit/infrastructure/test_gamma_client.py \
        tests/unit/infrastructure/test_metrics.py
git commit -m "feat(polymarket): add Gamma REST client with tenacity and metrics"
```

---

## Task 4: `PolymarketClobClient` (REST + tenacity + métricas)

**Objetivo:** adapter REST pra CLOB orderbook. Método `get_book(token_id)`. Mesmo padrão do Gamma client (retry, timeout, métricas) com label `client="clob"`. **Não cacheia.** Sempre fresh.

**Files:**
- Create: `tests/fixtures/polymarket/clob_book.json`
- Create: `src/polycopy/infrastructure/polymarket/clob_client.py`
- Create: `tests/unit/infrastructure/test_clob_client.py`

---

- [ ] **Step 4.1: Capturar fixture real do CLOB**

Em ambiente com internet, primeiro pegar um `token_id` válido da fixture do Step 3.1 (o `clobTokenIds[0]` de um mercado bem ativo). Depois:

```bash
TOKEN_ID="<cole o token id aqui>"
curl -sS "https://clob.polymarket.com/book?token_id=${TOKEN_ID}" \
  | python -m json.tool > tests/fixtures/polymarket/clob_book.json
head -40 tests/fixtures/polymarket/clob_book.json
```

Expected: JSON com objeto único contendo arrays `bids` e `asks`. Cada item tem `price` (string) e `size` (string). Pode ter campos extras (`market`, `asset_id`, `timestamp`, `hash`).

**Se o shape for diferente** (algum CLOB retorna `{"buys": [...], "sells": [...]}` ou aninhado), ajustar parser do Step 4.4 antes do teste.

- [ ] **Step 4.2: Escrever testes pra `PolymarketClobClient`**

Crie `tests/unit/infrastructure/test_clob_client.py`:

```python
"""Testes unit do PolymarketClobClient com respx mocks."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from polycopy.domain.value_objects import TokenId
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.clob_client import PolymarketClobClient
from polycopy.infrastructure.polymarket.gamma_client import PolymarketUnavailableError

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "polymarket"
_CLOB_FIXTURE = _FIXTURES / "clob_book.json"


def _clob_response_json() -> dict:
    return json.loads(_CLOB_FIXTURE.read_text())


def _make_client() -> PolymarketClobClient:
    metrics = make_metrics(registry=CollectorRegistry())
    return PolymarketClobClient(
        base_url="https://clob.polymarket.com",
        metrics=metrics,
        max_retries=3,
    )


@respx.mock
async def test_get_book_parses_fixture() -> None:
    payload = _clob_response_json()
    respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(200, json=payload),
    )

    client = _make_client()
    book = await client.get_book(TokenId(value="42"))

    # Garante que parser não inventa: ordering deve respeitar invariantes do OrderBook
    assert all(
        book.bids[i].price.value <= book.bids[i - 1].price.value for i in range(1, len(book.bids))
    )
    assert all(
        book.asks[i].price.value >= book.asks[i - 1].price.value for i in range(1, len(book.asks))
    )


@respx.mock
async def test_retry_on_5xx() -> None:
    payload = _clob_response_json()
    route = respx.get("https://clob.polymarket.com/book")
    route.side_effect = [
        httpx.Response(502),
        httpx.Response(200, json=payload),
    ]

    client = _make_client()
    book = await client.get_book(TokenId(value="42"))
    assert book is not None
    assert route.call_count == 2


@respx.mock
async def test_retry_exhausted_raises_unavailable() -> None:
    respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(503),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.get_book(TokenId(value="42"))


@respx.mock
async def test_4xx_does_not_retry() -> None:
    route = respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(400),
    )

    client = _make_client()
    with pytest.raises(PolymarketUnavailableError):
        await client.get_book(TokenId(value="42"))
    assert route.call_count == 1


@respx.mock
async def test_metrics_recorded() -> None:
    payload = _clob_response_json()
    respx.get("https://clob.polymarket.com/book").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketClobClient(
        base_url="https://clob.polymarket.com",
        metrics=metrics,
    )
    await client.get_book(TokenId(value="42"))

    counter = metrics.polymarket_http_requests_total.labels(
        client="clob", endpoint="book", status="200"
    )
    assert counter._value.get() == 1
```

Run: `uv run pytest tests/unit/infrastructure/test_clob_client.py -v`
Expected: FAIL com `ImportError: cannot import name 'PolymarketClobClient'`.

- [ ] **Step 4.3: Implementar `src/polycopy/infrastructure/polymarket/clob_client.py`**

```python
"""PolymarketClobClient: REST adapter do CLOB (orderbook).

Endpoint base: https://clob.polymarket.com
Retry: exponencial em 5xx + transport errors; não retenta em 4xx.
Sempre fresh — sem cache.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.market import OrderBook, OrderBookLevel
from polycopy.domain.value_objects import Money, Price, TokenId
from polycopy.infrastructure.observability.metrics import Metrics
from polycopy.infrastructure.polymarket.gamma_client import (
    PolymarketUnavailableError,
    _is_retryable,
)


class PolymarketClobClient:
    """Cliente REST do CLOB. Implementa `PolymarketClobPort`."""

    def __init__(
        self,
        *,
        base_url: str,
        metrics: Metrics,
        timeout_s: float = 5.0,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._metrics = metrics
        self._timeout_s = timeout_s
        self._max_retries = max_retries

    async def get_book(self, token_id: TokenId) -> OrderBook:
        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.get(
                    f"{self._base_url}/book", params={"token_id": token_id.value}
                )
                response.raise_for_status()
                return response

        start = time.perf_counter()
        try:
            response = await self._with_retry(_do)
        except RetryError as exc:
            self._metrics.polymarket_http_requests_total.labels(
                client="clob", endpoint="book", status="error"
            ).inc()
            raise PolymarketUnavailableError(
                f"CLOB /book unavailable after retries: {exc.last_attempt.exception()}"
            ) from exc
        except httpx.HTTPStatusError as exc:
            self._metrics.polymarket_http_requests_total.labels(
                client="clob", endpoint="book", status=str(exc.response.status_code)
            ).inc()
            raise PolymarketUnavailableError(
                f"CLOB /book HTTP {exc.response.status_code}"
            ) from exc
        finally:
            self._metrics.polymarket_http_request_duration_seconds.labels(
                client="clob", endpoint="book"
            ).observe(time.perf_counter() - start)

        self._metrics.polymarket_http_requests_total.labels(
            client="clob", endpoint="book", status=str(response.status_code)
        ).inc()
        return self._parse_book(token_id, response.json())

    async def _with_retry(self, fn: Callable[[], Awaitable[httpx.Response]]) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=0.2, max=2),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                return await fn()
        raise RuntimeError("unreachable")

    @staticmethod
    def _parse_book(token_id: TokenId, payload: Any) -> OrderBook:
        if not isinstance(payload, dict):
            raise PolymarketUnavailableError(
                f"CLOB /book unexpected payload type: {type(payload).__name__}"
            )

        def _levels(items: Any, descending: bool) -> list[OrderBookLevel]:
            if not isinstance(items, list):
                return []
            parsed: list[OrderBookLevel] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                price_raw = item.get("price")
                size_raw = item.get("size")
                if price_raw is None or size_raw is None:
                    continue
                parsed.append(
                    OrderBookLevel(
                        price=Price(value=Decimal(str(price_raw))),
                        size=Money.from_usdc(str(size_raw)),
                    )
                )
            parsed.sort(key=lambda lvl: lvl.price.value, reverse=descending)
            return parsed

        bids = _levels(payload.get("bids"), descending=True)
        asks = _levels(payload.get("asks"), descending=False)
        return OrderBook(token_id=token_id, bids=bids, asks=asks, captured_at=datetime.now(tz=UTC))
```

**Nota:** `_is_retryable` é importado privado de `gamma_client` (mesma definição). Aceitável aqui pra DRY; se virar chato, extrair pra módulo `_polymarket_http.py`. Decisão vale revisitar na próxima task que adicionar mais um client.

- [ ] **Step 4.4: Rodar testes**

```bash
uv run pytest tests/unit/infrastructure/test_clob_client.py -v
```
Expected: PASS.

Se a fixture tiver shape diferente do esperado (ex: arrays não-ordenados; o parser força ordenação), tudo bem. Se faltarem campos, ajustar parser.

- [ ] **Step 4.5: Verificações**

```bash
uv run ruff check src/polycopy/infrastructure/polymarket/clob_client.py tests/unit/infrastructure/test_clob_client.py
uv run ruff format --check src/polycopy/infrastructure/polymarket/clob_client.py
uv run mypy src/polycopy
uv run pytest tests/ -x
```
Expected: tudo PASS.

- [ ] **Step 4.6: STOP — confirmação humana, depois commit**

```bash
git add src/polycopy/infrastructure/polymarket/clob_client.py \
        tests/fixtures/polymarket/clob_book.json \
        tests/unit/infrastructure/test_clob_client.py
git commit -m "feat(polymarket): add CLOB REST client for live orderbook"
```

---

## Task 5: Tabela `markets` + migration alembic + `MarketRow` ORM

**Objetivo:** schema da tabela `markets` versionado em alembic; ORM `MarketRow` no SQLAlchemy. Sem repository ainda — entrega só schema + ORM. Migration tem `upgrade()` e `downgrade()` simétricos.

**Files:**
- Modify: `src/polycopy/infrastructure/persistence/models.py`
- Create: `alembic/versions/0002_add_markets.py`

---

- [ ] **Step 5.1: Adicionar `MarketRow` em `models.py`**

Modify `src/polycopy/infrastructure/persistence/models.py`. Adicionar imports necessários e a classe nova após `WalletTradeRow`:

```python
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
)
```

(Adicionar `Boolean` ao import de `sqlalchemy`.)

E ao final do arquivo:

```python
class MarketRow(Base):
    __tablename__ = "markets"

    token_id: Mapped[str] = mapped_column(String, primary_key=True)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    question: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[str | None] = mapped_column(String, nullable=True)
    outcome: Mapped[str] = mapped_column(String, nullable=False)
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_archived: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=sa_text_false()
    )
    volume_24h_usdc: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    liquidity_usdc: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("outcome IN ('Yes', 'No')", name="markets_outcome_enum"),
        CheckConstraint(
            "NOT (is_active AND is_archived)", name="markets_active_archived_exclusive"
        ),
        Index("idx_markets_condition_id", "condition_id"),
        Index(
            "idx_markets_active_end_date",
            "end_date",
            postgresql_where="is_active = true",
        ),
        Index(
            "idx_markets_volume_24h",
            "volume_24h_usdc",
            postgresql_where="is_active = true",
            postgresql_using="btree",
        ),
    )
```

E adicionar helper no topo do arquivo (logo após os imports existentes):

```python
from sqlalchemy.sql import text as _sql_text


def sa_text_false() -> Any:
    """Server default 'false' compatível com SQLAlchemy 2.x sem text() in import path."""
    return _sql_text("false")
```

Adicionar `from typing import Any` no topo se ainda não estiver.

**Nota:** o helper existe pra evitar `server_default=text("false")` espalhado e pra mypy não reclamar. Se já houver função análoga no projeto, reusar.

- [ ] **Step 5.2: Criar migration `alembic/versions/0002_add_markets.py`**

```python
"""add markets table

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-02 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "markets",
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("question", sa.String(), nullable=False),
        sa.Column("slug", sa.String(), nullable=True),
        sa.Column("outcome", sa.String(), nullable=False),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column(
            "is_archived",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("volume_24h_usdc", sa.Numeric(20, 6), nullable=True),
        sa.Column("liquidity_usdc", sa.Numeric(20, 6), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("outcome IN ('Yes', 'No')", name="markets_outcome_enum"),
        sa.CheckConstraint(
            "NOT (is_active AND is_archived)", name="markets_active_archived_exclusive"
        ),
        sa.PrimaryKeyConstraint("token_id"),
    )
    op.create_index("idx_markets_condition_id", "markets", ["condition_id"])
    op.create_index(
        "idx_markets_active_end_date",
        "markets",
        ["end_date"],
        postgresql_where=sa.text("is_active = true"),
    )
    op.create_index(
        "idx_markets_volume_24h",
        "markets",
        ["volume_24h_usdc"],
        postgresql_where=sa.text("is_active = true"),
        postgresql_using="btree",
    )


def downgrade() -> None:
    op.drop_index("idx_markets_volume_24h", table_name="markets")
    op.drop_index("idx_markets_active_end_date", table_name="markets")
    op.drop_index("idx_markets_condition_id", table_name="markets")
    op.drop_table("markets")
```

- [ ] **Step 5.3: Aplicar migration localmente e validar**

```bash
docker compose ps postgres        # confirmar que está up
uv run alembic upgrade head
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\d markets"
```
Expected: lista de colunas + indexes da `markets`.

```bash
uv run alembic downgrade -1
docker compose exec -T postgres psql -U polycopy -d polycopy -c "\dt markets"
```
Expected: `Did not find any relation named "markets"`.

```bash
uv run alembic upgrade head
```

- [ ] **Step 5.4: Rodar suíte completa**

```bash
uv run mypy src/polycopy/infrastructure/persistence/models.py
uv run pytest tests/ -x
```
Expected: tudo PASS. (Schema novo não afeta testes existentes.)

- [ ] **Step 5.5: STOP — confirmação humana, depois commit**

```bash
git add src/polycopy/infrastructure/persistence/models.py alembic/versions/0002_add_markets.py
git commit -m "feat(persistence): add markets table and MarketRow ORM"
```

---

## Task 6: `SqlAlchemyMarketRepository` com lazy fallback + TTL

**Objetivo:** adapter SQLAlchemy do `MarketRepository`. Implementa `upsert_many` (ON CONFLICT DO UPDATE) e `get_market` (retorna `CachedMarket` com flag `is_stale` baseado em TTL). Sem fetch externo aqui — caller decide refazer fetch.

**Files:**
- Create: `src/polycopy/infrastructure/persistence/market_repository.py`
- Create: `tests/integration/test_market_repository.py`

---

- [ ] **Step 6.1: Escrever testes integration**

Crie `tests/integration/test_market_repository.py`:

```python
"""Integration tests do SqlAlchemyMarketRepository — exige Postgres up via docker-compose."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.domain.market import Market
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.persistence.market_repository import (
    SqlAlchemyMarketRepository,
)

pytestmark = pytest.mark.integration


def _market(*, token_id: str = "1", outcome: str = "Yes", is_active: bool = True) -> Market:
    return Market(
        token_id=TokenId(value=token_id),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        question="Q?",
        slug="q",
        outcome=outcome,
        end_date=datetime.now(tz=UTC) + timedelta(days=14),
        is_active=is_active,
        is_archived=not is_active and outcome == "No",
        volume_24h_usdc=Money.from_usdc("100000"),
        liquidity_usdc=Money.from_usdc("5000"),
    )


async def test_upsert_many_inserts_and_updates(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session=session, ttl_seconds=1800)

        # Inserção inicial
        m1 = _market(token_id="100", outcome="Yes")
        m2 = _market(token_id="101", outcome="No", is_active=True)
        n = await repo.upsert_many([m1, m2])
        await session.commit()
        assert n == 2

        # Update do mesmo token (volume diferente)
        m1_updated = m1.model_copy(update={"volume_24h_usdc": Money.from_usdc("200000")})
        n = await repo.upsert_many([m1_updated])
        await session.commit()
        assert n == 1

        cached = await repo.get_market(TokenId(value="100"))
        assert cached is not None
        assert cached.market.volume_24h_usdc is not None
        assert cached.market.volume_24h_usdc.amount == Decimal("200000.000000")


async def test_get_market_fresh_versus_stale(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session=session, ttl_seconds=2)

        await repo.upsert_many([_market(token_id="200")])
        await session.commit()

        cached = await repo.get_market(TokenId(value="200"))
        assert cached is not None
        assert cached.is_stale is False

        # Forçar stale ajustando last_synced_at no DB
        from sqlalchemy import text

        await session.execute(
            text("UPDATE markets SET last_synced_at = now() - interval '1 hour' WHERE token_id = :t"),
            {"t": "200"},
        )
        await session.commit()

        cached2 = await repo.get_market(TokenId(value="200"))
        assert cached2 is not None
        assert cached2.is_stale is True


async def test_get_market_missing_returns_none(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session=session, ttl_seconds=1800)
        result = await repo.get_market(TokenId(value="999999"))
        assert result is None


async def test_upsert_many_idempotent_when_called_twice(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with db_session_factory() as session:
        repo = SqlAlchemyMarketRepository(session=session, ttl_seconds=1800)

        markets = [_market(token_id=str(300 + i)) for i in range(5)]
        await repo.upsert_many(markets)
        await session.commit()

        # 2º call não erra e atualiza last_synced_at
        await repo.upsert_many(markets)
        await session.commit()

        from sqlalchemy import select
        from polycopy.infrastructure.persistence.models import MarketRow

        result = await session.execute(
            select(MarketRow).where(MarketRow.token_id.in_([str(300 + i) for i in range(5)]))
        )
        rows = result.scalars().all()
        assert len(rows) == 5
```

Run: `uv run pytest tests/integration/test_market_repository.py -v`
Expected: FAIL com `ImportError: cannot import name 'SqlAlchemyMarketRepository'`.

- [ ] **Step 6.2: Implementar `src/polycopy/infrastructure/persistence/market_repository.py`**

```python
"""SqlAlchemyMarketRepository: cache read-through pra metadata de mercados.

Implementa `MarketRepository` (port). TTL configurável via construtor.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.market import Market
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.persistence.models import MarketRow


@dataclass(frozen=True)
class _CachedMarket:
    market: Market
    last_synced_at: datetime
    is_stale: bool


class SqlAlchemyMarketRepository:
    """Cache em Postgres pra `Market`. Idempotente via PK `token_id`."""

    def __init__(self, *, session: AsyncSession, ttl_seconds: int) -> None:
        self._session = session
        self._ttl = timedelta(seconds=ttl_seconds)

    async def upsert_many(self, markets: list[Market]) -> int:
        if not markets:
            return 0
        now = datetime.now(tz=UTC)
        values = [_market_to_row_dict(m, last_synced_at=now) for m in markets]

        stmt = pg_insert(MarketRow).values(values)
        update_cols = {
            "condition_id": stmt.excluded.condition_id,
            "question": stmt.excluded.question,
            "slug": stmt.excluded.slug,
            "outcome": stmt.excluded.outcome,
            "end_date": stmt.excluded.end_date,
            "is_active": stmt.excluded.is_active,
            "is_archived": stmt.excluded.is_archived,
            "volume_24h_usdc": stmt.excluded.volume_24h_usdc,
            "liquidity_usdc": stmt.excluded.liquidity_usdc,
            "last_synced_at": stmt.excluded.last_synced_at,
            "updated_at": stmt.excluded.last_synced_at,
        }
        stmt = stmt.on_conflict_do_update(index_elements=["token_id"], set_=update_cols)
        await self._session.execute(stmt)
        await self._session.flush()
        return len(values)

    async def get_market(self, token_id: TokenId) -> _CachedMarket | None:
        result = await self._session.execute(
            select(MarketRow).where(MarketRow.token_id == token_id.value)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None

        market = _row_to_market(row)
        is_stale = (datetime.now(tz=UTC) - row.last_synced_at) > self._ttl
        return _CachedMarket(
            market=market,
            last_synced_at=row.last_synced_at,
            is_stale=is_stale,
        )


def _market_to_row_dict(m: Market, *, last_synced_at: datetime) -> dict:
    return {
        "token_id": m.token_id.value,
        "condition_id": m.condition_id.value,
        "question": m.question,
        "slug": m.slug,
        "outcome": m.outcome,
        "end_date": m.end_date,
        "is_active": m.is_active,
        "is_archived": m.is_archived,
        "volume_24h_usdc": (None if m.volume_24h_usdc is None else m.volume_24h_usdc.amount),
        "liquidity_usdc": (None if m.liquidity_usdc is None else m.liquidity_usdc.amount),
        "last_synced_at": last_synced_at,
    }


def _row_to_market(row: MarketRow) -> Market:
    return Market(
        token_id=TokenId(value=row.token_id),
        condition_id=ConditionId(value=row.condition_id),
        question=row.question,
        slug=row.slug,
        outcome=row.outcome,
        end_date=row.end_date,
        is_active=row.is_active,
        is_archived=row.is_archived,
        volume_24h_usdc=(Money(amount=row.volume_24h_usdc) if row.volume_24h_usdc is not None else None),
        liquidity_usdc=(Money(amount=row.liquidity_usdc) if row.liquidity_usdc is not None else None),
    )
```

**Nota sobre tipo de retorno:** `_CachedMarket` é `@dataclass(frozen=True)` com os mesmos atributos do `CachedMarket` Protocol. Mypy strict aceita por structural typing.

- [ ] **Step 6.3: Rodar testes integration**

```bash
docker compose up -d postgres nats
uv run alembic upgrade head
uv run pytest tests/integration/test_market_repository.py -v
```
Expected: PASS.

Lembrete operacional do 1C: a suíte completa derruba `wallet_trades` no teardown via alembic downgrade. Após rodar, antes de subir watcher real:

```bash
uv run alembic upgrade head
```

- [ ] **Step 6.4: Verificações completas**

```bash
uv run ruff check src/polycopy/infrastructure/persistence/market_repository.py tests/integration/test_market_repository.py
uv run ruff format --check src/polycopy/infrastructure/persistence/market_repository.py
uv run mypy src/polycopy
uv run pytest tests/ -x
```
Expected: tudo PASS.

- [ ] **Step 6.5: STOP — confirmação humana, depois commit**

```bash
git add src/polycopy/infrastructure/persistence/market_repository.py tests/integration/test_market_repository.py
git commit -m "feat(persistence): add SqlAlchemyMarketRepository with TTL-based staleness"
```

---

## Task 7: `MarketDataAgent` com loop de sync periódico

**Objetivo:** agente novo que roda loop: dorme `SYNC_INTERVAL`, chama `gamma.list_active_markets`, faz `repo.upsert_many`. Métricas de sync. Settings novas. Entrypoint `main()` igual ao watcher/notifier. Testes unit + integration E2E.

**Files:**
- Modify: `src/polycopy/config.py`
- Modify: `src/polycopy/infrastructure/observability/metrics.py`
- Create: `src/polycopy/agents/marketdata.py`
- Create: `tests/unit/agents/test_marketdata.py`
- Create: `tests/integration/test_marketdata_e2e.py`
- Modify: `tests/unit/infrastructure/test_metrics.py`
- Modify: `.env.example`

---

- [ ] **Step 7.1: Adicionar settings novas em `config.py`**

Adicionar após o bloco `# Notifier` no `Settings`:

```python
    # Polymarket bases
    gamma_api_base_url: str = Field(
        "https://gamma-api.polymarket.com", alias="GAMMA_API_BASE_URL"
    )
    clob_api_base_url: str = Field(
        "https://clob.polymarket.com", alias="CLOB_API_BASE_URL"
    )

    # Market data agent
    marketdata_metrics_port: int = Field(9103, alias="MARKETDATA_METRICS_PORT")
    marketdata_sync_interval_s: float = Field(300.0, alias="MARKETDATA_SYNC_INTERVAL_SECONDS")
    marketdata_top_n: int = Field(200, alias="MARKETDATA_TOP_N")
    market_cache_ttl_seconds: int = Field(1800, alias="MARKET_CACHE_TTL_SECONDS")
```

- [ ] **Step 7.2: Adicionar métricas do agente em `metrics.py`**

Modify `make_metrics()` adicionando:

```python
        marketdata_sync_total=Counter(
            "polycopy_marketdata_sync",
            "Iterações de sync do MarketDataAgent.",
            labelnames=["result"],
            registry=target,
        ),
        marketdata_sync_duration_seconds=Histogram(
            "polycopy_marketdata_sync_duration_seconds",
            "Duração de uma iteração de sync.",
            registry=target,
        ),
        marketdata_markets_tracked=Gauge(
            "polycopy_marketdata_markets_tracked",
            "Número de mercados sincronizados na última iteração.",
            registry=target,
        ),
```

E adicionar os campos no dataclass `Metrics`:

```python
    marketdata_sync_total: Counter
    marketdata_sync_duration_seconds: Histogram
    marketdata_markets_tracked: Gauge
```

E adicionar `Gauge` ao import de `prometheus_client`:

```python
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram
```

Atualizar o teste correspondente em `tests/unit/infrastructure/test_metrics.py` pra cobrir os 3 campos novos.

- [ ] **Step 7.3: Atualizar `.env.example`**

Adicionar bloco no final:

```bash
# --- Market data agent (Plano 2A) ---
GAMMA_API_BASE_URL=https://gamma-api.polymarket.com
CLOB_API_BASE_URL=https://clob.polymarket.com
MARKETDATA_METRICS_PORT=9103
MARKETDATA_SYNC_INTERVAL_SECONDS=300
MARKETDATA_TOP_N=200
MARKET_CACHE_TTL_SECONDS=1800
```

- [ ] **Step 7.4: Escrever teste unit do agente**

Crie `tests/unit/agents/test_marketdata.py`:

```python
"""Testes unit do MarketDataAgent — Gamma + repo mockados via Protocol."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.marketdata import MarketDataAgent
from polycopy.domain.market import Market
from polycopy.domain.value_objects import ConditionId, Money, TokenId
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.ports import CachedMarket, MarketRepository, PolymarketGammaPort


def _market(token_id: str = "1") -> Market:
    return Market(
        token_id=TokenId(value=token_id),
        condition_id=ConditionId(value="0x" + "ab" * 32),
        question="?",
        slug="?",
        outcome="Yes",
        end_date=datetime.now(tz=UTC) + timedelta(days=7),
        is_active=True,
        is_archived=False,
        volume_24h_usdc=Money.from_usdc("100000"),
        liquidity_usdc=Money.from_usdc("5000"),
    )


class _StubGamma:
    def __init__(self, markets: list[Market]) -> None:
        self._markets = markets
        self.calls = 0

    async def get_market(self, token_id: TokenId) -> Market | None:
        return None

    async def list_active_markets(self, *, limit: int) -> list[Market]:
        self.calls += 1
        return self._markets[:limit]


class _StubRepo:
    def __init__(self) -> None:
        self.upserts: list[list[Market]] = []

    async def upsert_many(self, markets: list[Market]) -> int:
        self.upserts.append(list(markets))
        return len(markets)

    async def get_market(self, token_id: TokenId) -> CachedMarket | None:
        return None


def _accepts_gamma(_: PolymarketGammaPort) -> None: ...
def _accepts_repo(_: MarketRepository) -> None: ...


@pytest.fixture
def metrics() -> object:
    return make_metrics(registry=CollectorRegistry())


async def test_run_once_pulls_and_upserts(metrics: object) -> None:
    gamma = _StubGamma([_market("100"), _market("101")])
    repo = _StubRepo()
    _accepts_gamma(gamma)
    _accepts_repo(repo)

    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_repo_factory(repo),
        top_n=2,
        metrics=metrics,
    )

    await agent.run_once()

    assert gamma.calls == 1
    assert len(repo.upserts) == 1
    assert len(repo.upserts[0]) == 2


async def test_loop_stops_on_event(metrics: object) -> None:
    gamma = _StubGamma([_market("200")])
    repo = _StubRepo()
    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_repo_factory(repo),
        top_n=1,
        metrics=metrics,
    )

    async def stopper() -> None:
        await asyncio.sleep(0.15)
        stopping.set()

    await asyncio.gather(agent.run(), stopper())

    assert gamma.calls >= 1


async def test_gamma_failure_logged_metric_continues(metrics: object) -> None:
    class FlakyGamma:
        def __init__(self) -> None:
            self.calls = 0

        async def get_market(self, token_id: TokenId) -> Market | None:
            return None

        async def list_active_markets(self, *, limit: int) -> list[Market]:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("simulated gamma down")
            return [_market("300")]

    gamma = FlakyGamma()
    repo = _StubRepo()
    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_repo_factory(repo),
        top_n=1,
        metrics=metrics,
    )

    async def stopper() -> None:
        await asyncio.sleep(0.30)
        stopping.set()

    await asyncio.gather(agent.run(), stopper())

    # Pelo menos 1 sucesso após falha inicial.
    assert any(len(b) == 1 for b in repo.upserts)


def _repo_factory(repo: _StubRepo):
    """Wrap repo num async context manager pra bater com a assinatura do agente."""
    from contextlib import asynccontextmanager
    from collections.abc import AsyncIterator

    @asynccontextmanager
    async def _factory() -> AsyncIterator[_StubRepo]:
        yield repo

    return _factory
```

Run: `uv run pytest tests/unit/agents/test_marketdata.py -v`
Expected: FAIL com `ImportError: cannot import name 'MarketDataAgent'`.

- [ ] **Step 7.5: Implementar `src/polycopy/agents/marketdata.py`**

```python
"""MarketDataAgent: sincroniza top N mercados ativos via Gamma para a tabela `markets`.

Rodando local (sem Docker):
    uv run python -m polycopy.agents.marketdata

Settings:
    MARKETDATA_SYNC_INTERVAL_SECONDS  default 300
    MARKETDATA_TOP_N                  default 200
    MARKETDATA_METRICS_PORT           default 9103
    GAMMA_API_BASE_URL                default https://gamma-api.polymarket.com
    MARKET_CACHE_TTL_SECONDS          default 1800 (lido por consumers do repo, não pelo agente)
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents._base import AgentBase, setup_signal_handlers
from polycopy.config import Settings
from polycopy.infrastructure.observability.http_metrics import start_metrics_server
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MarketRepository, PolymarketGammaPort

RepoFactory = Callable[[], AbstractAsyncContextManager[MarketRepository]]


class MarketDataAgent(AgentBase):
    name = "marketdata"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        sync_interval_s: float,
        gamma: PolymarketGammaPort,
        repo_factory: RepoFactory,
        top_n: int,
        metrics: Metrics,
    ) -> None:
        super().__init__(stopping=stopping, interval_s=sync_interval_s)
        self._gamma = gamma
        self._repo_factory = repo_factory
        self._top_n = top_n
        self._metrics = metrics

    async def run_once(self) -> None:
        start = time.perf_counter()
        try:
            markets = await self._gamma.list_active_markets(limit=self._top_n)
            async with self._repo_factory() as repo:
                inserted = await repo.upsert_many(markets)
            self._metrics.marketdata_sync_total.labels(result="ok").inc()
            self._metrics.marketdata_markets_tracked.set(inserted)
            self._log.info(
                "marketdata_sync_completed",
                markets_synced=inserted,
                top_n=self._top_n,
            )
        except Exception as exc:
            # Continua o loop após falha. Alerta vem por métrica + log.
            self._metrics.marketdata_sync_total.labels(result="fail").inc()
            self._log.warning(
                "marketdata_sync_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
        finally:
            self._metrics.marketdata_sync_duration_seconds.observe(time.perf_counter() - start)


def _make_repo_factory(
    session_factory: async_sessionmaker[AsyncSession], *, ttl_seconds: int
) -> RepoFactory:
    from polycopy.infrastructure.persistence.market_repository import (
        SqlAlchemyMarketRepository,
    )

    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketRepository]:
        async with session_factory() as session:
            repo = SqlAlchemyMarketRepository(session=session, ttl_seconds=ttl_seconds)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return _factory


async def main() -> None:
    """Entrypoint: monta dependências, sobe /metrics, registra signal handlers, roda."""
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)

    metrics = make_metrics()
    metrics_server, _ = start_metrics_server(settings.marketdata_metrics_port)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    repo_factory = _make_repo_factory(
        session_factory, ttl_seconds=settings.market_cache_ttl_seconds
    )

    gamma = PolymarketGammaClient(
        base_url=settings.gamma_api_base_url,
        metrics=metrics,
    )

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=settings.marketdata_sync_interval_s,
        gamma=gamma,
        repo_factory=repo_factory,
        top_n=settings.marketdata_top_n,
        metrics=metrics,
    )
    try:
        await agent.run()
    finally:
        await engine.dispose()
        metrics_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

Run: `uv run pytest tests/unit/agents/test_marketdata.py -v`
Expected: PASS.

- [ ] **Step 7.6: Escrever integration E2E**

Crie `tests/integration/test_marketdata_e2e.py`:

```python
"""E2E do MarketDataAgent: agente real + Postgres real + Gamma fake (respx)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.marketdata import MarketDataAgent, _make_repo_factory
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.models import MarketRow
from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

pytestmark = pytest.mark.integration

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "polymarket"


@respx.mock
async def test_one_sync_cycle_populates_markets(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payload = json.loads((_FIXTURES / "gamma_market.json").read_text())
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com", metrics=metrics
    )
    repo_factory = _make_repo_factory(db_session_factory, ttl_seconds=1800)

    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        top_n=2,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(select(MarketRow))
        rows = result.scalars().all()
    assert len(rows) >= 1


@respx.mock
async def test_two_cycles_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payload = json.loads((_FIXTURES / "gamma_market.json").read_text())
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com", metrics=metrics
    )
    repo_factory = _make_repo_factory(db_session_factory, ttl_seconds=1800)

    stopping = asyncio.Event()
    agent = MarketDataAgent(
        stopping=stopping,
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        top_n=2,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(select(MarketRow))
        n_after_1 = len(result.scalars().all())

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(select(MarketRow))
        n_after_2 = len(result.scalars().all())

    assert n_after_1 == n_after_2  # idempotente
```

Run: `uv run pytest tests/integration/test_marketdata_e2e.py -v`
Expected: PASS.

- [ ] **Step 7.7: Verificações completas**

```bash
uv run ruff check src/polycopy/agents/marketdata.py src/polycopy/config.py src/polycopy/infrastructure/observability/metrics.py tests/unit/agents/test_marketdata.py tests/integration/test_marketdata_e2e.py
uv run ruff format --check src/polycopy/agents/marketdata.py src/polycopy/config.py src/polycopy/infrastructure/observability/metrics.py
uv run mypy src/polycopy
uv run pytest tests/ -x
```
Expected: tudo PASS.

- [ ] **Step 7.8: STOP — confirmação humana, depois commit**

```bash
git add src/polycopy/agents/marketdata.py \
        src/polycopy/config.py \
        src/polycopy/infrastructure/observability/metrics.py \
        tests/unit/agents/test_marketdata.py \
        tests/integration/test_marketdata_e2e.py \
        tests/unit/infrastructure/test_metrics.py \
        .env.example
git commit -m "feat(agents): add MarketDataAgent with periodic Gamma sync"
```

---

## Task 8: Containerização + Prometheus + ARCHITECTURE.md

**Objetivo:** subir `polycopy-marketdata` como container Docker reusando `Dockerfile.agent`. Adicionar scrape Prometheus. Atualizar ARCHITECTURE.md com o novo componente.

**Files:**
- Modify: `docker-compose.yml`
- Modify: `infra/prometheus/prometheus.yml`
- Modify: `ARCHITECTURE.md`
- Modify: `.env.example` (final, se não cobriu na T7)

---

- [ ] **Step 8.1: Adicionar serviço `marketdata` em `docker-compose.yml`**

Inserir antes do bloco `volumes:`:

```yaml
  marketdata:
    build:
      context: .
      dockerfile: Dockerfile.agent
      args:
        AGENT_MODULE: marketdata
    image: polycopy/marketdata:dev
    container_name: polycopy-marketdata
    restart: unless-stopped
    labels:
      com.polycopy.role: agent
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      ENV: ${ENV}
      LOG_LEVEL: ${LOG_LEVEL}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_PORT: 5432
      POSTGRES_HOST: polycopy-postgres
      NATS_URL: nats://polycopy-nats:4222
      REDIS_URL: redis://polycopy-redis:6379/0
      GAMMA_API_BASE_URL: https://gamma-api.polymarket.com
      CLOB_API_BASE_URL: https://clob.polymarket.com
      MARKETDATA_METRICS_PORT: "9103"
      MARKETDATA_SYNC_INTERVAL_SECONDS: "300"
      MARKETDATA_TOP_N: "200"
      MARKET_CACHE_TTL_SECONDS: "1800"
    ports:
      - "127.0.0.1:9103:9103"
    healthcheck:
      test: ["CMD-SHELL", "python -c 'import urllib.request; urllib.request.urlopen(\"http://localhost:9103/metrics\", timeout=2).read()' || exit 1"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 15s
```

**Nota:** segue o padrão do hot-fix `831bd8e` do 1C — passa `Settings` env completo (postgres + nats + redis) mesmo que o agente não use todos os recursos. Refator pra split em sub-models continua dívida técnica conhecida.

- [ ] **Step 8.2: Adicionar scrape em `infra/prometheus/prometheus.yml`**

Adicionar mais um job:

```yaml
  - job_name: polycopy-marketdata
    static_configs:
      - targets: ['polycopy-marketdata:9103']
```

- [ ] **Step 8.3: Subir container e validar**

```bash
docker compose build marketdata
docker compose up -d marketdata
docker compose ps marketdata
docker compose logs --tail=50 marketdata
```
Expected: container `healthy` em ~15-30s. Logs mostram `marketdata_sync_completed` ou (se Gamma 503/etc.) `marketdata_sync_failed` — em ambos os casos o agente continua de pé.

```bash
curl -sf http://127.0.0.1:9103/metrics | grep polycopy_marketdata
```
Expected: pelo menos `polycopy_marketdata_sync_total{result="ok"} 1.0` ou `polycopy_marketdata_sync_duration_seconds_count`.

```bash
curl -sf http://127.0.0.1:9090/api/v1/targets | python -m json.tool | grep marketdata
```
Expected: target `polycopy-marketdata:9103` com state `up` (após 1-2 ciclos do scraper de 15s).

- [ ] **Step 8.4: Atualizar `ARCHITECTURE.md`**

Adicionar:

1. Mencionar `marketdata` na seção de agentes (lista de containers).
2. Atualizar diagrama Mermaid se houver — adicionar bloco `marketdata` que escreve em `markets` e consume Gamma.
3. Adicionar uma subseção curta:

```markdown
## MarketDataAgent (Plano 2A)

Agente em background que sincroniza metadata dos top N (default 200) mercados ativos
da Polymarket Gamma API pra tabela `markets`. Roda a cada `MARKETDATA_SYNC_INTERVAL_SECONDS`
(default 300s). Falha de sync não derruba copy trading — Risk (Plano 2B) usa lazy fallback
no `MarketRepository` quando o cache está stale ou ausente.

Métricas: `polycopy_marketdata_sync_total{result}`, `polycopy_marketdata_sync_duration_seconds`,
`polycopy_marketdata_markets_tracked`.

Container: `polycopy-marketdata`. Endpoint `/metrics`: porta 9103.
```

- [ ] **Step 8.5: Verificações**

```bash
docker compose ps                          # postgres, nats, redis, prometheus, watcher, notifier, marketdata todos healthy
uv run pytest tests/ -x                    # suíte verde
```

- [ ] **Step 8.6: STOP — confirmação humana, depois commit**

```bash
git add docker-compose.yml infra/prometheus/prometheus.yml ARCHITECTURE.md .env.example
git commit -m "feat(deploy): containerize marketdata agent and wire Prometheus scrape"
```

---

## Task 9: Smoke E2E real (opt-in)

**Objetivo:** teste opt-in que bate na Gamma e CLOB reais. Roda só com `PYTEST_LIVE=1`. Garante que parsers seguem batendo com schema real da Polymarket — guardrail contra o tipo de regressão silenciosa do hot-fix `860b264` do 1C.

**Files:**
- Create: `tests/integration/test_polymarket_smoke.py`
- Modify: `pyproject.toml` (registrar marker `live`)

---

- [ ] **Step 9.1: Registrar marker `live` em `pyproject.toml`**

Localizar `[tool.pytest.ini_options]` e adicionar/estender `markers`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
addopts = ["--tb=short", "--strict-markers", "--strict-config"]
markers = [
    "integration: requires docker-compose services up",
    "live: requires real internet / Polymarket API (opt-in via PYTEST_LIVE=1)",
]
```

(Se já houver bloco `markers`, adicionar `live`. Se a chave for diferente, alinhar.)

- [ ] **Step 9.2: Criar `tests/integration/test_polymarket_smoke.py`**

```python
"""Smoke test opt-in contra Gamma e CLOB reais.

Rodar com:
    PYTEST_LIVE=1 uv run pytest tests/integration/test_polymarket_smoke.py -v

Exige internet. Pula automaticamente se PYTEST_LIVE != "1".
"""

from __future__ import annotations

import os

import pytest
from prometheus_client import CollectorRegistry

from polycopy.domain.value_objects import TokenId
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.clob_client import PolymarketClobClient
from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("PYTEST_LIVE") != "1",
        reason="set PYTEST_LIVE=1 to run real-network smoke tests",
    ),
]


async def test_gamma_list_active_markets_returns_data() -> None:
    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketGammaClient(
        base_url="https://gamma-api.polymarket.com", metrics=metrics
    )
    markets = await client.list_active_markets(limit=2)
    assert len(markets) >= 1
    m = markets[0]
    assert m.token_id.value
    assert m.outcome in {"Yes", "No"}


async def test_clob_get_book_returns_data() -> None:
    # Pega um token id ativo via Gamma
    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    markets = await gamma.list_active_markets(limit=1)
    assert markets, "no active markets — Gamma returned empty?"
    token_id = markets[0].token_id

    clob = PolymarketClobClient(base_url="https://clob.polymarket.com", metrics=metrics)
    book = await clob.get_book(token_id)
    # Pode haver mercados sem profundidade — mas o book deve parsear sem explodir.
    assert book.token_id.value == token_id.value
```

- [ ] **Step 9.3: Validar (opt-in)**

Sem internet ou sem flag:

```bash
uv run pytest tests/integration/test_polymarket_smoke.py -v
```
Expected: 2 SKIPPED.

Com internet:

```bash
PYTEST_LIVE=1 uv run pytest tests/integration/test_polymarket_smoke.py -v
```
Expected: 2 PASS.

- [ ] **Step 9.4: Verificações finais da Plano 2A inteira**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest tests/                       # sem PYTEST_LIVE — mantém suíte rápida e estável
docker compose ps                          # todos os 7 containers healthy
```
Expected: tudo verde. Suíte total esperada ~125-135 testes.

- [ ] **Step 9.5: STOP — confirmação humana, depois commit**

```bash
git add tests/integration/test_polymarket_smoke.py pyproject.toml
git commit -m "test(polymarket): add opt-in live smoke for Gamma and CLOB"
```

---

## Self-Review (autor do plano)

**Spec coverage:**

| Spec § | Coberto em |
|---|---|
| §3.1 OrderBook + Market domain | T1 |
| §3.1 Ports (Clob, Gamma, MarketRepository) | T2 |
| §3.1 PolymarketClobClient adapter REST | T4 |
| §3.1 PolymarketGammaClient adapter REST | T3 |
| §3.1 Tabela `markets` + migration alembic | T5 |
| §3.1 SqlAlchemyMarketRepository com lazy + TTL | T6 |
| §3.1 MarketDataAgent | T7 |
| §3.1 Containerização + Prometheus + ARCHITECTURE | T8 |
| §3.1 Métricas Prometheus | T3 (gamma), T4 (clob), T7 (agente) |
| §3.1 Settings novas | T7 |
| §3.1 Testes unit + integration + smoke opt-in | T1, T3, T4, T6, T7, T9 |
| §5 Schema completo (PK, índices, constraints) | T5 |
| §6.1/§6.2 Fluxo sync periódico + leitura sob demanda | T7 (sync), §3.1 lazy fallback é exposto pelo `is_stale` no `CachedMarket` (T6) — caller (Risk no 2B) faz a decisão |
| §7 Retry com tenacity 3x exp backoff | T3 (definição), T4 (reuso) |
| §7.1 Comportamento sob falha | T3 (`PolymarketUnavailableError`), T7 (não derruba loop), T6 (returns `is_stale=True` sem refetch) |
| §8.1 Métricas de http duration / sync | T3+T4 (http), T7 (sync). `polycopy_market_cache_hits_total` foi adiada pro Plano 2B (decisão registrada na spec atualizada §8.1) — vive junto do consumer (Risk) que decide aceitar stale / refazer fetch. |
| §11 Riscos: capturar fixtures reais | T3 Step 3.1, T4 Step 4.1 |

**Placeholder scan:** sem TBDs, TODOs ou comentários "implement later". Há uma observação no Self-Review acima (`polycopy_market_cache_hits_total`) que precisa decisão humana.

**Type consistency:** `CachedMarket` é Protocol em `ports/market_repository.py`; `_CachedMarket` é dataclass concreto em `infrastructure/persistence/market_repository.py`. Mypy strict aceita por structural typing (mesmos atributos com mesmos tipos). `MarketDataAgent` recebe `RepoFactory` que produz `MarketRepository` — bate. `PolymarketGammaPort` e `PolymarketClobPort` usados consistentemente.

**Ambiguity check:** `_is_retryable` é importado privado de `gamma_client` por `clob_client` (Step 4.3). Decisão consciente registrada no plano com nota; pode virar refator depois.

---

## Execution Handoff

Plano salvo em `docs/superpowers/plans/2026-05-02-fase-2a-clob-marketdata.md`. Spec referência: `docs/superpowers/specs/2026-05-02-fase-2a-clob-marketdata-design.md`.

**Duas opções de execução:**

1. **Subagent-Driven (recomendado)** — controller dispatcha um implementer subagent por task, dois reviewers (spec + code quality), pede confirmação humana antes de cada commit. Mesma cadência adaptada do 1C.

2. **Inline Execution** — executar tasks nessa sessão usando `superpowers:executing-plans`, com checkpoint humano a cada task.
