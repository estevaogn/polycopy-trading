"""Testes unit do ResolverAgent — Gamma + repo mockados via Protocol."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from prometheus_client import CollectorRegistry

from polycopy.agents.resolver import ResolverAgent, _classify_resolution
from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution, ResolvedMarketDTO
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics
from polycopy.ports import MarketResolutionRepository

_VALID_COND = "0x" + "ab" * 32
_TOKEN_YES = "111"
_TOKEN_NO = "222"


def _dto(
    *,
    closed: bool = True,
    outcome_prices_raw: str = '["1.0", "0.0"]',
    uma_raw: str | None = '["resolved"]',
    closed_time: datetime | None = None,
) -> ResolvedMarketDTO:
    return ResolvedMarketDTO(
        condition_id=_VALID_COND,
        yes_token_id=_TOKEN_YES,
        no_token_id=_TOKEN_NO,
        closed=closed,
        closed_time=closed_time or datetime.now(tz=UTC),
        outcome_prices_raw=outcome_prices_raw,
        uma_resolution_statuses_raw=uma_raw,
    )


class _StubGamma:
    def __init__(self, response: list[ResolvedMarketDTO] | None = None) -> None:
        self._response = response or []
        self.calls: list[list[str]] = []

    async def get_market(self, token_id):  # pragma: no cover
        return None

    async def list_active_markets(self, *, limit: int):  # pragma: no cover
        return []

    async def list_markets_by_condition_ids_closed(
        self, *, condition_ids: list[str], limit: int
    ) -> list[ResolvedMarketDTO]:
        self.calls.append(list(condition_ids))
        return self._response


class _StubResolutionRepo:
    def __init__(
        self, *, unresolved: list[str] | None = None, insert_returns_new: bool = True
    ) -> None:
        self.inserted: list[MarketResolution] = []
        self._unresolved = unresolved or []
        self._returns_new = insert_returns_new

    async def insert(self, resolution: MarketResolution) -> bool:
        self.inserted.append(resolution)
        return self._returns_new

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        return self._unresolved[:limit]


@pytest.fixture
def metrics() -> Metrics:
    return make_metrics(registry=CollectorRegistry())


def _make_agent(*, metrics: Metrics, gamma: _StubGamma, repo: _StubResolutionRepo) -> ResolverAgent:
    @asynccontextmanager
    async def _factory() -> AsyncIterator[MarketResolutionRepository]:
        yield repo

    return ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=_factory,
        batch_size=100,
        metrics=metrics,
    )


# ----- _classify_resolution: 6 cenários -----


def test_classify_settled_yes() -> None:
    dto = _dto(outcome_prices_raw='["1.0", "0.0"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.YES
    assert r.winning_token_id == _TOKEN_YES


def test_classify_settled_no() -> None:
    dto = _dto(outcome_prices_raw='["0.0", "1.0"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.NO
    assert r.winning_token_id == _TOKEN_NO


def test_classify_invalid_50_50() -> None:
    dto = _dto(outcome_prices_raw='["0.5", "0.5"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.INVALID
    assert r.winning_token_id is None


def test_classify_pending_non_terminal() -> None:
    """Preços fora das tolerâncias terminais E fora do split INVALID — pending."""
    dto = _dto(outcome_prices_raw='["0.7", "0.3"]')
    r = _classify_resolution(dto)
    assert r is None


def test_classify_edge_rounding_yes() -> None:
    """0.999/0.001 ainda dentro da tolerância 0.99/0.01 → YES."""
    dto = _dto(outcome_prices_raw='["0.999", "0.001"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.YES


def test_classify_edge_invalid_skewed() -> None:
    """0.49/0.51 dentro da tolerância 0.45-0.55 → INVALID."""
    dto = _dto(outcome_prices_raw='["0.49", "0.51"]')
    r = _classify_resolution(dto)
    assert r is not None
    assert r.resolved_outcome == ResolvedOutcome.INVALID


def test_classify_returns_none_when_closed_false() -> None:
    """Defensivo: dto.closed=False (query falhou em filtrar) → None."""
    dto = _dto(closed=False, outcome_prices_raw='["1.0", "0.0"]')
    assert _classify_resolution(dto) is None


def test_classify_returns_none_for_malformed_json() -> None:
    """outcome_prices_raw inválido como JSON → None (não levanta)."""
    dto = _dto(outcome_prices_raw="not a json")
    assert _classify_resolution(dto) is None


def test_classify_returns_none_for_wrong_array_length() -> None:
    """outcome_prices_raw com tamanho ≠ 2 → None."""
    dto_empty = _dto(outcome_prices_raw="[]")
    dto_one = _dto(outcome_prices_raw='["1.0"]')
    dto_three = _dto(outcome_prices_raw='["1.0", "0.0", "0.0"]')
    assert _classify_resolution(dto_empty) is None
    assert _classify_resolution(dto_one) is None
    assert _classify_resolution(dto_three) is None


# ----- run_once -----


async def test_run_once_happy_path_inserts_resolution(metrics: Metrics) -> None:
    repo = _StubResolutionRepo(unresolved=[_VALID_COND])
    gamma = _StubGamma(response=[_dto(outcome_prices_raw='["1.0", "0.0"]')])
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    assert len(repo.inserted) == 1
    assert repo.inserted[0].resolved_outcome == ResolvedOutcome.YES
    assert len(gamma.calls) == 1
    assert gamma.calls[0] == [_VALID_COND]


async def test_run_once_empty_unresolved_skips_gamma(metrics: Metrics) -> None:
    repo = _StubResolutionRepo(unresolved=[])
    gamma = _StubGamma()
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    assert len(gamma.calls) == 0
    assert len(repo.inserted) == 0


async def test_run_once_pending_market_not_inserted(metrics: Metrics) -> None:
    repo = _StubResolutionRepo(unresolved=[_VALID_COND])
    gamma = _StubGamma(response=[_dto(outcome_prices_raw='["0.7", "0.3"]')])
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    assert len(repo.inserted) == 0  # pending → skip


async def test_run_once_gamma_exception_records_fail(metrics: Metrics) -> None:
    class _RaisingGamma(_StubGamma):
        async def list_markets_by_condition_ids_closed(self, *, condition_ids, limit):
            raise RuntimeError("gamma down")

    repo = _StubResolutionRepo(unresolved=[_VALID_COND])
    agent = _make_agent(metrics=metrics, gamma=_RaisingGamma(), repo=repo)

    # Não propaga (capturada no try/except do run_once)
    await agent.run_once()

    assert len(repo.inserted) == 0
    fail_count = metrics.resolver_sync_total.labels(result="fail")._value.get()
    assert fail_count == 1.0


async def test_run_once_duplicate_does_not_increment_detected(metrics: Metrics) -> None:
    """repo.insert retorna False (PK conflict) — métrica não conta como detected."""
    repo = _StubResolutionRepo(unresolved=[_VALID_COND], insert_returns_new=False)
    gamma = _StubGamma(response=[_dto(outcome_prices_raw='["1.0", "0.0"]')])
    agent = _make_agent(metrics=metrics, gamma=gamma, repo=repo)

    await agent.run_once()

    yes_count = metrics.resolver_resolutions_detected_total.labels(outcome="yes")._value.get()
    assert yes_count == 0  # PK conflict — não conta
