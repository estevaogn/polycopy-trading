"""E2E do ResolverAgent: agente real + Postgres real + Gamma fake (respx).

Exige `docker compose up -d postgres` antes.
Recomendado: `docker compose stop resolver` antes pra evitar interferência
do container de produção rodando.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from polycopy.agents.resolver import ResolverAgent, _make_repo_factory
from polycopy.domain.events import ResolvedOutcome
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.persistence.models import MarketResolutionRow
from polycopy.infrastructure.polymarket.gamma_client import PolymarketGammaClient

pytestmark = pytest.mark.integration


_VALID_WALLET = "0x" + "1" * 40


def _unique_cond() -> str:
    """Gera condition_id único pra evitar contaminação entre testes."""
    return "0x" + uuid.uuid4().hex.ljust(64, "0")[:64]


async def _seed_wallet_trade(session: AsyncSession, condition_id: str, log_index: int) -> None:
    await session.execute(
        text(
            "INSERT INTO wallet_trades "
            "(tx_hash, log_index, wallet, condition_id, token_id, side, "
            " price, size_usdc, occurred_at) "
            "VALUES (:tx, :idx, :w, :c, '999', 'BUY', 0.5, 10, now())"
        ),
        {
            "tx": "0x" + uuid.uuid4().hex.ljust(64, "0")[:64],
            "idx": log_index,
            "w": _VALID_WALLET,
            "c": condition_id,
        },
    )
    await session.commit()


def _gamma_fixture(condition_id: str, outcome_prices: str) -> dict:
    return {
        "conditionId": condition_id,
        "clobTokenIds": '["111", "222"]',
        "outcomes": '["Yes", "No"]',
        "closed": True,
        "closedTime": "2026-04-15T10:00:00Z",
        "outcomePrices": outcome_prices,
        "umaResolutionStatuses": '["resolved"]',
    }


@respx.mock
async def test_e2e_yes_resolution_detected(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E: wallet_trade existe + Gamma retorna YES → DB tem row YES."""
    cond = _unique_cond()
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, cond, 1)

    payload = [_gamma_fixture(cond, '["1.0", "0.0"]')]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    repo_factory = _make_repo_factory(db_session_factory)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(
            select(MarketResolutionRow).where(MarketResolutionRow.condition_id == cond)
        )
        row = result.scalar_one()
    assert row.resolved_outcome == ResolvedOutcome.YES.value
    assert row.winning_token_id == "111"


@respx.mock
async def test_e2e_invalid_resolution_detected(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E: market 50/50 → DB tem row INVALID com winning_token_id NULL."""
    cond = _unique_cond()
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, cond, 2)

    payload = [_gamma_fixture(cond, '["0.5", "0.5"]')]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    repo_factory = _make_repo_factory(db_session_factory)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(
            select(MarketResolutionRow).where(MarketResolutionRow.condition_id == cond)
        )
        row = result.scalar_one()
    assert row.resolved_outcome == ResolvedOutcome.INVALID.value
    assert row.winning_token_id is None


@respx.mock
async def test_e2e_pending_market_not_resolved(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E: market closed mas preço 0.7/0.3 → não cria row em market_resolutions."""
    cond = _unique_cond()
    async with db_session_factory() as session:
        await _seed_wallet_trade(session, cond, 3)

    payload = [_gamma_fixture(cond, '["0.7", "0.3"]')]
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(200, json=payload),
    )

    metrics = make_metrics(registry=CollectorRegistry())
    gamma = PolymarketGammaClient(base_url="https://gamma-api.polymarket.com", metrics=metrics)
    repo_factory = _make_repo_factory(db_session_factory)

    agent = ResolverAgent(
        stopping=asyncio.Event(),
        sync_interval_s=0.05,
        gamma=gamma,
        repo_factory=repo_factory,
        batch_size=100,
        metrics=metrics,
    )

    await agent.run_once()

    async with db_session_factory() as session:
        result = await session.execute(
            select(MarketResolutionRow).where(MarketResolutionRow.condition_id == cond)
        )
        row = result.scalar_one_or_none()
    assert row is None  # pending — não inserido
