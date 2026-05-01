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
        ext = await conn.fetchval("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'")
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
