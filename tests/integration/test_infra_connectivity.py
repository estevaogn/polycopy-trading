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
        ext = await conn.fetchval("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'")
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
