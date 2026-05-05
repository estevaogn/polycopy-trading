"""Tests for structlog configuration."""

from __future__ import annotations

import json
from collections.abc import Iterator
from io import StringIO

import pytest
import structlog

from polycopy.config import Environment, LogLevel
from polycopy.infrastructure.observability.logging import (
    configure_logging,
    get_logger,
)


@pytest.fixture(autouse=True)
def _reset_structlog() -> Iterator[None]:
    structlog.reset_defaults()
    yield
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
    assert "hello-dev" in captured.out
    with pytest.raises(json.JSONDecodeError):
        json.loads(captured.out.strip().splitlines()[-1])
