"""Structlog configuration: JSON em prod, console em dev. Filtro de secrets."""

from __future__ import annotations

import logging
import sys
from typing import Any, TextIO, cast

import structlog
from structlog.types import EventDict

from polycopy.config import Environment, LogLevel


class _LazyStream:
    """Stream proxy: resolve `sys.stdout` lazily; fallback se stream explícita fechar.

    Workaround pra structlog segurar referência a stream que vira inválida
    cross-test (ex: StringIO de fixture sem teardown). Em produção, `stream=None`
    sempre resolve `sys.stdout` atual a cada write — comportamento idempotente.
    """

    def __init__(self, explicit: TextIO | None) -> None:
        self._explicit = explicit

    def _target(self) -> TextIO:
        s = self._explicit
        if s is not None:
            try:
                if not s.closed:
                    return s
            except (AttributeError, ValueError):
                pass
        return sys.stdout

    def write(self, data: str) -> int:
        return self._target().write(data)

    def flush(self) -> None:
        self._target().flush()


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
    stream: TextIO | None = None,
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
    target_stream = _LazyStream(stream)
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
        logger_factory=structlog.PrintLoggerFactory(file=cast(TextIO, target_stream)),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    logger = structlog.get_logger(name) if name else structlog.get_logger()
    return cast(structlog.stdlib.BoundLogger, logger)
