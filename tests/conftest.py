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
